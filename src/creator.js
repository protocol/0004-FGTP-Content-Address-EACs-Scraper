import { create } from 'ipfs-http-client'
import fs from 'fs'
import axios from 'axios'
import Papa from 'papaparse'
import { Octokit } from '@octokit/core'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/creator-${(new Date()).toISOString()}.log`);
process.stdout.write = process.stderr.write = access.write.bind(access);

// Define "source of thruth" github repo and conventions
const REPO = 'filecoin-renewables-purchases'
const REPO_OWNER = 'redransil'
const STEP_2_FILE_NAME = '_step2_orderSupply.csv'
const STEP_3_FILE_NAME = '_step3_match.csv'

// Define global vars
let contracts = {}
let demands = {}

// Create / attach to node 
const ipfs = create('http://127.0.0.1:5001')

// Get CSV
function getCSV(getUri)
{
    return axios(getUri, {
        method: 'get'
    })
}

// Get CSV from Github repo
async function getCSVfromGithub(path, fileName) {
    const uri = `https://raw.githubusercontent.com/${REPO_OWNER}/${REPO}/main/${path}/${fileName}`
    const resp = await getCSV(uri)
    if(resp.status != 200)
    {
        console.error('Didn\'t get valid \'CSV\' response')
        await new Promise(resolve => setTimeout(resolve, 1000));
        process.exit()
    }
    const csv = resp.data
    
    let rows = []

    return new Promise((resolve) => {
        Papa.parse(csv, {
            worker: true,
            header: true,
            dynamicTyping: true,
            comments: "#",
            step: (row) => {
                rows.push(row.data)
            },
            complete: () => {
                resolve(rows)
            }
        })
    })
}

// Init Octokit
const octokit = new Octokit({
    auth: `${process.env.github_personal_access_token}`
})

// Get contents of base repo directory
const repoItems = await octokit.request('GET /repos/{owner}/{repo}/contents', {
    owner: REPO_OWNER,
    repo: REPO
})
if(repoItems.status != 200)
{
    console.error('Didn\'t get valid \'repoItems\' response')
    process.exit()
}

// Search through the base repo directory for folders containing "_transaction_" in its name
const transactionFolders = repoItems.data.filter((item) => {
    return item.name.indexOf('_transaction_') > -1
        && item.type == 'dir'
})

for (const transactionFolder of transactionFolders) {
    let demandsCache = {}
    let contractsCache = []

    // Get contents of transactions directory
    const transactionFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
        owner: REPO_OWNER,
        repo: REPO,
        path: transactionFolder.path
    })
    if(transactionFolderItems.status != 200)
    {
        console.error('Didn\'t get valid \'transactionFolderItems\' response')
        await new Promise(resolve => setTimeout(resolve, 1000));
        process.exit()
    }

    // Search for order CSV file
    const orderCsvFileName = transactionFolder.name + STEP_2_FILE_NAME

    // Check if CSV file is present in the folder
    const orderCsvFile = transactionFolderItems.data.filter((item) => {
        return item.name == orderCsvFileName
            && item.type == 'file'
    })

    if(orderCsvFile.length == 1) {
        // Get CSV content (acctually contracts for this specific order)
        contracts[transactionFolder.name] = await getCSVfromGithub(transactionFolder.path, orderCsvFileName)

        // Search for match CSV file
        const matchName = transactionFolder.name + STEP_3_FILE_NAME

        // Check if CSV file is present in the folder
        const match = transactionFolderItems.data.filter((item) => {
            return item.name == matchName
                && item.type == 'file'
        })

        if(match.length == 1) {
            // Get CSV content (acctually demands for this specific order)
            demands[transactionFolder.name] = await getCSVfromGithub(transactionFolder.path, matchName)

            // Delete mutable columns and at same create DAG structures for demands
            for (const demand of demands[transactionFolder.name]) {
                // Delete mutable columns
                delete demand.step4_ZL_contract_complete
                delete demand.step5_redemption_data_complete
                delete demand.step6_attestation_info_complete
                delete demand.step7_certificates_matched_to_supply
                delete demand.step8_IPLDrecord_complete
                delete demand.step9_transaction_complete
                delete demand.step10_volta_complete
                delete demand.step11_finalRecord_complete

                // Make sure MWh are Numbers
                if(typeof demand.volume_MWh == "String") {
                    demand.volume_MWh = demand.volume_MWh.trim()
                    demand.volume_MWh = Number(demand.volume_MWh)
                }

                // Create DAG structures
                const demandCid = await ipfs.dag.put(demand, {
                    storeCodec: 'dag-cbor',
                    hashAlg: 'sha2-256',
                    pin: true
                })
    
                console.log(`Demand CID for ${demand.contract_id} / ${demand.minerID}: ${demandCid}`)

                // Relate demand CIDs with contract Id so that we do
                // not have to traverse whole JSON structure
                if(demandsCache[demand.contract_id] == null)
                    demandsCache[demand.contract_id] = []
                demandsCache[demand.contract_id].push(demandCid)
            }

            // Delete mutable columns and at same create DAG structures for contracts
            for (const contract of contracts[transactionFolder.name]) {
// Remove when error in source data structure is fixed
if(contract.contract_id == null && contract.tranche_id != null) {
    contract.contract_id = contract.tranche_id.replace('_line_', '_contract_')
    delete contract.tranche_id
}
                // Delete mutable columns
                delete contract.step2_order_complete
                delete contract.step3_match_complete
                delete contract.step4_ZL_contract_complete
                delete contract.step5_redemption_data_complete
                delete contract.step6_attestation_info_complete
                delete contract.step7_certificates_matched_to_supply
                delete contract.step8_IPLDrecord_complete
                delete contract.step9_transaction_complete
                delete contract.step10_volta_complete
                delete contract.step11_finalRecord_complete

                // Make sure MWh are Numbers
                if(typeof contract.volume_MWh == "String") {
                    contract.volume_MWh = contract.volume_MWh.trim()
                    contract.volume_MWh = Number(contract.volume_MWh)
                }
                
                // Add links to demands
                contract.demands = demandsCache[contract.contract_id]

                // Create DAG structures
                const contractCid = await ipfs.dag.put(contract, {
                    storeCodec: 'dag-cbor',
                    hashAlg: 'sha2-256',
                    pin: true
                })

                console.log(`Contract CID for ${contract.contract_id}: ${contractCid}`)

                // Remeber contract IDs and CIDs
                contractsCache.push({
                    id: contract.contract_id,
                    cid: contractCid
                })
            }

            // Create order object
            const order = {
                name: transactionFolder.name,
                contracts: contractsCache
            }

            // Create DAG structure
            const orderCid = await ipfs.dag.put(order, {
                storeCodec: 'dag-cbor',
                hashAlg: 'sha2-256',
                pin: true
            })

            console.log(`Order CID for ${transactionFolder.name}: ${orderCid}`)
        }
        else if(match.length > 1) {
           console.error(`Can't have many '${matchName}' CSV files in '${transactionFolder.path}'`)
           await new Promise(resolve => setTimeout(resolve, 1000));
           process.exit()
        }
            else {
                console.error(`Didn't find '${matchName}' CSV file in '${transactionFolder.path}'`)
                await new Promise(resolve => setTimeout(resolve, 1000));
                process.exit()
        }
    }
    else if(orderCsvFile.length > 1) {
        console.error(`Can't have many '${orderCsvFileName}' CSV files in '${transactionFolder.path}'`)
        await new Promise(resolve => setTimeout(resolve, 1000));
        process.exit()
    }
    else {
        console.error(`Didn't find '${orderCsvFileName}' CSV file in '${transactionFolder.path}'`)
        await new Promise(resolve => setTimeout(resolve, 1000));
        process.exit()
    }
}

await new Promise(resolve => setTimeout(resolve, 5000));

// exit program
process.exit()