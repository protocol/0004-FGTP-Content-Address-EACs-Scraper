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
const STEP_5_FILE_NAME = '_step5_redemption_information.csv'
const STEP_6_FILE_NAME = '_step6_generationRecords.csv'
const STEP_7_FILE_NAME = '_step7_certificate_to_contract.csv'

// Define global vars
let contracts = {}
let demands = {}
let redemptions = {}
let certificates = {}
let supplies = {}

// Create / attach to node 
const ipfs = create('http://127.0.0.1:5001')

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
    await new Promise(resolve => setTimeout(resolve, 100));
    process.exit()
}

// Search through the base repo directory for folders containing "_transaction_" in its name
const transactionFolders = repoItems.data.filter((item) => {
    return item.name.indexOf('_transaction_') > -1
        && item.type == 'dir'
})

// Look for provided parameters
const args = process.argv.slice(2)
const activities = args[0]
switch (activities) {
    case 'order-contracts-allocations':
        await createOrderContractAllocations()
        break;
    case 'attestations-certificates':
        await createAttestationsCertificates()
        break;
    default:
        console.error(`Error! Bad argument provided. ${activities} is not supported.`)
}
await new Promise(resolve => setTimeout(resolve, 3000));
process.exit()

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

async function createOrderContractAllocations() {
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
                    delete demand.UUID
                    delete demand.step4_ZL_contract_complete
                    delete demand.step5_redemption_data_complete
                    delete demand.step6_attestation_info_complete
                    delete demand.step7_certificates_matched_to_supply
                    delete demand.step8_IPLDrecord_complete
                    delete demand.step9_transaction_complete
                    delete demand.step10_volta_complete
                    delete demand.step11_finalRecord_complete

                    // Make sure MWh are Numbers
                    if(typeof demand.volume_MWh == "string") {
                        demand.volume_MWh = demand.volume_MWh.replace(',', '')
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
                    if(typeof contract.volume_MWh == "string") {
                        contract.volume_MWh = contract.volume_MWh.replace(',', '')
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
}

async function createAttestationsCertificates() {
    for (const transactionFolder of transactionFolders) {
        let suppliesCache = {}
        let certificatesCache = []

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

        // Search for redemption CSV file
        const redemptionsCsvFileName = transactionFolder.name + STEP_5_FILE_NAME

        // Check if CSV file is present in the folder
        const redemptionsCsvFile = transactionFolderItems.data.filter((item) => {
            return item.name == redemptionsCsvFileName
                && item.type == 'file'
        })

        if(redemptionsCsvFile.length == 1) {
            // Get CSV content (acctually contracts for this specific order)
            redemptions[transactionFolder.name] = await getCSVfromGithub(transactionFolder.path, redemptionsCsvFileName)

            // Theoretically we should search folder path with each attestation
            // but since all attestations point to the same folder let take it from line 1
            if(redemptions[transactionFolder.name].length == 0) {
                console.error(`Didn't get valid 'redemptions' response for ${transactionFolder.name}`)
                await new Promise(resolve => setTimeout(resolve, 1000));
                process.exit()
            }
            const attestationFolder = redemptions[transactionFolder.name][0].attestation_folder

            // Look for attestation folder and its contents
            const attestationFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
                owner: REPO_OWNER,
                repo: REPO,
                path: attestationFolder
            })
            if(attestationFolderItems.status != 200)
            {
                console.error('Didn\'t get valid \'attestationFolderItems\' response')
                await new Promise(resolve => setTimeout(resolve, 1000));
                process.exit()
            }

            // Search for certificates CSV file
            const certificatesCsvFileName = attestationFolder + STEP_6_FILE_NAME

            // Check if CSV file is present in the folder
            const certificatesCsvFile = attestationFolderItems.data.filter((item) => {
                return item.name == certificatesCsvFileName
                    && item.type == 'file'
            })

            if(certificatesCsvFile.length == 1) {
                // Get CSV content (acctually certificates and attestations)
                certificates[attestationFolder] = await getCSVfromGithub(attestationFolder, certificatesCsvFileName)

                // Search for match / supply CSV file
                const matchName = attestationFolder + STEP_7_FILE_NAME

                // Check if CSV file is present in the folder
                const match = attestationFolderItems.data.filter((item) => {
                    return item.name == matchName
                        && item.type == 'file'
                })

                if(match.length == 1) {
                    // Get CSV content (acctually supplies for certificates/attestations)
                    supplies[attestationFolder] = await getCSVfromGithub(attestationFolder, matchName)

                    // Delete mutable columns and at same create DAG structures for supplies
                    for (const supply of supplies[attestationFolder]) {
                        // Make sure MWh are Numbers
                        if(typeof supply.volume_MWh == "string") {
                            supply.volume_MWh = supply.volume_MWh.replace(',', '')
                            supply.volume_MWh = supply.volume_MWh.trim()
                            supply.volume_MWh = Number(supply.volume_MWh)
                        }

                        // Create DAG structures
                        const supplyCid = await ipfs.dag.put(supply, {
                            storeCodec: 'dag-cbor',
                            hashAlg: 'sha2-256',
                            pin: true
                        })
            
                        console.log(`Supply CID for ${supply.certificate} / ${supply.contract} / ${supply.minerID}: ${supplyCid}`)

                        // Relate supply CIDs with certificate Id so that we do
                        // not have to traverse whole JSON structure
                        if(suppliesCache[supply.certificate] == null)
                            suppliesCache[supply.certificate] = []
                        suppliesCache[supply.certificate].push(supplyCid)
                    }

                    // Delete mutable columns and at same create DAG structures for certificates
                    for (const certificate of certificates[attestationFolder]) {
                        // Delete mutable columns
                        delete certificate.attestation_cid
                        delete certificate.certificate_cid

                        // Make sure MWh are Numbers
                        if(typeof certificate.volume_Wh == "string") {
                            certificate.volume_Wh = certificate.volume_Wh.replace(',', '')
                            certificate.volume_Wh = certificate.volume_Wh.trim()
                            certificate.volume_Wh = Number(certificate.volume_Wh)
                        }

                        // Represent Date fields as Strings
                        certificate.reportingStart = String(certificate.reportingStart)
                        certificate.reportingEnd = String(certificate.reportingEnd)
                        certificate.generationStart = String(certificate.generationStart)
                        certificate.generationEnd = String(certificate.generationEnd)

                        // Add links to supplies
                        certificate.supplies = suppliesCache[certificate.certificate]

                        // Create DAG structures
                        const certificateCid = await ipfs.dag.put(certificate, {
                            storeCodec: 'dag-cbor',
                            hashAlg: 'sha2-256',
                            pin: true
                        })

                        console.log(`Certificate CID for ${certificate.certificate}: ${certificateCid}`)

                        // Remeber certificate IDs and CIDs
                        certificatesCache.push({
                            id: certificate.certificate,
                            cid: certificateCid
                        })
                    }

                    // Create attestation object
                    const attestation = {
                        name: attestationFolder,
                        certificates: certificatesCache
                    }

                    // Create DAG structure
                    const attestationCid = await ipfs.dag.put(attestation, {
                        storeCodec: 'dag-cbor',
                        hashAlg: 'sha2-256',
                        pin: true
                    })

                    console.log(`Attestation CID for ${attestationFolder}: ${attestationCid}`)
                }
                else if(match.length > 1) {
                    console.error(`Can't have many '${matchName}' CSV files in '${attestationFolder}'`)
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    process.exit()
                }
                else {
                    console.error(`Didn't find '${matchName}' CSV file in '${attestationFolder}'`)
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    process.exit()
                }
            }
            else if(certificatesCsvFile.length > 1) {
                console.error(`Can't have many '${certificatesCsvFileName}' CSV files in '${attestationFolder}'`)
                await new Promise(resolve => setTimeout(resolve, 1000));
                process.exit()
            }
            else {
                console.error(`Didn't find '${certificatesCsvFileName}' CSV file in '${attestationFolder}'`)
                await new Promise(resolve => setTimeout(resolve, 1000));
                process.exit()
            }

        }
        else if(redemptionsCsvFile.length > 1) {
            console.error(`Can't have many '${redemptionsCsvFileName}' CSV files in '${transactionFolder.path}'`)
//            await new Promise(resolve => setTimeout(resolve, 1000));
//            process.exit()
        }
        else {
            console.error(`Didn't find '${redemptionsCsvFileName}' CSV file in '${transactionFolder.path}'`)
//            await new Promise(resolve => setTimeout(resolve, 1000));
//            process.exit()
        }
    }
}
