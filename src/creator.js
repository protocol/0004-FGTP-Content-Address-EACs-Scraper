import { create } from 'ipfs-http-client'
import { CID } from 'multiformats/cid'
import fs from 'fs'
import { Blob } from 'buffer'
import axios from 'axios'
import Papa from 'papaparse'
import { Octokit } from '@octokit/core'
import { Base64 } from 'js-base64'
import isEqual from 'lodash.isequal'
import cloneDeep from 'lodash.clonedeep'
import moment from 'moment'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/creator-${(new Date()).toISOString()}.log`);
process.stdout.write = process.stderr.write = access.write.bind(access);

// Define "source of thruth" github repo and conventions
const REPO = 'filecoin-renewables-purchases'
const REPO_OWNER = 'protocol'
const STEP_2_FILE_NAME = '_step2_orderSupply.csv'
const STEP_3_FILE_NAME = '_step3_match.csv'
const STEP_5_FILE_NAME = '_step5_redemption_information.csv'
const STEP_6_FILE_NAME = '_step6_generationRecords.csv'
const STEP_7_FILE_NAME = '_step7_certificate_to_contract.csv'

// Define global vars
let nodeKeys

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

// Check IPNS keys
function keyExists(key, keys) {
    return {
        exists: keys.filter((k) => {return k.name == key}).length > 0,
        index: keys.map((k) => {return k.name}).indexOf(key)
    }
}

// Get content from URI
function getUriContent(getUri, headers, responseType) {
    return axios(getUri, {
        method: 'get',
        headers: headers,
        responseType: responseType
    })
}

// Get raw content from Github repo
async function getRawFromGithub(path, fileName, type, contentType) {
    const uri = `https://raw.githubusercontent.com/${REPO_OWNER}/${REPO}/main/${path}/${fileName}`
    const headers = {
        'Authorization': `token ${process.env.github_personal_access_token}`
    }
    let responseType
    switch (type) {
        case 'arraybuffer':
            responseType = 'arraybuffer'
            break
        default:
            responseType = null
            break
    }

    const resp = await getUriContent(uri, headers, responseType)
    if(resp.status != 200)
    {
        console.error('Didn\'t get valid \'UriContent\' response')
        return new Promise((resolve) => {
            resolve(null)
        })
    }

    switch (type) {
        case 'csv':
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
            break
        case 'arraybuffer':
            return new Promise((resolve) => {
                let content = []
                content.push(resp.data)
                const blob = new Blob(content, {type: contentType})
                resolve(blob)
            })
            break
        default:
            return new Promise((resolve) => {
                resolve(resp.data)
            })
            break
    }
}

async function createOrderContractAllocations() {
    let contracts = {}
    let demands = {}
    let transactions = {}

    // Get existing node keys
    nodeKeys = await ipfs.key.list()

    for (const transactionFolder of transactionFolders) {
        let demandsCache = {}
        let contractsCache = {}
        let allocations = {}

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
            continue
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
            contracts[transactionFolder.name] = await getRawFromGithub(transactionFolder.path, orderCsvFileName, 'csv')
            
            // Keep only allocated contracts
//            contracts[transactionFolder.name] = contracts[transactionFolder.name].filter((c) => {return c.step3_match_complete == 1})

            // Search for match CSV file
            const matchName = transactionFolder.name + STEP_3_FILE_NAME

            // Check if CSV file is present in the folder
            const match = transactionFolderItems.data.filter((item) => {
                return item.name == matchName
                    && item.type == 'file'
            })

            if(match.length == 1) {
                // Get CSV content (acctually demands for this specific order)
                demands[transactionFolder.name] = await getRawFromGithub(transactionFolder.path, matchName, 'csv')

                // Make redundant allocations structure for step 3 CSV update
                let demandsWithCids = []
                
                // Delete mutable columns and at same create DAG structures for demands
                for (let demand of demands[transactionFolder.name]) {
                    // Check if this is non-empty row in CSV
                    if(!demand.allocation_id || !demand.contract_id)
                        continue

                    // Create an helper object to update step 3 CSV with the created allocation CID
                    let demandWithCid = JSON.parse(JSON.stringify(demand))

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

                    // Add allocation_cid to allocation object for step 3 CSV update
                    demandWithCid.allocation_cid = demandCid.toString()
                    demandsWithCids.push(demandWithCid)
                
                    // Relate demand CIDs with contract Id so that we do
                    // not have to traverse whole JSON structure
                    if(demandsCache[demand.contract_id] == null)
                        demandsCache[demand.contract_id] = []
                    demandsCache[demand.contract_id].push(demandCid)

                    // Make vice-versa linking for allocations
                     allocations[demand.allocation_id] = {
                        "minerID": demand.minerID,
                        "volume_MWh": demand.volume_MWh,
                        "defaulted": demand.defaulted,
                        "contract_id": demand.contract_id,
                        "allocation_cid": demandCid
                    }
                }

                // Update step 3 CSV
                const step3Header = ['"allocation_id"', '"UUID"', '"contract_id"', '"minerID"', '"volume_MWh"', '"defaulted"',
                    '"step4_ZL_contract_complete"', '"step5_redemption_data_complete"', '"step6_attestation_info_complete"',
                    '"step7_certificates_matched_to_supply"', '"step8_IPLDrecord_complete"', '"step9_transaction_complete"',
                    '"step10_volta_complete"', '"step11_finalRecord_complete"', '"allocation_cid"']
                const step3ColumnTypes = ["string", "string", "string", "string", "number", "number",
                    "number", "number", "number",
                    "number", "number", "number",
                    "number", "number", "string"]
                const step3CsvFileSha = match[0].sha
                
                await updateCsvInGithub(transactionFolder.name, matchName, step3CsvFileSha,
                    step3Header, demandsWithCids, step3ColumnTypes)
                
                // Delete mutable columns and at same create DAG structures for contracts
                for (const contract of contracts[transactionFolder.name]) {
                    // Check if this is non-empty row in CSV
                    if(!contract.contract_id)
                        continue

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
                    contract.allocations = (demandsCache[contract.contract_id]) ? demandsCache[contract.contract_id] : []

                    // Create DAG structures
                    const contractCid = await ipfs.dag.put(contract, {
                        storeCodec: 'dag-cbor',
                        hashAlg: 'sha2-256',
                        pin: true
                    })

                    console.log(`Contract CID for ${contract.contract_id}: ${contractCid}`)

                    // Remeber contract IDs and CIDs
                    contractsCache[contract.contract_id] = {
                        "contract_id": contract.contract_id,
                        "label": contract.label,
                        "region": contract.region,
                        "country": contract.country,
                        "sellerName": contract.sellerName,
                        "volume_MWh": contract.volume_MWh,
                        "productType": contract.productType,
                        "contractDate": contract.contractDate,
                        "deliveryDate": contract.deliveryDate,
                        "reportingEnd": contract.reportingEnd,
                        "energySources": contract.energySources,
                        "sellerAddress": contract.sellerAddress,
                        "reportingStart": contract.reportingStart,
                        "contract_cid": contractCid
                    }

                }

                // Create order object
                const order = {
                    name: transactionFolder.name,
                    contracts: contractsCache,
                    allocations: allocations
                }

                // Create DAG structure
                const orderCid = await ipfs.dag.put(order, {
                    storeCodec: 'dag-cbor',
                    hashAlg: 'sha2-256',
                    pin: true
                })

                console.log(`Order CID for ${transactionFolder.name}: ${orderCid}`)

                // Add allocations to transaction object for this transaction
                transactions[transactionFolder.name] = {
                    "order_cid": orderCid
                }
            }
            else if(match.length > 1) {
                console.error(`Can't have many '${matchName}' CSV files in '${transactionFolder.path}'`)
                await new Promise(resolve => setTimeout(resolve, 1000));
                continue
            }
            else {
                console.error(`Didn't find '${matchName}' CSV file in '${transactionFolder.path}'`)
                await new Promise(resolve => setTimeout(resolve, 1000));
                continue
            }
        }
        else if(orderCsvFile.length > 1) {
            console.error(`Can't have many '${orderCsvFileName}' CSV files in '${transactionFolder.path}'`)
            await new Promise(resolve => setTimeout(resolve, 1000));
            continue
        }
        else {
            console.error(`Didn't find '${orderCsvFileName}' CSV file in '${transactionFolder.path}'`)
            await new Promise(resolve => setTimeout(resolve, 1000));
            continue
        }
    }
    // Create DAG structure for transactions
    const transactionsCid = await ipfs.dag.put(transactions, {
        storeCodec: 'dag-cbor',
        hashAlg: 'sha2-256',
        pin: true
    })

    console.log(`Transactions CID: ${transactionsCid}`)

    // Chek do we already have key for transactions' keys and create it if not
    let transactionsChain = {
        "transactions_cid": transactionsCid
    }
    const transactionsChainKeyId = 'transactions'
    const transactionsChainKeyCheck = keyExists(transactionsChainKeyId, nodeKeys)
    let transactionsChainKey = null
    let transactionsChainSub = null
    let transactionsChainCid = null
    if(!transactionsChainKeyCheck.exists)
    {
        // If there is no key create one
        transactionsChainKey = await ipfs.key.gen(transactionsChainKeyId, {
            type: 'ed25519',
            size: 2048
        })

        // If there was no key there was no sub as well
        // Create simple transactions chain to keep track of changes
        transactionsChain.parent = null    // First block

        // Put DAG
        transactionsChainCid = await ipfs.dag.put(transactionsChain, {
            storeCodec: 'dag-cbor',
            hashAlg: 'sha2-256',
            pin: true
        })

        // Publish pubsub
        transactionsChainSub = await ipfs.name.publish(transactionsChainCid, {
            lifetime: '87600h',
            key: transactionsChainKey.id
        })
    }
    else
    {
        // If there was a key for transactionsChain get it
        transactionsChainKey = nodeKeys[transactionsChainKeyCheck.index]
        const transactionsChainKeyName = `/ipns/${transactionsChainKey.id}`

        // Resolve IPNS name
        for await (const name of ipfs.name.resolve(transactionsChainKeyName)) {
            transactionsChainCid = name.replace('/ipfs/', '')
        }

        transactionsChainCid = CID.parse(transactionsChainCid)

        // Get last chained transactionsChain DAG
        let lastBlock = await ipfs.dag.get(transactionsChainCid)

        // remember and remove parent block CID
        lastBlock = cloneDeep(lastBlock.value)
        const parentChainCid = lastBlock.parent
        delete lastBlock.parent

//        if(!isEqual(transactionsChain, lastBlock))
        if(lastBlock.transactions_cid.toString() != transactionsChain.transactions_cid.toString())
        {
            // Create new DAG, add new block to the transactionsChain chain
            // and refresh subs
            transactionsChain.parent = transactionsChainCid

            // Put new child block DAG
            transactionsChainCid = await ipfs.dag.put(transactionsChain, {
                storeCodec: 'dag-cbor',
                hashAlg: 'sha2-256',
                pin: true
            })

            // Publish pubsub
            transactionsChainSub = await ipfs.name.publish(transactionsChainCid, {
                lifetime: '87600h',
                key: transactionsChainKey.id
            })
        }
    }

    console.log(`Chain Key:`)
    console.dir(transactionsChainKey, { depth: null })
    console.log(`Chain Sub:`)
    console.dir(transactionsChainSub, { depth: null })
    console.log(`Chain CID:`)
    console.dir(transactionsChainCid, { depth: null })

    await new Promise(resolve => setTimeout(resolve, 1000));
}

async function createAttestationsCertificates() {
    let redemptions = {}
    let certificates = {}
    let supplies = {}
    let deliveries = {}

    // Get existing node keys
    nodeKeys = await ipfs.key.list()

    for (const transactionFolder of transactionFolders) {
        let suppliesCache = {}
        let certificatesCache = []
        let attestationDocumentsCache = {}

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
            redemptions[transactionFolder.name] = await getRawFromGithub(transactionFolder.path, redemptionsCsvFileName, 'csv')

            // Theoretically we should search folder path with each attestation
            // but since all attestations point to the same folder let take it from line 1
            if(redemptions[transactionFolder.name].length == 0) {
                console.error(`Didn't get valid 'redemptions' response for ${transactionFolder.name}`)
                await new Promise(resolve => setTimeout(resolve, 1000));
                process.exit()
            }

            let attestationFolders = redemptions[transactionFolder.name].map((r)=>{
                return r.attestation_folder
            })
            attestationFolders = attestationFolders.filter((item, index) => attestationFolders.indexOf(item) === index) // remove duplicates

            // itterate through all attestation folders
            for (const attestationFolder of attestationFolders) {
                // Look for attestation folder and its contents
                let attestationFolderItems
                try {
                    attestationFolderItems = await octokit.request('GET /repos/{owner}/{repo}/contents/{path}', {
                        owner: REPO_OWNER,
                        repo: REPO,
                        path: attestationFolder
                    })
                }
                catch (error) {
                    console.error('Didn\'t get valid \'attestationFolderItems\' response')
                    await new Promise(resolve => setTimeout(resolve, 1000))
                    continue
                }
                if(attestationFolderItems.status != 200)
                {
                    console.error('Didn\'t get valid \'attestationFolderItems\' response')
                    await new Promise(resolve => setTimeout(resolve, 1000))
                    continue
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
                    certificates[attestationFolder] = await getRawFromGithub(attestationFolder, certificatesCsvFileName, 'csv')

                    // Search for match / supply CSV file
                    const matchName = attestationFolder + STEP_7_FILE_NAME

                    // Check if CSV file is present in the folder
                    const match = attestationFolderItems.data.filter((item) => {
                        return item.name == matchName
                            && item.type == 'file'
                    })

                    if(match.length == 1) {
                        // Get CSV content (acctually supplies for certificates/attestations)
                        supplies[attestationFolder] = await getRawFromGithub(attestationFolder, matchName, 'csv')

                        // Delete mutable columns and at same create DAG structures for supplies
                        for (const supply of supplies[attestationFolder]) {
                            // skip if invalid / empty line
                            if(!supply || !supply.certificate)
                                continue

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
                
                            console.log(`Supply CID for ${supply.certificate}: ${supplyCid}`)

                            // Relate supply CIDs with certificate Id so that we do
                            // not have to traverse whole JSON structure
                            if(suppliesCache[supply.certificate] == null)
                                suppliesCache[supply.certificate] = []
                            suppliesCache[supply.certificate].push(supplyCid)
                        }

                        // Reset attestation documents cache
                        attestationDocumentsCache = {}
                        // Prepare an separate object for updating step 6 CSV
                        let step6CsvObj = JSON.parse(JSON.stringify(certificates[attestationFolder]))
                        let step6CsvInd = 0
                        // Delete mutable columns and at same create DAG structures for certificates
                        for (let certificate of certificates[attestationFolder]) {
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
                            if(certificate.reportingStart instanceof Date && Object.prototype.toString.call(certificate.reportingStart) === '[object Date]')
                                certificate.reportingStart = certificate.reportingStart.toISOString()
                            else
                                certificate.reportingStart = moment(certificate.reportingStart).toISOString()
                            if(certificate.reportingEnd instanceof Date && Object.prototype.toString.call(certificate.reportingEnd) === '[object Date]')
                                certificate.reportingEnd = certificate.reportingEnd.toISOString()
                            else
                                certificate.reportingEnd = moment(certificate.reportingEnd).toISOString()
                            if(certificate.generationStart instanceof Date && Object.prototype.toString.call(certificate.generationStart) === '[object Date]') {
                                certificate.generationStart = certificate.generationStart.toISOString()
                            }
                            else {
                                certificate.generationStart = moment(certificate.generationStart).toISOString()
                            }
                            if(certificate.generationEnd instanceof Date && Object.prototype.toString.call(certificate.generationEnd) === '[object Date]')
                                certificate.generationEnd = certificate.generationEnd.toISOString()
                            else
                                certificate.generationEnd = moment(certificate.generationEnd).toISOString()

                            // Add links to supplies
                            const sc = suppliesCache[certificate.certificate]
                            certificate.supplies = (sc != undefined) ? sc : null

                            // Add attestation document link
                            let attestationDocumentCid
                            // Get attestation document
                            const attestationDocumentName = certificate.attestation_file
                            // Check do we have attestation document name
                            if(!attestationDocumentName) {
                                console.error(`No attestation file specified for ${certificate.attestation_id}`)
                                continue
                            }
                            // Did we already add this attestation document
                            if(attestationDocumentsCache[attestationDocumentName] == null) {

                                const attestationDocumentResp =  await getRawFromGithub(attestationFolder, attestationDocumentName, 'arraybuffer', 'application/pdf')
                                if(attestationDocumentResp == null)
                                {
                                    console.error(`Didn\'t get valid \'attestation document\' ${attestationDocumentName} in ${attestationFolder}`)
                                    await new Promise(resolve => setTimeout(resolve, 1000));
                                    process.exit()
                                }
                                
                                // Add attestation document to IPFS
                                attestationDocumentCid = await ipfs.add(attestationDocumentResp, {
                                    'cidVersion': 1,
                                    'hashAlg': 'sha2-256'
                                })
                                console.log(`Attestation document CID for ${attestationFolder}/${attestationDocumentName}, ${attestationDocumentCid.cid}`)

                                attestationDocumentsCache[attestationDocumentName] = attestationDocumentCid
                            }
                            else {
                                attestationDocumentCid = attestationDocumentsCache[attestationDocumentName]
                            }

                            certificate.attestationDocumentCid = attestationDocumentCid.cid

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
                            
                            // Add CIDs to update step 6 CSV file with certificate and attestation CIDs
                            step6CsvObj[step6CsvInd].attestation_cid = attestationDocumentCid.cid.toString()
                            step6CsvObj[step6CsvInd].certificate_cid = certificateCid.toString()
                            step6CsvInd++
                        }

                        // Update step 6 CSV file
                        const step6Header = ['"attestation_id"', '"attestation_file"', '"attestation_cid"', '"certificate"',
                            '"certificate_cid"', '"reportingStart"', '"reportingStartTimezoneOffset"', '"reportingEnd"', '"reportingEndTimezoneOffset"',
                            '"sellerName"', '"sellerAddress"', '"country"', '"region"', '"volume_Wh"', '"generatorName"', '"productType"', '"label"',
                            '"energySource"', '"generationStart"', '"generationStartTimezoneOffset"', '"generationEnd"', '"generationEndTimezoneOffset"']
                        const step6ColumnTypes = ["string", "string", "string", "string",
                            "string", "string", "number", "string", "number",
                            "string", "string", "string", "string", "number", "string", "string", "string",
                            "string", "string", "number", "string", "number"]
                        const step6CsvFileSha = certificatesCsvFile[0].sha
                        
                        await updateCsvInGithub(attestationFolder, certificatesCsvFileName, step6CsvFileSha,
                            step6Header, step6CsvObj, step6ColumnTypes)

                        // Create attestations object
                        const attestations = {
                            name: attestationFolder,
                            certificates: certificatesCache
                        }

                        // Create DAG structure
                        const attestationsCid = await ipfs.dag.put(attestations, {
                            storeCodec: 'dag-cbor',
                            hashAlg: 'sha2-256',
                            pin: true
                        })

                        console.log(`Attestations CID for ${attestationFolder}: ${attestationsCid}`)

                        // Add certificates to deliveries object for this delivery
                        deliveries[transactionFolder.name] = {
                            "deliveries_cid": attestationsCid
                        }
                    }
                    else if(match.length > 1) {
                        console.error(`Can't have many '${matchName}' CSV files in '${attestationFolder}'`)
                        await new Promise(resolve => setTimeout(resolve, 1000));
                        continue
                    }
                    else {
                        console.error(`Didn't find '${matchName}' CSV file in '${attestationFolder}'`)
                        await new Promise(resolve => setTimeout(resolve, 1000));
                        continue
                    }
                }
                else if(certificatesCsvFile.length > 1) {
                    console.error(`Can't have many '${certificatesCsvFileName}' CSV files in '${attestationFolder}'`)
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    continue
                }
                else {
                    console.error(`Didn't find '${certificatesCsvFileName}' CSV file in '${attestationFolder}'`)
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    continue
                }
            }
        }
        else if(redemptionsCsvFile.length > 1) {
            console.error(`Can't have many '${redemptionsCsvFileName}' CSV files in '${transactionFolder.path}'`)
            await new Promise(resolve => setTimeout(resolve, 1000));
            continue
        }
        else {
            console.error(`Didn't find '${redemptionsCsvFileName}' CSV file in '${transactionFolder.path}'`)
            await new Promise(resolve => setTimeout(resolve, 1000));
            continue
        }
    }
    // Create DAG structure for deliveries
    const deliveriesCid = await ipfs.dag.put(deliveries, {
        storeCodec: 'dag-cbor',
        hashAlg: 'sha2-256',
        pin: true
    })

    console.log(`Deliveries CID: ${deliveriesCid}`)

    // Chek do we already have key for deliveries' keys and create it if not
    let deliveriesChain = {
        "deliveries_cid": deliveriesCid
    }
    const deliveriesChainKeyId = 'deliveries'
    const deliveriesChainKeyCheck = keyExists(deliveriesChainKeyId, nodeKeys)
    let deliveriesChainKey = null
    let deliveriesChainSub = null
    let deliveriesChainCid = null
    if(!deliveriesChainKeyCheck.exists)
    {
        // If there is no key create one
        deliveriesChainKey = await ipfs.key.gen(deliveriesChainKeyId, {
            type: 'ed25519',
            size: 2048
        })

        // If there was no key there was no sub as well
        // Create simple deliveries chain to keep track of changes
        deliveriesChain.parent = null    // First block

        // Put DAG
        deliveriesChainCid = await ipfs.dag.put(deliveriesChain, {
            storeCodec: 'dag-cbor',
            hashAlg: 'sha2-256',
            pin: true
        })

        // Publish pubsub
        deliveriesChainSub = await ipfs.name.publish(deliveriesChainCid, {
            lifetime: '87600h',
            key: deliveriesChainKey.id
        })
    }
    else
    {
        // If there was a key for deliveriesChain get it
        deliveriesChainKey = nodeKeys[deliveriesChainKeyCheck.index]
        const deliveriesChainKeyName = `/ipns/${deliveriesChainKey.id}`

        // Resolve IPNS name
        for await (const name of ipfs.name.resolve(deliveriesChainKeyName)) {
            deliveriesChainCid = name.replace('/ipfs/', '')
        }

        deliveriesChainCid = CID.parse(deliveriesChainCid)

        // Get last chained deliveriesChain DAG
        let lastBlock = await ipfs.dag.get(deliveriesChainCid)

        // remember and remove parent block CID
        lastBlock = cloneDeep(lastBlock.value)
        const parentChainCid = lastBlock.parent
        delete lastBlock.parent

//        if(!isEqual(deliveriesChain, lastBlock))
        if(lastBlock.deliveries_cid.toString() != deliveriesChain.deliveries_cid.toString())
        {
            // Create new DAG, add new block to the deliveriesChain chain
            // and refresh subs
            deliveriesChain.parent = deliveriesChainCid

            // Put new child block DAG
            deliveriesChainCid = await ipfs.dag.put(deliveriesChain, {
                storeCodec: 'dag-cbor',
                hashAlg: 'sha2-256',
                pin: true
            })

            // Publish pubsub
            deliveriesChainSub = await ipfs.name.publish(deliveriesChainCid, {
                lifetime: '87600h',
                key: deliveriesChainKey.id
            })
        }
    }

    console.log(`Chain Key:`)
    console.dir(deliveriesChainKey, { depth: null })
    console.log(`Chain Sub:`)
    console.dir(deliveriesChainSub, { depth: null })
    console.log(`Chain CID:`)
    console.dir(deliveriesChainCid, { depth: null })

    await new Promise(resolve => setTimeout(resolve, 1000));
}

// Update CSV file in github repo
async function updateCsvInGithub(folder, csvFileName, sha, csvHeader, csvObj, csvColumnTypes) {
    console.log(folder, csvFileName, sha)
    let csv = csvHeader.join(",") + "\r\n" +
        Papa.unparse(csvObj, {
            quotes: csvColumnTypes.map((ct) => {return ct != 'number'}),
            quoteChar: '"',
            escapeChar: '"',
            delimiter: ",",
            header: false,
            newline: "\r\n",
            skipEmptyLines: false,
            columns: null
        })
    console.log(csv)
    const contentEncoded = Base64.encode(csv)

    const response = await octokit.request('PUT /repos/{owner}/{repo}/contents/{path}', {
        owner: REPO_OWNER,
        repo: REPO,
        path: `${folder}/${csvFileName}`,
        message: `Octokit bot: Updating file ${csvFileName}`,
        committer: {
            name: 'Momcilo Dzunic | Octokit bot',
            email: 'momcilo.dzunic@protocol.ai'
        },
        sha: sha,
        content: contentEncoded
    })
    console.dir(response, { depth: null })
}
