import { create } from 'ipfs-http-client'
import { CID } from 'multiformats/cid'
import axios from 'axios'
import { Blob } from 'buffer'
import fs from 'fs'
import isEqual from 'lodash.isequal'
import syntheticLocations from '../assets/synthetic-country-state-province-locations-latest.json' assert { type: "json" }

let access = fs.createWriteStream(`./logs/scraper-${(new Date()).toISOString()}.log`);
process.stdout.write = process.stderr.write = access.write.bind(access);

// Create / attach to node 
const ipfs = create('http://127.0.0.1:5001')

// Get miners who has redeemed EACs
function getMinersFromZL()
{
    const getUri = `https://proofs-api.zerolabs.green/api/partners/filecoin/nodes`
    return axios(getUri, {
        method: 'get'
    })
}

// Get all miners with locations from Jim Pick
function getMinersFromJP()
{
    const getUri = `https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeibjg7kky45npdwnogui5ffla7dint62xpttvvlzrsbewlrfmbusya/synthetic-country-state-province-locations-latest.json`
    return axios(getUri, {
        method: 'get'
    })
}

// Update miner's location data
function updateMinerWithLocationData(miner, minersWithLocations)
{
    let locations = minersWithLocations
        .filter((mnr) => {return mnr.provider == miner.id})
        .map((mnr) => {
            delete mnr.provider
            return mnr
        })
    miner.locations = JSON.parse(JSON.stringify(locations))
    return miner
}

// Get EACs transactions
function getTransactions(miner)
{
    const getUri = `https://proofs-api.zerolabs.green/api/partners/filecoin/nodes/${miner}/transactions`;
    return axios(getUri, {
        method: 'get'
    });
}

// Get EACs purchases
function getPurchases(transaction) {
    const getUri = `https://proofs-api.zerolabs.green/api/partners/filecoin/purchases/${transaction}`;
    return axios(getUri, {
        method: 'get',
        headers: {
            'X-API-key': `${process.env.zerolabs_key}`
        }
    });
}

// Get EACs contracts (non-redeemed RECs)
function getContracts(miner)
{
    const getUri = `https://proofs-api.zerolabs.green/api/partners/filecoin/nodes/${miner}/contracts`;
    return axios(getUri, {
        method: 'get'
    });
}

// Get contract
function getContract(contractId)
{
    const getUri = `https://proofs-api.zerolabs.green/api/partners/filecoin/contracts/${contractId}`;
    return axios(getUri, {
        method: 'get',
        headers: {
            'X-API-key': `${process.env.zerolabs_key}`
        }
    });
}

// Get the document
async function getDocument(link, type)
{
    let response = await axios(link,
        {
            method: 'get',
            headers: {
                'X-API-key': `${process.env.zerolabs_key}`
            },
            responseType: 'arraybuffer'
        }
    )
   if(response.status == 200)
   {
        let content = []
        content.push(response.data)
        const blob = new Blob(content, {type: type})
        return blob
   }
   return null
}

// Check IPNS keys
function keyExists(key, keys)
{
    return {
        exists: keys.filter((k) => {return k.name == key}).length > 0,
        index: keys.map((k) => {return k.name}).indexOf(key)
    }
}

// Check IPNS subs
function pubsubExists(id, subs)
{
    return {
        exists: subs.filter((sub) => {return sub == '/ipns/' + id}).length > 0,
        index: subs.indexOf('/ipns/' + id)
    }
}

// Init keys stores
let certificateKeys = []
let sellerKeys = []
let buyerKeys = []
let minerKeys = []

// Get miners from ZeroLabs API
const minersZLResp = await getMinersFromZL()
if(minersZLResp.status != 200)
{
    console.error('Didn\'t get valid \'miners\' response from ZeroLabs API')
    process.exit()
}

let miners = minersZLResp.data
console.dir(miners, { depth: null })

/*
Get miners from Jim Pick
const minersJPResp = await getMinersFromJP()
if(minersJPResp.status != 200)
{
    console.error('Didn\'t get valid \'miners\' response from Jim Pick')
    process.exit()
}
*/

let minersWithLocations = []
try {
//    minersWithLocations = minersJPResp.data.providerLocations
    minersWithLocations = syntheticLocations.providerLocations
}
catch (error)
{
    console.error('Didn\'t get valid \'miners\' response from Jim Pick\'s synthetic locations')
}

let nodeKeys, nodeSubs

// Itterate miners and create DAGs/CIDs
for(let miner of miners)
{
    // Set this miner as not buyer
    let thisMinerisBuyer = false

    // Update miner with location data from Jim Pick
    miner = updateMinerWithLocationData(miner, minersWithLocations)
    console.log('MINER')
    console.dir(miner, { depth: null })

    // Get miner's transactions
    const transactionsResp = await getTransactions(miner.id)
    if(transactionsResp.status != 200)
    {
        console.error(`Didn\'t get valid \'transactions\' object for miner ${miner.id} from ZeroLabs API`)
        process.exit()
    }
    const transactions = transactionsResp.data
    console.log('TRANSACTIONS')
    console.dir(transactions, { depth: null })

    let minerTransactions = []
    let minerBuyer = null

    // Get miner's contracts
    const contractsResp = await getContracts(miner.id)
    if(contractsResp.status != 200)
    {
        console.error(`Didn\'t get valid \'contracts\'object for miner ${miner.id} from ZeroLabs API`)
        process.exit()
    }
    const contracts = contractsResp.data
    console.log('CONTRACTS')
    console.dir(contracts, { depth: null })

    for(const contract of contracts.contracts)
    {
        // Get contract by id
        const cntResp = await getContract(contract.id)
        if(cntResp.status != 200)
        {
            console.error(`Didn\'t get valid \'contract\' object for contract Id ${contract.id} from ZeroLabs API`)
            process.exit()
        }
        const cnt = cntResp.data
        console.log(`CONTRACT ${contract.id}`)
        console.dir(cnt, { depth: null })
    }

    for(const transaction of transactions.transactions)
    {
        // Get miner's transactions purchases
        const purchasesResp = await getPurchases(transaction.id)
        if(purchasesResp.status != 200)
        {
            console.error(`Didn\'t get valid \'purchases\' object for miner ${miner.id} from ZeroLabs API`)
            process.exit()
        }
        const purchase = purchasesResp.data
        console.log(`TRANSACTION ${transaction.id}, PURCHASE ${purchase.id}`)
        console.dir(purchase, { depth: null })

        // Filter documents, search for REDEMPTION_STATEMENT only
        let documents = purchase.files
            .filter((file) => {return file.fileType == 'REDEMPTION_STATEMENT'})

        let documentCid = null
        for(const document of documents)
        {
            const documentResp = await getDocument(document.url, 'application/pdf')
            if(documentResp == null)
            {
                console.error(`Didn\'t get valid \'document\' object ${document.fileName} from ZeroLabs API`)
                process.exit()
            }

            // Let add PDFs to IPFS
            documentCid = await ipfs.add(documentResp, {
                'cidVersion': 1,
                'hashAlg': 'sha2-256'
            })
            console.log(`DOCUMENT ${document.id}, ${document.fileName}, ${documentCid}`)
            console.dir(documentResp, { depth: null })
        }

        // Create generation DAG
        let generation = JSON.parse(JSON.stringify(transaction.generation))

        // Remove ZL internal properties like Ids, creation dates, etc.
        delete generation.id
        delete generation.generatorId
        delete generation.txHash
        delete generation.initialSellerId
        delete generation.commissioningDate
        delete generation.beneficiary
        delete generation.createdAt
        delete generation.updatedAt
        delete generation.generationStartLocal
        delete generation.generationEndLocal

        // Add link to Redemption Statement document
        generation.attestation_document = documentCid.cid

        // Add reporting start and reporting end from purchase metadata
        generation.reportingStart = purchase.reportingStart
        generation.reportingStartTimezoneOffset = purchase.reportingStartTimezoneOffset
        generation.reportingEnd = purchase.reportingEnd
        generation.reportingEndTimezoneOffset = purchase.reportingEndTimezoneOffset

        // Put DAG
        const generationCid = await ipfs.dag.put(generation, {
            storeCodec: 'dag-cbor',
            hashAlg: 'sha2-256',
            pin: true
        })

        console.log(`Generation CID: ${generationCid}`)
        
        // Get existing node keys
        nodeKeys = await ipfs.key.list()

        // Get existing node pubsubs
        nodeSubs = await ipfs.name.pubsub.subs()

        // Create distribution DAG
        // Chek do we already have key for distribution.name and create it if not
        const distributionName = documentCid.cid.toString()
        const distributionKeyCheck = keyExists(distributionName, nodeKeys)
        let distributionKey = null
        let distributionSub = null
        let distributionCid = null
        if(!distributionKeyCheck.exists)
        {
            // If there is no key create one
            distributionKey = await ipfs.key.gen(distributionName, {
                type: 'ed25519',
                size: 2048
            })

            // If there was no key there was no sub as well
            // Create simple distribution chain to keep track of changes
            const distribution = {
                "parent": null,         // First block
                "attestation_document": documentCid.cid,
                "certificates": [
                    {
                        "miner": miner.id,
                        "certificate": generationCid
                    }
                ]
            }
    
            // Put DAG
            distributionCid = await ipfs.dag.put(distribution, {
                storeCodec: 'dag-cbor',
                hashAlg: 'sha2-256',
                pin: true
            })

            // Publish pubsub
            distributionSub = await ipfs.name.publish(distributionCid, {
                lifetime: '87600h',
                key: distributionKey.id
            })
        }
        else
        {
            // If there was a key for distribution.name get it
            distributionKey = nodeKeys[distributionKeyCheck.index]
            const distributionKeyName = `/ipns/${distributionKey.id}`

            // Resolve IPNS name
            for await (const name of ipfs.name.resolve(distributionKeyName)) {
                distributionCid = name.replace('/ipfs/', '')
            }

            distributionCid = CID.parse(distributionCid)

            // Get last chained distribution DAG
            let chainedDistribution = await ipfs.dag.get(distributionCid)

            // remember and remove parent block CID
            let distribution = chainedDistribution.value

            // Get existing certificates / lines
            const generations = distribution.certificates.map((certs) => {return certs.certificate.toString()})

            // Check if we already have this generation line
            if(generations.indexOf(generationCid.toString()) == -1)
            {
                // Create new DAG, add new block to the distribution chain
                // and refresh subs
                distribution.parent = distributionCid
                distribution.certificates.push({
                    "miner": miner.id,
                    "certificate": generationCid
                })

                // Put new child block DAG
                distributionCid = await ipfs.dag.put(distribution, {
                    storeCodec: 'dag-cbor',
                    hashAlg: 'sha2-256',
                    pin: true
                })

                // Publish pubsub
                distributionSub = await ipfs.name.publish(distributionCid, {
                    lifetime: '87600h',
                    key: distributionKey.id
                })
            }
        }
        
        console.log(`Certificate Key:`)
        console.dir(distributionKey, { depth: null })
        console.log(`Certificate Sub:`)
        console.dir(distributionSub, { depth: null })
        console.log(`Certificate CID:`)
        console.dir(distributionCid, { depth: null })

        // Add key to certificates store
        certificateKeys.push(distributionKey)

        // Create seller DAG
        let seller = JSON.parse(JSON.stringify(purchase.seller))

        // Remove ZL internal properties like Ids, creation dates, etc.
        delete seller.id
        delete seller.createdAt
        delete seller.updatedAt

        // Chek do we already have key for seller.name and create it if not
        const sellerKeyCheck = keyExists(seller.name, nodeKeys)
        let sellerKey = null
        let sellerSub = null
        let sellerCid = null
        if(!sellerKeyCheck.exists)
        {
            // If there is no key create one
            sellerKey = await ipfs.key.gen(seller.name, {
                type: 'ed25519',
                size: 2048
            })

            // If there was no key there was no sub as well
            // Create simple seller chain to keep track of changes
            seller.parent = null    // First block

            // Put DAG
            sellerCid = await ipfs.dag.put(seller, {
                storeCodec: 'dag-cbor',
                hashAlg: 'sha2-256',
                pin: true
            })

            // Publish pubsub
            sellerSub = await ipfs.name.publish(sellerCid, {
                lifetime: '87600h',
                key: sellerKey.id
            })
        }
        else
        {
            // If there was a key for seller.name get it
            sellerKey = nodeKeys[sellerKeyCheck.index]
            const sellerKeyName = `/ipns/${sellerKey.id}`

            // Resolve IPNS name
            for await (const name of ipfs.name.resolve(sellerKeyName)) {
                sellerCid = name.replace('/ipfs/', '')
            }

            sellerCid = CID.parse(sellerCid)

            // Get last chained seller DAG
            let chainedSeller = await ipfs.dag.get(sellerCid)

            // remember and remove parent block CID
            chainedSeller = JSON.parse(JSON.stringify(chainedSeller.value))
            const parentSellerCid = chainedSeller.parent
            delete chainedSeller.parent

            // Check two objects
            if(!isEqual(seller, chainedSeller))
            {
                // Create new DAG, add new block to the seller chain
                // and refresh subs
                seller.parent = sellerCid

                // Put new child block DAG
                sellerCid = await ipfs.dag.put(seller, {
                    storeCodec: 'dag-cbor',
                    hashAlg: 'sha2-256',
                    pin: true
                })

                // Publish pubsub
                sellerSub = await ipfs.name.publish(sellerCid, {
                    lifetime: '87600h',
                    key: sellerKey.id
                })
            }
        }
        
        console.log(`Seller Key:`)
        console.dir(sellerKey, { depth: null })
        console.log(`Seller Sub:`)
        console.dir(sellerSub, { depth: null })
        console.log(`Seller CID:`)
        console.dir(sellerCid, { depth: null })

        // Add key to sellers store
        sellerKeys.push(sellerKey)

        // Create buyer DAG
        let buyer = JSON.parse(JSON.stringify(purchase.buyer))

        // Check if this miner is buyer
        thisMinerisBuyer = false
        if(miner.buyerId == buyer.id)
            thisMinerisBuyer = true

        // Remove ZL internal properties like Ids, creation dates, etc.
        delete buyer.id
        delete buyer.createdAt
        delete buyer.updatedAt
        for(let filecoinNode of buyer.filecoinNodes)
        {
            delete filecoinNode.buyerId
            delete filecoinNode.createdAt
            delete filecoinNode.updatedAt
        }

        // Chek do we already have key for buyer.name and create it if not
        const buyerKeyCheck = keyExists(buyer.name, nodeKeys)
        let buyerKey = null
        let buyerSub = null
        let buyerCid = null
        if(!buyerKeyCheck.exists)
        {
            // If there is no key create one
            buyerKey = await ipfs.key.gen(buyer.name, {
                type: 'ed25519',
                size: 2048
            })

            // If there was no key there was no sub as well
            // Create simple buyer chain to keep track of changes
            buyer.parent = null    // First block

            // Put DAG
            buyerCid = await ipfs.dag.put(buyer, {
                storeCodec: 'dag-cbor',
                hashAlg: 'sha2-256',
                pin: true
            })

            // Publish pubsub
            buyerSub = await ipfs.name.publish(buyerCid, {
                lifetime: '87600h',
                key: buyerKey.id
            })
        }
        else
        {
            // If there was a key for buyer.name get it
            buyerKey = nodeKeys[buyerKeyCheck.index]
            const buyerKeyName = `/ipns/${buyerKey.id}`

            // Resolve IPNS name
            for await (const name of ipfs.name.resolve(buyerKeyName)) {
                buyerCid = name.replace('/ipfs/', '')
            }

            buyerCid = CID.parse(buyerCid)

            // Get last chained buyer DAG
            let chainedBuyer = await ipfs.dag.get(buyerCid)

            // remember and remove parent block CID
            chainedBuyer = JSON.parse(JSON.stringify(chainedBuyer.value))
            const parentBuyerCid = chainedBuyer.parent
            delete chainedBuyer.parent

            // Check two objects
            if(!isEqual(buyer, chainedBuyer))
            {
                // Create new DAG, add new block to the buyer chain
                // and refresh subs
                buyer.parent = buyerCid

                // Put new child block DAG
                buyerCid = await ipfs.dag.put(buyer, {
                    storeCodec: 'dag-cbor',
                    hashAlg: 'sha2-256',
                    pin: true
                })

                // Publish pubsub
                buyerSub = await ipfs.name.publish(buyerCid, {
                    lifetime: '87600h',
                    key: buyerKey.id
                })
            }
        }
        
        // Add miner a link to buyer CID if this miner is buyer
        if(thisMinerisBuyer == true)
        {
            minerBuyer = buyerCid
            thisMinerisBuyer = false
        }

        console.log(`Buyer Key:`)
        console.dir(buyerKey, { depth: null })
        console.log(`Buyer Sub:`)
        console.dir(buyerSub, { depth: null })
        console.log(`Buyer CID:`)
        console.dir(buyerCid, { depth: null })
       
        // Add key to buyers store
        buyerKeys.push(buyerKey)

        // Create certificate DAG
        let certificate = JSON.parse(JSON.stringify(purchase.certificate))

        // Remove ZL internal properties like Ids, creation dates, etc.
        delete certificate.id
        delete certificate.initialSellerId
        delete certificate.beneficiary
        delete certificate.certificateCid
        delete certificate.createdAt
        delete certificate.updatedAt

        // Add link to Redemption Statement document
        certificate.attestation_document = documentCid.cid
        certificate.seller = sellerCid
        certificate.buyer = buyerCid

        // Put DAG
        const certificateCid = await ipfs.dag.put(certificate, {
            storeCodec: 'dag-cbor',
            hashAlg: 'sha2-256',
            pin: true
        })

        console.log(`Certificate CID: ${certificateCid}`)

        // Create transaction DAG
        let trans = JSON.parse(JSON.stringify(purchase))

        // Remove ZL internal properties like Ids, creation dates, etc.
        delete trans.id
        delete trans.certificateId
        delete trans.buyerId
        delete trans.sellerId
        delete trans.contractId
        delete trans.createdAt
        delete trans.updatedAt
        delete trans.buyer
        delete trans.seller
        delete trans.filecoinNodes
        delete trans.certificate
        delete trans.files
        delete trans.pageUrl

        // Add link to Redemption Statement document
        trans.generation = generationCid
        trans.certificate = certificateCid
        trans.seller = sellerCid
        trans.buyer = buyerCid
        trans.contract = null

        // Put DAG
        const transCid = await ipfs.dag.put(trans, {
            storeCodec: 'dag-cbor',
            hashAlg: 'sha2-256',
            pin: true
        })

        console.log(`Transaction CID: ${transCid}`)

        // Add transaction CID to list of transactions for miner
        minerTransactions.push(transCid)
    }
    // Create miner DAG
    let mnr = JSON.parse(JSON.stringify(miner))

    // Remove ZL internal properties like Ids, creation dates, etc.
    delete mnr.buyerId
    delete mnr.createdAt
    delete mnr.updatedAt

    // Add miner's transactions
    mnr.transactions = minerTransactions

    // Add miner's buyer account
    mnr.buyer = minerBuyer

    // Chek do we already have key for mnr.id and create it if not
    const mnrKeyCheck = keyExists(mnr.id, nodeKeys)
    let mnrKey = null
    let mnrSub = null
    let mnrCid = null
    if(!mnrKeyCheck.exists)
    {
        // If there is no key create one
        mnrKey = await ipfs.key.gen(mnr.id, {
            type: 'ed25519',
            size: 2048
        })

        // If there was no key there was no sub as well
        // Create simple mnr chain to keep track of changes
        mnr.parent = null    // First block

        // Put DAG
        mnrCid = await ipfs.dag.put(mnr, {
            storeCodec: 'dag-cbor',
            hashAlg: 'sha2-256',
            pin: true
        })

        // Publish pubsub
        mnrSub = await ipfs.name.publish(mnrCid, {
            lifetime: '87600h',
            key: mnrKey.id
        })
    }
    else
    {
        // If there was a key for mnr.name get it
        mnrKey = nodeKeys[mnrKeyCheck.index]
        const mnrKeyName = `/ipns/${mnrKey.id}`

        // Resolve IPNS name
        for await (const name of ipfs.name.resolve(mnrKeyName)) {
            mnrCid = name.replace('/ipfs/', '')
        }

        mnrCid = CID.parse(mnrCid)

        // Get last chained mnr DAG
        let chainedMiner = await ipfs.dag.get(mnrCid)

        // remember and remove parent block CID
        chainedMiner = JSON.parse(JSON.stringify(chainedMiner.value))
        const parentMinerCid = chainedMiner.parent
        delete chainedMiner.parent

        // Check two objects
        if(!isEqual(mnr, chainedMiner))
        {
            // Create new DAG, add new block to the mnr chain
            // and refresh subs
            mnr.parent = mnrCid

            // Put new child block DAG
            mnrCid = await ipfs.dag.put(mnr, {
                storeCodec: 'dag-cbor',
                hashAlg: 'sha2-256',
                pin: true
            })

            // Publish pubsub
            mnrSub = await ipfs.name.publish(mnrCid, {
                lifetime: '87600h',
                key: mnrKey.id
            })
        }
    }
    
    console.log(`Miner Key:`)
    console.dir(mnrKey, { depth: null })
    console.log(`Miner Sub:`)
    console.dir(mnrSub, { depth: null })
    console.log(`Miner CID:`)
    console.dir(mnrCid, { depth: null })

    // Add key to miners store
    minerKeys.push(mnrKey)
}

// Publish stores
console.log(`Certificates store:`)
console.dir(certificateKeys, { depth: null })
console.log(`Sellers store:`)
console.dir(sellerKeys, { depth: null })
console.log(`Buyers store:`)
console.dir(buyerKeys, { depth: null })
console.log(`Miners store:`)
console.dir(minerKeys, { depth: null })

// Chek do we already have key for keys.id and create it if not
let keys = {
    certificates: certificateKeys,
    sellers: sellerKeys,
    buyers: buyerKeys,
    miners: minerKeys
}
const keysKeyId = 'keys'
const keysKeyCheck = keyExists(keysKeyId, nodeKeys)
let keysKey = null
let keysSub = null
let keysCid = null
if(!keysKeyCheck.exists)
{
    // If there is no key create one
    keysKey = await ipfs.key.gen(keysKeyId, {
        type: 'ed25519',
        size: 2048
    })

    // If there was no key there was no sub as well
    // Create simple keys chain to keep track of changes
    keys.parent = null    // First block

    // Put DAG
    keysCid = await ipfs.dag.put(keys, {
        storeCodec: 'dag-cbor',
        hashAlg: 'sha2-256',
        pin: true
    })

    // Publish pubsub
    keysSub = await ipfs.name.publish(keysCid, {
        lifetime: '87600h',
        key: keysKey.id
    })
}
else
{
    // If there was a key for keys.name get it
    keysKey = nodeKeys[keysKeyCheck.index]
    const keysKeyName = `/ipns/${keysKey.id}`

    // Resolve IPNS name
    for await (const name of ipfs.name.resolve(keysKeyName)) {
        keysCid = name.replace('/ipfs/', '')
    }

    keysCid = CID.parse(keysCid)

    // Get last chained keys DAG
    let chainedKeys = await ipfs.dag.get(keysCid)

    // remember and remove parent block CID
    chainedKeys = JSON.parse(JSON.stringify(chainedKeys.value))
    const parentKeysCid = chainedKeys.parent
    delete chainedKeys.parent

    // Check two objects
    if(!isEqual(keys, chainedKeys))
    {
        // Create new DAG, add new block to the keys chain
        // and refresh subs
        keys.parent = keysCid

        // Put new child block DAG
        keysCid = await ipfs.dag.put(keys, {
            storeCodec: 'dag-cbor',
            hashAlg: 'sha2-256',
            pin: true
        })

        // Publish pubsub
        keysSub = await ipfs.name.publish(keysCid, {
            lifetime: '87600h',
            key: keysKey.id
        })
    }
}

console.log(`Keys Key:`)
console.dir(keysKey, { depth: null })
console.log(`Keys Sub:`)
console.dir(keysSub, { depth: null })
console.log(`Keys CID:`)
console.dir(keysCid, { depth: null })

await new Promise(resolve => setTimeout(resolve, 10000));

// Exit program
process.exit()