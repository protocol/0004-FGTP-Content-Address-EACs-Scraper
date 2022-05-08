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
let orders = []
let contracts = {}
let demands = {}

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
                console.log(rows)
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
console.dir(repoItems, { depth: null })

// Search through the base repo directory for folders containing "_transaction_" in its name
const transactionFolders = repoItems.data.filter((item) => {
    return item.name.indexOf('_transaction_') > -1
        && item.type == 'dir'
})
console.dir(transactionFolders, { depth: null })

for (const transactionFolder of transactionFolders) {
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
    console.dir(transactionFolderItems, { depth: null })

    // Search for order CSV file
    const orderCsvFileName = transactionFolder.name + STEP_2_FILE_NAME

    // Check if CSV file is present in the folder
    const orderCsvFile = transactionFolderItems.data.filter((item) => {
        return item.name == orderCsvFileName
            && item.type == 'file'
    })

    if(orderCsvFile.length == 1) {
        // Get CSV content (acctually contracts for this specific order)
        orders.push(transactionFolder.name)
        console.log(orders)
        contracts[transactionFolder.name] = await getCSVfromGithub(transactionFolder.path, orderCsvFileName)
        console.dir(contracts, { depth: null })

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
            console.dir(demands, { depth: null })
        }
        else if(match.length > 1) {
            console.error(`Can't have many '${matchName}' CSV files in '${transactionFolder.path}'`)
    //        await new Promise(resolve => setTimeout(resolve, 1000));
    //        process.exit()
        }
            else {
            console.error(`Didn't find '${matchName}' CSV file in '${transactionFolder.path}'`)
        //        await new Promise(resolve => setTimeout(resolve, 1000));
        //        process.exit()
        }
    }
    else if(orderCsvFile.length > 1) {
        console.error(`Can't have many '${orderCsvFileName}' CSV files in '${transactionFolder.path}'`)
//        await new Promise(resolve => setTimeout(resolve, 1000));
//        process.exit()
    }
    else {
        console.error(`Didn't find '${orderCsvFileName}' CSV file in '${transactionFolder.path}'`)
//        await new Promise(resolve => setTimeout(resolve, 1000));
//        process.exit()
    }
}

await new Promise(resolve => setTimeout(resolve, 5000));

// exit program
process.exit()