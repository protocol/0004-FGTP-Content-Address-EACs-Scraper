import { create } from 'ipfs-http-client'

// My peer's PK (IPNS by default takey this as a key if not provided)
const myPeerPK = 'k51qzi5uqu5dgq34m7xw5xdime5vs334ucw1mue5pibnxurar6rxka2fz9okdq'
// My peer node test record
const myPeerTestRecord = '/ipfs/bafkreigvmueic3oi6rrz7ys3ep65dcbyrg36vzmrhktbmlyjmi2owm4wha'

// create / attach to node 
const ipfs = create('http://127.0.0.1:5001')

// Check pubsub subscriptions
const currentSubs = await ipfs.name.pubsub.subs()
for(const sub of currentSubs)
{
    const result = await ipfs.name.pubsub.cancel(sub.replace('/ipns/', ''))
    console.log(result.canceled)
}
console.log(await ipfs.name.pubsub.subs())

const nodeKeys = await ipfs.key.list()
for(const key of nodeKeys)
{
    if(key.name == 'self')
        continue
    await ipfs.key.rm(key.name)
}
console.log(await ipfs.key.list())

// exit program
process.exit()