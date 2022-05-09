# 0004-FGTP-Content-Address-EACs | Scraper
by [Momcilo Dzunic aka smartbee.eth](https://twitter.com/mdzunic)

Develop base data schema (DAG) to streamline the purchase, exchange, and retirement of Renewable Energy Certificates (RECs) from Brokers to PL (via Zero Labs) -> [0004-FGTP-Content-Address-EACs](https://github.com/protocol/FilecoinGreen-tools/blob/main/0004-FGTP-Content-Address-EACs.md)

1. Design reusable DAG structures related to purchase, exchange, and retirement of RECs.
2. Scrap existing RECs (attestation documents) with relatable metadata from existing web2 sources.
3. Make this data available on IPFS → Filcoin for future on-chain use (EnergyWeb)

### Example IPLD structure on IPFS
https://explore.ipld.io/#/explore/bafyreiafzzl7rn6ebuyfmprvws2lyvjxhlb2qvrpsqheaa24acvrv5f6uu

### Use
To run the scraper (scraping data from WEB2 API) 

    npm run scraper

To run the creator (creating IPFS structures from CSV sources)

    npm run creator

### ToDo

~~1. Script for creating [ordering IPLD structures](https://docs.google.com/presentation/d/1fSjbg9dwdabxtgs2Uy8Km0WhB66-vOUC9WP08zBnnWE/edit#slide=id.g125378e22b0_0_0)~~ (done)

### License
Licensed under the MIT license.
http://www.opensource.org/licenses/mit-license.php
