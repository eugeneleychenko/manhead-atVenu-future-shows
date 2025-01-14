const { GraphQLClient } = require('graphql-request');

const API_ENDPOINT = "https://api.atvenu.com";
const API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU";

const client = new GraphQLClient(API_ENDPOINT, {
    headers: {
        'x-api-key': `${API_TOKEN}`,
    },
});

const query = `
  query getShows($cursor: String) {
    node(uuid: "show_32d732ba-3ad3-44bf-bba6-eb0c15e77bc5") {
      ... on Show {
        uuid
        itemizedTransactions(first: 200, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          transactionNodes: nodes {
            orderId
            soldQuantity
            netSoldAmount
            itemName
            unitPriceAmount
          }
        }
      }
    }
  }
`;

let totalSoldQuantity = {};

async function fetchData(cursor = null) {
    try {
        const data = await client.request(query, { cursor });
        const { itemizedTransactions } = data.node;

        itemizedTransactions.transactionNodes.forEach(transaction => {
            totalSoldQuantity[transaction.itemName] = (totalSoldQuantity[transaction.itemName] || 0) + transaction.soldQuantity;
        });

        if (itemizedTransactions.pageInfo.hasNextPage) {
            await fetchData(itemizedTransactions.pageInfo.endCursor);
        } else {
            console.log("Total sold quantity per item:", totalSoldQuantity);
        }
    } catch (error) {
        console.error("Error fetching data:", error);
    }
}

fetchData();
