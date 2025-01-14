const fs = require("fs");
const { request } = require("graphql-request");
const { parse } = require("json2csv");

const API_ENDPOINT = "https://api.atvenu.com";
const API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU";

async function fetchWithRetry(
  url,
  query,
  variables,
  headers,
  retries = 3,
  backoff = 300
) {
  try {
    const response = await request(url, query, variables, headers);
    return response;
  } catch (error) {
    if (retries > 0) {
      console.log(`Retrying... ${retries} attempts left`);
      await new Promise((resolve) => setTimeout(resolve, backoff));
      return fetchWithRetry(
        url,
        query,
        variables,
        headers,
        retries - 1,
        backoff * 2
      );
    } else {
      throw error;
    }
  }
}

const ACCOUNTS_QUERY = `
  query GetAccounts($first: Int!, $after: String, $toursAfter: String, $showsAfter: String, $itemizedTransactionsAfter: String) {
    organization {
      accounts(first: $first, after: $after) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          name
          tours(first: 3, after: $toursAfter) {
            pageInfo {
              endCursor
              hasNextPage
            }
            nodes {
              name
              shows(first: 3, after: $showsAfter) {
                pageInfo {
                  endCursor
                  hasNextPage
                }
                nodes {
                  uuid
                  showDate
                  showEndDate
                  attendance
                  capacity
                  location {
                    city
                    country
                    stateProvince
                  }
                  itemizedTransactions(first: 200, after: $itemizedTransactionsAfter) {
                    pageInfo {
                      endCursor
                      hasNextPage
                    }
                    nodes {
                      artistName
                      beforeTaxPriceAmount
                      cardholderName
                      device
                      discountName
                      grossSoldAmount
                      grossSoldAmountWithModifiers
                      itemName
                      itemizationType
                      modifiers {
                        discountAmount
                        grossAmount
                        modifierName
                        refundedDiscountAmount
                        refundedGrossAmount
                        refundedTaxOneAmount
                        refundedTaxTwoAmount
                        taxOneAmount
                        taxTwoAmount
                        unitPriceAmount
                      }
                      netSoldAmount
                      netSoldAmountWithModifiers
                      orderId
                      orderType
                      paymentTimestamp
                      productType
                      refundedDiscountAmount
                      refundedQuantity
                      refundedTaxOneAmount
                      refundedTaxTwoAmount
                      size
                      soldQuantity
                      staffName
                      standName
                      taxOneAmount
                      taxTwoAmount
                      tenderType
                      totalDiscountAmount
                      totalRefundedAmount
                      totalRefundedAmountWithModifiers
                      unitPriceAmount
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
`;

const downloadAlanParsonsLiveAccounts = async () => {
  const alanParsonsLiveAccounts = [];
  let accountsAfter = null;
  let accountsHasNextPage = true;

  while (accountsHasNextPage) {
    const variables = { first: 20, after: accountsAfter };
    const headers = { "x-api-key": API_TOKEN };

    try {
      const data = await fetchWithRetry(
        API_ENDPOINT,
        ACCOUNTS_QUERY,
        variables,
        headers
      );
      console.log("Fetched Accounts Data:", data);
      const fetchedAccounts = data.organization.accounts.nodes.filter(
        (account) => account.name === "Alan Parsons Live"
      );

      if (fetchedAccounts.length > 0) {
        const account = fetchedAccounts[0]; // Assuming only one account matches "Alan Parsons Live"
        let toursAfter = null;
        let toursHasNextPage = true;

        while (toursHasNextPage) {
          variables.toursAfter = toursAfter;
          const toursData = await fetchWithRetry(
            API_ENDPOINT,
            ACCOUNTS_QUERY,
            variables,
            headers
          );
          console.log(
            "Fetched Tours Data for Account:",
            account.name,
            toursData
          );
          const fetchedTours =
            toursData.organization.accounts.nodes[0].tours.nodes;

          for (const tour of fetchedTours) {
            let showsAfter = null;
            let showsHasNextPage = true;

            while (showsHasNextPage) {
              variables.showsAfter = showsAfter;
              const showsData = await fetchWithRetry(
                API_ENDPOINT,
                ACCOUNTS_QUERY,
                variables,
                headers
              );
              console.log("Fetched Shows Data for Tour:", tour.name, showsData);
              const fetchedShows =
                showsData.organization.accounts.nodes[0].tours.nodes[0].shows
                  .nodes;

              for (const show of fetchedShows) {
                let totalTransactionsFetched = 0;
                let itemizedTransactionsAfter = null;
                let itemizedTransactionsHasNextPage = true;
                show.itemizedTransactions = { nodes: [] }; // Initialize the itemizedTransactions property
                totalTransactionsFetched = 0; // Reset the total transactions fetched for each show

                while (
                  itemizedTransactionsHasNextPage &&
                  totalTransactionsFetched < show.transactionCount
                ) {
                  const itemizedTransactionsVariables = {
                    ...variables, // This includes first, after, toursAfter, showsAfter
                    itemizedTransactionsAfter: itemizedTransactionsAfter,
                  };
                  const itemizedTransactionsData = await fetchWithRetry(
                    API_ENDPOINT,
                    ACCOUNTS_QUERY,
                    itemizedTransactionsVariables,
                    headers
                  );
                  console.log(
                    "Fetched Itemized Transactions Data for Show:",
                    show.uuid,
                    itemizedTransactionsData
                  );

                  const fetchedItemizedTransactions =
                    itemizedTransactionsData.organization.accounts.nodes[0]
                      .tours.nodes[0].shows.nodes[0].itemizedTransactions.nodes;

                  show.itemizedTransactions.nodes.push(
                    ...fetchedItemizedTransactions
                  );
                  totalTransactionsFetched +=
                    fetchedItemizedTransactions.length;

                  itemizedTransactionsHasNextPage =
                    itemizedTransactionsData.organization.accounts.nodes[0]
                      .tours.nodes[0].shows.nodes[0].itemizedTransactions
                      .pageInfo.hasNextPage;
                  itemizedTransactionsAfter =
                    itemizedTransactionsData.organization.accounts.nodes[0]
                      .tours.nodes[0].shows.nodes[0].itemizedTransactions
                      .pageInfo.endCursor;
                }
              }
              tour.shows.nodes.push(...fetchedShows);
              showsHasNextPage =
                showsData.organization.accounts.nodes[0].tours.nodes[0].shows
                  .pageInfo.hasNextPage;
              showsAfter =
                showsData.organization.accounts.nodes[0].tours.nodes[0].shows
                  .pageInfo.endCursor;
            }
          }

          account.tours.nodes.push(...fetchedTours);
          toursHasNextPage =
            toursData.organization.accounts.nodes[0].tours.pageInfo.hasNextPage;
          toursAfter =
            toursData.organization.accounts.nodes[0].tours.pageInfo.endCursor;
        }
        alanParsonsLiveAccounts.push(account);
      }

      accountsHasNextPage = data.organization.accounts.pageInfo.hasNextPage;
      accountsAfter = data.organization.accounts.pageInfo.endCursor;
    } catch (error) {
      console.error("Error fetching Alan Parsons Live accounts:", error);
      break;
    }
  }

  return alanParsonsLiveAccounts;
};
const filterShowsFrom2022 = (accounts) => {
  const startDate = new Date("2022-01-01T00:00:00Z");
  return accounts.flatMap((account) =>
    account.tours.nodes.flatMap((tour) =>
      tour.shows.nodes.flatMap((show) => {
        const showDate = new Date(show.showDate);
        if (showDate >= startDate) {
          return show.itemizedTransactions.nodes.map((transaction) => ({
            bandName: account.name,
            tourName: tour.name,
            showDate: show.showDate,
            showEndDate: show.showEndDate,
            attendance: show.attendance,
            capacity: show.capacity,
            city: show.location.city,
            country: show.location.country,
            stateProvince: show.location.stateProvince,
            artistName: transaction.artistName,
            beforeTaxPriceAmount: transaction.beforeTaxPriceAmount,
            cardholderName: transaction.cardholderName,
            device: transaction.device,
            discountName: transaction.discountName,
            grossSoldAmount: transaction.grossSoldAmount,
            grossSoldAmountWithModifiers:
              transaction.grossSoldAmountWithModifiers,
            itemName: transaction.itemName,
            itemizationType: transaction.itemizationType,
            modifiers: transaction.modifiers.map((modifier) => ({
              discountAmount: modifier.discountAmount,
              grossAmount: modifier.grossAmount,
              modifierName: modifier.modifierName,
              refundedDiscountAmount: modifier.refundedDiscountAmount,
              refundedGrossAmount: modifier.refundedGrossAmount,
              refundedTaxOneAmount: modifier.refundedTaxOneAmount,
              refundedTaxTwoAmount: modifier.refundedTaxTwoAmount,
              taxOneAmount: modifier.taxOneAmount,
              taxTwoAmount: modifier.taxTwoAmount,
              unitPriceAmount: modifier.unitPriceAmount,
            })),
            netSoldAmount: transaction.netSoldAmount,
            netSoldAmountWithModifiers: transaction.netSoldAmountWithModifiers,
            orderId: transaction.orderId,
            orderType: transaction.orderType,
            paymentTimestamp: transaction.paymentTimestamp,
            productType: transaction.productType,
            refundedDiscountAmount: transaction.refundedDiscountAmount,
            refundedQuantity: transaction.refundedQuantity,
            refundedTaxOneAmount: transaction.refundedTaxOneAmount,
            refundedTaxTwoAmount: transaction.refundedTaxTwoAmount,
            size: transaction.size,
            soldQuantity: transaction.soldQuantity,
            staffName: transaction.staffName,
            standName: transaction.standName,
            taxOneAmount: transaction.taxOneAmount,
            taxTwoAmount: transaction.taxTwoAmount,
            tenderType: transaction.tenderType,
            totalDiscountAmount: transaction.totalDiscountAmount,
            totalRefundedAmount: transaction.totalRefundedAmount,
            totalRefundedAmountWithModifiers:
              transaction.totalRefundedAmountWithModifiers,
            unitPriceAmount: transaction.unitPriceAmount,
          }));
        }
        return [];
      })
    )
  );
};

const exportShowsToCSV = (shows) => {
  if (shows.length === 0) {
    console.error("No shows to export.");
    return;
  }
  // Expanded fields to match the new structure
  const fields = [
    "bandName",
    "tourName",
    "showDate",
    "showEndDate",
    "attendance",
    "capacity",
    "city",
    "country",
    "stateProvince",
    "artistName",
    "beforeTaxPriceAmount",
    "cardholderName",
    "device",
    "discountName",
    "grossSoldAmount",
    "grossSoldAmountWithModifiers",
    "itemName",
    "itemizationType",
    "modifiers",
    "netSoldAmount",
    "netSoldAmountWithModifiers",
    "orderId",
    "orderType",
    "paymentTimestamp",
    "productType",
    "refundedDiscountAmount",
    "refundedQuantity",
    "refundedTaxOneAmount",
    "refundedTaxTwoAmount",
    "size",
    "soldQuantity",
    "staffName",
    "standName",
    "taxOneAmount",
    "taxTwoAmount",
    "tenderType",
    "totalDiscountAmount",
    "totalRefundedAmount",
    "totalRefundedAmountWithModifiers",
    "unitPriceAmount",
  ];
  try {
    const csvString = parse(shows, { fields });
    fs.writeFileSync("ShowsFrom2022 - 4pm - AP.csv", csvString);
    console.log("Successfully exported shows from 2022 to CSV.");
  } catch (error) {
    console.error("Error exporting shows to CSV:", error);
  }
};

// Usage
downloadAlanParsonsLiveAccounts()
  .then((accounts) => {
    const showsFrom2022 = filterShowsFrom2022(accounts); // Use the updated function
    exportShowsToCSV(showsFrom2022);
  })
  .catch((error) => {
    console.error("Error:", error);
  });
