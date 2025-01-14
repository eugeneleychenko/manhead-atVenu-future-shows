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
  query GetAccounts($first: Int!, $after: String, $toursFirst: Int!, $toursAfter: String, $showsFirst: Int!, $showsAfter: String, $transactionsFirst: Int!, $transactionsAfter: String) {
    organization {
      accounts(first: $first, after: $after) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          name
          tours(first: $toursFirst, after: $toursAfter) {
            pageInfo {
              endCursor
              hasNextPage
            }
            nodes {
              name
              shows(first: $showsFirst, after: $showsAfter) {
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
                  itemizedTransactions(first: $transactionsFirst, after: $transactionsAfter) {
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

const downloadAllAccounts = async () => {
  const accounts = [];
  let after = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const variables = {
      first: 10,
      after,
      toursFirst: 10,
      toursAfter: null,
      showsFirst: 10,
      showsAfter: null,
      transactionsFirst: 10,
      transactionsAfter: null,
    };
    const headers = { "x-api-key": API_TOKEN };

    try {
      const data = await fetchWithRetry(
        API_ENDPOINT,
        ACCOUNTS_QUERY,
        variables,
        headers
      );

      if (data.organization && data.organization.accounts) {
        const fetchedAccounts = data.organization.accounts.nodes;

        for (const account of fetchedAccounts) {
          const tours = account.tours ? account.tours.nodes || [] : [];

          for (const tour of tours) {
            console.log("Current tour:", tour);
            if (!tour.shows || !Array.isArray(tour.shows.nodes)) {
              console.warn("No shows found for tour:", tour.name);
              continue; // Skip to the next iteration of the loop if `tour.shows` is undefined or not an array
            }
            const shows = tour.shows.nodes;

            for (const show of shows) {
              if (
                !show.itemizedTransactions ||
                !Array.isArray(show.itemizedTransactions.nodes)
              ) {
                console.warn(
                  "No itemized transactions found for show:",
                  show.uuid
                );
                continue; // Skip to the next iteration of the loop if conditions are not met
              }
              const transactions = show.itemizedTransactions.nodes;
              show.itemizedTransactions = {
                nodes: transactions,
                pageInfo: show.itemizedTransactions?.pageInfo || {
                  hasNextPage: false,
                  endCursor: null,
                },
              };

              let transactionsHasNextPage =
                show.itemizedTransactions.pageInfo.hasNextPage;
              let transactionsAfter =
                show.itemizedTransactions.pageInfo.endCursor;

              while (transactionsHasNextPage) {
                variables.transactionsAfter = transactionsAfter;
                const transactionsData = await fetchWithRetry(
                  API_ENDPOINT,
                  ACCOUNTS_QUERY,
                  variables,
                  headers
                );
                const fetchedTransactions =
                  transactionsData.organization.accounts.nodes[0].tours.nodes[0]
                    .shows.nodes[0].itemizedTransactions;
                if (fetchedTransactions && fetchedTransactions.nodes) {
                  show.itemizedTransactions.nodes.push(
                    ...fetchedTransactions.nodes
                  );
                  transactionsHasNextPage =
                    fetchedTransactions.pageInfo.hasNextPage;
                  transactionsAfter = fetchedTransactions.pageInfo.endCursor;
                } else {
                  transactionsHasNextPage = false;
                }
              }
            }

            tour.shows = {
              nodes: shows,
              pageInfo: tour.shows?.pageInfo || {
                hasNextPage: false,
                endCursor: null,
              },
            };

            let showsHasNextPage = tour.shows.pageInfo.hasNextPage;
            let showsAfter = tour.shows.pageInfo.endCursor;

            while (showsHasNextPage) {
              variables.showsAfter = showsAfter;
              const showsData = await fetchWithRetry(
                API_ENDPOINT,
                ACCOUNTS_QUERY,
                variables,
                headers
              );
              const fetchedShows =
                showsData.organization.accounts.nodes[0].tours.nodes[0].shows;
              if (fetchedShows && fetchedShows.nodes) {
                tour.shows.nodes.push(...fetchedShows.nodes);
                showsHasNextPage = fetchedShows.pageInfo.hasNextPage;
                showsAfter = fetchedShows.pageInfo.endCursor;
              } else {
                showsHasNextPage = false;
              }
            }
          }

          account.tours = {
            nodes: tours,
            pageInfo: account.tours?.pageInfo || {
              hasNextPage: false,
              endCursor: null,
            },
          };

          let toursHasNextPage = account.tours.pageInfo.hasNextPage;
          let toursAfter = account.tours.pageInfo.endCursor;

          while (toursHasNextPage) {
            variables.toursAfter = toursAfter;
            const toursData = await fetchWithRetry(
              API_ENDPOINT,
              ACCOUNTS_QUERY,
              variables,
              headers
            );
            const fetchedTours = toursData.organization.accounts.nodes[0].tours;
            if (fetchedTours && fetchedTours.nodes) {
              account.tours.nodes.push(...fetchedTours.nodes);
              toursHasNextPage = fetchedTours.pageInfo.hasNextPage;
              toursAfter = fetchedTours.pageInfo.endCursor;
            } else {
              toursHasNextPage = false;
            }
          }
        }

        accounts.push(...fetchedAccounts);

        const pageInfo = data.organization.accounts.pageInfo;
        hasNextPage = pageInfo.hasNextPage;
        after = pageInfo.endCursor;
      } else {
        console.warn("Unexpected data structure:", data);
        hasNextPage = false;
      }
    } catch (error) {
      console.error("Error fetching accounts:", error);
      break;
    }
  }

  return accounts;
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
    fs.writeFileSync("ShowsFrom2022_4-26-10am.csv", csvString);
    console.log("Successfully exported shows from 2022 to CSV.");
  } catch (error) {
    console.error("Error exporting shows to CSV:", error);
  }
};

// Usage
downloadAllAccounts()
  .then((accounts) => {
    const showsFrom2022 = filterShowsFrom2022(accounts); // Use the updated function
    exportShowsToCSV(showsFrom2022);
  })
  .catch((error) => {
    console.error("Error:", error);
  });
