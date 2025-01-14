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
  query GetAccounts($first: Int!, $after: String) {
    organization {
      accounts(first: $first, after: $after) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          name
          tours(first: 15) {
            nodes {
              name
              shows(first: 15) {
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
                  itemizedTransactions(first: 15) {
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
    const variables = { first: 20, after };
    const headers = { "x-api-key": API_TOKEN };

    try {
      const data = await fetchWithRetry(
        API_ENDPOINT,
        ACCOUNTS_QUERY,
        variables,
        headers
      );
      const fetchedAccounts = data.organization.accounts.nodes;
      accounts.push(...fetchedAccounts);

      const pageInfo = data.organization.accounts.pageInfo;
      hasNextPage = pageInfo.hasNextPage;
      after = pageInfo.endCursor;
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
    fs.writeFileSync("ShowsFrom2022.csv", csvString);
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
