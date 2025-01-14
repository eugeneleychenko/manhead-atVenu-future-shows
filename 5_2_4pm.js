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
    // console.log("API request:");
    // console.log("URL:", url);
    // console.log("Query:", query);
    // console.log("Variables:", variables);
    // console.log("Headers:", headers);

    const response = await request(url, query, variables, headers);
    console.log("Fetched data:", response); // Log the fetched data
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
  query accounts($cursor: String) {
    organization {
      accounts(first: 20, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          artistName: name
          uuid
        }
      }
    }
  }
`;

const MERCHANDISE_QUERY = `
  query merchandise($uuid: UUID!, $cursor: String) {
    account: node(uuid: $uuid) {
      ... on Account {
        uuid
        merchItems(first: 200, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            name
            category
            uuid
            productType {
              name
            }
            merchVariants {
              sku
              size
              uuid
              price
            }
          }
        }
      }
    }
  }
`;

const TOURS_QUERY = `
  query tours($uuid: UUID!) {
    account: node(uuid: $uuid) {
      ... on Account {
        tours(first: 200, open: false) {
          pageInfo {
            hasNextPage
          }
          nodes {
            tourName: name
            uuid
          }
        }
      }
    }
  }
`;

const SHOWS_QUERY = `
  query shows($uuid: UUID!, $cursor: String, $startDate: Date!, $endDate: Date!) {
    tour: node(uuid: $uuid) {
      ... on Tour {
        shows(first: 200, after: $cursor, showsOverlap: { start: $startDate, end: $endDate }) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            uuid
            showDate
            showEndDate
            state
            attendance
            capacity
            currencyFormat {
              code
            }
            location {
              capacity
              city
              stateProvince
              country
            }
          }
        }
      }
    }
  }
`;

const COUNTS_QUERY = `
  query counts($uuid: UUID!, $cursor: String) {
    show: node(uuid: $uuid) {
      ... on Show {
        settlements {
          path
          mainCounts(first: 100, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              merchVariantUuid
              priceOverride
              countIn
              countOut
              comps
              merchAdds {
                quantity
              }
            }
          }
        }
      }
    }
  }
`;

async function fetchAccounts(cursor = null) {
  const variables = { cursor };
  const headers = { "x-api-key": API_TOKEN };

  try {
    const response = await fetchWithRetry(
      API_ENDPOINT,
      ACCOUNTS_QUERY,
      variables,
      headers
    );
    console.log("API response:", response);

    if (response.organization && response.organization.accounts) {
      //   console.log("Accounts data:", response.organization.accounts);
      return response.organization.accounts;
    } else {
      throw new Error("Unexpected response format from the GraphQL API");
    }
  } catch (error) {
    console.error("Error fetching accounts:", error);
    throw error;
  }
}

async function fetchMerchandise(accountUuid, cursor = null) {
  const variables = { uuid: accountUuid, cursor };
  const headers = { "x-api-key": API_TOKEN };
  const data = await fetchWithRetry(
    API_ENDPOINT,
    MERCHANDISE_QUERY,
    variables,
    headers
  );
  return data.account.merchItems;
}

async function fetchTours(accountUuid) {
  const variables = { uuid: accountUuid };
  const headers = { "x-api-key": API_TOKEN };
  const data = await fetchWithRetry(
    API_ENDPOINT,
    TOURS_QUERY,
    variables,
    headers
  );
  return data.account.tours;
}

async function fetchShows(tourUuid, startDate, endDate, cursor = null) {
  const variables = { uuid: tourUuid, cursor, startDate, endDate };
  const headers = { "x-api-key": API_TOKEN };
  const data = await fetchWithRetry(
    API_ENDPOINT,
    SHOWS_QUERY,
    variables,
    headers
  );
  return data.tour.shows;
}

async function fetchCounts(showUuid, cursor = null) {
  const variables = { uuid: showUuid, cursor };
  const headers = { "x-api-key": API_TOKEN };
  const data = await fetchWithRetry(
    API_ENDPOINT,
    COUNTS_QUERY,
    variables,
    headers
  );
  return data.show.settlements[0].mainCounts;
}

async function fetchAllAccounts() {
  let allAccounts = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    try {
      const { nodes, pageInfo } = await fetchAccounts(cursor);
      console.log("Fetched accounts:", nodes); // Log the accounts fetched in the current batch
      allAccounts = [...allAccounts, ...nodes];
      cursor = pageInfo.endCursor;
      hasNextPage = pageInfo.hasNextPage;
    } catch (error) {
      console.error("Error fetching all accounts:", error);
      break;
    }
  }

  if (allAccounts.length === 0) {
    console.warn("No accounts found");
  }

  return allAccounts;
}

async function fetchAllMerchandise(accountUuid) {
  let allMerchandise = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const { nodes, pageInfo } = await fetchMerchandise(accountUuid, cursor);
    allMerchandise = [...allMerchandise, ...nodes];
    cursor = pageInfo.endCursor;
    hasNextPage = pageInfo.hasNextPage;
  }

  return allMerchandise;
}

async function fetchAllShows(tourUuid, startDate, endDate) {
  let allShows = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const { nodes, pageInfo } = await fetchShows(
      tourUuid,
      startDate,
      endDate,
      cursor
    );
    allShows = [...allShows, ...nodes];
    cursor = pageInfo.endCursor;
    hasNextPage = pageInfo.hasNextPage;
  }

  return allShows;
}

async function fetchAllCounts(showUuid) {
  let allCounts = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const { nodes, pageInfo } = await fetchCounts(showUuid, cursor);
    allCounts = [...allCounts, ...nodes];
    cursor = pageInfo.endCursor;
    hasNextPage = pageInfo.hasNextPage;
  }

  return allCounts;
}

function calculateSoldQuantity(count) {
  const { countIn, countOut, comps, merchAdds } = count;
  const addsQuantity = merchAdds.reduce(
    (total, add) => total + add.quantity,
    0
  );
  return countIn + addsQuantity - countOut - comps;
}

async function fetchManheadData(startDate, endDate) {
  const accounts = await fetchAllAccounts();
  console.log("Accounts:", accounts);
  let isFirstBatch = true; // Ensure headers are only included in the first batch
  const allManheadData = [];

  for (const account of accounts) {
    const manheadData = []; // Reset for each account
    const merchandise = await fetchAllMerchandise(account.uuid);
    console.log("Merchandise:", merchandise);
    const merchandiseMap = new Map(
      merchandise.map((item) => [item.uuid, item])
    );

    const toursResponse = await fetchTours(account.uuid);
    console.log("Tours Response:", toursResponse);
    const tours = toursResponse.nodes || [];

    if (toursResponse.pageInfo && toursResponse.pageInfo.hasNextPage) {
      throw new Error(
        "More than one page of tours found. Refactor to handle pagination."
      );
    }

    for (const tour of tours) {
      const shows = await fetchAllShows(tour.uuid, startDate, endDate);
      console.log("Shows:", shows);

      for (const show of shows) {
        const counts = await fetchAllCounts(show.uuid);
        console.log("Counts:", counts);

        for (const count of counts) {
          const variant = merchandiseMap.get(count.merchVariantUuid);
          console.log("Variant:", variant);
          if (variant) {
            const dataPoint = {
              artistName: account.artistName,
              tourName: tour.tourName,
              showDate: show.showDate,
              showEndDate: show.showEndDate,
              state: show.state,
              attendance: show.attendance,
              capacity: show.capacity || show.location.capacity,
              city: show.location.city,
              stateProvince: show.location.stateProvince,
              country: show.location.country,
              currencyCode: show.currencyFormat.code,
              category: variant.category,
              productType: variant.productType.name,
              variantName: variant.name,
              variantSku: variant.sku,
              variantSize: variant.size,
              variantPrice: count.priceOverride || variant.price,
              soldQuantity: calculateSoldQuantity(count),
            };
            console.log("Data Point:", dataPoint);
            manheadData.push(dataPoint);
          }
        }
      }
      allManheadData.push(...manheadData);
      console.log("Final manheadData:", manheadData);
    }
    // Export the data for the current account
    console.log("Processed data for CSV:", manheadData); // Log the data before exporting to CSV
    exportToCSV(manheadData, isFirstBatch);
    isFirstBatch = false; // Ensure headers are only included in the first batch
  }
}

function exportToCSV(data, isFirstBatch) {
  const fields = [
    "artistName",
    "tourName",
    "showDate",
    "showEndDate",
    "state",
    "attendance",
    "capacity",
    "city",
    "stateProvince",
    "country",
    "currencyCode",
    "category",
    "productType",
    "variantName",
    "variantSku",
    "variantSize",
    "variantPrice",
    "soldQuantity",
  ];

  const csvOptions = { fields, header: isFirstBatch };
  const csv = parse(data, csvOptions);

  if (isFirstBatch) {
    fs.writeFileSync("manhead_data.csv", csv + "\n");
  } else {
    fs.appendFileSync("manhead_data.csv", csv + "\n");
  }
}

(async () => {
  try {
    const startDate = "2022-04-14";
    const endDate = "2022-04-15";
    await fetchManheadData(startDate, endDate);
    // exportToCSV(manheadData);
    console.log("Data exported to manhead_data.csv");
  } catch (error) {
    console.error("Error:", error);
  }
})();
