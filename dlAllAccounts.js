const fs = require("fs");
const { request } = require("graphql-request");
const { parse } = require("json2csv");

const API_ENDPOINT = "https://api.atvenu.com";
const API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU";

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
          tours(first: 35) {
            nodes {
              name
              shows(first: 35) {
                nodes {                
                  showDate
                  attendance
                  capacity
                  location {
                    city              
                    phone
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
      const data = await request(
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

const filterShowsInApril = (accounts) => {
  const startApril2024 = new Date("2024-04-01T00:00:00Z");
  const endApril2024 = new Date("2024-12-31T23:59:59Z");
  return accounts.flatMap((account) =>
    account.tours.nodes.flatMap((tour) =>
      tour.shows.nodes.flatMap((showNode) => {
        const showDate = new Date(showNode.showDate);
        if (showDate >= startApril2024 && showDate <= endApril2024) {
          return [
            {
              bandName: account.name,
              showDate: showNode.showDate,
              capacity: showNode.capacity,
              attendance: showNode.attendance,
              city: showNode.location.city,
              phone: showNode.location.phone,
            },
          ];
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
  const fields = [
    "bandName",
    "showDate",
    "capacity",
    "attendance",
    "city",
    "phone",
  ];
  try {
    const csv = parse(shows, { fields });
    fs.writeFileSync("April2024Shows.csv", csv);
    console.log("Successfully exported April 2024 shows to CSV.");
  } catch (error) {
    console.error("Error exporting shows to CSV:", error);
  }
};

// Usage
downloadAllAccounts()
  .then((accounts) => {
    const aprilShows = filterShowsInApril(accounts);
    exportShowsToCSV(aprilShows);
  })
  .catch((error) => {
    console.error("Error:", error);
  });
