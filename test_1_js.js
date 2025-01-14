const axios = require("axios");

const API_ENDPOINT = "https://api.atvenu.com";
const API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU";
const QUERY = `
query GetAllShowsAndTransactions($startDate: DateTime!, $endDate: DateTime!) {
  organization {
    accounts(first: 10) {
      nodes {
        name
        tours(first: 10) {
          nodes {
            name
            shows(first: 10, filter: {showDate: {greaterThanOrEqualTo: $startDate, lessThanOrEqualTo: $endDate}}) {
              nodes {
                uuid
                showDate
              }
            }
          }
        }
      }
    }
  }
}
`;

async function fetchData(startDate, endDate) {
  const headers = {
    "x-api-key": API_TOKEN,
    "Content-Type": "application/json",
  };
  const variables = {
    startDate,
    endDate,
  };
  try {
    const response = await axios({
      method: "post",
      url: API_ENDPOINT,
      headers: headers,
      data: {
        query: QUERY,
        variables: variables,
      },
    });
    console.log(JSON.stringify(response.data, null, 2)); // Pretty print the JSON response
  } catch (error) {
    console.error("Error fetching data: ", error);
  }
}

fetchData("2023-01-01", "2023-12-31");
