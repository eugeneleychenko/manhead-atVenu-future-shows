import csv
import requests

# Set up the GraphQL endpoint and authentication headers
endpoint = "https://api.atvenu.com/"
headers = {
    "x-api-key": "live_yvYLBo32dRE9z_yCdhwU"
}

# Function to fetch data from the GraphQL API with pagination
def fetch_data(query, variables, data_path):
    all_data = []
    has_next_page = True
    cursor = None

    while has_next_page:
        variables["cursor"] = cursor
        response = requests.post(endpoint, json={"query": query, "variables": variables}, headers=headers)
        
        if response.status_code == 200:
            json_data = response.json()
            if "data" in json_data:
                data = json_data["data"]
                for path in data_path.split("."):
                    data = data[path]

                all_data.extend(data["nodes"])
                has_next_page = data["pageInfo"]["hasNextPage"]
                cursor = data["pageInfo"]["endCursor"]
            else:
                print(f"Error: Unexpected response format. 'data' key not found in the response.")
                break
        else:
            print(f"Error: Request failed with status code {response.status_code}")
            break

    return all_data

# Read accounts from CSV
accounts = []
with open("accounts.csv", mode="r") as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        accounts.append({"artistName": row["artistName"], "uuid": row["uuid"]})

# Prepare CSV file to store all tours
with open("all_tours_5_9.csv", "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["artistName", "account_uuid", "tour_name", "tour_uuid"])
    writer.writeheader()

    # Check if accounts list is populated
    if not accounts:
        print("No accounts found. Please check the accounts.csv file.")
    else:
        # Fetch and cache tours for each account and append to CSV
        for account in accounts:
            print(f"Fetching tours for account: {account['artistName']} ({account['uuid']})")
            tours_query = """
                query tours($uuid: UUID!, $cursor: String) {
                    account: node(uuid: $uuid) {
                        ... on Account {
                            uuid
                            tours(first: 200, after: $cursor )) {
                                pageInfo {
                                    hasNextPage
                                    endCursor
                                }
                                nodes {
                                    name
                                    uuid
                                }
                            }
                        }
                    }
                }
            """
            tours = fetch_data(tours_query, {"uuid": account["uuid"]}, "account.tours")
            
            # Check if tours data is fetched
            if not tours:
                print(f"No tours found for account UUID: {account['uuid']}")
            else:
                print(f"Appending tours for account UUID: {account['uuid']} to all_tours_5_9.csv")
                for tour in tours:
                    writer.writerow({
                        "artistName": account["artistName"],
                        "account_uuid": account["uuid"],
                        "tour_name": tour["name"],
                        "tour_uuid": tour["uuid"]
                    })
        print("All tours appended to all_tours_5_9.csv")