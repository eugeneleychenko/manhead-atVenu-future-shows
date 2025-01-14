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

# Fetch and cache merchandise for each account and export to CSV
with open("all_merch_items.csv", "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["account_uuid", "name", "category", "uuid", "productType", "sku", "size", "price"])
    writer.writeheader()
    
    for account in accounts:
        print(f"Fetching merchandise for account: {account['artistName']} ({account['uuid']})")
        merchandise_query = """
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
        """
        merch_items = fetch_data(merchandise_query, {"uuid": account["uuid"]}, "account.merchItems")
        
        print(f"Exporting merchandise for account UUID: {account['uuid']}")
        for item in merch_items:
            for variant in item["merchVariants"]:
                writer.writerow({
                    "account_uuid": account["uuid"],
                    "name": item["name"],
                    "category": item["category"],
                    "uuid": item["uuid"],
                    "productType": item["productType"]["name"],
                    "sku": variant["sku"],
                    "size": variant["size"],
                    "price": variant["price"]
                })
