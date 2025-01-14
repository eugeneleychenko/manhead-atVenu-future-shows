import requests
import json
import csv

# Set up the GraphQL endpoint and authentication headers
endpoint = "https://api.atvenu.com/"
headers = {
    "x-api-key": "live_yvYLBo32dRE9z_yCdhwU"
}

# GraphQL query to fetch the list of organization's accounts
accounts_query = """
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
"""

def fetch_accounts():
    variables = {"cursor": None}  # Start with no cursor to fetch the first page
    all_accounts = []
    has_next_page = True

    while has_next_page:
        response = requests.post(endpoint, json={"query": accounts_query, "variables": variables}, headers=headers)
        
        if response.status_code == 200:
            json_data = response.json()
            if "data" in json_data and "organization" in json_data["data"] and "accounts" in json_data["data"]["organization"]:
                accounts_data = json_data["data"]["organization"]["accounts"]
                all_accounts.extend(accounts_data["nodes"])
                has_next_page = accounts_data["pageInfo"]["hasNextPage"]
                variables["cursor"] = accounts_data["pageInfo"]["endCursor"]
            else:
                print("Error: Unexpected response format. 'data' key not found in the response.")
                break
        else:
            print(f"Error: Request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            break

    return all_accounts

def export_accounts_to_csv(accounts):
    with open("accounts.csv", "w", newline="") as csvfile:
        fieldnames = ["artistName", "uuid"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for account in accounts:
            writer.writerow(account)

if __name__ == "__main__":
    print("Fetching accounts...")
    accounts = fetch_accounts()
    print("Accounts fetched successfully.")
    print(json.dumps(accounts, indent=2))
    export_accounts_to_csv(accounts)
    print("Accounts exported to accounts.csv")