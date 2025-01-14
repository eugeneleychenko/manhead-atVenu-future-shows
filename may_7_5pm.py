import requests
import csv

print("Starting script...")

# Set up the GraphQL endpoint and authentication headers
endpoint = "https://api.atvenu.com/"
headers = {
    "x-api-key": "live_yvYLBo32dRE9z_yCdhwU"
}

print("Endpoint and headers configured.")

# Function to fetch data from the GraphQL API with pagination
def fetch_data(query, variables, data_path):
    print(f"Fetching data for path: {data_path}")
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
                print(f"Response: {json_data}")
                break
        else:
            print(f"Error: Request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            break

    print(f"Data fetching complete for path: {data_path}")
    return all_data

print("Defined fetch_data function.")

# Fetch and cache the list of organization's accounts
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
print("Fetching accounts...")
accounts = fetch_data(accounts_query, {}, "organization.accounts")
print("Accounts fetched successfully.")

# Export accounts to CSV
print("Exporting accounts to CSV...")
with open("accounts.csv", "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["artistName", "uuid"])
    writer.writeheader()
    writer.writerows(accounts)

print("Accounts exported to accounts.csv")

# Fetch and cache merchandise for each account
merch_items_by_account = {}
print("Fetching merchandise for each account...")
for account in accounts:
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
    merch_items_by_account[account["uuid"]] = merch_items
print("Merchandise fetched successfully.")

# Export merchandise to CSV
print("Exporting merchandise to CSV...")
if merch_items_by_account:
    for account_uuid, merch_items in merch_items_by_account.items():
        with open(f"merch_items_{account_uuid}.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["name", "category", "uuid", "productType", "sku", "size", "price"])
            writer.writeheader()
            for item in merch_items:
                for variant in item["merchVariants"]:
                    writer.writerow({
                        "name": item["name"],
                        "category": item["category"],
                        "uuid": item["uuid"],
                        "productType": item["productType"]["name"],
                        "sku": variant["sku"],
                        "size": variant["size"],
                        "price": variant["price"]
                    })
    print(f"Merchandise exported to merch_items_{{account_uuid}}.csv for each account")
else:
    print("No merchandise found for any account.")

# Fetch and cache tours for each account
tours_by_account = {}
print("Fetching tours for each account...")
for account in accounts:
    tours_query = """
        query tours($uuid: UUID!) {
            account: node(uuid: $uuid) {
                ... on Account {
                    tours(first: 200, open: false) {
                        pageInfo { hasNextPage }
                        nodes {
                            tourName: name
                            uuid
                        }
                    }
                }
            }
        }
    """
    tours = fetch_data(tours_query, {"uuid": account["uuid"]}, "account.tours")
    tours_by_account[account["uuid"]] = tours
print("Tours fetched successfully.")

# Export tours to CSV
print("Exporting tours to CSV...")
for account_uuid, tours in tours_by_account.items():
    with open(f"tours_{account_uuid}.csv", "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["tourName", "uuid"])
        writer.writeheader()
        writer.writerows(tours)

print(f"Tours exported to tours_{account_uuid}.csv for each account")

# Fetch and cache shows for each tour
shows_by_tour = {}
print("Fetching shows for each tour...")
for account_uuid, tours in tours_by_account.items():
    for tour in tours:
        shows_query = """
            query shows($uuid: UUID!, $cursor: String, $startDate: Date!, $endDate: Date!) {
                tour: node(uuid: $uuid) {
                    ... on Tour {
                        shows(first: 200, after: $cursor, showsOverlap: {start: $startDate, end: $endDate}) {
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
        """
        shows = fetch_data(shows_query, {"uuid": tour["uuid"], "startDate": "2022-01-01", "endDate": "2023-12-31"}, "tour.shows")
        shows_by_tour[tour["uuid"]] = shows
print("Shows fetched successfully.")

# Export shows to CSV
print("Exporting shows to CSV...")
for tour_uuid, shows in shows_by_tour.items():
    with open(f"shows_{tour_uuid}.csv", "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["uuid", "showDate", "showEndDate", "state", "attendance", "capacity", "currencyFormat", "location"])
        writer.writeheader()
        for show in shows:
            writer.writerow({
                "uuid": show["uuid"],
                "showDate": show["showDate"],
                "showEndDate": show["showEndDate"],
                "state": show["state"],
                "attendance": show["attendance"],
                "capacity": show["capacity"],
                "currencyFormat": show["currencyFormat"]["code"],
                "location": f"{show['location']['city']}, {show['location']['stateProvince']}, {show['location']['country']}"
            })

print(f"Shows exported to shows_{tour_uuid}.csv for each tour")

# Fetch main counts for each show and calculate the number sold
sales_data = []
print("Fetching sales data for each show...")
for tour_uuid, shows in shows_by_tour.items():
    for show in shows:
        counts_query = """
            query counts($uuid: UUID!, $cursor: String) {
                show: node(uuid: $uuid) {
                    ... on Show {
                        settlements {
                            path
                            mainCounts(first: 200, after: $cursor) {
                                pageInfo {
                                    hasNextPage
                                    endCursor
                                }
                                nodes {
                                    merchVariantUuid
                                    merchItemUuid
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
        """
        counts = fetch_data(counts_query, {"uuid": show["uuid"]}, "show.settlements.0.mainCounts")

        for count in counts:
            variant_uuid = count["merchVariantUuid"]
            merch_item_uuid = count["merchItemUuid"]
            price_override = count["priceOverride"]
            count_in = count["countIn"]
            count_out = count["countOut"]
            comps = count["comps"]
            adds_quantity = sum(add["quantity"] for add in count["merchAdds"])

            number_sold = count_in + adds_quantity - count_out - comps

            sales_data.append({
                "show_uuid": show["uuid"],
                "variant_uuid": variant_uuid,
                "merch_item_uuid": merch_item_uuid,
                "price_override": price_override,
                "number_sold": number_sold
            })
print("Sales data fetched successfully.")

# Export sales data to CSV
print("Exporting sales data to CSV...")
with open("sales_data.csv", "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["show_uuid", "variant_uuid", "merch_item_uuid", "price_override", "number_sold"])
    writer.writeheader()
    writer.writerows(sales_data)

print("Sales data exported to sales_data.csv")

# Process and analyze the sales data as needed
# ...

print("Sales data collected successfully.")