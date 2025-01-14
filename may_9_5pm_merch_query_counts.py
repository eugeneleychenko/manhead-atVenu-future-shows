import csv
import requests

# Set up the GraphQL endpoint and authentication headers
endpoint = "https://api.atvenu.com/"
head = {
    "x-api-key": "live_yvYLBo32dRE9z_yCdhwU"
}

# Define the GraphQL query for fetching counts
counts_query = """
query counts($uuid: UUID!, $cursor: String) {
    show: node(uuid: $uuid) {
        ... on Show {
            settlements {
                mainCounts(first: 200, after: $cursor) {
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
"""

# Function to fetch counts data from the GraphQL API with pagination
def fetch_counts_data(uuid, cursor=None):
    all_counts = []
    has_next_page = True

    while has_next_page:
        response = requests.post(endpoint, json={"query": counts_query, "variables": {"uuid": uuid, "cursor": cursor}}, headers=head)
        if response.status_code == 200:
            json_data = response.json()
            if "data" in json_data:
                data = json_data["data"]
                if "show" in data:
                    show_data = data["show"]
                    if show_data is not None:
                        if "settlements" in show_data:
                            settlements = show_data["settlements"]
                            if settlements and "mainCounts" in settlements[0]:
                                counts = settlements[0]["mainCounts"]
                                all_counts.extend(counts["nodes"])
                                has_next_page = counts["pageInfo"]["hasNextPage"]
                                cursor = counts["pageInfo"]["endCursor"]
                                print(f"Fetched counts data for show UUID: {uuid}")
                            else:
                                print(f"No counts data found for show UUID: {uuid}")
                                break
                        else:
                            print(f"No settlements found for show UUID: {uuid}")
                            break
                    else:
                        print(f"Show data is None for show UUID: {uuid}")
                        break
                else:
                    print(f"No show data found for show UUID: {uuid}")
                    break
            else:
                print(f"No data found in the response for show UUID: {uuid}")
                break
        else:
            print(f"Failed to fetch counts data for show UUID: {uuid}, status code: {response.status_code}")
            break

    return all_counts

# Read the show UUIDs from the CSV file and fetch counts data for each
with open('all_shows_5_9.csv', mode='r') as csvfile, open('all_counts_5_9.csv', mode='w', newline='') as countsfile:
    csv_reader = csv.reader(csvfile)
    csv_writer = csv.writer(countsfile)
    headers_written = False

    for row in csv_reader:
        show_uuid = row[4]  # Assuming the UUID is in the fifth column
        counts_data = fetch_counts_data(show_uuid)

        for count in counts_data:
            count_in = count.get('countIn')
            count_out = count.get('countOut')
            comps = count.get('comps')
            merch_adds = count.get('merchAdds', [])

            # Filter out None values from merchAdds before summing
            merch_adds_quantity = sum(add.get('quantity', 0) for add in merch_adds if add.get('quantity') is not None)

            # Calculate quantity_sold considering None as 0
            quantity_sold = (count_in if count_in is not None else 0) + merch_adds_quantity - (count_out if count_out is not None else 0) - (comps if comps is not None else 0)

            # Include all relevant columns in the output, showing 'null' for None values
            count_row = [
                show_uuid,
                count['merchVariantUuid'],
                count.get('priceOverride', ''),
                count_in if count_in is not None else 'null',
                merch_adds_quantity,
                count_out if count_out is not None else 'null',
                comps if comps is not None else 0,
                quantity_sold
            ]

            if not headers_written:
                # Define headers including all relevant columns
                headers = [
                    'show_uuid',
                    'merchVariantUuid',
                    'priceOverride',
                    'countIn',
                    'merchAddsQuantity',
                    'countOut',
                    'comps',
                    'quantitySold'
                ]
                csv_writer.writerow(headers)
                headers_written = True
            csv_writer.writerow(count_row)
            print(f"Fetched counts data: {count_row}")

print("All counts data written to all_counts.csv")