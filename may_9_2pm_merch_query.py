import csv
import requests

# Set up the GraphQL endpoint and authentication headers from the provided file
endpoint = "https://api.atvenu.com/"
head = {
    "x-api-key": "live_yvYLBo32dRE9z_yCdhwU"
}

# Define the GraphQL query for fetching merch items
query = """
query merch($uuid: UUID!, $cursor: String) {
  show: node(uuid: $uuid) {
    ... on Show {
      settlements {
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
}
"""

# Function to fetch merch data from the GraphQL API with pagination
def fetch_merch_data(uuid):
    all_merch = []
    has_next_page = True
    cursor = None

    while has_next_page:
        response = requests.post(endpoint, json={"query": query, "variables": {"uuid": uuid, "cursor": cursor}}, headers=head)

        if response.status_code == 200:
            json_data = response.json()

            if "data" in json_data:
                data = json_data["data"]

                if "show" in data:
                    show_data = data["show"]

                    if show_data is not None:
                        if "settlements" in show_data:
                            settlements = show_data["settlements"]

                            if settlements and "merchItems" in settlements[0]:
                                merch_items = settlements[0]["merchItems"]
                                all_merch.extend(merch_items["nodes"])
                                has_next_page = merch_items["pageInfo"]["hasNextPage"]
                                cursor = merch_items["pageInfo"]["endCursor"]
                                print(f"Fetched merch data for show UUID: {uuid}")
                            else:
                                print(f"No merchItems found for show UUID: {uuid}")
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
            print(f"Failed to fetch merch data for show UUID: {uuid}, status code: {response.status_code}")
            break

    return all_merch

# Read the show UUIDs from the CSV file and fetch merch data for each
with open('all_shows.csv', mode='r') as csvfile, open('all_merch.csv', mode='w', newline='') as merchfile:
    csv_reader = csv.reader(csvfile)
    csv_writer = csv.writer(merchfile)
    headers_written = False

    for row in csv_reader:
        show_uuid = row[4]  # Assuming the UUID is in the fifth column
        merch_data = fetch_merch_data(show_uuid)

        for merch in merch_data:
            if not headers_written:
                # Add 'show_uuid' to the headers
                headers = ['show_uuid'] + list(merch.keys()) + ['variant_' + key for key in merch['merchVariants'][0].keys()]
                csv_writer.writerow(headers)
                headers_written = True

            for variant in merch['merchVariants']:
                # Include 'show_uuid' in each merch row
                merch_row = [show_uuid] + list(merch.values())[:-1] + list(variant.values())
                csv_writer.writerow(merch_row)

print("All merch data written to all_merch.csv")
