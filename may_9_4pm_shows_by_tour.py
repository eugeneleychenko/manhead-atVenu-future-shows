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
        
        print("api response",response.text)
         
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

# Read tours from CSV and prepare CSV file called 'all_shows' to store all shows
tours = []
with open("all_tours.csv", mode="r") as csvfile, open("all_shows_5_9.csv", "w", newline="") as showsfile:
    csvreader = csv.DictReader(csvfile)
    writer = csv.DictWriter(showsfile, fieldnames=["artistName", "account_uuid", "tour_name", "tour_uuid", "show_uuid", "showDate", "showEndDate", "state", "attendance", "capacity", "currencyFormat", "location"])
    writer.writeheader()
    
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
    
    for row in csvreader:
        tour = {"artistName": row["artistName"], "account_uuid": row["account_uuid"], "tour_name": row["tour_name"], "tour_uuid": row["tour_uuid"]}
        shows = fetch_data(shows_query, {"uuid": tour["tour_uuid"], "startDate": "2022-01-01", "endDate": "2023-12-31"}, "tour.shows")
        
        for show in shows:
            writer.writerow({
                "artistName": tour["artistName"],
                "account_uuid": tour["account_uuid"],
                "tour_name": tour["tour_name"],
                "tour_uuid": tour["tour_uuid"],
                "show_uuid": show["uuid"],
                "showDate": show["showDate"],
                "showEndDate": show["showEndDate"],
                "state": show["state"],
                "attendance": show["attendance"],
                "capacity": show["capacity"],
                "currencyFormat": show["currencyFormat"]["code"],
                "location": f"{show['location']['city']}, {show['location']['stateProvince']}, {show['location']['country']}"
            })
            print(f"Added show: ({show['uuid']}) to all_shows.csv")
    print("All shows appended to all_shows.csv")
