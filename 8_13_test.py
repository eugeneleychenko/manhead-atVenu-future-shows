import requests
from datetime import datetime
import json

class AtVenuDataFetcher:
    def __init__(self, api_key):
        self.endpoint = "https://api.atvenu.com/"
        self.headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }

    def execute_query(self, query, variables=None):
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed with status code: {response.status_code}. Response: {response.text}")

    def fetch_accounts(self):
        query = """
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
        accounts = []
        cursor = None
        while True:
            result = self.execute_query(query, {"cursor": cursor})
            accounts.extend(result['data']['organization']['accounts']['nodes'])
            if not result['data']['organization']['accounts']['pageInfo']['hasNextPage']:
                break
            cursor = result['data']['organization']['accounts']['pageInfo']['endCursor']
        return accounts

    def fetch_merchandise(self, account_uuid):
        query = """
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
        merch_items = []
        cursor = None
        while True:
            result = self.execute_query(query, {"uuid": account_uuid, "cursor": cursor})
            merch_items.extend(result['data']['account']['merchItems']['nodes'])
            if not result['data']['account']['merchItems']['pageInfo']['hasNextPage']:
                break
            cursor = result['data']['account']['merchItems']['pageInfo']['endCursor']
        return merch_items

    def fetch_tours(self, account_uuid):
        query = """
        query tours($uuid: UUID!) {
          account: node(uuid: $uuid) {
            ... on Account {
              tours(first:200, open:false) {
                pageInfo{hasNextPage}
                nodes{
                  tourName: name
                  uuid
                }
              }
            }
          }
        }
        """
        result = self.execute_query(query, {"uuid": account_uuid})
        tours = result['data']['account']['tours']['nodes']
        if result['data']['account']['tours']['pageInfo']['hasNextPage']:
            raise Exception("More than 200 tours found. Script needs to be updated to handle pagination for tours.")
        return tours

    def fetch_shows(self, tour_uuid, start_date, end_date):
        query = """
        query shows($uuid: UUID!, $cursor: String, $startDate: Date!, $endDate: Date!) {
          tour: node(uuid: $uuid) {
            ... on Tour {
              shows(first: 200, after: $cursor, showsOverlap:{start: $startDate, end: $endDate}) {
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
        shows = []
        cursor = None
        while True:
            result = self.execute_query(query, {
                "uuid": tour_uuid,
                "cursor": cursor,
                "startDate": start_date,
                "endDate": end_date
            })
            shows.extend(result['data']['tour']['shows']['nodes'])
            if not result['data']['tour']['shows']['pageInfo']['hasNextPage']:
                break
            cursor = result['data']['tour']['shows']['pageInfo']['endCursor']
        return shows

    def fetch_counts(self, show_uuid):
        query = """
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
        """
        counts = []
        cursor = None
        while True:
            result = self.execute_query(query, {"uuid": show_uuid, "cursor": cursor})
            counts.extend(result['data']['show']['settlements'][0]['mainCounts']['nodes'])
            if not result['data']['show']['settlements'][0]['mainCounts']['pageInfo']['hasNextPage']:
                break
            cursor = result['data']['show']['settlements'][0]['mainCounts']['pageInfo']['endCursor']
        return counts

    def safe_int(self, value):
        """Convert value to int, return 0 if value is None."""
        return int(value) if value is not None else 0

    def fetch_all_data(self, start_date, end_date):
        all_data = []
        accounts = self.fetch_accounts()
        
        for account in accounts:
            merchandise = self.fetch_merchandise(account['uuid'])
            tours = self.fetch_tours(account['uuid'])
            
            for tour in tours:
                shows = self.fetch_shows(tour['uuid'], start_date, end_date)
                
                for show in shows:
                    counts = self.fetch_counts(show['uuid'])
                    
                    for count in counts:
                        merch_item = next((item for item in merchandise for variant in item['merchVariants'] if variant['uuid'] == count['merchVariantUuid']), None)
                        if merch_item:
                            variant = next((v for v in merch_item['merchVariants'] if v['uuid'] == count['merchVariantUuid']), None)
                            
                            count_in = self.safe_int(count['countIn'])
                            count_out = self.safe_int(count['countOut'])
                            comps = self.safe_int(count['comps'])
                            adds = sum(self.safe_int(add['quantity']) for add in count['merchAdds'])
                            
                            sold = count_in + adds - count_out - comps
                            
                            all_data.append({
                                'Band': account['artistName'],
                                'Tour Name': tour['tourName'],  # Added tour name here
                                'Venue': f"{show['location']['city']}, {show['location']['stateProvince']}, {show['location']['country']}",
                                'Product Name': merch_item['name'],
                                'Size': variant['size'] if variant else 'N/A',
                                'SKU': variant['sku'] if variant else 'N/A',
                                'In': count_in,
                                'Out': count_out,
                                'Sold': sold,
                                'Show Date': show['showDate'],
                                'Price': count['priceOverride'] or variant['price'] if variant else 'N/A'
                            })
        
        return all_data

# Usage
if __name__ == "__main__":
    api_key = "live_yvYLBo32dRE9z_yCdhwU"
    fetcher = AtVenuDataFetcher(api_key)
    
    start_date = "2024-08-10"
    end_date = "2024-08-10"
    
    data = fetcher.fetch_all_data(start_date, end_date)
    
    # Save the data to a JSON file
    with open('atvenu_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data fetched and saved to atvenu_data.json")