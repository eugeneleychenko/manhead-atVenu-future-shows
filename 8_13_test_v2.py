import requests
from datetime import datetime
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AtVenuDataFetcher:
    def __init__(self, api_key):
        self.endpoint = "https://api.atvenu.com/"
        self.headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }

    def execute_query(self, query, variables=None):
        logging.info(f"Executing query with variables: {variables}")
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers
        )
        if response.status_code == 200:
            result = response.json()
            if 'errors' in result:
                logging.error(f"GraphQL query returned errors: {result['errors']}")
                raise Exception(f"GraphQL query failed: {result['errors']}")
            return result
        else:
            logging.error(f"Query failed with status code: {response.status_code}. Response: {response.text}")
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
                uuid
                artistName: name
              }
            }
          }
        }
        """
        accounts = []
        cursor = None
        while True:
            result = self.execute_query(query, {"cursor": cursor})
            accounts_data = result['data']['organization']['accounts']
            accounts.extend(accounts_data['nodes'])
            if not accounts_data['pageInfo']['hasNextPage']:
                break
            cursor = accounts_data['pageInfo']['endCursor']
        return accounts

    def fetch_tours(self, account_uuid):
        query = """
        query tours($accountUuid: UUID!, $cursor: String) {
          account: node(uuid: $accountUuid) {
            ... on Account {
              tours(first: 200, after: $cursor) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                nodes {
                  uuid
                  tourName: name
                }
              }
            }
          }
        }
        """
        tours = []
        cursor = None
        while True:
            result = self.execute_query(query, {"accountUuid": account_uuid, "cursor": cursor})
            tours_data = result['data']['account']['tours']
            tours.extend(tours_data['nodes'])
            if not tours_data['pageInfo']['hasNextPage']:
                break
            cursor = tours_data['pageInfo']['endCursor']
        return tours

    def fetch_shows(self, tour_uuid, start_date, end_date):
        query = """
        query shows($tourUuid: UUID!, $startDate: Date!, $endDate: Date!, $cursor: String) {
          tour: node(uuid: $tourUuid) {
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
        shows = []
        cursor = None
        while True:
            result = self.execute_query(query, {
                "tourUuid": tour_uuid,
                "startDate": start_date,
                "endDate": end_date,
                "cursor": cursor
            })
            shows_data = result['data']['tour']['shows']
            shows.extend(shows_data['nodes'])
            if not shows_data['pageInfo']['hasNextPage']:
                break
            cursor = shows_data['pageInfo']['endCursor']
        return shows

    def fetch_shows_in_date_range(self, start_date, end_date):
        logging.info(f"Fetching shows between {start_date} and {end_date}")
        all_shows = []
        
        accounts = self.fetch_accounts()
        for account in accounts:
            logging.info(f"Fetching tours for account: {account['artistName']}")
            tours = self.fetch_tours(account['uuid'])
            for tour in tours:
                logging.info(f"Fetching shows for tour: {tour['tourName']}")
                shows = self.fetch_shows(tour['uuid'], start_date, end_date)
                for show in shows:
                    show['account'] = account
                    show['tour'] = tour
                    all_shows.append(show)

        logging.info(f"Fetched {len(all_shows)} shows in total")
        return all_shows


    def fetch_merchandise(self, account_uuid):
        logging.info(f"Fetching merchandise for account {account_uuid}")
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
        logging.info(f"Fetched {len(merch_items)} merchandise items for account {account_uuid}")
        return merch_items

    def fetch_counts(self, show_uuid):
        logging.info(f"Fetching counts for show {show_uuid}")
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
        logging.info(f"Fetched {len(counts)} counts for show {show_uuid}")
        return counts

    def calculate_sold(self, count):
        logging.debug(f"Calculating sold for count: {count}")
        count_in = count.get('countIn', 0) or 0
        count_out = count.get('countOut', 0) or 0
        comps = count.get('comps', 0) or 0
        adds = sum((add.get('quantity', 0) or 0) for add in count.get('merchAdds', []))
        sold = count_in + adds - count_out - comps
        logging.debug(f"Calculated sold: {sold}")
        return sold

    def fetch_all_data(self, start_date, end_date):
        logging.info(f"Fetching all data between {start_date} and {end_date}")
        all_data = []
        shows = self.fetch_shows_in_date_range(start_date, end_date)
        
        # Create a dictionary to cache merchandise data for each account
        merchandise_cache = {}
        
        for show in shows:
            logging.info(f"Processing show: {show['uuid']} on {show['showDate']}")
            
            account_uuid = show['account']['uuid']
            if account_uuid not in merchandise_cache:
                merchandise_cache[account_uuid] = self.fetch_merchandise(account_uuid)
            
            merchandise = merchandise_cache[account_uuid]
            counts = self.fetch_counts(show['uuid'])
            
            for count in counts:
                merch_item = next((item for item in merchandise for variant in item['merchVariants'] if variant['uuid'] == count['merchVariantUuid']), None)
                if merch_item:
                    variant = next((v for v in merch_item['merchVariants'] if v['uuid'] == count['merchVariantUuid']), None)
                    
                    sold = self.calculate_sold(count)
                    
                    all_data.append({
                        'Band': show['account']['artistName'],
                        'Tour Name': show['tour']['tourName'],
                        'Venue': f"{show['location']['city']}, {show['location']['stateProvince']}, {show['location']['country']}",
                        'Product Name': merch_item['name'],
                        'Size': variant['size'] if variant else 'N/A',
                        'SKU': variant['sku'] if variant else 'N/A',
                        'In': count.get('countIn', 0) or 0,
                        'Out': count.get('countOut', 0) or 0,
                        'Sold': sold,
                        'Show Date': show['showDate'],
                        'Price': count['priceOverride'] or (variant['price'] if variant else 'N/A')
                    })
        
        logging.info(f"Fetched {len(all_data)} total data points")
        return all_data

# Usage
if __name__ == "__main__":
    api_key = "live_yvYLBo32dRE9z_yCdhwU"
    fetcher = AtVenuDataFetcher(api_key)
    
    start_date = "2024-08-10"
    end_date = "2024-08-10"
    
    try:
        data = fetcher.fetch_all_data(start_date, end_date)
        
        # Save the data to a JSON file
        with open('atvenu_data.json', 'w') as f:
            json.dump(data, f, indent=2)
        
        logging.info("Data fetched and saved to atvenu_data.json")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")