import requests
import json

API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"
QUERY = '''
query GetAccounts($first: Int!, $after: String) {
  organization {
    accounts(first: $first, after: $after) {
      pageInfo {
        endCursor
        hasNextPage
      }
      nodes {
        name
        tours(first: 10) {  # Adjust the number of tours as needed
          nodes {
            name
            shows(first: 10) {  # Adjust the number of shows as needed
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
'''

def fetch_data(first, after=None):
    headers = {"x-api-key": API_TOKEN}
    variables = {"first": first, "after": after}
    response = requests.post(API_ENDPOINT, json={"query": QUERY, "variables": variables}, headers=headers)
    return response.json()

def download_all_accounts():
    accounts = []
    after = None
    has_next_page = True

    while has_next_page:
        data = fetch_data(10, after)  # Adjust the 'first' parameter as needed
        if 'errors' in data:
            print("Error fetching data:", data['errors'])
            break

        fetched_accounts = data['data']['organization']['accounts']['nodes']
        accounts.extend(fetched_accounts)

        page_info = data['data']['organization']['accounts']['pageInfo']
        has_next_page = page_info['hasNextPage']
        after = page_info['endCursor']

    return accounts

def main():
    accounts = download_all_accounts()
    # Here you can filter the shows as needed, similar to the JavaScript example
    print(json.dumps(accounts, indent=2))

if __name__ == "__main__":
    main()