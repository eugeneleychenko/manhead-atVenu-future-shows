from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import json

# Constants from concert_system.py
API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"

# GraphQL query for merch variants
MERCH_QUERY = gql("""
    query GetMerchItems($firstForAccounts: Int!, $afterForAccounts: String) {
        organization {
            accounts(first: $firstForAccounts, after: $afterForAccounts) {
                nodes {
                    name
                    merchItems(first: 100) {
                        nodes {
                            name
                            category
                            imageUrl
                            uuid
                            upcOne
                            upcTwo
                        }
                    }
                }
            }
        }
    }
""")

def fetch_merch_variants():
    """
    Fetch merchandise variant data from AtVenu API
    """
    transport = RequestsHTTPTransport(
        url=API_ENDPOINT,
        headers={"x-api-key": API_TOKEN}
    )
    
    client = Client(transport=transport)
    
    variables = {
        "firstForAccounts": 5,  # Limit to first 5 accounts for sample
        "afterForAccounts": None
    }
    
    result = client.execute(MERCH_QUERY, variable_values=variables)
    
    # Pretty print results
    print(json.dumps(result, indent=2))
    
    return result

if __name__ == "__main__":
    print("Fetching merchandise variants sample...")
    fetch_merch_variants()
