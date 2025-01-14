import requests
import json
from datetime import date

def get_paginated_data(api_key, query, variables):
    url = "https://api.atvenu.com/"
    headers = {
        "x-api-key": api_key
    }
    
    all_nodes = []
    has_next_page = True
    
    while has_next_page:
        payload = {
            "query": query,
            "variables": variables
        }
        
        response = requests.post(url, headers=headers, json=payload)
        
        print(f"Query Response for {variables}:")
        print(json.dumps(response.json(), indent=2))
        
        if response.status_code != 200 or 'errors' in response.json():
            raise Exception(f"Query failed: {response.text}")
        
        data = response.json().get('data', {})
        
        # Navigate to the nodes based on the query structure
        current_data = data
        for key in ['organization', 'accounts', 'nodes']:
            current_data = current_data.get(key, [])
            if not current_data:
                break

        if current_data and isinstance(current_data, list) and len(current_data) > 0:
            tours = current_data[0].get('tours', {}).get('nodes', [])
            if tours and len(tours) > 0:
                shows = tours[0].get('shows', {})
                nodes = shows.get('nodes', [])
                all_nodes.extend(nodes)
                
                page_info = shows.get('pageInfo', {})
                has_next_page = page_info.get('hasNextPage', False)
                end_cursor = page_info.get('endCursor')
                
                if has_next_page:
                    variables['after'] = end_cursor
            else:
                has_next_page = False
        else:
            has_next_page = False
    
    return all_nodes

def get_shows_data(api_key, query_date):
    shows_query = """
    query getShowsOnDate($date: Date!, $first: Int!, $after: String) {
      organization {
        accounts(first: 1) {
          nodes {
            tours(first: 1) {
              nodes {
                name
                shows(first: $first, after: $after, showsOverlap: { start: $date, end: $date }) {
                  nodes {
                    uuid
                    location {
                      name
                    }
                  }
                  pageInfo {
                    hasNextPage
                    endCursor
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    
    shows_variables = {
        "date": query_date.isoformat(),
        "first": 10,  # Adjust this number as needed
        "after": None
    }
    
    shows_data = get_paginated_data(api_key, shows_query, shows_variables)
    
    if not shows_data:
        print(f"No shows found for the date {query_date}")
        return []

    # Now, for each show, get the settlement data
    settlement_query = """
    query getSettlementData($showUuid: UUID!, $firstForItems: Int!, $firstForCounts: Int!) {
      node(uuid: $showUuid) {
        ... on Show {
          settlements {
            merchItems(first: $firstForItems) {
              nodes {
                name
                merchVariants {
                  uuid
                  size
                  sku
                }
              }
            }
            mainCounts(first: $firstForCounts) {
              nodes {
                merchVariantUuid
                countIn
                countOut
              }
            }
          }
        }
      }
    }
    """
    
    all_data = []
    
    for show in shows_data:
        settlement_variables = {
            "showUuid": show['uuid'],
            "firstForItems": 100,
            "firstForCounts": 100
        }
        
        settlement_payload = {
            "query": settlement_query,
            "variables": settlement_variables
        }
        
        url = "https://api.atvenu.com/"
        headers = {
            "x-api-key": api_key
        }
        
        settlement_response = requests.post(url, headers=headers, json=settlement_payload)
        
        print(f"Settlement Query Response for show {show['uuid']}:")
        print(json.dumps(settlement_response.json(), indent=2))
        
        if settlement_response.status_code != 200 or 'errors' in settlement_response.json():
            print(f"Settlement query failed for show {show['uuid']}: {settlement_response.text}")
            continue
        
        settlement_data = settlement_response.json()
        
        if 'data' not in settlement_data or 'node' not in settlement_data['data']:
            print(f"No settlement data for show {show['uuid']}: {settlement_data}")
            continue
        
        all_data.append({
            "tour_name": "Unknown",  # We don't have tour name in this query
            "show_uuid": show['uuid'],
            "venue_name": show['location']['name'],
            "settlement_data": settlement_data['data']['node']['settlements'] if settlement_data['data']['node'] else []
        })
    
    return all_data

def process_response(all_data):
    formatted_data = []
    
    for show_data in all_data:
        tour_name = show_data['tour_name']
        venue_name = show_data['venue_name']
        
        for settlement in show_data['settlement_data']:
            variants = {}
            for item in settlement['merchItems']['nodes']:
                for variant in item['merchVariants']:
                    variants[variant['uuid']] = {
                        'name': item['name'],
                        'size': variant.get('size', 'N/A'),
                        'sku': variant.get('sku', 'N/A')
                    }
            
            for count in settlement['mainCounts']['nodes']:
                variant_uuid = count['merchVariantUuid']
                if variant_uuid in variants:
                    variant_info = variants[variant_uuid]
                    sold = count['countIn'] - count['countOut']
                    
                    formatted_data.append({
                        'Band': tour_name,
                        'Venue': venue_name,
                        'Product Name': variant_info['name'],
                        'Size': variant_info['size'],
                        'SKU': variant_info['sku'],
                        'In': count['countIn'],
                        'Out': count['countOut'],
                        'Sold': sold
                    })
    
    return formatted_data

def main():
    api_key = "live_yvYLBo32dRE9z_yCdhwU"
    query_date = date(2024, 8, 10)  # August 11, 2024
    
    try:
        # Fetch data from API
        api_response = get_shows_data(api_key, query_date)
        
        # Process the response
        formatted_results = process_response(api_response)
        
        if not formatted_results:
            print("No data found for the specified date.")
            return
        
        # Print the formatted results
        for item in formatted_results:
            print(f"{item['Band']} | {item['Venue']} | {item['Product Name']} | {item['Size']} | {item['SKU']} | {item['In']} | {item['Out']} | {item['Sold']}")
        
        # Optionally, save the results to a CSV file
        import csv
        with open('show_data.csv', 'w', newline='') as csvfile:
            fieldnames = ['Band', 'Venue', 'Product Name', 'Size', 'SKU', 'In', 'Out', 'Sold']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in formatted_results:
                writer.writerow(item)
        print("\nResults have been saved to 'show_data.csv'")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("Full traceback:")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()