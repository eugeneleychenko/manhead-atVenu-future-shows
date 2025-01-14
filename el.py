import requests
import csv
from datetime import datetime

API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"

QUERY = '''
query GetAllShowsAndTransactions($startDate: DateTime!, $endDate: DateTime!) {
  organization {
    accounts(first: 10) {
      nodes {
        name
        tours(first: 10) {
          nodes {
            name
            shows(first: 10, filter: {showDate: {greaterThanOrEqualTo: $startDate, lessThanOrEqualTo: $endDate}}) {
              nodes {
                uuid
                showDate
                showEndDate
                attendance
                capacity
                location {
                  city
                  country
                  stateProvince
                }
                itemizedTransactions(first: 10) {
                  nodes {
                    artistName
                    beforeTaxPriceAmount
                    cardholderName
                    device
                    discountName
                    grossSoldAmount
                    grossSoldAmountWithModifiers
                    itemName
                    itemizationType
                    modifiers {
                      discountAmount
                      grossAmount
                      modifierName
                      refundedDiscountAmount
                      refundedGrossAmount
                      refundedTaxOneAmount
                      refundedTaxTwoAmount
                      taxOneAmount
                      taxTwoAmount
                      unitPriceAmount
                    }
                    netSoldAmount
                    netSoldAmountWithModifiers
                    orderId
                    orderType
                    paymentTimestamp
                    productType
                    refundedDiscountAmount
                    refundedQuantity
                    refundedTaxOneAmount
                    refundedTaxTwoAmount
                    size
                    soldQuantity
                    staffName
                    standName
                    taxOneAmount
                    taxTwoAmount
                    tenderType
                    totalDiscountAmount
                    totalRefundedAmount
                    totalRefundedAmountWithModifiers
                    unitPriceAmount
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
'''

def fetch_data(start_date, end_date):
    print(f"Fetching data for dates between {start_date} and {end_date}")
    headers = {"x-api-key": API_TOKEN}
    variables = {
        "startDate": start_date,
        "endDate": end_date
    }
    response = requests.post(API_ENDPOINT, json={"query": QUERY, "variables": variables}, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        # You can also add checks here for the data content if necessary
        print("Data fetched successfully.")
        fetched_data = data.get("data", {}).get("organization", {}).get("accounts", {}).get("nodes", [])
        print("Fetched data:", fetched_data)
        return fetched_data
    else:
        # Log or handle HTTP errors
        error_message = f"Failed to fetch data: {response.status_code} - {response.text}"
        print(error_message)
        # Depending on your needs, you can raise an exception to halt the script
        # or handle the error gracefully
        raise Exception(error_message)

def flatten_show_data(account):
    print(f"Flattening data for account: {account['name']}")
    shows = []
    for tour in account.get("tours", {}).get("nodes", []):
        for show in tour.get("shows", {}).get("nodes", []):
            for transaction in show.get("itemizedTransactions", {}).get("nodes", []):
                show_data = {
                    "Account Name": account["name"],
                    "Tour Name": tour["name"],
                    "UUID": show["uuid"],
                    "Show Date": show["showDate"],
                    "Show End Date": show["showEndDate"],
                    "Attendance": show["attendance"],
                    "Capacity": show,
                    "City": show["location"]["city"],
                    "Country": show["location"]["country"],
                    "State/Province": show["location"]["stateProvince"],
                    "Artist Name": transaction["artistName"],
                    "Before Tax Price Amount": transaction["beforeTaxPriceAmount"],
                    "Cardholder Name": transaction["cardholderName"],
                    "Device": transaction["device"],
                    "Discount Name": transaction["discountName"],
                    "Gross Sold Amount": transaction["grossSoldAmount"],
                    "Gross Sold Amount With Modifiers": transaction["grossSoldAmountWithModifiers"],
                    "Item Name": transaction["itemName"],
                    "Itemization Type": transaction["itemizationType"],
                    "Discount Amount": transaction["modifiers"][0]["discountAmount"] if transaction["modifiers"] else "",
                    "Gross Amount": transaction["modifiers"][0]["grossAmount"] if transaction["modifiers"] else "",
                    "Modifier Name": transaction["modifiers"][0]["modifierName"] if transaction["modifiers"] else "",
                    "Refunded Discount Amount": transaction["modifiers"][0]["refundedDiscountAmount"] if transaction["modifiers"] else "",
                    "Refunded Gross Amount": transaction["modifiers"][0]["refundedGrossAmount"] if transaction["modifiers"] else "",
                    "Refunded Tax One Amount": transaction["modifiers"][0]["refundedTaxOneAmount"] if transaction["modifiers"] else "",
                    "Refunded Tax Two Amount": transaction["modifiers"][0]["refundedTaxTwoAmount"] if transaction["modifiers"] else "",
                    "Tax One Amount": transaction["modifiers"][0]["taxOneAmount"] if transaction["modifiers"] else "",
                    "Tax Two Amount": transaction["modifiers"][0]["taxTwoAmount"] if transaction["modifiers"] else "",
                    "Unit Price Amount": transaction["modifiers"][0]["unitPriceAmount"] if transaction["modifiers"] else "",
                    "Net Sold Amount": transaction["netSoldAmount"],
                    "Net Sold Amount With Modifiers": transaction["netSoldAmountWithModifiers"],
                    "Order ID": transaction["orderId"],
                    "Order Type": transaction["orderType"],
                    "Payment Timestamp": transaction["paymentTimestamp"],
                    "Product Type": transaction["productType"],
                    "Refunded Discount Amount": transaction["refundedDiscountAmount"],
                    "Refunded Quantity": transaction["refundedQuantity"],
                    "Refunded Tax One Amount": transaction["refundedTaxOneAmount"],
                    "Refunded Tax Two Amount": transaction["refundedTaxTwoAmount"],
                    "Size": transaction["size"],
                    "Sold Quantity": transaction["soldQuantity"],
                    "Staff Name": transaction["staffName"],
                    "Stand Name": transaction["standName"],
                    "Tax One Amount": transaction["taxOneAmount"],
                    "Tax Two Amount": transaction["taxTwoAmount"],
                    "Tender Type": transaction["tenderType"],
                    "Total Discount Amount": transaction["totalDiscountAmount"],
                    "Total Refunded Amount": transaction["totalRefundedAmount"],
                    "Total Refunded Amount With Modifiers": transaction["totalRefundedAmountWithModifiers"],
                    "Unit Price Amount": transaction["unitPriceAmount"]
                }
                shows.append(show_data)
    print(f"Data flattened for account: {account['name']}")
    return shows

def export_to_csv(data):
    if not data:
        print("No data to export.")
        return
    print(f"Exporting {len(data)} records to CSV")
    fields = data[0].keys()
    with open("shows_data_jan_2022.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)
    print("Data exported to shows_data_jan_2022.csv")

if __name__ == "__main__":
    start_date = "2023-01-01T00:00:00Z"
    end_date = "2023-12-31T23:59:59Z"
    print("Starting data fetch process...")
    accounts = fetch_data(start_date, end_date)
    flattened_data = []
    for account in accounts:
        flattened_data.extend(flatten_show_data(account))
    export_to_csv(flattened_data)
    print("Data processing complete.")