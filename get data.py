import requests
import csv
from datetime import datetime

API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"

QUERY = '''
query GetAllShowsAndTransactions {
  organization {
    accounts(first: 10) {
      nodes {
        name
        tours(first: 10) {
          nodes {
            name
            shows(first: 10) {
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

def fetch_data():
    headers = {"x-api-key": API_TOKEN}
    response = requests.post(API_ENDPOINT, json={"query": QUERY}, headers=headers)
    data = response.json()
    return data.get("data", {}).get("organization", {}).get("accounts", {}).get("nodes", [])

def flatten_show_data(account):
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
                    "Capacity": show["capacity"],
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
    return shows

def export_to_csv(data):
    if not data:
        print("No data to export.")
        return
    fields = data[0].keys()
    with open("shows_data.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)
    print("Data exported to shows_data.csv")

if __name__ == "__main__":
    accounts = fetch_data()
    flattened_data = []
    for account in accounts:
        flattened_data.extend(flatten_show_data(account))
    export_to_csv(flattened_data)
