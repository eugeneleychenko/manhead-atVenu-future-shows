import requests

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
def test_data_retrieval(start_date, end_date):
    headers = {"x-api-key": API_TOKEN}
    variables = {
        "startDate": start_date,
        "endDate": end_date
    }
    response = requests.post(API_ENDPOINT, json={"query": QUERY, "variables": variables}, headers=headers)
    data = response.json()

    if "data" in data and "organization" in data["data"]:
        accounts = data["data"]["organization"]["accounts"]["nodes"]
        if accounts:
            print("Data retrieved successfully!")
            for account in accounts:
                print(f"Account Name: {account['name']}")
                for tour in account["tours"]["nodes"]:
                    print(f"  Tour Name: {tour['name']}")
                    for show in tour["shows"]["nodes"]:
                        print(f"    Show UUID: {show['uuid']}")
                        print(f"    Show Date: {show['showDate']}")
                        print(f"    Show End Date: {show['showEndDate']}")
                        print(f"    Attendance: {show['attendance']}")
                        print(f"    Capacity: {show['capacity']}")
                        print(f"    Location: {show['location']['city']}, {show['location']['stateProvince']}, {show['location']['country']}")
                        for transaction in show["itemizedTransactions"]["nodes"]:
                            print(f"      Transaction Details:")
                            print(f"        Artist Name: {transaction['artistName']}")
                            print(f"        Before Tax Price Amount: {transaction['beforeTaxPriceAmount']}")
                            print(f"        Cardholder Name: {transaction['cardholderName']}")
                            print(f"        Device: {transaction['device']}")
                            print(f"        Discount Name: {transaction['discountName']}")
                            print(f"        Gross Sold Amount: {transaction['grossSoldAmount']}")
                            print(f"        Gross Sold Amount With Modifiers: {transaction['grossSoldAmountWithModifiers']}")
                            print(f"        Item Name: {transaction['itemName']}")
                            print(f"        Itemization Type: {transaction['itemizationType']}")
                            if transaction["modifiers"]:
                                modifier = transaction["modifiers"][0]
                                print(f"        Modifier Details:")
                                print(f"          Discount Amount: {modifier['discountAmount']}")
                                print(f"          Gross Amount: {modifier['grossAmount']}")
                                print(f"          Modifier Name: {modifier['modifierName']}")
                                print(f"          Refunded Discount Amount: {modifier['refundedDiscountAmount']}")
                                print(f"          Refunded Gross Amount: {modifier['refundedGrossAmount']}")
                                print(f"          Refunded Tax One Amount: {modifier['refundedTaxOneAmount']}")
                                print(f"          Refunded Tax Two Amount: {modifier['refundedTaxTwoAmount']}")
                                print(f"          Tax One Amount: {modifier['taxOneAmount']}")
                                print(f"          Tax Two Amount: {modifier['taxTwoAmount']}")
                                print(f"          Unit Price Amount: {modifier['unitPriceAmount']}")
                            print(f"        Net Sold Amount: {transaction['netSoldAmount']}")
                            print(f"        Net Sold Amount With Modifiers: {transaction['netSoldAmountWithModifiers']}")
                            print(f"        Order ID: {transaction['orderId']}")
                            print(f"        Order Type: {transaction['orderType']}")
                            print(f"        Payment Timestamp: {transaction['paymentTimestamp']}")
                            print(f"        Product Type: {transaction['productType']}")
                            print(f"        Refunded Discount Amount: {transaction['refundedDiscountAmount']}")
                            print(f"        Refunded Quantity: {transaction['refundedQuantity']}")
                            print(f"        Refunded Tax One Amount: {transaction['refundedTaxOneAmount']}")
                            print(f"        Refunded Tax Two Amount: {transaction['refundedTaxTwoAmount']}")
                            print(f"        Size: {transaction['size']}")
                            print(f"        Sold Quantity: {transaction['soldQuantity']}")
                            print(f"        Staff Name: {transaction['staffName']}")
                            print(f"        Stand Name: {transaction['standName']}")
                            print(f"        Tax One Amount: {transaction['taxOneAmount']}")
                            print(f"        Tax Two Amount: {transaction['taxTwoAmount']}")
                            print(f"        Tender Type: {transaction['tenderType']}")
                            print(f"        Total Discount Amount: {transaction['totalDiscountAmount']}")
                            print(f"        Total Refunded Amount: {transaction['totalRefundedAmount']}")
                            print(f"        Total Refunded Amount With Modifiers: {transaction['totalRefundedAmountWithModifiers']}")
                            print(f"        Unit Price Amount: {transaction['unitPriceAmount']}")
        else:
            print("No accounts found.")
    else:
        print("Error retrieving data. Please check the API response.")
