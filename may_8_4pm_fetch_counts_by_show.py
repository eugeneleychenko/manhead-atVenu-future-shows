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
        variables["transactionsCursor"] = cursor
        response = requests.post(endpoint, json={"query": query, "variables": variables}, headers=headers)
        
        # print("api response",response.text)
         
        if response.status_code == 200:
            json_data = response.json()
            if "data" in json_data:
                data = json_data["data"]
                for path in data_path.split("."):
                    data = data[path]

                all_data.extend(data["transactionNodes"])
                has_next_page = data["pageInfo"]["hasNextPage"]
                cursor = data["pageInfo"]["endCursor"]
            else:
                print(f"Error: Unexpected response format. 'data' key not found in the response.")
                break
        else:
            print(f"Error: Request failed with status code {response.status_code}")
            break

    return all_data

# Read show UUIDs from 'all_shows.csv' and prepare CSV file called 'all_transactions.csv' to store all transactions
with open("all_shows.csv", mode="r") as showsfile, open("all_transactions.csv", "w", newline="") as transactionsfile:
    shows_reader = csv.DictReader(showsfile)
    fieldnames = ["artistName", "beforeTaxPriceAmount", "cardholderName", "device", "discountName", "grossSoldAmount",
                  "grossSoldAmountWithModifiers", "itemName", "itemizationType", "modifiers_discountAmount",
                  "modifiers_grossAmount", "modifiers_modifierName", "modifiers_refundedDiscountAmount",
                  "modifiers_refundedGrossAmount", "modifiers_refundedTaxOneAmount", "modifiers_refundedTaxTwoAmount",
                  "modifiers_taxOneAmount", "modifiers_taxTwoAmount", "modifiers_unitPriceAmount", "netSoldAmount",
                  "netSoldAmountWithModifiers", "orderId", "orderType", "paymentTimestamp", "refundedDiscountAmount",
                  "refundedQuantity", "refundedTaxOneAmount", "refundedTaxTwoAmount", "size", "soldQuantity", "staffName",
                  "standName", "taxOneAmount", "taxTwoAmount", "tenderType", "totalDiscountAmount", "totalRefundedAmount",
                  "totalRefundedAmountWithModifiers", "unitPriceAmount"]
    writer = csv.DictWriter(transactionsfile, fieldnames=fieldnames)
    writer.writeheader()
    
    transactions_query = """
        query getTransactionsForShow($pageSize: Int, $showUuid: UUID!, $transactionsCursor: String) {
            showNode: node(uuid: $showUuid) {
                ... on Show {
                    itemizedTransactions(first: $pageSize, after: $transactionsCursor) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        transactionNodes: nodes {
                            artistName
                            beforeTaxPriceAmount: unitPriceAmount
                            cardholderName
                            device
                            discountName: totalDiscountAmount
                            grossSoldAmount: netSoldAmount
                            grossSoldAmountWithModifiers: netSoldAmount
                            itemName
                            itemizationType: orderType
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
                            netSoldAmountWithModifiers: netSoldAmount
                            orderId
                            orderType
                            paymentTimestamp
                            refundedDiscountAmount: totalRefundedAmount
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
                            totalRefundedAmountWithModifiers: totalRefundedAmount
                            unitPriceAmount
                        }
                    }
                }
            }
        }
    """
    
    for show in shows_reader:
        show_uuid = show["show_uuid"]
        transactions = fetch_data(transactions_query, {"pageSize": 200, "showUuid": show_uuid}, "showNode.itemizedTransactions")
        
        for transaction in transactions:
            transaction_data = {
                "artistName": transaction.get("artistName", ""),
                "beforeTaxPriceAmount": transaction.get("beforeTaxPriceAmount", ""),
                "cardholderName": transaction.get("cardholderName", ""),
                "device": transaction.get("device", ""),
                "discountName": transaction.get("discountName", ""),
                "grossSoldAmount": transaction.get("grossSoldAmount", ""),
                "grossSoldAmountWithModifiers": transaction.get("grossSoldAmountWithModifiers", ""),
                "itemName": transaction.get("itemName", ""),
                "itemizationType": transaction.get("itemizationType", ""),
                "netSoldAmount": transaction.get("netSoldAmount", ""),
                "netSoldAmountWithModifiers": transaction.get("netSoldAmountWithModifiers", ""),
                "orderId": transaction.get("orderId", ""),
                "orderType": transaction.get("orderType", ""),
                "paymentTimestamp": transaction.get("paymentTimestamp", ""),
                "refundedDiscountAmount": transaction.get("refundedDiscountAmount", ""),
                "refundedQuantity": transaction.get("refundedQuantity", ""),
                "refundedTaxOneAmount": transaction.get("refundedTaxOneAmount", ""),
                "refundedTaxTwoAmount": transaction.get("refundedTaxTwoAmount", ""),
                "size": transaction.get("size", ""),
                "soldQuantity": transaction.get("soldQuantity", ""),
                "staffName": transaction.get("staffName", ""),
                "standName": transaction.get("standName", ""),
                "taxOneAmount": transaction.get("taxOneAmount", ""),
                "taxTwoAmount": transaction.get("taxTwoAmount", ""),
                "tenderType": transaction.get("tenderType", ""),
                "totalDiscountAmount": transaction.get("totalDiscountAmount", ""),
                "totalRefundedAmount": transaction.get("totalRefundedAmount", ""),
                "totalRefundedAmountWithModifiers": transaction.get("totalRefundedAmountWithModifiers", ""),
                "unitPriceAmount": transaction.get("unitPriceAmount", "")
            }
            
            modifiers = transaction.get("modifiers", [])
            if modifiers:
                modifier = modifiers[0]  # Assuming only one modifier per transaction
                transaction_data.update({
                    "modifiers_discountAmount": modifier.get("discountAmount", ""),
                    "modifiers_grossAmount": modifier.get("grossAmount", ""),
                    "modifiers_modifierName": modifier.get("modifierName", ""),
                    "modifiers_refundedDiscountAmount": modifier.get("refundedDiscountAmount", ""),
                    "modifiers_refundedGrossAmount": modifier.get("refundedGrossAmount", ""),
                    "modifiers_refundedTaxOneAmount": modifier.get("refundedTaxOneAmount", ""),
                    "modifiers_refundedTaxTwoAmount": modifier.get("refundedTaxTwoAmount", ""),
                    "modifiers_taxOneAmount": modifier.get("taxOneAmount", ""),
                    "modifiers_taxTwoAmount": modifier.get("taxTwoAmount", ""),
                    "modifiers_unitPriceAmount": modifier.get("unitPriceAmount", "")
                })
            
            writer.writerow(transaction_data)
            print(f"Fetched transaction: {transaction_data}")
        
        print(f"Fetched transactions for show: {show_uuid}")
        
print("All transactions written to all_transactions.csv")