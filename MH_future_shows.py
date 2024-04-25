import streamlit as st
from datetime import datetime
import pandas as pd
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from datetime import datetime

API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"

ACCOUNTS_QUERY = gql("""
    query GetAccounts($first: Int!, $after: String, $toursFirst: Int!, $showsFirst: Int!) {
        organization {
            accounts(first: $first, after: $after) {
                pageInfo {
                    endCursor
                    hasNextPage
                }
                nodes {
                    name
                    tours(first: $toursFirst) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        nodes {
                            name
                            shows(first: $showsFirst) {
                                pageInfo {
                                    endCursor
                                    hasNextPage
                                }
                                nodes {
                                    showDate
                                    attendance
                                    capacity
                                    location {
                                        city
                                        phone
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
""")

@st.cache_data
def fetch_accounts():
    accounts = []
    after = None
    has_next_page = True
    tours_first = 35  # Adjust based on your needs or limits
    shows_first = 35  # Adjust based on your needs or limits

    transport = RequestsHTTPTransport(
        url=API_ENDPOINT,
        headers={"x-api-key": API_TOKEN}
    )
    client = Client(transport=transport)

    while has_next_page:
        variables = {"first": 20, "after": after, "toursFirst": tours_first, "showsFirst": shows_first}
        result = client.execute(ACCOUNTS_QUERY, variable_values=variables)
        fetched_accounts = result["organization"]["accounts"]["nodes"]
        # Here you would need to add logic to handle pagination for tours and shows within each account
        accounts.extend(fetched_accounts)
        page_info = result["organization"]["accounts"]["pageInfo"]
        has_next_page = page_info["hasNextPage"]
        after = page_info["endCursor"]

    return accounts

def filter_shows_by_date(accounts, start_date, end_date):
    filtered_shows = []

    for account in accounts:
        for tour in account["tours"]["nodes"]:
            for show in tour["shows"]["nodes"]:
                show_date = datetime.strptime(show["showDate"], "%Y-%m-%d").date()
                if start_date <= show_date <= end_date:
                    filtered_shows.append({
                        "bandName": account["name"],
                        "showDate": show["showDate"],
                        "capacity": show["capacity"],
                        "attendance": show["attendance"],
                        "city": show["location"]["city"],
                        "phone": show["location"]["phone"]
                    })

    return filtered_shows

def main():
    st.title("Upcoming Manhead Artists' Concerts via AtVenu")

    st.sidebar.markdown("### Choose the range of the concerts you would like to pull", unsafe_allow_html=True)
    start_date = st.sidebar.date_input("Start Date")
    end_date = st.sidebar.date_input("End Date")

    if st.sidebar.button("Run"):
        accounts = fetch_accounts()
        filtered_shows = filter_shows_by_date(accounts, start_date, end_date)

        if not filtered_shows:
            st.info("No shows found for the selected date range.")
        df = pd.DataFrame(filtered_shows)
        st.dataframe(df)

        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="filtered_shows.csv",
            mime="text/csv"
        )
 

if __name__ == "__main__":
    main()
    