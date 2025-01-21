import streamlit as st
from datetime import datetime
import pandas as pd
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import os
from anthropic import Anthropic
from dotenv import load_dotenv
import re
import logging

API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"

SHOWS_QUERY = gql("""
    query GetShows($firstForAccounts: Int!, $afterForAccounts: String, $firstForTours: Int!, $firstForShows: Int!, $range: DateRange) {
        organization {
            accounts(first: $firstForAccounts, after: $afterForAccounts) {
                pageInfo {
                    endCursor
                    hasNextPage
                }
                nodes {
                    name
                    tours(first: $firstForTours, open: true) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        nodes {
                            shows(first: $firstForShows, showsOverlap: $range) {
                                pageInfo {
                                    endCursor
                                    hasNextPage
                                }
                                nodes {
                                    uuid
                                    showDate
                                    location {
                                        name
                                        city
                                        country
                                        stateProvince
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

# Load environment variables
load_dotenv()
anthropic = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def is_valid_state_code(state):
    """Check if state is a valid 2-letter code"""
    # List of valid US state codes
    US_STATES = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }
    return bool(state and isinstance(state, str) and state.upper() in US_STATES)
def get_state_from_claude(city, venue, current_state):
    """Use Claude to determine state based on city, venue, and current state info"""
    logger.info(f"Processing state for: City={city}, Venue={venue}, Current State={current_state}")
    
    if not city:
        logger.warning("Skipping state processing - no city provided")
        return None
        
    prompt = f"""Given a venue in the United States:
City: {city}
Venue: {venue}
Current State Info: {current_state}
Please respond with ONLY the 2-letter state abbreviation (e.g., MA for Massachusetts, CA for California).
If you cannot determine the state with high confidence, respond with 'UNK'."""
    try:
        message = anthropic.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=10,
            temperature=0,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Access the content correctly from the message
        if hasattr(message, 'content') and isinstance(message.content, list):
            logger.info(f"Got list response from Claude: {message.content}")
            if len(message.content) > 0 and hasattr(message.content[0], 'text'):
                content = message.content[0].text
            else:
                logger.error("No text content found in Claude response")
                return None
        else:
            logger.error(f"Unexpected message structure: {message}")
            return None
            
        state_code = content.strip().upper()
        logger.info(f"Processed state code from Claude: {state_code}")
        
        if is_valid_state_code(state_code):
            logger.info(f"Valid state code found: {state_code}")
            return state_code
        else:
            logger.warning(f"Invalid state code returned: {state_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error calling Claude API: {str(e)}")
        return None
@st.cache_data
def fetch_shows(start_date, end_date):
    shows = []
    after_accounts = None
    has_next_page_accounts = True
    first_for_accounts = 20
    first_for_tours = 50
    first_for_shows = 50

    transport = RequestsHTTPTransport(
        url=API_ENDPOINT,
        headers={"x-api-key": API_TOKEN}
    )
    client = Client(transport=transport)

    while has_next_page_accounts:
        variables = {
            "firstForAccounts": first_for_accounts,
            "afterForAccounts": after_accounts,
            "firstForTours": first_for_tours,
            "firstForShows": first_for_shows,
            "range": {"start": start_date.strftime("%Y-%m-%d"), "end": end_date.strftime("%Y-%m-%d")}
        }
        result = client.execute(SHOWS_QUERY, variable_values=variables)
        fetched_accounts = result["organization"]["accounts"]["nodes"]
        for account in fetched_accounts:
            for tour in account["tours"]["nodes"]:
                for show in tour["shows"]["nodes"]:
                    shows.append({
                        "date": show["showDate"],
                        "band": account["name"],
                        "city": show["location"]["city"],
                        "venue": show["location"]["name"],
                        "country": show["location"]["country"],
                        "state": show["location"]["stateProvince"]
                    })
        page_info_accounts = result["organization"]["accounts"]["pageInfo"]
        has_next_page_accounts = page_info_accounts["hasNextPage"]
        after_accounts = page_info_accounts["endCursor"]

    # Process states after fetching all shows
    processed_shows = []
    for show in shows:
        if show['country'] == 'United States':
            state = show['state']
            logger.info(f"\nProcessing show: {show['city']} - {show['venue']}")
            logger.info(f"Original state value: {state}")
            
            # Check if state needs processing
            if not is_valid_state_code(state):
                logger.info(f"State '{state}' needs processing")
                new_state = get_state_from_claude(
                    show['city'],
                    show['venue'],
                    state
                )
                if new_state:
                    show['state'] = new_state
                    logger.info(f"Updated state to: {new_state}")
                else:
                    logger.warning(f"Could not determine state for {show['city']}, {show['venue']}")
            else:
                logger.info(f"State '{state}' is already valid")
                
        processed_shows.append(show)
    
    return processed_shows

def main():
    st.title("Upcoming Manhead Artists' Concerts via AtVenu")
    
    # Add logging info to Streamlit
    if st.sidebar.checkbox("Show Debug Logs"):
        log_placeholder = st.empty()
        
    # Add info about state processing
    st.sidebar.markdown("### Note")
    st.sidebar.markdown("State abbreviations are automatically standardized for US venues.")

    st.sidebar.markdown("### Choose the range of the concerts you would like to pull", unsafe_allow_html=True)
    start_date = st.sidebar.date_input("Start Date")
    end_date = st.sidebar.date_input("End Date")

    if st.sidebar.button("Run"):
        shows = fetch_shows(start_date, end_date)

        if not shows:
            st.info("No shows found for the selected date range.")
        else:
            df = pd.DataFrame(shows)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df[["date", "band", "city", "state", "venue", "country"]]
            st.dataframe(df)

            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="shows.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()