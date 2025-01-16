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
import boto3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@st.cache_data(ttl=None)
def fetch_shows(start_date, end_date):
    """Fetch shows from CSV URL and filter by date range"""
    try:
        # Set up S3 client
        s3_client = boto3.client('s3',
                               region_name='nyc3',
                               endpoint_url='https://nyc3.digitaloceanspaces.com',
                               aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
                               aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4')
        
        # Get the file's metadata
        response = s3_client.head_object(
            Bucket='mh-upcoming-concerts',
            Key='latest_concerts.csv'
        )
        
        digital_ocean_timestamp = response['LastModified']
        etag = response['ETag']
        
        logger.info(f"Digital Ocean file timestamp: {digital_ocean_timestamp}")
        logger.info(f"Digital Ocean ETag: {etag}")
        
        # Load CSV from URL
        url = "https://mh-upcoming-concerts.nyc3.digitaloceanspaces.com/latest_concerts.csv"
        df = pd.read_csv(url)
        
        logger.info(f"Successfully loaded CSV with {len(df)} rows")
        logger.info(f"DataFrame memory usage: {df.memory_usage().sum() / 1024:.2f} KB")
        
        # Rest of processing...
        df['date'] = pd.to_datetime(df['date'])
        mask = (df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))
        filtered_df = df[mask]
        shows = filtered_df.to_dict('records')
        
        logger.info(f"Filtered to {len(shows)} shows between {start_date} and {end_date}")
        return shows
        
    except Exception as e:
        logger.error(f"Error fetching shows: {str(e)}")
        return []

@st.cache_data(ttl=None)
def get_last_update_time():
    """Fetch the last update timestamp from Digital Ocean Space"""
    try:
        # Set up S3 client
        s3_client = boto3.client('s3',
                               region_name='nyc3',
                               endpoint_url='https://nyc3.digitaloceanspaces.com',
                               aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
                               aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4')
        
        # Get object metadata
        response = s3_client.head_object(
            Bucket='mh-upcoming-concerts',
            Key='latest_concerts.csv'
        )
        
        # Get last modified time and ETag
        last_modified = response['LastModified']
        etag = response['ETag']
        
        logger.info(f"get_last_update_time - ETag: {etag}")
        logger.info(f"get_last_update_time - LastModified: {last_modified}")
        
        # Use ETag to force cache invalidation when file changes
        st.session_state['current_etag'] = etag
        
        return last_modified.strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception as e:
        logger.error(f"Error fetching last update time: {str(e)}")
        return "Unknown"

def main():
    # Initialize session state for first run
    if 'first_run' not in st.session_state:
        st.session_state.first_run = True
        fetch_shows.clear()
        get_last_update_time.clear()
        st.rerun()
    
    with st.sidebar:
        st.title("MH Future Shows")
        
        # Date inputs
        start_date = st.date_input("Start Date", datetime.now().date())
        end_date = st.date_input("End Date", datetime(2025, 12, 31).date())
        
        # Run button right after date fields
        if st.button("Run"):
            shows = fetch_shows(start_date, end_date)
            st.session_state['shows'] = shows
            st.session_state['filtered_shows'] = shows
        
        # Filter options in the middle
        # st.write("---")
        # st.write("Filter Options:")
        
        if 'shows' in st.session_state:
            # ... existing filter code ...
            pass
            
        # Debug and last modified at the bottom
        st.write("---")
        last_update = get_last_update_time()
        st.caption(f"Data last updated: {last_update}")
        
        show_debug = st.checkbox("Show debug logs")
        if show_debug:
            st.write("Cache Info:")
            st.write(f"fetch_shows cache info: {fetch_shows.cache_info if hasattr(fetch_shows, 'cache_info') else 'No cache info available'}")
            st.write("Session State:")
            st.write(st.session_state)

        # Add refresh button
        if st.button("Refresh Data"):
            st.session_state.first_run = True  # Reset first_run state
            fetch_shows.clear()
            get_last_update_time.clear()
            st.rerun()

    st.title("Upcoming Manhead Artists' Concerts via AtVenu")
    
    # if st.sidebar.checkbox("Show Debug Logs"):
    #     log_placeholder = st.empty()
        
    # Add info about state processing
    st.sidebar.markdown("### Note")
    st.sidebar.markdown("State abbreviations are automatically standardized for US venues.")

    if 'shows' in st.session_state:
        shows = st.session_state['shows']

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