import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import boto3

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"
DO_SPACE_REGION = "nyc3"
DO_SPACE_NAME = "mh-upcoming-concerts"
LATEST_FILE = "latest_concerts.csv"
PREVIOUS_FILE = "previous_concerts.csv"
NOTIFICATIONS_FILE = "notifications.json"
NOTIFICATIONS_READY_FILE = "notifications_ready.json"
LAST_RUN_FILE = "last_successful_run.txt"
TODAY = datetime.now().strftime('%Y-%m-%d')

# Add recipient configurations from concert_system.py
RECIPIENTS = {
    'chris': {
        'states': ['NY', 'NJ'],
        'email': 'nycspicebo+chris@gmail.com'
    },
    'steve': {
        'states': ['NY', 'NJ', 'MA', 'CT', 'NC'],
        'email': 'nycspicebo+steve@gmail.com'
    }
}

# Add GraphQL query
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

def fetch_shows(start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Fetch shows from AtVenu API.

    Args:
        start_date: Start date for fetching shows
        end_date: End date for fetching shows

    Returns:
        List of dictionaries containing show information
    """
    logger.info(f"Beginning fetch_shows from {start_date} to {end_date}")
    
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

    page_count = 0
    while has_next_page_accounts:
        page_count += 1
        logger.info(f"Requesting GraphQL page {page_count} with after_accounts={after_accounts or 'None'}")

        variables = {
            "firstForAccounts": first_for_accounts,
            "afterForAccounts": after_accounts,
            "firstForTours": first_for_tours,
            "firstForShows": first_for_shows,
            "range": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d")
            }
        }
        logger.debug(f"GraphQL variables: {variables}")

        result = client.execute(SHOWS_QUERY, variable_values=variables)

        fetched_accounts = result["organization"]["accounts"]["nodes"]
        logger.info(f"Fetched {len(fetched_accounts)} accounts in this chunk")
        
        for account in fetched_accounts:
            tours = account["tours"]["nodes"]
            logger.debug(f"Account: {account['name']} => {len(tours)} tours")
            for tour in tours:
                for show in tour["shows"]["nodes"]:
                    shows.append({
                        "uuid": show["uuid"],
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

    logger.info(f"Finished GraphQL fetch, total shows: {len(shows)}")
    return shows

def safe_upload_file(
    local_file: str,
    bucket: str,
    remote_file: str,
    s3_client: 'boto3.client'
) -> bool:
    """
    Safely upload a file to S3/DigitalOcean Spaces.

    Args:
        local_file: Path to local file to upload
        bucket: Name of the bucket/space
        remote_file: Destination path in bucket
        s3_client: Initialized boto3 S3 client

    Returns:
        bool: True if upload successful, False otherwise
    """
    try:
        s3_client.upload_file(local_file, bucket, remote_file)
        return True
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return False

def safe_download_file(
    bucket: str,
    remote_file: str,
    local_file: str,
    s3_client: 'boto3.client'
) -> bool:
    """
    Safely download a file from S3/DigitalOcean Spaces.

    Args:
        bucket: Name of the bucket/space
        remote_file: Source path in bucket
        local_file: Path to local file to save to
        s3_client: Initialized boto3 S3 client

    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        s3_client.download_file(bucket, remote_file, local_file)
        return True
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return False

def find_new_concerts(latest_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare latest and previous concert data to find new concerts.

    Args:
        latest_df: DataFrame containing latest concert data
        previous_df: DataFrame containing previous concert data

    Returns:
        DataFrame containing only new concerts
    """
    # Find concerts in latest that aren't in previous using UUID
    new_concerts = latest_df[~latest_df['uuid'].isin(previous_df['uuid'])]
    return new_concerts

def main() -> None:
    """
    Main function to fetch concert data and upload to DigitalOcean Spaces.
    """
    # Set up S3 client
    logger.info("Setting up S3/DigitalOcean spaces client")
    s3_client = boto3.client(
        's3',
        region_name=DO_SPACE_REGION,
        endpoint_url=f"https://{DO_SPACE_REGION}.digitaloceanspaces.com",
        aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
        aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4'
    )

    # Download and backup existing latest file
    logger.info("Downloading and backing up existing concert data")
    if safe_download_file(DO_SPACE_NAME, LATEST_FILE, LATEST_FILE, s3_client):
        # Copy latest to previous
        if os.path.exists(PREVIOUS_FILE):
            os.remove(PREVIOUS_FILE)
        if os.path.exists(LATEST_FILE):
            os.rename(LATEST_FILE, PREVIOUS_FILE)
            safe_upload_file(PREVIOUS_FILE, DO_SPACE_NAME, PREVIOUS_FILE, s3_client)
            logger.info("Successfully backed up previous concert data")

    # Fetch shows for next year
    one_year_later = datetime.now() + timedelta(days=365)
    shows = fetch_shows(datetime.now(), one_year_later)
    
    if shows:
        # Create DataFrame and save to CSV
        latest_df = pd.DataFrame(shows)
        latest_df.to_csv(LATEST_FILE, index=False)
        logger.info(f"Saved {len(latest_df)} shows to temporary file")

        # Upload to DigitalOcean Space
        if safe_upload_file(LATEST_FILE, DO_SPACE_NAME, LATEST_FILE, s3_client):
            logger.info("Successfully uploaded to DigitalOcean Space")
        
            # Compare with previous data to find new concerts
            if os.path.exists(PREVIOUS_FILE):
                previous_df = pd.read_csv(PREVIOUS_FILE)
                new_concerts = find_new_concerts(latest_df, previous_df)
                
                if not new_concerts.empty:
                    logger.info(f"Found {len(new_concerts)} new concerts:")
                    print("\nNew Concerts:")
                    print("=============")
                    for _, concert in new_concerts.iterrows():
                        print(f"{concert['date']} - {concert['band']} at {concert['venue']} ({concert['city']}, {concert['state']})")
                else:
                    logger.info("No new concerts found")
            else:
                logger.info("No previous concert data found for comparison")
        else:
            logger.error("Failed to upload to DigitalOcean Space")

        # Cleanup
        for file in [LATEST_FILE, PREVIOUS_FILE]:
            if os.path.exists(file):
                os.remove(file)
                logger.info(f"Cleaned up temporary file: {file}")
    else:
        logger.warning("No shows found")

if __name__ == "__main__":
    main()
