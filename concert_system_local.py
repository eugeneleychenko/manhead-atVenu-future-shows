import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import boto3
import json
from anthropic import Anthropic
from flask import Flask, jsonify

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

# Initialize Anthropic client
anthropic = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

app = Flask(__name__)

def normalize_state_code(state):
    """
    Normalize state names to two-letter codes.
    
    Args:
        state: String containing state name or code
        
    Returns:
        str: Two-letter state code if valid, None otherwise
    """
    if not state:
        return None
        
    # State name to code mapping
    STATE_MAPPING = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY'
    }
    
    state_str = str(state).strip().upper()
    
    # If it's already a valid 2-letter code
    if len(state_str) == 2 and state_str in STATE_MAPPING.values():
        return state_str
        
    # If it's a full state name
    if state_str in STATE_MAPPING:
        return STATE_MAPPING[state_str]
        
    return None

def is_valid_state_code(state):
    """Check if state is a valid 2-letter code or can be converted to one"""
    normalized = normalize_state_code(state)
    return normalized is not None

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
            model="claude-3-5-haiku-20241022",
            max_tokens=10,
            temperature=0,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        if hasattr(message, 'content') and isinstance(message.content, list):
            logger.info(f"Got list response from Claude: {message.content}")
            if len(message.content) > 0 and hasattr(message.content[0], 'text'):
                content = message.content[0].text
            else:
                logger.error("No text field found in the Claude response list")
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

def fetch_shows(start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Fetch shows from AtVenu API and sanitize state codes using Anthropic.
    
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

    logger.info(f"Finished GraphQL fetch, total shows before state processing: {len(shows)}")

    processed_shows = []
    for show in shows:
        if show["country"] == "United States":
            state = show["state"]
            logger.debug(f"Show => Date: {show['date']}, City: {show['city']}, Venue: {show['venue']}")
            logger.debug(f"Original state value: {state}")

            # Try to normalize the state code first
            normalized_state = normalize_state_code(state)
            if normalized_state:
                show["state"] = normalized_state
                logger.debug(f"Normalized state to: {normalized_state}")
            elif not is_valid_state_code(state):
                logger.info(f"State '{state}' is invalid or missing, calling Claude for correction")
                new_state = get_state_from_claude(
                    city=show["city"],
                    venue=show["venue"],
                    current_state=state,
                )
                if new_state:
                    show["state"] = new_state
                    logger.info(f"Updated state to: {new_state}")
                else:
                    logger.warning(f"Could not determine state for {show['city']}, {show['venue']}")

        processed_shows.append(show)

    logger.info(f"Finished processing shows, total returned: {len(processed_shows)}")
    return processed_shows

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

def format_concert(concert):
    """
    Format a single concert into an HTML string.
    
    Args:
        concert: Dictionary containing concert details
        
    Returns:
        str: Formatted concert HTML string
    """
    # Parse and format the date
    date_obj = datetime.strptime(concert['date'], '%Y-%m-%d')
    formatted_date = date_obj.strftime('%B %d, %Y')
    
    return f"""<div class="concert" style="margin-bottom: 10px;">
<p style="margin: 0;">🎸 {concert['band']} Concert</p>
<p style="margin: 0;">📅 {formatted_date}</p>
<p style="margin: 0;">📍 {concert['venue']}, {concert['city']}, {concert['state']}</p>
</div>"""

@app.route('/update', methods=['GET'])
def trigger_update():
    """
    API endpoint to trigger the update process.
    
    Returns:
        tuple: (JSON response, HTTP status code)
    """
    logger.info("Entered /update endpoint")
    try:
        # Move main() logic here
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
                        
                        # Prepare notifications for each recipient
                        notifications = {}
                        today = datetime.now().date()
                        
                        for recipient, info in RECIPIENTS.items():
                            logger.info(f"Processing notifications for {recipient}")
                            notifications[recipient] = {
                                'email': info['email'],
                                'new_concerts': [],
                                'all_upcoming': []
                            }
                            
                            # Process all concerts for this recipient
                            for _, concert in latest_df.iterrows():
                                if concert['state'] in info['states']:
                                    concert_date = datetime.strptime(concert['date'], '%Y-%m-%d').date()
                                    
                                    # Add to all_upcoming if concert is in the future
                                    if concert_date >= today:
                                        concert_info = format_concert(concert)
                                        notifications[recipient]['all_upcoming'].append({
                                            'date': concert['date'],
                                            'info': concert_info
                                        })
                        
                            # Process new concerts for this recipient
                            for _, concert in new_concerts.iterrows():
                                if concert['state'] in info['states']:
                                    concert_date = datetime.strptime(concert['date'], '%Y-%m-%d').date()
                                    if concert_date >= today:
                                        concert_info = format_concert(concert)
                                        notifications[recipient]['new_concerts'].append({
                                            'date': concert['date'],
                                            'info': concert_info
                                        })
                            
                            # Sort both lists by date and extract just the concert info
                            notifications[recipient]['all_upcoming'].sort(key=lambda x: x['date'])
                            notifications[recipient]['new_concerts'].sort(key=lambda x: x['date'])
                            notifications[recipient]['all_upcoming'] = [x['info'] for x in notifications[recipient]['all_upcoming']]
                            notifications[recipient]['new_concerts'] = [x['info'] for x in notifications[recipient]['new_concerts']]
                            
                            # Join the concert info strings without any separator
                            notifications[recipient]['all_upcoming'] = ''.join(notifications[recipient]['all_upcoming'])
                            notifications[recipient]['new_concerts'] = ''.join(notifications[recipient]['new_concerts'])
                            
                            logger.info(f"Found {len(notifications[recipient]['new_concerts'])} new and "
                                      f"{len(notifications[recipient]['all_upcoming'])} total concerts for {recipient}")

                        # Save notifications to file and upload to DigitalOcean
                        with open(NOTIFICATIONS_FILE, 'w') as f:
                            json.dump(notifications, f, indent=2)
                        safe_upload_file(NOTIFICATIONS_FILE, DO_SPACE_NAME, NOTIFICATIONS_FILE, s3_client)
                        logger.info("Uploaded notifications.json to DigitalOcean")

                        # Create and upload notifications_ready.json
                        ready_data = {
                            "ready": True,
                            "timestamp": datetime.now().isoformat()
                        }
                        with open(NOTIFICATIONS_READY_FILE, 'w') as f:
                            json.dump(ready_data, f)
                        safe_upload_file(NOTIFICATIONS_READY_FILE, DO_SPACE_NAME, NOTIFICATIONS_READY_FILE, s3_client)
                        logger.info("Uploaded notifications_ready.json to DigitalOcean")

                        # Cleanup
                        for file in [LATEST_FILE, PREVIOUS_FILE, NOTIFICATIONS_FILE, NOTIFICATIONS_READY_FILE]:
                            if os.path.exists(file):
                                os.remove(file)
                                logger.info(f"Cleaned up temporary file: {file}")

                        return jsonify({
                            "status": "success",
                            "message": "Concert data updated successfully",
                            "timestamp": datetime.now().isoformat(),
                        }), 200
                    else:
                        logger.warning("No previous concert data found for comparison")
                        return jsonify({
                            "status": "warning",
                            "message": "No previous concert data found for comparison",
                            "timestamp": datetime.now().isoformat(),
                        }), 200
                else:
                    logger.error("Failed to upload to DigitalOcean Space")
                    return jsonify({
                        "status": "error",
                        "message": "Failed to upload to DigitalOcean Space",
                        "timestamp": datetime.now().isoformat(),
                    }), 500
            else:
                logger.warning("No shows found")
                return jsonify({
                    "status": "warning",
                    "message": "No shows found",
                    "timestamp": datetime.now().isoformat(),
                }), 200

    except Exception as e:
        logger.error(f"Exception in /update endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat(),
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """
    Simple health-check endpoint.
    
    Returns:
        tuple: (JSON response, HTTP status code)
    """
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5001))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port)
