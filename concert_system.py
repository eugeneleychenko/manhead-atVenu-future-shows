from flask import Flask, jsonify
import os
import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from anthropic import Anthropic

app = Flask(__name__)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
DO_SPACE_REGION = 'nyc3'
DO_SPACE_NAME = 'mh-upcoming-concerts'
LATEST_FILE = 'latest_concerts.csv'
PREVIOUS_FILE = 'previous_concerts.csv'
NOTIFICATIONS_FILE = 'notifications.json'
NOTIFICATIONS_READY_FILE = 'notifications_ready.json'
LAST_RUN_FILE = 'last_successful_run.txt'
TODAY = datetime.now().strftime('%Y-%m-%d')

# Recipient configurations
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

# Add new constants
API_ENDPOINT = "https://api.atvenu.com"
API_TOKEN = "live_yvYLBo32dRE9z_yCdhwU"

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

# At the start of the script
today = datetime.strptime(TODAY, '%Y-%m-%d').date()

def is_valid_state_code(state):
    """Check if state is a valid 2-letter code"""
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

def is_new_concert(concert, previous_concerts):
    """Check if a concert is new by comparing with previous concerts data"""
    try:
        # Convert concert date to datetime for comparison
        concert_date = datetime.strptime(concert['date'], '%Y-%m-%d').date()
        
        # Look for matching concerts in previous data
        matches = previous_concerts[
            (previous_concerts['band'] == concert['band']) &
            (previous_concerts['date'] == concert['date']) &
            (previous_concerts['venue'] == concert['venue'])
        ]
        
        if len(matches) == 0:
            return True
            
        # If found, check if first_seen date is different
        previous_first_seen = datetime.strptime(matches.iloc[0]['first_seen'], '%Y-%m-%d').date()
        current_first_seen = datetime.strptime(concert['first_seen'], '%Y-%m-%d').date()
        
        return previous_first_seen != current_first_seen
        
    except Exception as e:
        logger.warning(f"Error comparing concert: {str(e)}")
        return False

def fetch_shows(start_date, end_date):
    """
    Fetch shows from AtVenu API and sanitize state codes using Anthropic.
    
    Args:
        start_date: datetime object for start of date range
        end_date: datetime object for end of date range
        
    Returns:
        list: List of processed show dictionaries
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

        # Log how many accounts we fetched in this chunk
        fetched_accounts = result["organization"]["accounts"]["nodes"]
        logger.info(f"Fetched {len(fetched_accounts)} accounts in this chunk")
        
        for account in fetched_accounts:
            tours = account["tours"]["nodes"]
            logger.debug(f"Account: {account['name']} => {len(tours)} tours")
            for tour in tours:
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

    logger.info(f"Finished GraphQL fetch, total shows before state processing: {len(shows)}")

    processed_shows = []
    for show in shows:
        if show["country"] == "United States":
            state = show["state"]
            logger.debug(f"Show => Date: {show['date']}, City: {show['city']}, Venue: {show['venue']}")
            logger.debug(f"Original state value: {state}")

            if not is_valid_state_code(state):
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
            else:
                logger.debug(f"State '{state}' is already valid")

        processed_shows.append(show)

    logger.info(f"Finished processing shows, total returned: {len(processed_shows)}")
    return processed_shows

def format_concert(concert):
    """
    Format a single concert into a readable string.
    
    Args:
        concert: Dictionary containing concert details
        
    Returns:
        str: Formatted concert string
    """
    return f"""
    {concert['band']} at {concert['venue']}
    Date: {concert['date']}
    Location: {concert['city']}, {concert['state']}
    First seen: {concert.get('first_seen', 'N/A')}
    """

def ensure_bucket_public(s3_client, bucket_name):
    """
    Ensure the S3 bucket is publicly accessible.
    
    Args:
        s3_client: Boto3 S3 client
        bucket_name: Name of the bucket
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Set bucket policy to public
        public_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                }
            ]
        }
        
        # Convert policy to JSON string
        policy_json = json.dumps(public_policy)
        
        # Apply the public policy to the bucket
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=policy_json
        )
        
        logger.info(f"Successfully set bucket {bucket_name} to public")
        return True
    except Exception as e:
        logger.error(f"Error setting bucket policy: {e}")
        return False

def safe_upload_file(filename: str, bucket: str, key: str, s3_client) -> bool:
    """
    Safely upload a file to S3, with retries and error handling.
    
    Args:
        filename: Local path to file
        bucket: S3 bucket name
        key: S3 object key
        s3_client: Boto3 S3 client
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # First ensure bucket is public
        ensure_bucket_public(s3_client, bucket)
        
        with open(filename, 'rb') as f:
            s3_client.upload_fileobj(
                f, 
                bucket, 
                key,
                ExtraArgs={
                    'ACL': 'public-read',
                    'ContentType': 'text/csv'  # Add proper content type
                }
            )
        return True
    except Exception as e:
        logging.error(f"Error uploading {filename} to {bucket}/{key}: {e}")
        return False

def update_everything():
    """
    Main function that updates all necessary files and notifications.
    
    Fetches new concert data, compares with previous data, generates notifications,
    and updates all relevant files in S3 storage.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting update_everything()")
    try:
        # Set up S3 client
        logger.info("Setting up S3/DigitalOcean spaces client")
        s3_client = boto3.client(
            's3',
            region_name=DO_SPACE_REGION,
            endpoint_url=f"https://{DO_SPACE_REGION}.digitaloceanspaces.com",
            aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
            aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4'
        )

        # 1. Fetch new data
        one_year_later = datetime.now() + timedelta(days=365)
        logger.info(f"Fetching shows from now to {one_year_later}")
        shows = fetch_shows(datetime.now(), one_year_later)
        logger.info(f"Fetched {len(shows)} shows from fetch_shows()")
        if not shows:
            logger.warning("No shows found.")
            return False

        # 2. Create new DataFrame
        logger.info("Creating new DataFrame with cleaned results")
        new_df = pd.DataFrame(shows)
        new_df["date"] = pd.to_datetime(new_df["date"]).dt.strftime('%Y-%m-%d')
        new_df = new_df[["date", "band", "city", "state", "venue", "country"]]
        logger.info(f"New DataFrame has {len(new_df)} rows")

        # 3. Move current latest to previous
        logger.info("Moving latest file to previous (if exists)")
        try:
            s3_client.copy_object(
                Bucket=DO_SPACE_NAME,
                CopySource={'Bucket': DO_SPACE_NAME, 'Key': LATEST_FILE},
                Key=PREVIOUS_FILE
            )
            logger.info("Successfully copied latest file to previous")
        except Exception as e:
            logger.warning(f"Could not move latest to previous: {str(e)}")

        # 4. Compare with previous if exists
        logger.info("Attempting to compare with previous data")
        try:
            logger.info("Downloading 'previous_concerts.csv' for comparison")
            s3_client.download_file(DO_SPACE_NAME, PREVIOUS_FILE, '/tmp/previous.csv')
            previous_df = pd.read_csv('/tmp/previous.csv')
            logger.info(f"Loaded previous data with {len(previous_df)} rows")

            # Find new concerts - compare without first_seen
            logger.info("Finding newly added concerts compared to previous")
            new_concerts = pd.merge(
                new_df[["date", "band", "city", "state", "venue", "country"]],
                previous_df[["date", "band", "city", "state", "venue", "country"]],
                on=["band", "venue", "date", "city", "state", "country"],
                how="left",
                indicator=True
            )
            new_concerts = new_concerts[new_concerts["_merge"] == "left_only"].drop("_merge", axis=1)
            logger.info(f"Found {len(new_concerts)} new concerts")

            # Add first_seen date to both new_concerts and new_df
            today = datetime.now().strftime("%Y-%m-%d")
            
            # Check if first_seen exists in previous_df
            if 'first_seen' in previous_df.columns:
                logger.info("Found first_seen in previous data, merging dates")
                new_df = pd.merge(
                    new_df[["date", "band", "city", "state", "venue", "country"]],
                    previous_df[["band", "venue", "date", "city", "state", "first_seen"]],
                    on=["band", "venue", "date", "city", "state"],
                    how="left"
                )
            else:
                logger.info("No first_seen in previous data, creating new column")
                new_df = new_df[["date", "band", "city", "state", "venue", "country"]].copy()
            
            # Fill in missing first_seen dates with today
            if "first_seen" not in new_df.columns:
                new_df["first_seen"] = today
            else:
                new_df["first_seen"] = new_df["first_seen"].fillna(today)
            
            # Add first_seen to new_concerts
            new_concerts["first_seen"] = today

            # Make sure new_df has the correct column order
            new_df = new_df[["date", "band", "city", "state", "venue", "country", "first_seen"]]
            
            logger.info(f"Processed DataFrame has {len(new_df)} rows with first_seen dates")

            # After processing new data and before notifications
            logger.info("Saving and uploading latest concerts file")
            new_df.to_csv('/tmp/latest.csv', index=False)
            if not safe_upload_file(
                '/tmp/latest.csv',
                DO_SPACE_NAME,
                LATEST_FILE,
                s3_client
            ):
                raise Exception("Failed to upload latest concerts file")
            logger.info("Successfully uploaded new latest_concerts.csv to DigitalOcean")

            # Get today's date
            today = datetime.strptime(today, '%Y-%m-%d').date()  # Convert string to datetime.date

            print(f"Today's date: {today}")
            print(f"Total concerts loaded: {len(new_df)}")
            print(f"Sample concert state: {new_df.iloc[0]['state']}")

            # 4. Prepare notification data for each recipient
            notifications = {}
            for recipient, info in RECIPIENTS.items():
                print(f"\nProcessing {recipient} who is interested in states: {info['states']}")
                notifications[recipient] = {
                    'email': info['email'],
                    'new_concerts': [],
                    'all_upcoming': []
                }
                
                # Get all upcoming concerts in recipient's states
                for _, concert in new_df.iterrows():
                    try:
                        if concert['state'] in info['states']:
                            print(f"State match found for {concert['state']}")
                            # Convert concert date to datetime.date
                            concert_date = datetime.strptime(str(concert['date']), '%Y-%m-%d').date()
                            print(f"Concert date: {concert_date}, Today: {today}")
                            
                            if concert_date >= today:
                                concert_info = format_concert(concert)
                                print(f"Adding to all_upcoming: {concert_info}")
                                notifications[recipient]['all_upcoming'].append(concert_info)
                                
                                # Only check for new concerts if we successfully loaded previous data
                                if previous_df is not None and is_new_concert(concert, previous_df):
                                    notifications[recipient]['new_concerts'].append(concert_info)
                    except Exception as e:
                        logger.warning(f"Error processing concert {concert.get('band', 'unknown')}: {str(e)}")
                        continue
                
                logger.info(f"Processed notifications for {recipient}: {len(notifications[recipient]['new_concerts'])} new, {len(notifications[recipient]['all_upcoming'])} total")

            # Save notifications to JSON file
            try:
                with open('notifications.json', 'w') as f:
                    json.dump(notifications, f, indent=2)
                logger.info("Successfully saved notifications to notifications.json")
            except Exception as e:
                logger.error(f"Error saving notifications.json: {str(e)}")

            # Always save notifications, even if empty
            logger.info("Saving notifications file to /tmp/notifications.json")
            with open('/tmp/notifications.json', 'w') as f:
                json.dump(notifications, f, indent=2)
            
            # Upload notifications.json
            safe_upload_file(
                '/tmp/notifications.json',
                DO_SPACE_NAME,
                NOTIFICATIONS_FILE,
                s3_client
            )

            # Always update notifications_ready.json
            timestamp = datetime.now().isoformat()
            try:
                s3_client.put_object(
                    Bucket=DO_SPACE_NAME,
                    Key=NOTIFICATIONS_READY_FILE,
                    Body=json.dumps({"ready": True, "timestamp": timestamp}),
                    ContentType='application/json',
                    ACL='public-read'
                )
                logger.info("Successfully updated notifications_ready.json")
            except Exception as e:
                logger.error(f"Failed to update notifications_ready.json: {str(e)}")

            # Save new concerts file if there are any
            if len(new_concerts) > 0:
                changes_file = f'new_concerts_{today}.csv'
                new_concerts.to_csv('/tmp/changes.csv', index=False)
                safe_upload_file(
                    '/tmp/changes.csv',
                    DO_SPACE_NAME,
                    changes_file,
                    s3_client
                )

            # Update last successful run timestamp
            timestamp = datetime.now().isoformat()
            try:
                s3_client.put_object(
                    Bucket=DO_SPACE_NAME,
                    Key=LAST_RUN_FILE,
                    Body=timestamp.encode(),
                    ContentLength=len(timestamp.encode()),
                    ContentType='text/plain',
                    ACL='public-read'
                )
                logger.info("Successfully updated last run timestamp")
            except Exception as e:
                logger.error(f"Failed to update last run timestamp: {str(e)}")

        except Exception as e:
            logger.warning(f"Could not load or compare previous data: {str(e)}", exc_info=True)

        # Cleanup
        logger.info("Cleaning up temporary files")
        for f in ['/tmp/latest.csv', '/tmp/previous.csv', '/tmp/changes.csv', '/tmp/notifications.json']:
            if os.path.exists(f):
                try:
                    os.remove(f)
                    logger.info(f"Removed {f}")
                except Exception as e:
                    logger.warning(f"Failed to remove {f}: {str(e)}")

        logger.info("Update process completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error in update_everything(): {str(e)}", exc_info=True)
        return False

@app.route('/update', methods=['GET'])
def trigger_update():
    """
    API endpoint to trigger the update process.
    
    Returns:
        tuple: (JSON response, HTTP status code)
    """
    logger.info("Entered /update endpoint")
    try:
        success = update_everything()
        if success:
            logger.info("update_everything() succeeded")
            return jsonify({
                "status": "success",
                "message": "Concert data updated successfully",
                "timestamp": datetime.now().isoformat(),
            }), 200
        else:
            logger.error("update_everything() returned False")
            return jsonify({
                "status": "error",
                "message": "Failed to update concert data",
                "timestamp": datetime.now().isoformat(),
            }), 500
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