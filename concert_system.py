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
NOTIFICATIONS_FILE = 'notifications_ready.json'
LAST_RUN_FILE = 'last_successful_run.txt'

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

def fetch_shows(start_date, end_date):
    """
    Fetch shows from AtVenu API using the same GraphQL query as in stream_paste.py
    Then sanitize state codes using Anthropics. 
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
    """Format a single concert into a readable string"""
    return f"""
    {concert['band']} at {concert['venue']}
    Date: {concert['date']}
    Location: {concert['city']}, {concert['state']}
    First seen: {concert.get('first_seen', 'N/A')}
    """

def update_everything():
    """Main function that updates all necessary files and notifications"""
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

            # Find new concerts
            logger.info("Finding newly added concerts compared to previous")
            new_concerts = pd.merge(
                new_df,
                previous_df,
                on=["band", "venue", "date", "city", "state", "country"],
                how="left",
                indicator=True
            )
            new_concerts = new_concerts[new_concerts["_merge"] == "left_only"].drop("_merge", axis=1)
            logger.info(f"Found {len(new_concerts)} new concerts")

            # Add first_seen date to both new_concerts and new_df
            today = datetime.now().strftime("%Y-%m-%d")
            
            # Add first_seen to new_df by merging with previous_df
            new_df = pd.merge(
                new_df,
                previous_df[["band", "venue", "date", "city", "state", "first_seen"]],
                on=["band", "venue", "date", "city", "state"],
                how="left"
            )
            # Where first_seen is NaN (new concerts), set to today
            new_df["first_seen"] = new_df["first_seen"].fillna(today)
            
            # Add first_seen to new_concerts
            new_concerts["first_seen"] = today

            # Make sure new_df has the correct column order
            new_df = new_df[["date", "band", "city", "state", "venue", "country", "first_seen"]]

            # Generate notifications if there are new concerts
            if len(new_concerts) > 0:
                logger.info("Generating notifications for newly added concerts")
                notifications = {}
                for recipient, config in RECIPIENTS.items():
                    logger.info(f"Processing notifications for {recipient}")
                    recipient_states = config["states"]

                    # Filter for recipient's states
                    new_for_recipient = new_concerts[new_concerts["state"].isin(recipient_states)]
                    all_for_recipient = new_df[new_df["state"].isin(recipient_states)]

                    if len(new_for_recipient) > 0:
                        logger.info(f"{recipient} has {len(new_for_recipient)} new shows")
                        new_concerts_formatted = [
                            format_concert(c) for c in new_for_recipient.to_dict("records")
                        ]
                        all_upcoming_formatted = [
                            format_concert(c) for c in all_for_recipient.to_dict("records")
                        ]

                        notifications[recipient] = {
                            "email": config["email"],
                            "new_concerts": new_concerts_formatted,
                            "all_upcoming": all_upcoming_formatted
                        }

                # Modified file upload section
                def safe_upload_file(local_path, bucket, key, content_type):
                    """Helper function to safely upload files with proper content length"""
                    try:
                        with open(local_path, 'rb') as file_obj:
                            content = file_obj.read()
                            s3_client.put_object(
                                Bucket=bucket,
                                Key=key,
                                Body=content,
                                ContentLength=len(content),
                                ContentType=content_type,
                                ACL='public-read'
                            )
                        logger.info(f"Successfully uploaded {local_path} to {bucket}/{key}")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to upload {local_path}: {str(e)}")
                        return False

                # Use the safe upload function for all files
                if notifications:
                    logger.info("Saving notifications file to /tmp/notifications.json")
                    with open('/tmp/notifications.json', 'w') as f:
                        json.dump(notifications, f, indent=2)
                    
                    safe_upload_file(
                        '/tmp/notifications.json',
                        DO_SPACE_NAME,
                        NOTIFICATIONS_FILE,
                        'application/json'
                    )

                # Save new concerts file if there are any
                if len(new_concerts) > 0:
                    changes_file = f'new_concerts_{today}.csv'
                    new_concerts.to_csv('/tmp/changes.csv', index=False)
                    safe_upload_file(
                        '/tmp/changes.csv',
                        DO_SPACE_NAME,
                        changes_file,
                        'text/csv'
                    )

                # Save latest concerts file
                logger.info("Saving latest concerts file")
                new_df.to_csv('/tmp/latest.csv', index=False)
                if not safe_upload_file(
                    '/tmp/latest.csv',
                    DO_SPACE_NAME,
                    LATEST_FILE,
                    'text/csv'
                ):
                    raise Exception("Failed to upload latest concerts file")

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
    """API endpoint to trigger the update process"""
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
    """Simple health-check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5001))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port)