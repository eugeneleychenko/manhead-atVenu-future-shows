import os
import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
import json

# Constants
DO_SPACE_REGION = 'nyc3'
DO_SPACE_NAME = 'mh-upcoming-concerts'
LATEST_FILE = 'latest_concerts.csv'
NOTIFICATIONS_FILE = 'notifications_ready.json'

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

def fetch_shows(start_date, end_date):
    """
    Fetch shows from AtVenu API
    Returns list of shows in the format:
    {
        "date": "YYYY-MM-DD",
        "band": "Band Name",
        "city": "City",
        "state": "ST",
        "venue": "Venue Name",
        "country": "Country"
    }
    """
    # TODO: Implement real AtVenu API call here
    # This is where you'll add the actual API integration
    raise NotImplementedError("AtVenu API integration not implemented yet")

def format_concert(concert):
    """Format a single concert into a readable string"""
    return f"""
    {concert['band']} at {concert['venue']}
    Date: {concert['date']}
    Location: {concert['city']}, {concert['state']}
    First seen: {concert['first_seen']}
    """

def update_concerts():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Set up S3 client
        s3_client = boto3.client('s3',
                               region_name=DO_SPACE_REGION,
                               endpoint_url=f'https://{DO_SPACE_REGION}.digitaloceanspaces.com',
                               aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
                               aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4')

        # 1. Fetch new data from AtVenu
        shows = fetch_shows(datetime.now(), datetime.now() + timedelta(days=365))
        if not shows:
            logging.error("No shows found")
            return False

        # 2. Create new DataFrame with explicit date format
        new_df = pd.DataFrame(shows)
        new_df["date"] = pd.to_datetime(new_df["date"], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
        # Preserve first_seen dates from test data
        new_df["first_seen"] = pd.to_datetime(new_df["first_seen"], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
        
        # 3. Load existing data with first_seen dates
        try:
            s3_client.download_file(DO_SPACE_NAME, LATEST_FILE, '/tmp/existing.csv')
            existing_df = pd.read_csv('/tmp/existing.csv')
            existing_df["date"] = pd.to_datetime(existing_df["date"], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
            existing_df["first_seen"] = pd.to_datetime(existing_df["first_seen"], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
            
            # Merge to preserve first_seen dates
            merged_df = pd.merge(
                new_df,
                existing_df[["date", "band", "venue", "first_seen"]],
                on=["date", "band", "venue"],
                how="left"
            )
            
            # Only set first_seen for truly new entries
            merged_df["first_seen"] = merged_df["first_seen_x"].fillna(merged_df["first_seen_y"])
            merged_df["first_seen"] = merged_df["first_seen"].fillna(datetime.now().strftime('%Y-%m-%d'))
            
        except:
            # If no existing file, use the test data's first_seen dates
            merged_df = new_df.copy()
            
        # Drop the merge columns
        merged_df = merged_df.drop(['first_seen_x', 'first_seen_y'], axis=1, errors='ignore')
        
        # Find new concerts for notifications
        today = datetime.now().strftime('%Y-%m-%d')
        new_concerts = merged_df[merged_df["first_seen"] == today]
        
        # 4. Prepare notification data for each recipient
        notifications = {}
        for recipient, config in RECIPIENTS.items():
            recipient_states = config['states']
            
            # Filter for recipient's states
            new_for_recipient = new_concerts[new_concerts['state'].isin(recipient_states)]
            all_for_recipient = merged_df[merged_df['state'].isin(recipient_states)]
            
            if len(new_for_recipient) > 0:
                # Format concerts into readable strings
                new_concerts_formatted = [format_concert(concert) for concert in new_for_recipient.to_dict('records')]
                all_upcoming_formatted = [format_concert(concert) for concert in all_for_recipient.to_dict('records')]
                
                notifications[recipient] = {
                    'email': config['email'],
                    'new_concerts': new_concerts_formatted,
                    'all_upcoming': all_upcoming_formatted
                }

        # 5. Save notifications to DO if there are any
        if notifications:
            with open('/tmp/notifications.json', 'w') as f:
                json.dump(notifications, f, indent=2)
            
            s3_client.upload_file(
                '/tmp/notifications.json',
                DO_SPACE_NAME,
                NOTIFICATIONS_FILE,
                ExtraArgs={'ACL': 'public-read', 'ContentType': 'application/json'}
            )

        # 6. Save updated concerts file
        merged_df.to_csv('/tmp/latest.csv', index=False)
        s3_client.upload_file(
            '/tmp/latest.csv',
            DO_SPACE_NAME,
            LATEST_FILE,
            ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/csv'}
        )

        # 7. Cleanup
        for file in ['/tmp/latest.csv', '/tmp/existing.csv', '/tmp/notifications.json']:
            if os.path.exists(file):
                os.remove(file)

        return True

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    update_concerts()