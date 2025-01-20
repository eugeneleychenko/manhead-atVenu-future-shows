from flask import Flask, jsonify
import os
import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
import json

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
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

def fetch_shows(start_date, end_date):
    """Fetch shows from CSV URL and filter by date range"""
    try:
        # Load CSV from URL
        url = f"https://{DO_SPACE_NAME}.{DO_SPACE_REGION}.digitaloceanspaces.com/{LATEST_FILE}"
        df = pd.read_csv(url)
        
        # Process dates
        df['date'] = pd.to_datetime(df['date'])
        mask = (df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))
        filtered_df = df[mask]
        shows = filtered_df.to_dict('records')
        
        logger.info(f"Filtered to {len(shows)} shows between {start_date} and {end_date}")
        return shows
        
    except Exception as e:
        logger.error(f"Error fetching shows: {str(e)}")
        return []

def format_concert(concert):
    """Format a single concert into a readable string"""
    return f"""
    {concert['band']} at {concert['venue']}
    Date: {concert['date']}
    Location: {concert['city']}, {concert['state']}
    First seen: {concert['first_seen']}
    """

def update_everything():
    """Main function that updates all necessary files and notifications"""
    try:
        # Set up S3 client
        s3_client = boto3.client('s3',
                               region_name=DO_SPACE_REGION,
                               endpoint_url=f'https://{DO_SPACE_REGION}.digitaloceanspaces.com',
                               aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
                               aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4')

        # 1. Fetch new data
        shows = fetch_shows(datetime.now(), datetime.now() + timedelta(days=365))
        if not shows:
            logger.error("No shows found")
            return False

        # 2. Create new DataFrame
        new_df = pd.DataFrame(shows)
        new_df["date"] = pd.to_datetime(new_df["date"]).dt.strftime('%Y-%m-%d')
        new_df = new_df[["date", "band", "city", "state", "venue", "country"]]

        # 3. Move current latest to previous
        try:
            s3_client.copy_object(
                Bucket=DO_SPACE_NAME,
                CopySource={'Bucket': DO_SPACE_NAME, 'Key': LATEST_FILE},
                Key=PREVIOUS_FILE
            )
        except:
            logger.warning("No existing latest file to move")

        # 4. Compare with previous if exists
        try:
            s3_client.download_file(DO_SPACE_NAME, PREVIOUS_FILE, '/tmp/previous.csv')
            previous_df = pd.read_csv('/tmp/previous.csv')
            
            # Find new concerts
            new_concerts = pd.merge(
                new_df, 
                previous_df,
                on=['band', 'venue', 'date', 'city', 'state', 'country'],
                how='left',
                indicator=True
            )
            new_concerts = new_concerts[new_concerts['_merge'] == 'left_only']
            new_concerts = new_concerts.drop('_merge', axis=1)
            
            # Add first_seen date to new concerts
            today = datetime.now().strftime('%Y-%m-%d')
            new_concerts['first_seen'] = today

            # Generate notifications if there are new concerts
            if len(new_concerts) > 0:
                notifications = {}
                for recipient, config in RECIPIENTS.items():
                    recipient_states = config['states']
                    
                    # Filter for recipient's states
                    new_for_recipient = new_concerts[new_concerts['state'].isin(recipient_states)]
                    all_for_recipient = new_df[new_df['state'].isin(recipient_states)]
                    
                    if len(new_for_recipient) > 0:
                        # Format concerts into readable strings
                        new_concerts_formatted = [format_concert(concert) for concert in new_for_recipient.to_dict('records')]
                        all_upcoming_formatted = [format_concert(concert) for concert in all_for_recipient.to_dict('records')]
                        
                        notifications[recipient] = {
                            'email': config['email'],
                            'new_concerts': new_concerts_formatted,
                            'all_upcoming': all_upcoming_formatted
                        }

                # Save notifications
                if notifications:
                    with open('/tmp/notifications.json', 'w') as f:
                        json.dump(notifications, f, indent=2)
                    
                    s3_client.upload_file(
                        '/tmp/notifications.json',
                        DO_SPACE_NAME,
                        NOTIFICATIONS_FILE,
                        ExtraArgs={'ACL': 'public-read', 'ContentType': 'application/json'}
                    )

                # Save new concerts file
                changes_file = f'new_concerts_{today}.csv'
                new_concerts.to_csv('/tmp/changes.csv', index=False)
                s3_client.upload_file(
                    '/tmp/changes.csv',
                    DO_SPACE_NAME,
                    changes_file,
                    ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/csv'}
                )
                
        except Exception as e:
            logger.warning(f"Could not perform comparison: {str(e)}")

        # 5. Save updated concerts file
        new_df.to_csv('/tmp/latest.csv', index=False)
        s3_client.upload_file(
            '/tmp/latest.csv',
            DO_SPACE_NAME,
            LATEST_FILE,
            ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/csv'}
        )

        # 6. Update last successful run timestamp
        s3_client.put_object(
            Bucket=DO_SPACE_NAME,
            Key=LAST_RUN_FILE,
            Body=datetime.now().isoformat(),
            ACL='public-read',
            ContentType='text/plain'
        )

        # 7. Cleanup
        for file in ['/tmp/latest.csv', '/tmp/previous.csv', '/tmp/changes.csv', '/tmp/notifications.json']:
            if os.path.exists(file):
                os.remove(file)

        return True

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        return False

@app.route('/update', methods=['POST'])
def trigger_update():
    """API endpoint to trigger the update process"""
    try:
        success = update_everything()
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Concert data updated successfully',
                'timestamp': datetime.now().isoformat()
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update concert data',
                'timestamp': datetime.now().isoformat()
            }), 500
    except Exception as e:
        logger.error(f"API error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)