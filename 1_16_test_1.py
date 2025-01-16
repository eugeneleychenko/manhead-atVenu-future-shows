# This script updates concert data in a Digital Ocean Space:
# 1. Fetches upcoming shows for the next year using fetch_shows()
# 2. Creates a new DataFrame with the show data
# 3. Moves the current latest_concerts.csv to previous_concerts.csv in the DO Space
# 4. Uploads the new show data as latest_concerts.csv
# 5. Compares latest and previous files to identify new concerts
# 6. If new concerts are found, creates and uploads a dated CSV with just the new shows
# The script helps track changes in concert listings over time and identify newly added shows


import os
import boto3
import pandas as pd
from datetime import datetime, timedelta
from MH_future_shows import fetch_shows
import logging

# Constants
DO_SPACE_REGION = 'nyc3'
DO_SPACE_NAME = 'mh-upcoming-concerts'
LATEST_FILE = 'latest_concerts.csv'
PREVIOUS_FILE = 'previous_concerts.csv'
LAST_RUN_FILE = 'last_successful_run.txt'

def update_concerts():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
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
            logging.error("No shows found")
            return False

        # 2. Create new DataFrame
        new_df = pd.DataFrame(shows)
        new_df["date"] = pd.to_datetime(new_df["date"]).dt.date
        new_df = new_df[["date", "band", "city", "state", "venue", "country"]]

        # 3. Move current latest to previous
        try:
            s3_client.copy_object(
                Bucket=DO_SPACE_NAME,
                CopySource={'Bucket': DO_SPACE_NAME, 'Key': LATEST_FILE},
                Key=PREVIOUS_FILE
            )
        except:
            logging.warning("No existing latest file to move")

        # 4. Save and upload new latest
        new_df.to_csv('/tmp/latest.csv', index=False)
        with open('/tmp/latest.csv', 'rb') as file_data:
            s3_client.put_object(
                Bucket=DO_SPACE_NAME,
                Key=LATEST_FILE,
                Body=file_data,
                ACL='public-read',
                ContentType='text/csv'
            )

        # 5. Compare with previous if exists
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

            if len(new_concerts) > 0:
                current_date = datetime.now().strftime('%Y_%m_%d')
                changes_file = f'new_concerts_{current_date}.csv'
                new_concerts.to_csv('/tmp/changes.csv', index=False)
                
                with open('/tmp/changes.csv', 'rb') as file_data:
                    s3_client.put_object(
                        Bucket=DO_SPACE_NAME,
                        Key=changes_file,
                        Body=file_data,
                        ACL='public-read',
                        ContentType='text/csv'
                    )
                logging.info(f"Found {len(new_concerts)} new concerts")
        except:
            logging.warning("Could not perform comparison - no previous file exists")

        # 6. Update last successful run timestamp
        s3_client.put_object(
            Bucket=DO_SPACE_NAME,
            Key=LAST_RUN_FILE,
            Body=datetime.now().isoformat(),
            ACL='public-read',
            ContentType='text/plain'
        )

        # 7. Cleanup
        for file in ['/tmp/latest.csv', '/tmp/previous.csv', '/tmp/changes.csv']:
            if os.path.exists(file):
                os.remove(file)

        return True

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    update_concerts()
