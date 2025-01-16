# This script compares concert files stored in Digital Ocean Spaces cloud storage
# It does the following:
# 1. Sets up an S3 client to connect to Digital Ocean Spaces
# 2. Lists all CSV files in the bucket and sorts them by name
# 3. Gets the latest and previous CSV files for comparison
# 4. Downloads both files locally for processing
# 5. Uses logging to track the execution flow and any errors
# 6. Requires at least 2 files in the bucket to perform comparison


import os
import boto3
import pandas as pd
from datetime import datetime
import logging

# Constants
DO_SPACE_REGION = 'nyc3'
DO_SPACE_NAME = 'mh-upcoming-concerts'

def compare_concert_files():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        logging.info("Starting concert file comparison")
        
        # Set up S3 client
        s3_client = boto3.client('s3',
                               region_name=DO_SPACE_REGION,
                               endpoint_url=f'https://{DO_SPACE_REGION}.digitaloceanspaces.com',
                               aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
                               aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4')

        # List objects in bucket
        logging.info(f"Fetching files from bucket: {DO_SPACE_NAME}")
        response = s3_client.list_objects_v2(Bucket=DO_SPACE_NAME)
        
        # Get list of CSV files and sort by name
        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
        files.sort()
        logging.info(f"Found {len(files)} CSV files in bucket")
        
        if len(files) < 2:
            logging.warning("Not enough files to compare. Need at least 2 files.")
            return
            
        # Get latest and previous file names
        latest_file = files[-1]
        previous_file = files[-2]
        logging.info(f"Comparing files: Latest={latest_file}, Previous={previous_file}")
        
        # Download both files
        for filename in [latest_file, previous_file]:
            logging.info(f"Downloading file: {filename}")
            s3_client.download_file(DO_SPACE_NAME, filename, filename)
            
        # Read CSVs into dataframes
        logging.info("Reading CSV files into dataframes")
        latest_df = pd.read_csv(latest_file)
        previous_df = pd.read_csv(previous_file)
        
        # Standardize date formats
        logging.info("Standardizing date formats")
        # Convert dates to datetime objects for consistent comparison
        latest_df['date'] = pd.to_datetime(latest_df['date'])
        previous_df['date'] = pd.to_datetime(previous_df['date'])
        
        # Standardize column names (band vs artist)
        if 'band' in latest_df.columns:
            latest_df = latest_df.rename(columns={'band': 'artist'})
        if 'band' in previous_df.columns:
            previous_df = previous_df.rename(columns={'band': 'artist'})
            
        logging.info(f"Latest file rows: {len(latest_df)}, Previous file rows: {len(previous_df)}")
        
        # Find new concerts
        logging.info("Comparing dataframes to find new concerts")
        new_concerts = pd.merge(
            latest_df, 
            previous_df,
            on=['artist', 'venue', 'date', 'city', 'state', 'country'],
            how='left',
            indicator=True
        )
        new_concerts = new_concerts[new_concerts['_merge'] == 'left_only']
        new_concerts = new_concerts.drop('_merge', axis=1)
        
        if len(new_concerts) > 0:
            # Format the date properly for the filename
            current_date = datetime.now().strftime('%Y_%m_%d')
            output_file = f'new_concerts_{current_date}.csv'
            
            # Reorder and select only needed columns
            new_concerts = new_concerts[['date', 'artist', 'venue', 'city', 'state']]
            
            # Save locally first
            new_concerts.to_csv(output_file, index=False)
            
            # Upload to Digital Ocean
            logging.info(f"Uploading new concerts file to Digital Ocean: {output_file}")
            with open(output_file, 'rb') as file_data:
                s3_client.put_object(
                    Bucket=DO_SPACE_NAME,
                    Key=output_file,
                    Body=file_data,
                    ACL='public-read',
                    ContentType='text/csv'
                )
            
            # Clean up local file
            os.remove(output_file)
            
            logging.info(f"New concerts file uploaded: https://{DO_SPACE_NAME}.{DO_SPACE_REGION}.digitaloceanspaces.com/{output_file}")
            print(f"Found {len(new_concerts)} new concerts")
        else:
            logging.info("No new concerts found")
            
        # Clean up downloaded files
        logging.info("Cleaning up downloaded files")
        os.remove(latest_file)
        os.remove(previous_file)
        
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    compare_concert_files()
