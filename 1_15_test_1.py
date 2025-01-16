# This script uploads concert show data to Digital Ocean Spaces cloud storage
# It does the following:
# 1. Sets up an S3 client to connect to Digital Ocean Spaces
# 2. Gets show data for the next 365 days using fetch_shows()
# 3. Creates a pandas DataFrame with the show data
# 4. Formats the date column and selects specific columns
# 5. Saves the data to a CSV file named with today's date
# 6. Uploads the CSV file to Digital Ocean Spaces with public read access


import os
import boto3
from datetime import datetime, timedelta
from MH_future_shows import fetch_shows
import pandas as pd

# Constants
DO_SPACE_REGION = 'nyc3'
DO_SPACE_NAME = 'mh-upcoming-concerts'

def upload_shows_to_do():
    try:
        # Set up S3 client for Digital Ocean Spaces
        s3_client = boto3.client('s3',
                               region_name=DO_SPACE_REGION,
                               endpoint_url=f'https://{DO_SPACE_REGION}.digitaloceanspaces.com',
                               aws_access_key_id='DO00WDV2DH8U9KN8WPWG',
                               aws_secret_access_key='2/FXMxY/zX5n+VtFsODjYedO+4acJElNIv9zJDgH4r4')

        # Get dates for full year
        start_date = datetime.now()
        end_date = start_date + timedelta(days=365)

        # Fetch shows data
        shows = fetch_shows(start_date, end_date)

        if not shows:
            print("No shows found for the date range")
            return False

        # Create DataFrame
        df = pd.DataFrame(shows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[["date", "band", "city", "state", "venue", "country"]]

        # Generate CSV filename with date only (YYYY-MM-DD)
        filename = f"{datetime.now().strftime('%Y-%m-%d')}.csv"

        # Save to temporary file
        temp_file = filename
        df.to_csv(temp_file, index=True)

        # Upload to Digital Ocean (directly to root, not in exports/)
        key = filename  # Removed 'exports/' prefix
        with open(temp_file, 'rb') as file_data:
            s3_client.put_object(Bucket=DO_SPACE_NAME,
                               Key=key,
                               Body=file_data,
                               ACL='public-read',
                               ContentType='text/csv')

        # Clean up temp file
        os.remove(temp_file)

        print(f"Successfully uploaded shows to: https://{DO_SPACE_NAME}.{DO_SPACE_REGION}.digitaloceanspaces.com/{key}")
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        return False

if __name__ == "__main__":
    upload_shows_to_do()
