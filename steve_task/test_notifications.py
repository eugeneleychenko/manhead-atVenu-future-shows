import os
import json
import pandas as pd
from datetime import datetime, timedelta
import logging

# Test data - add more concerts as needed
TEST_CONCERTS = [
    {
        "date": "2025-01-15",
        "band": "Air Supply", 
        "city": "Philadelphia",
        "state": "NY",
        "venue": "Wells Fargo Center",
        "country": "United States",
        "first_seen": "2025-01-12"  # Seen a week ago
    },
    {
        "date": "2025-02-08",
        "band": "Air Supply",
        "city": "Albany", 
        "state": "NY",
        "venue": "Times Union Center",
        "country": "United States",
        "first_seen": "2025-01-20"  # New today
    },
    {
        "date": "2025-02-14",
        "band": "Air Supply",
        "city": "New York",
        "state": "NY", 
        "venue": "Madison Square Garden",
        "country": "United States",
        "first_seen": "2025-01-19"  # Seen yesterday
    },
    {
        "date": "2025-02-15",
        "band": "Air Supply",
        "city": "Boston",
        "state": "MA",
        "venue": "TD Garden", 
        "country": "United States",
        "first_seen": "2025-01-20"  # New today
    },
    {
        "date": "2025-02-20",
        "band": "Air Supply",
        "city": "Buffalo",
        "state": "NY",
        "venue": "KeyBank Center",
        "country": "United States",
        "first_seen": "2025-01-20"  # New today
    }
]

# Add more test concerts for different dates/states

def test_notification_logic():
    logging.basicConfig(level=logging.INFO)
    
    # Create test DataFrame
    df = pd.DataFrame(TEST_CONCERTS)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["first_seen"] = pd.to_datetime(df["first_seen"]).dt.date
    
    # Define test recipients
    recipients = {
        'chris': {
            'states': ['NY', 'NJ'],
            'email': 'nycspicebo+chris@gmail.com'
        },
        'steve': {
            'states': ['NY', 'NJ', 'MA', 'CT', 'NC'],
            'email': 'nycspicebo+steve@gmail.com'
        }
    }
    
    # Process notifications
    notifications = {}
    for recipient, config in recipients.items():
        recipient_states = config['states']
        
        # Filter for recipient's states
        relevant_shows = df[df['state'].isin(recipient_states)]
        new_shows = relevant_shows[relevant_shows["first_seen"] == datetime.now().date()]
        
        if len(new_shows) > 0:
            # Convert dates to strings before JSON serialization
            new_concerts = new_shows.to_dict('records')
            all_upcoming = relevant_shows.to_dict('records')
            
            # Convert date if it's not already a string
            for concert in new_concerts:
                if not isinstance(concert['date'], str):
                    concert['date'] = concert['date'].strftime('%Y-%m-%d')
                if not isinstance(concert['first_seen'], str):
                    concert['first_seen'] = concert['first_seen'].strftime('%Y-%m-%d')
            
            for concert in all_upcoming:
                if not isinstance(concert['date'], str):
                    concert['date'] = concert['date'].strftime('%Y-%m-%d')
                if not isinstance(concert['first_seen'], str):
                    concert['first_seen'] = concert['first_seen'].strftime('%Y-%m-%d')
            
            notifications[recipient] = {
                'email': config['email'],
                'new_concerts': new_concerts,
                'all_upcoming': all_upcoming
            }
    
    # Save test output
    with open('test_notifications.json', 'w') as f:
        json.dump(notifications, f, indent=2)
    
    # Print summary
    logging.info("\nTest Results:")
    for recipient, data in notifications.items():
        logging.info(f"\n{recipient.upper()}:")
        logging.info(f"Email: {data['email']}")
        logging.info(f"New concerts: {len(data['new_concerts'])}")
        for concert in data['new_concerts']:
            logging.info(f"- {concert['band']} at {concert['venue']} on {concert['date']}")
        logging.info(f"Total upcoming concerts in their states: {len(data['all_upcoming'])}")

if __name__ == "__main__":
    test_notification_logic()