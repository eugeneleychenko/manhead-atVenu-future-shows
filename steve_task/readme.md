Concert Notification System Setup Guide

1. Python Scripts Setup
   a. Main Script (concert_tracker.py)

b. Test Script (test_notifications.py)

2. Testing Steps

First, test locally:
bashCopy# Run the test script
python test_notifications.py

# Check the output:

# - Look at console output

# - Examine test_notifications.json

Test DO Space integration:
bashCopy# Run the main script
python concert_tracker.py

# Check DO Space for:

# - latest_concerts.csv

# - notifications_ready.json

3. Zapier Setup

Create a new Zap:

Trigger: Schedule
Set to run daily after your script runs (e.g., 9 AM)

Add an action to fetch the notifications JSON:

Action: Webhooks by Zapier
Action Type: GET
URL: https://mh-upcoming-concerts.nyc3.digitaloceanspaces.com/notifications_ready.json

Add a Formatter step:

Action: Formatter by Zapier
Action Type: Utilities
Transform: JSON Parse
Input: The response from step 2

Add a Filter step:

Only continue if the JSON is not empty

Add an Email action for each recipient:

For Chris:

To: chris@email.com
Subject: "New Concert Updates - {date}"
Body Template:
CopyNew Concerts in Your Area:
{{step3.chris.new_concerts}}

All Upcoming Concerts in Your Area:
{{step3.chris.all_upcoming}}

For Steve:

To: steve@email.com
Subject: "New Concert Updates - {date}"
Body Template:
CopyNew Concerts in Your Area:
{{step3.steve.new_concerts}}

All Upcoming Concerts in Your Area:
{{step3.steve.all_upcoming}}

4. Monitoring
   The script includes logging, so you can:

Check the Python script logs for errors
Monitor the DO Space for file updates
Test the Zapier workflow manually

5. Maintenance
   Regular tasks:

Monitor disk usage in /tmp
Check DO Space usage
Verify email deliverability
Update recipient configurations as needed
