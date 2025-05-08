import os
import random
import json
from datetime import datetime, timedelta
from google.cloud import storage

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev

# CONFIGURATION
BUCKET_NAME = "event-data-raw"  # <-- Replace this with your bucket name
NUM_EVENTS = 20
DATE = datetime.now() - timedelta(days=1)  # Use yesterday's date
PREFIX = DATE.strftime("events/%Y/%m/%d/")

EVENT_TYPES = ["click", "view", "purchase"]

def generate_event(timestamp, event_type):
    return {
        "timestamp": timestamp.isoformat(),
        "type": event_type
    }

def upload_sample_events():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for i in range(NUM_EVENTS):
        # Generate random time in the past 24 hours
        delta = timedelta(minutes=random.randint(0, 1439))
        event_time = DATE.replace(hour=0, minute=0, second=0, microsecond=0) + delta
        event_type = random.choice(EVENT_TYPES)
        event = generate_event(event_time, event_type)

        # Create unique file name
        file_name = f"{PREFIX}event_{i+1:03d}.json"
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(event), content_type="application/json")

        print(f"Uploaded: {file_name} | {event}")

if __name__ == "__main__":
    upload_sample_events()
