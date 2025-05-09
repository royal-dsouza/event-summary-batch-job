import os
import io
import csv
import json
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from collections import defaultdict

start_time = datetime.now()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev

# Environment variables
SOURCE_BUCKET_NAME = os.environ.get('SOURCE_BUCKET', 'event-data-raw')
SUMMARY_BUCKET_NAME = os.environ.get('SUMMARY_BUCKET', 'event-data-summary')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET','platform_event_data')
BIGQUERY_TABLE = os.environ.get('BIGQUERY_TABLE','event_hourly_summary')
BIGQUERY_STG_TABLE = os.environ.get('BIGQUERY_STG_TABLE','event_hourly_summary_staging')
HOURS_THRESHOLD = int(os.environ.get('HOURS_THRESHOLD', '0'))

# Setup client
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# bucket and table reference
SOURCE_BUCKET = storage_client.bucket(SOURCE_BUCKET_NAME)
SUMMARY_BUCKET = storage_client.bucket(SUMMARY_BUCKET_NAME)
STG_TABLE_REF = bigquery_client.dataset(BIGQUERY_DATASET).table(BIGQUERY_STG_TABLE)
PRD_TABLE_REF = bigquery_client.dataset(BIGQUERY_DATASET).table(BIGQUERY_TABLE)

def list_old_json_blobs(bucket, prefix, min_age_hours):
    """List all JSON files older than hours_threshold in the bucket with prefix."""

    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=min_age_hours)
    blobs = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith(".json") and blob.updated < cutoff_time:
            blobs.append(blob)

    logger.info(f"Found {len(blobs)} JSON files to process")
    return blobs

def aggregate_events(blobs):
    """Process JSON files and return hourly aggregated data."""

    # First pass: count per hour and event type
    raw_summary = defaultdict(lambda: defaultdict(int))
    all_event_types = set()
    processed_count = 0
        
    for blob in blobs:
        try:
            content = json.loads(blob.download_as_text())
            event_time = datetime.fromisoformat(content['timestamp'])
            hour = event_time.strftime('%Y-%m-%dT%H:00:00')
            event_type = content['type']
            raw_summary[hour][event_type] += 1
            all_event_types.add(event_type)
            processed_count += 1
            
            # Log progress for large datasets
            if processed_count % 1000 == 0:
                logger.info(f"Processed {processed_count} events so far")
                
        except Exception as e:
            logger.error(f"Error processing blob {blob.name}: {str(e)}")
            continue

    logger.info(f"Successfully processed {processed_count} events")
    logger.info(f"Found {len(all_event_types)} unique event types: {all_event_types}")

    # Second pass: build consistent rows with all event types
    # Prepare CSV output
    sorted_event_types = sorted(all_event_types)
    header = ['hour'] + [f"{event_type}_event_count" for event_type in sorted_event_types]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()

    for hour, counts in sorted(raw_summary.items()):
        row = {"hour": hour}
        for event_type in sorted_event_types:
            row[f"{event_type}_event_count"] = counts.get(event_type, 0)
        writer.writerow(row)

    csv_content = output.getvalue()
    output.close()

    logger.info(f"There are {len(raw_summary)} rows in the summary")
    return csv_content

def write_summary_to_gcs(bucket, summary_data, date):
    """Save hourly data to CSV files in the specified bucket."""

    date_path = date.strftime("summary/%Y/%m/%d/")
    filename = f"hourly_summary_{date.strftime('%Y-%m-%d')}.csv"
    blob_path = f"{date_path}{filename}"

    blob = bucket.blob(blob_path)

    blob.upload_from_string(summary_data, content_type="text/csv")

    gcs_uri = f"gs://{bucket.name}/{blob_path}"

    logger.info(f"Uploaded summary to: {gcs_uri}")
    return gcs_uri

def load_to_bigquery_staging(gcs_uri, table_ref):
    """Load hourly data to BigQuery Staging table"""

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )

    logger.info(f"Starting BQ load job: {load_job.job_id}")
    load_job.result()  # Wait for job to complete
    logger.info(f"Loaded data from {gcs_uri} into staging table: {table_ref.table_id}")

def merge_staging_to_main(dataset_id, staging_table_id, main_table_id):
    """Merge data from staging to main table and avoid loading duplicates."""

    staging = f"{dataset_id}.{staging_table_id}"
    main = f"{dataset_id}.{main_table_id}"

    merge_query = f"""
    MERGE `{main}` T
    USING `{staging}` S
    ON T.hour = S.hour
    WHEN MATCHED THEN
      UPDATE SET
        click_event_count = S.click_event_count,
        view_event_count = S.view_event_count,
        purchase_event_count = S.purchase_event_count
    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    query_job = bigquery_client.query(merge_query)
    query_job.result()
    logger.info(f"Merged data from {staging} into {main}")

def main():
    """main function"""

    # Determine the date prefix for yesterday
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    prefix = yesterday.strftime("events/%Y/%m/%d/")

    blobs = list_old_json_blobs(SOURCE_BUCKET, prefix, HOURS_THRESHOLD)

    if not blobs:
        logger.info("No JSON blobs found older than {min_age_hours} hours.")
        logger.info(f"script completed successfully in {datetime.now() - start_time}")
        return "No data to process", 200
    
    summary = aggregate_events(blobs)
    gcs_uri = write_summary_to_gcs(SUMMARY_BUCKET, summary, yesterday)
    load_to_bigquery_staging(gcs_uri, STG_TABLE_REF)
    merge_staging_to_main(BIGQUERY_DATASET, BIGQUERY_STG_TABLE, BIGQUERY_TABLE)

    logger.info(f"Job completed successfully in {(datetime.now() - start_time).total_seconds():.2f} seconds")


if __name__ == "__main__":
    main()





