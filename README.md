# Event Summary Batch Job – Cloud Run
**Cloud Run Job** that aggregates JSON event data from **GCS** into hourly CSV summaries and uploads results to **Cloud Storage** and **BigQuery**. Scheduled via **Cloud Scheduler** with **CI/CD** through **Cloud Build**.

---

## Features

* Runs daily using **Cloud Scheduler**
* Reads raw JSON event data older than 24 hours from `events/YYYY/MM/DD/` in GCS
* Aggregates events by hour and event type (e.g., `click`, `view`, `purchase`)
* Writes hourly summaries to `summary/YYYY/MM/DD/` in GCS
* Loads data into a partitioned BigQuery table
* Ensures no duplicate data is loaded (based on event hour)
* Supports **CI/CD** deployment with Cloud Build on Git tags (e.g., v1.0.0)

---

## Project Structure

```bash
.
├── main.py                     # main script for Cloud Run job
├── requirements.txt            # Python dependencies
├── cloudbuild.yaml             # Cloud Build config for CI/CD
├── bigquery_schema.json        # BigQuery schema definition
├── test_main.py                # unit tests
├── dockerfile 	                # dockerfile with container specs
└── .dockerignore
├── event_data_generator.py     # Not required - this is used for generating sample source data

```

---

## System Workflow

