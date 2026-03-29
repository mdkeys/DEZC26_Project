**About**

Kestra (running locally via Docker) orchestrates the data pipeline: It executes extract_311.py which pulls NYC 311 service request data from the Socrata API month by month, converts it to Parquet files, and uploads them to GCS partitioned by year and month. Kestra then creates a BigQuery external table in nyc_311_raw that points directly at those Parquet files in GCS, making the raw data queryable without loading it into BigQuery storage. 

**Kestra Files**

1. 'docker-compose.yml' creates two containers:
   1. Postgres (Kestra's backend database)
   2. Kestra
2. 'nyc_311_ingestion.yml' is a file under '02_kestra/flows/' with 3 tasks and 2 run modes:
   1. Tasks:
      1. Task 1 (`extract_and_upload`): fetches data from the Socrata API and uploads Parquet files to GCS
      2. Task 2 (`create_external_table`): creates or replaces the BigQuery external table pointing at the GCS data
      3. Task 3 (`log_success`): logs completion details
   2. Modes:
      1. Mode 1 - Backfill mode: runs with no inputs, processes all months from January 2020 to the last completed month
      2. Mode 2 - Single month mode: passes `year` + `month` inputs, great for testing or reruns
         1. The monthly schedule trigger was initially set to `disabled: true` — enable it (set to `false`) once the backfill is complete.