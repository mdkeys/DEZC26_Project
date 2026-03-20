"""
extract_311.py

Extracts NYC 311 Service Request data from the Socrata API month by month,
converts each month to Parquet, and uploads to GCS.

Usage:
    # Backfill from 2020 to present:
    python extract_311.py

    # Single month (for testing or reruns):
    python extract_311.py --year 2022 --month 6

Requirements:
    pip install requests pandas pyarrow python-dotenv google-cloud-storage
"""

import os
import io
import argparse
import logging
from datetime import date
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
GCS_BUCKET_NAME   = os.getenv("GCS_BUCKET_NAME")        # e.g. "nyc-311-de-project"
GCS_PREFIX        = os.getenv("GCS_PREFIX", "nyc_311")  # e.g. "nyc_311"

# ---------------------------------------------------------------------------
# GCP auth: supports both a JSON string (Kestra secret) and a file path
# ---------------------------------------------------------------------------
def get_gcs_client() -> storage.Client:
    """
    Returns a GCS client. Supports two auth methods:
    1. GCP_SERVICE_ACCOUNT env var — JSON string of the service account key
       (used when running inside Kestra, passed as a decoded secret)
    2. GOOGLE_APPLICATION_CREDENTIALS env var — path to a local JSON key file
       (used when running locally)
    """
    import json
    from google.oauth2 import service_account

    sa_json = os.getenv("GCP_SERVICE_ACCOUNT")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return storage.Client(credentials=creds, project=info.get("project_id"))

    # Fall back to GOOGLE_APPLICATION_CREDENTIALS file path (local dev)
    return storage.Client()

DATASET_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE        = 50_000   # rows per API request
START_DATE       = date(2020, 1, 1)

COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "complaint_type",
    "descriptor",
    "descriptor_2",
    "location_type",
    "status",
    "agency",
    "borough",
    "resolution_description",
    "incident_zip",
    "community_board",
    "council_district",
    "police_precinct",
    "due_date",
    "city",
    "latitude",
    "longitude",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_month(year: int, month: int) -> pd.DataFrame:
    """
    Pull all rows for a given year/month from the Socrata API.
    Paginates automatically until all rows for that month are fetched.
    """
    start = date(year, month, 1)
    end   = start + relativedelta(months=1)

    where = (
        f"created_date >= '{start.isoformat()}T00:00:00.000' "
        f"AND created_date < '{end.isoformat()}T00:00:00.000'"
    )
    select = ", ".join(COLUMNS)

    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN
    else:
        log.warning("No SOCRATA_APP_TOKEN set — using anonymous access (lower rate limits)")

    all_rows = []
    offset   = 0

    while True:
        params = {
            "$select": select,
            "$where":  where,
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  "created_date ASC",
        }

        log.info(f"  Fetching rows {offset}–{offset + PAGE_SIZE} for {year}-{month:02d}...")
        response = requests.get(DATASET_ENDPOINT, headers=headers, params=params, timeout=60)
        response.raise_for_status()

        batch = response.json()
        if not batch:
            break  # no more rows

        all_rows.extend(batch)
        offset += PAGE_SIZE

        if len(batch) < PAGE_SIZE:
            break  # last page

    log.info(f"  → {len(all_rows):,} rows fetched for {year}-{month:02d}")
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=COLUMNS)


def cast_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast columns to appropriate types before writing to Parquet.
    Socrata returns everything as strings.
    """
    timestamp_cols = ["created_date", "closed_date", "due_date"]
    numeric_cols   = ["latitude", "longitude"]

    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def upload_to_gcs(df: pd.DataFrame, year: int, month: int) -> str:
    """
    Write DataFrame to Parquet in memory and upload to GCS.
    Path pattern: {GCS_PREFIX}/year=YYYY/month=MM/data.parquet
    """
    gcs_path = f"{GCS_PREFIX}/year={year}/month={month:02d}/data.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob   = bucket.blob(gcs_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    log.info(f"  → Uploaded to gs://{GCS_BUCKET_NAME}/{gcs_path}")
    return gcs_path


def month_range(start: date, end: date):
    """Yield (year, month) tuples from start up to (but not including) end."""
    current = start.replace(day=1)
    while current < end:
        yield current.year, current.month
        current += relativedelta(months=1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_month(year: int, month: int):
    log.info(f"Processing {year}-{month:02d}...")
    df = fetch_month(year, month)

    if df.empty:
        log.warning(f"  No data found for {year}-{month:02d}, skipping.")
        return

    df = cast_dtypes(df)
    upload_to_gcs(df, year, month)
    log.info(f"  Done: {year}-{month:02d}")


def main():
    parser = argparse.ArgumentParser(description="Extract NYC 311 data to GCS")
    parser.add_argument("--year",  type=int, help="Single year to process (use with --month)")
    parser.add_argument("--month", type=int, help="Single month to process (use with --year)")
    args = parser.parse_args()

    if args.year and args.month:
        # Single month mode — useful for testing or reruns
        process_month(args.year, args.month)
    else:
        # Backfill mode — loop from START_DATE to today
        today = date.today()
        end   = today.replace(day=1)  # don't pull the current in-progress month
        log.info(f"Starting backfill from {START_DATE} to {end}...")
        for year, month in month_range(START_DATE, end):
            process_month(year, month)
        log.info("Backfill complete.")


if __name__ == "__main__":
    main()
