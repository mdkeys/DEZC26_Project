terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ---------------------------------------------------------------
# GCS Bucket — Data Lake
# ---------------------------------------------------------------

resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true  # allows bucket deletion even if it contains files

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90  # auto-delete objects older than 90 days (cost protection)
    }
  }

  uniform_bucket_level_access = true
}

# ---------------------------------------------------------------
# BigQuery Datasets
# ---------------------------------------------------------------

# Raw dataset — external tables pointing at GCS Parquet files
resource "google_bigquery_dataset" "raw" {
  dataset_id    = var.bq_raw_dataset
  project       = var.project_id
  location      = var.region
  friendly_name = "NYC 311 Raw"
  description   = "External tables over raw Parquet files in GCS"

  delete_contents_on_destroy = true
}

# Production dataset — dbt-managed transformed tables
resource "google_bigquery_dataset" "prod" {
  dataset_id    = var.bq_prod_dataset
  project       = var.project_id
  location      = var.region
  friendly_name = "NYC 311 Production"
  description   = "Transformed tables managed by dbt"

  delete_contents_on_destroy = true
}
