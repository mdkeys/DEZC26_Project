variable "credentials_file" {
  description = "Path to your GCP service account key JSON file"
  type        = string
}

variable "project_id" {
  description = "Your GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-east1"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket (must be globally unique)"
  type        = string
}

variable "bq_raw_dataset" {
  description = "BigQuery dataset for raw external tables"
  type        = string
  default     = "nyc_311_raw"
}

variable "bq_prod_dataset" {
  description = "BigQuery dataset for dbt-managed production tables"
  type        = string
  default     = "nyc_311_prod"
}
