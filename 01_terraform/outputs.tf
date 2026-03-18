output "gcs_bucket_name" {
  description = "Name of the created GCS bucket"
  value       = google_storage_bucket.data_lake.name
}

output "gcs_bucket_url" {
  description = "GCS bucket URL"
  value       = google_storage_bucket.data_lake.url
}

output "bq_raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_prod_dataset" {
  description = "BigQuery production dataset ID"
  value       = google_bigquery_dataset.prod.dataset_id
}
