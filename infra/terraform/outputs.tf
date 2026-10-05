output "bronze_bucket" {
  value = google_storage_bucket.bronze.name
}

output "silver_bucket" {
  value = google_storage_bucket.silver.name
}

output "bigquery_datasets" {
  value = { for key, dataset in google_bigquery_dataset.datasets : key => dataset.dataset_id }
}

output "service_account_emails" {
  value = { for key, account in google_service_account.workloads : key => account.email }
}

output "secret_ids" {
  value       = { for key, secret in google_secret_manager_secret.configuration : key => secret.secret_id }
  description = "Secret resource IDs only. Secret values are intentionally outside Terraform state."
}

