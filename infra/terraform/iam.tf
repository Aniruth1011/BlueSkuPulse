locals {
  service_accounts = {
    ingestion    = "Reads Jetstream and writes only to the broker; emits operational metrics."
    processor    = "Writes Bronze/Silver objects and loads BigQuery staging tables."
    orchestrator = "Runs bounded Airflow jobs and dbt transformations."
    dashboard    = "Read-only identity for analytical dashboards."
  }
}

resource "google_service_account" "workloads" {
  for_each = local.service_accounts

  project      = var.project_id
  account_id   = "ap-${substr(sha1(local.name_prefix), 0, 8)}-${each.key}"
  display_name = "AtmosPulse ${title(each.key)} (${var.environment})"
  description  = each.value

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "metrics_writer" {
  for_each = toset(["ingestion", "processor", "orchestrator"])

  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.workloads[each.value].email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  for_each = toset(["processor", "orchestrator"])

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.workloads[each.value].email}"
}

resource "google_storage_bucket_iam_member" "bronze_processor_creator" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.workloads["processor"].email}"
}

resource "google_storage_bucket_iam_member" "bronze_processor_viewer" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workloads["processor"].email}"
}

resource "google_storage_bucket_iam_member" "bronze_orchestrator_viewer" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workloads["orchestrator"].email}"
}

resource "google_storage_bucket_iam_member" "silver_processor_admin" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.workloads["processor"].email}"
}

resource "google_storage_bucket_iam_member" "silver_orchestrator_viewer" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workloads["orchestrator"].email}"
}

resource "google_bigquery_dataset_iam_member" "staging_processor_editor" {
  dataset_id = google_bigquery_dataset.datasets["staging"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.workloads["processor"].email}"
}

resource "google_bigquery_dataset_iam_member" "staging_orchestrator_viewer" {
  dataset_id = google_bigquery_dataset.datasets["staging"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.workloads["orchestrator"].email}"
}

resource "google_bigquery_dataset_iam_member" "analytics_orchestrator_editor" {
  dataset_id = google_bigquery_dataset.datasets["analytics"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.workloads["orchestrator"].email}"
}

resource "google_bigquery_dataset_iam_member" "analytics_dashboard_viewer" {
  dataset_id = google_bigquery_dataset.datasets["analytics"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.workloads["dashboard"].email}"
}

resource "google_bigquery_dataset_iam_member" "monitoring_orchestrator_editor" {
  dataset_id = google_bigquery_dataset.datasets["monitoring"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.workloads["orchestrator"].email}"
}

resource "google_bigquery_dataset_iam_member" "monitoring_dashboard_viewer" {
  dataset_id = google_bigquery_dataset.datasets["monitoring"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.workloads["dashboard"].email}"
}
