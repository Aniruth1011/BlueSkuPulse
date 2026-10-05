resource "google_bigquery_dataset" "datasets" {
  for_each = local.datasets

  project                    = var.project_id
  dataset_id                 = each.value
  friendly_name              = "AtmosPulse ${title(each.key)} (${var.environment})"
  description                = "AtmosPulse ${each.key} dataset. Relation ownership is documented in infra/terraform/README.md."
  location                   = var.bigquery_location
  delete_contents_on_destroy = var.environment == "prod" ? false : var.force_destroy_nonprod
  labels                     = merge(local.labels, { layer = each.key })

  depends_on = [google_project_service.required]
}

resource "google_bigquery_table" "reconciliation_runs" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.datasets["monitoring"].dataset_id
  table_id            = "reconciliation_runs"
  deletion_protection = var.environment == "prod"
  description         = "Immutable audit records emitted by Bronze/Silver/Gold reconciliation jobs."
  labels              = merge(local.labels, { purpose = "reconciliation" })

  time_partitioning {
    type  = "DAY"
    field = "run_at"
  }

  clustering = ["status", "interval_start"]
  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "run_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "interval_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "interval_end", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "bronze_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "silver_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "gold_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "delta_ratio", type = "FLOAT", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED" },
    { name = "details_json", type = "JSON", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "benchmark_results" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.datasets["monitoring"].dataset_id
  table_id            = "benchmark_results"
  deletion_protection = var.environment == "prod"
  description         = "Versioned benchmark evidence; TARGET rows are not achieved claims."
  labels              = merge(local.labels, { purpose = "benchmarking" })

  time_partitioning {
    type  = "DAY"
    field = "run_at"
  }

  clustering = ["benchmark_name", "git_commit", "status"]
  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "run_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "benchmark_name", type = "STRING", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED" },
    { name = "git_commit", type = "STRING", mode = "REQUIRED" },
    { name = "environment_json", type = "JSON", mode = "REQUIRED" },
    { name = "parameters_json", type = "JSON", mode = "REQUIRED" },
    { name = "measurements_json", type = "JSON", mode = "REQUIRED" },
  ])
}

