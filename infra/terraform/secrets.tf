resource "google_secret_manager_secret" "configuration" {
  for_each = var.secret_names

  project   = var.project_id
  secret_id = "${local.name_prefix}-${each.value}"
  labels    = local.labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_iam_member" "orchestrator_access" {
  for_each = google_secret_manager_secret.configuration

  project   = var.project_id
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.workloads["orchestrator"].email}"
}

