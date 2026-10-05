resource "google_storage_bucket" "bronze" {
  name                        = "${local.name_prefix}-bronze"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.environment == "prod" ? false : var.force_destroy_nonprod
  labels                      = merge(local.labels, { layer = "bronze" })

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = var.bronze_retention_days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket" "silver" {
  name                        = "${local.name_prefix}-silver"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.environment == "prod" ? false : var.force_destroy_nonprod
  labels                      = merge(local.labels, { layer = "silver" })

  lifecycle_rule {
    condition {
      age = var.silver_retention_days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required]
}

