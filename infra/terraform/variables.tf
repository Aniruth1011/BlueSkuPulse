variable "project_id" {
  description = "GCP project that owns AtmosPulse resources."
  type        = string
}

variable "region" {
  description = "Default regional location for GCS and provider operations."
  type        = string
  default     = "us-west1"
}

variable "bigquery_location" {
  description = "BigQuery dataset location. Keep this compatible with the GCS region."
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Deployment boundary used in names and labels."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "resource_prefix" {
  description = "Globally unique, lowercase prefix for bucket names."
  type        = string
  default     = "atmospulse"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,39}$", var.resource_prefix))
    error_message = "resource_prefix must be 3-40 lowercase letters, digits, or hyphens."
  }
}

variable "bronze_retention_days" {
  description = "Days before immutable Bronze objects are deleted."
  type        = number
  default     = 365

  validation {
    condition     = var.bronze_retention_days >= 30
    error_message = "Bronze retention must be at least 30 days."
  }
}

variable "silver_retention_days" {
  description = "Days before replaceable Silver objects are deleted."
  type        = number
  default     = 90

  validation {
    condition     = var.silver_retention_days >= 7
    error_message = "Silver retention must be at least 7 days."
  }
}

variable "force_destroy_nonprod" {
  description = "Allow Terraform to delete non-production buckets that still contain objects."
  type        = bool
  default     = false
}

variable "labels" {
  description = "Additional labels merged onto supported resources."
  type        = map(string)
  default     = {}
}

variable "alert_email" {
  description = "Optional email notification channel. Empty means policies have no channel."
  type        = string
  default     = ""
}

variable "secret_names" {
  description = "Secret containers to create. Terraform never creates secret versions or values."
  type        = set(string)
  default     = ["alert-webhook", "grafana-admin-password"]
}

