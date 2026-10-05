locals {
  name_prefix = "${var.resource_prefix}-${var.environment}"
  labels = merge(
    {
      application = "atmospulse"
      environment = var.environment
      managed_by  = "terraform"
    },
    var.labels,
  )
  datasets = {
    staging    = "atmospulse_${var.environment}_staging"
    analytics  = "atmospulse_${var.environment}_analytics"
    monitoring = "atmospulse_${var.environment}_monitoring"
  }
}

