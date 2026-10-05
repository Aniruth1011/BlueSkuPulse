resource "google_monitoring_notification_channel" "email" {
  count = var.alert_email == "" ? 0 : 1

  project      = var.project_id
  display_name = "AtmosPulse ${var.environment} operations"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }

  depends_on = [google_project_service.required]
}

resource "google_monitoring_dashboard" "platform" {
  project = var.project_id
  dashboard_json = jsonencode({
    displayName = "AtmosPulse ${var.environment} - cloud resources"
    mosaicLayout = {
      columns = 12
      tiles = [
        {
          width  = 12
          height = 2
          widget = {
            text = {
              content = "## AtmosPulse ${var.environment}\nCloud resource health. Pipeline-level SLOs are exported through the repository's Prometheus/Grafana stack."
              format  = "MARKDOWN"
            }
          }
        },
        {
          width  = 6
          height = 4
          widget = {
            title = "BigQuery query count"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"bigquery.googleapis.com/query/count\" resource.type=\"global\""
                    aggregation = {
                      alignmentPeriod  = "60s"
                      perSeriesAligner = "ALIGN_RATE"
                    }
                  }
                }
              }]
              yAxis = { label = "queries/s", scale = "LINEAR" }
            }
          }
        },
        {
          xPos   = 6
          width  = 6
          height = 4
          widget = {
            title = "GCS request count"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"storage.googleapis.com/api/request_count\" resource.type=\"gcs_bucket\""
                    aggregation = {
                      alignmentPeriod  = "60s"
                      perSeriesAligner = "ALIGN_RATE"
                    }
                  }
                }
              }]
              yAxis = { label = "requests/s", scale = "LINEAR" }
            }
          }
        },
      ]
    }
  })

  depends_on = [google_project_service.required]
}

