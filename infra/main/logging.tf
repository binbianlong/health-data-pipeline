resource "google_logging_project_exclusion" "oauth_callback_request" {
  project     = var.project_id
  name        = "${var.name_prefix}-oauth-callback-request"
  description = "Exclude OAuth callback request URLs containing code and state from Cloud Run and API Gateway logs in the _Default sink."
  disabled    = false

  filter = <<-EOT
    (
      resource.type="cloud_run_revision"
      log_id("run.googleapis.com/requests")
      httpRequest.requestUrl=~"/setup/oauth/callback([?].*)?$"
    )
    OR
    (
      resource.type="apigateway.googleapis.com/Gateway"
      httpRequest.requestUrl=~"/setup/oauth/callback([?].*)?$"
    )
    OR
    (
      resource.type="api"
      httpRequest.requestUrl=~"/setup/oauth/callback([?].*)?$"
    )
  EOT

  depends_on = [google_project_service.required["logging.googleapis.com"]]
}

resource "google_logging_metric" "class_a_warning" {
  project     = var.project_id
  name        = "${var.name_prefix}/class_a_warning"
  description = "Application warning that the monthly Class A estimate crossed a warning threshold."
  filter      = "resource.type=\"cloud_run_job\" AND jsonPayload.event=\"class_a_warning\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }

  depends_on = [google_project_service.required["logging.googleapis.com"]]
}

resource "google_logging_metric" "fitbit_sync_stale" {
  project     = var.project_id
  name        = "${var.name_prefix}/fitbit_sync_stale"
  description = "Application event emitted once when Fitbit cloud sync has been stale for five days."
  filter      = "resource.type=\"cloud_run_job\" AND jsonPayload.event=\"fitbit_sync_stale\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }

  depends_on = [google_project_service.required["logging.googleapis.com"]]
}
