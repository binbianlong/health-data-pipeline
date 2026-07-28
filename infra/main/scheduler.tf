resource "google_cloud_scheduler_job" "hourly_sync" {
  count = var.deploy_workloads ? 1 : 0

  project          = var.project_id
  region           = var.region
  name             = local.scheduler_name
  description      = "Hourly recovery and reconciliation for the health data pipeline"
  schedule         = "0 * * * *"
  time_zone        = "Asia/Tokyo"
  attempt_deadline = "180s"
  paused           = var.scheduler_paused

  retry_config {
    retry_count          = 3
    max_retry_duration   = "900s"
    min_backoff_duration = "10s"
    max_backoff_duration = "300s"
    max_doublings        = 3
  }

  http_target {
    uri         = local.job_run_uri
    http_method = "POST"
    body        = base64encode("{}")

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.scheduler_caller.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_project_service.required["cloudscheduler.googleapis.com"],
    google_cloud_run_v2_job_iam_member.scheduler_caller,
  ]
}
