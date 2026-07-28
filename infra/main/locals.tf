locals {
  service_name       = "${var.name_prefix}-service"
  job_name           = "${var.name_prefix}-sync"
  queue_name         = "${var.name_prefix}-dispatch"
  repository_name    = var.name_prefix
  scheduler_name     = "${var.name_prefix}-hourly"
  job_run_uri        = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${local.job_name}:run"
  project_number     = data.google_project.current.number
  service_url        = "https://${local.service_name}-${local.project_number}.${var.region}.run.app"
  cloud_build_member = "serviceAccount:${google_service_account.cloud_build.email}"

  required_services = toset([
    "artifactregistry.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudtasks.googleapis.com",
    "health.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
    "sts.googleapis.com",
  ])

  secret_names = toset([
    "google-oauth-client",
    "google-oauth-refresh-token",
    "notion-token",
    "application-hmac-key",
    "webhook-authorization",
    "oauth-session-aead-key",
  ])

  common_environment = {
    PROJECT_ID            = var.project_id
    GCP_PROJECT_ID        = var.project_id
    REGION                = var.region
    GCP_REGION            = var.region
    HEALTH_BUCKET         = google_storage_bucket.health.name
    TASKS_QUEUE           = google_cloud_tasks_queue.dispatch.name
    SYNC_JOB_NAME         = local.job_name
    SYNC_JOB_RUN_URI      = local.job_run_uri
    CLOUD_RUN_SERVICE_URL = local.service_url
    PUBLIC_BASE_URL       = local.service_url
    NOTION_DATA_SOURCE_ID = var.notion_data_source_id
    OWNER_TIME_ZONE       = "Asia/Tokyo"
  }

  secret_environment = {
    GOOGLE_OAUTH_CLIENT_SECRET_ID  = google_secret_manager_secret.secrets["google-oauth-client"].secret_id
    GOOGLE_OAUTH_REFRESH_SECRET_ID = google_secret_manager_secret.secrets["google-oauth-refresh-token"].secret_id
    NOTION_TOKEN_SECRET_ID         = google_secret_manager_secret.secrets["notion-token"].secret_id
    APPLICATION_HMAC_SECRET_ID     = google_secret_manager_secret.secrets["application-hmac-key"].secret_id
    WEBHOOK_AUTH_SECRET_ID         = google_secret_manager_secret.secrets["webhook-authorization"].secret_id
    OAUTH_SESSION_AEAD_SECRET_ID   = google_secret_manager_secret.secrets["oauth-session-aead-key"].secret_id
  }
}
