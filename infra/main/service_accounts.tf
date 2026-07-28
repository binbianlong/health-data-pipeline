resource "google_service_account" "service_runtime" {
  project      = var.project_id
  account_id   = "hdp-service"
  display_name = "Health data pipeline Cloud Run service"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

resource "google_service_account" "job_runtime" {
  project      = var.project_id
  account_id   = "hdp-job"
  display_name = "Health data pipeline Cloud Run job"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

resource "google_service_account" "task_caller" {
  project      = var.project_id
  account_id   = "hdp-tasks"
  display_name = "Health data pipeline Cloud Tasks caller"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

resource "google_service_account" "scheduler_caller" {
  project      = var.project_id
  account_id   = "hdp-scheduler"
  display_name = "Health data pipeline Cloud Scheduler caller"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

resource "google_service_account" "cloud_build" {
  project      = var.project_id
  account_id   = "hdp-cloud-build"
  display_name = "Health data pipeline Cloud Build"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

resource "google_service_account" "api_gateway" {
  project      = var.project_id
  account_id   = "hdp-api-gateway"
  display_name = "Health data pipeline API Gateway backend"

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

resource "google_project_iam_member" "runtime_monitoring" {
  for_each = {
    service = google_service_account.service_runtime.email
    job     = google_service_account.job_runtime.email
  }

  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "runtime_logging" {
  for_each = {
    service = google_service_account.service_runtime.email
    job     = google_service_account.job_runtime.email
  }

  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${each.value}"
}

locals {
  cloud_build_project_roles = toset([
    "roles/apigateway.admin",
    "roles/artifactregistry.admin",
    "roles/cloudscheduler.admin",
    "roles/cloudtasks.admin",
    "roles/iam.securityAdmin",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.workloadIdentityPoolAdmin",
    "roles/logging.admin",
    "roles/monitoring.admin",
    "roles/run.admin",
    "roles/secretmanager.admin",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/storage.admin",
  ])
}

resource "google_project_iam_member" "cloud_build" {
  for_each = local.cloud_build_project_roles

  project = var.project_id
  role    = each.value
  member  = local.cloud_build_member
}

resource "google_service_account_iam_member" "cloud_build_runtime_act_as" {
  for_each = {
    gateway   = google_service_account.api_gateway.name
    service   = google_service_account.service_runtime.name
    job       = google_service_account.job_runtime.name
    tasks     = google_service_account.task_caller.name
    scheduler = google_service_account.scheduler_caller.name
  }

  service_account_id = each.value
  role               = "roles/iam.serviceAccountUser"
  member             = local.cloud_build_member
}

resource "google_service_account_iam_member" "runtime_can_use_task_caller" {
  for_each = {
    service = google_service_account.service_runtime.email
    job     = google_service_account.job_runtime.email
  }

  service_account_id = google_service_account.task_caller.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${each.value}"
}
