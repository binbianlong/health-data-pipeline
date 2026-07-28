resource "google_cloud_tasks_queue" "dispatch" {
  project  = var.project_id
  location = var.region
  name     = local.queue_name

  rate_limits {
    max_dispatches_per_second = 1
    max_concurrent_dispatches = 1
  }

  retry_config {
    max_attempts       = 10
    max_retry_duration = "3600s"
    min_backoff        = "10s"
    max_backoff        = "300s"
    max_doublings      = 5
  }

  stackdriver_logging_config {
    sampling_ratio = 1
  }

  depends_on = [google_project_service.required["cloudtasks.googleapis.com"]]
}

resource "google_cloud_tasks_queue_iam_member" "service_enqueuer" {
  project  = var.project_id
  location = google_cloud_tasks_queue.dispatch.location
  name     = google_cloud_tasks_queue.dispatch.name
  role     = "roles/cloudtasks.enqueuer"
  member   = "serviceAccount:${google_service_account.service_runtime.email}"
}

resource "google_cloud_tasks_queue_iam_member" "job_enqueuer" {
  project  = var.project_id
  location = google_cloud_tasks_queue.dispatch.location
  name     = google_cloud_tasks_queue.dispatch.name
  role     = "roles/cloudtasks.enqueuer"
  member   = "serviceAccount:${google_service_account.job_runtime.email}"
}
