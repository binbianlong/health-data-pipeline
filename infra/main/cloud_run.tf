resource "google_cloud_run_v2_service" "api" {
  count = var.deploy_workloads ? 1 : 0

  project              = var.project_id
  name                 = local.service_name
  location             = var.region
  ingress              = "INGRESS_TRAFFIC_ALL"
  invoker_iam_disabled = true
  deletion_protection  = var.deletion_protection

  template {
    service_account = google_service_account.service_runtime.email
    timeout         = "300s"

    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }

    containers {
      name    = "service"
      image   = var.container_image
      command = ["health-data-pipeline-service"]

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle = true
      }

      ports {
        name           = "http1"
        container_port = 8080
      }

      dynamic "env" {
        for_each = merge(
          local.common_environment,
          local.secret_environment,
          {
            RUNTIME_KIND                       = "service"
            TASK_CALLER_SERVICE_ACCOUNT        = google_service_account.task_caller.email
            EXPECTED_TASK_CALLER_SERVICE_EMAIL = google_service_account.task_caller.email
          },
        )
        content {
          name  = env.key
          value = env.value
        }
      }

      startup_probe {
        initial_delay_seconds = 0
        timeout_seconds       = 3
        period_seconds        = 5
        failure_threshold     = 12

        http_get {
          path = "/healthz"
          port = 8080
        }
      }

      liveness_probe {
        initial_delay_seconds = 10
        timeout_seconds       = 3
        period_seconds        = 30
        failure_threshold     = 3

        http_get {
          path = "/healthz"
          port = 8080
        }
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  lifecycle {
    precondition {
      condition     = var.container_image != ""
      error_message = "container_image is required when deploy_workloads=true."
    }
    precondition {
      condition     = var.notion_data_source_id != ""
      error_message = "notion_data_source_id is required when deploy_workloads=true."
    }
  }

  depends_on = [
    google_project_service.required["run.googleapis.com"],
    google_storage_bucket_iam_member.service_objects,
    google_secret_manager_secret_iam_member.accessors,
  ]
}

resource "google_cloud_run_v2_job" "sync" {
  count = var.deploy_workloads ? 1 : 0

  project             = var.project_id
  name                = local.job_name
  location            = var.region
  deletion_protection = var.deletion_protection

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.job_runtime.email
      timeout         = "3000s"
      max_retries     = 0

      containers {
        name    = "sync"
        image   = var.container_image
        command = ["health-data-pipeline-job"]

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        dynamic "env" {
          for_each = merge(
            local.common_environment,
            local.secret_environment,
            {
              RUNTIME_KIND                = "job"
              CLOUD_RUN_SERVICE_URL       = google_cloud_run_v2_service.api[0].uri
              TASK_CALLER_SERVICE_ACCOUNT = google_service_account.task_caller.email
            },
          )
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }

  lifecycle {
    precondition {
      condition     = var.container_image != ""
      error_message = "container_image is required when deploy_workloads=true."
    }
  }

  depends_on = [
    google_project_service.required["run.googleapis.com"],
    google_storage_bucket_iam_member.job_objects,
    google_secret_manager_secret_iam_member.accessors,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "task_caller" {
  count = var.deploy_workloads ? 1 : 0

  project  = var.project_id
  location = google_cloud_run_v2_service.api[0].location
  name     = google_cloud_run_v2_service.api[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.task_caller.email}"
}

resource "google_cloud_run_v2_job_iam_member" "task_caller" {
  count = var.deploy_workloads ? 1 : 0

  project  = var.project_id
  location = google_cloud_run_v2_job.sync[0].location
  name     = google_cloud_run_v2_job.sync[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.task_caller.email}"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_caller" {
  count = var.deploy_workloads ? 1 : 0

  project  = var.project_id
  location = google_cloud_run_v2_job.sync[0].location
  name     = google_cloud_run_v2_job.sync[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_caller.email}"
}
