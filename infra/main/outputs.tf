output "region" {
  description = "Primary Google Cloud region."
  value       = var.region
}

output "health_bucket_name" {
  description = "Private GCS health data bucket."
  value       = google_storage_bucket.health.name
}

output "cloud_build_source_bucket_name" {
  description = "Private staging bucket used only for Cloud Build source archives."
  value       = google_storage_bucket.cloud_build_source.name
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository resource name."
  value       = google_artifact_registry_repository.containers.name
}

output "container_repository" {
  description = "Docker repository prefix used by Cloud Build."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.containers.repository_id}"
}

output "cloud_run_service_url" {
  description = "Public service URL after deploy_workloads is enabled."
  value       = try(google_cloud_run_v2_service.api[0].uri, null)
}

output "cloud_run_job_run_uri" {
  description = "Cloud Run Admin API endpoint used by Cloud Tasks and Scheduler."
  value       = local.job_run_uri
}

output "cloud_tasks_queue" {
  description = "Single dispatch queue resource name."
  value       = google_cloud_tasks_queue.dispatch.id
}

output "cloud_build_service_account" {
  description = "Custom service account for Cloud Build executions."
  value       = google_service_account.cloud_build.email
}

output "github_actions_service_account" {
  description = "Keyless GitHub Actions identity when github_repository is configured."
  value       = try(google_service_account.github_actions[0].email, null)
}

output "github_workload_identity_provider" {
  description = "Provider name for google-github-actions/auth."
  value       = try(google_iam_workload_identity_pool_provider.github[0].name, null)
}

output "secret_ids" {
  description = "Secret containers. Values are added out of band and never stored in Terraform state."
  value = {
    for key, secret in google_secret_manager_secret.secrets : key => secret.secret_id
  }
}
