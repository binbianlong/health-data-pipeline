resource "google_secret_manager_secret" "secrets" {
  for_each = local.secret_names

  project   = var.project_id
  secret_id = "${var.name_prefix}-${each.value}"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.required["secretmanager.googleapis.com"]]
}

locals {
  secret_accessor_bindings = {
    "service-google-oauth-client" = {
      secret = "google-oauth-client"
      member = "serviceAccount:${google_service_account.service_runtime.email}"
    }
    "service-notion-token" = {
      secret = "notion-token"
      member = "serviceAccount:${google_service_account.service_runtime.email}"
    }
    "service-application-hmac-key" = {
      secret = "application-hmac-key"
      member = "serviceAccount:${google_service_account.service_runtime.email}"
    }
    "service-webhook-authorization" = {
      secret = "webhook-authorization"
      member = "serviceAccount:${google_service_account.service_runtime.email}"
    }
    "service-oauth-session-aead-key" = {
      secret = "oauth-session-aead-key"
      member = "serviceAccount:${google_service_account.service_runtime.email}"
    }
    "job-google-oauth-client" = {
      secret = "google-oauth-client"
      member = "serviceAccount:${google_service_account.job_runtime.email}"
    }
    "job-google-oauth-refresh-token" = {
      secret = "google-oauth-refresh-token"
      member = "serviceAccount:${google_service_account.job_runtime.email}"
    }
    "job-application-hmac-key" = {
      secret = "application-hmac-key"
      member = "serviceAccount:${google_service_account.job_runtime.email}"
    }
  }
}

resource "google_secret_manager_secret_iam_member" "accessors" {
  for_each = local.secret_accessor_bindings

  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.value.secret].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value.member
}

resource "google_secret_manager_secret_iam_member" "refresh_token_writer" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets["google-oauth-refresh-token"].secret_id
  role      = "roles/secretmanager.secretVersionAdder"
  member    = "serviceAccount:${google_service_account.service_runtime.email}"
}
