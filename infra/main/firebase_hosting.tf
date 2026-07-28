resource "google_firebase_project" "default" {
  provider = google-beta
  count    = var.deploy_workloads ? 1 : 0

  project = var.project_id

  depends_on = [
    google_project_iam_member.cloud_build["roles/firebase.admin"],
    google_project_service.required["firebase.googleapis.com"],
  ]
}

resource "google_firebase_hosting_site" "public" {
  provider = google-beta
  count    = var.deploy_workloads ? 1 : 0

  project         = var.project_id
  site_id         = var.project_id
  deletion_policy = "ABANDON"

  depends_on = [
    google_firebase_project.default,
    google_project_service.required["firebasehosting.googleapis.com"],
  ]
}

resource "google_firebase_hosting_version" "public" {
  provider = google-beta
  count    = var.deploy_workloads ? 1 : 0

  site_id = google_firebase_hosting_site.public[0].site_id

  config {
    dynamic "rewrites" {
      for_each = toset([
        "/healthz",
        "/public/steps.svg",
        "/setup",
        "/setup/oauth/callback",
        "/setup/oauth/start",
        "/setup/session",
        "/webhooks/google-health",
      ])

      content {
        glob = rewrites.value
        run {
          service_id = google_cloud_run_v2_service.api[0].name
          region     = google_cloud_run_v2_service.api[0].location
        }
      }
    }
  }
}

resource "google_firebase_hosting_release" "public" {
  provider = google-beta
  count    = var.deploy_workloads ? 1 : 0

  site_id      = google_firebase_hosting_site.public[0].site_id
  version_name = google_firebase_hosting_version.public[0].name
  message      = "Public routes for the health data pipeline"
}
