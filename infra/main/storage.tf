resource "google_storage_bucket" "health" {
  name                        = var.health_bucket_name
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 30
      matches_prefix = ["webhook-events/", "blobs/api-pages/"]
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
      matches_prefix = [
        "blobs/normalized/record_kind=INTERVAL/",
        "blobs/normalized/record_kind=SAMPLE/",
        "state/runs/",
      ]
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
      matches_prefix = [
        "blobs/normalized/record_kind=DAILY/",
        "blobs/normalized/record_kind=SESSION/",
        "manifests/ingestion/",
      ]
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 2
      matches_prefix = ["state/oauth/sessions/"]
    }
  }

  depends_on = [google_project_service.required["storage.googleapis.com"]]
}

resource "google_storage_bucket" "cloud_build_source" {
  name                        = "${var.project_id}-cloud-build-source"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 7
      matches_prefix = ["source/"]
    }
  }

  depends_on = [google_project_service.required["storage.googleapis.com"]]
}

resource "google_storage_bucket_iam_member" "service_objects" {
  bucket = google_storage_bucket.health.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.service_runtime.email}"
}

resource "google_storage_bucket_iam_member" "job_objects" {
  bucket = google_storage_bucket.health.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.job_runtime.email}"
}

resource "google_storage_bucket_iam_member" "cloud_build_state" {
  bucket = var.terraform_state_bucket_name
  role   = "roles/storage.objectAdmin"
  member = local.cloud_build_member
}

resource "google_storage_bucket_iam_member" "github_cloud_build_source_bucket_reader" {
  count = var.github_repository == "" ? 0 : 1

  bucket = google_storage_bucket.cloud_build_source.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.github_actions[0].email}"
}

resource "google_storage_bucket_iam_member" "github_cloud_build_source_creator" {
  count = var.github_repository == "" ? 0 : 1

  bucket = google_storage_bucket.cloud_build_source.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.github_actions[0].email}"
}

resource "google_storage_bucket_iam_member" "cloud_build_source_reader" {
  bucket = google_storage_bucket.cloud_build_source.name
  role   = "roles/storage.objectViewer"
  member = local.cloud_build_member
}
