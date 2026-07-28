resource "google_monitoring_notification_channel" "email" {
  count = var.notification_email == "" ? 0 : 1

  project      = var.project_id
  display_name = "Health data pipeline owner"
  type         = "email"

  labels = {
    email_address = var.notification_email
  }

  depends_on = [google_project_service.required["monitoring.googleapis.com"]]
}

resource "google_monitoring_alert_policy" "job_not_completed" {
  count = var.deploy_workloads ? 1 : 0

  project      = var.project_id
  display_name = "Health pipeline job has not completed"
  combiner     = "OR"

  conditions {
    display_name = "No completed execution for two hours"

    condition_absent {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_job\"",
        "metric.type = \"run.googleapis.com/job/completed_execution_count\"",
        "resource.label.job_name = \"${local.job_name}\"",
        "resource.label.location = \"${var.region}\"",
      ])
      duration = "7200s"

      aggregations {
        alignment_period   = "3600s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = var.notification_email == "" ? [] : [
    google_monitoring_notification_channel.email[0].name,
  ]

  alert_strategy {
    auto_close = "86400s"
  }

  depends_on = [
    google_cloud_run_v2_job.sync,
    google_project_service.required["monitoring.googleapis.com"],
  ]
}

resource "google_monitoring_alert_policy" "class_a_warning" {
  project      = var.project_id
  display_name = "Health pipeline GCS Class A warning"
  combiner     = "OR"

  conditions {
    display_name = "Class A warning event emitted"

    condition_threshold {
      filter          = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.class_a_warning.name}\" AND resource.type = \"cloud_run_job\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = var.notification_email == "" ? [] : [
    google_monitoring_notification_channel.email[0].name,
  ]

  alert_strategy {
    auto_close = "86400s"
  }
}

resource "google_monitoring_alert_policy" "health_bucket_size" {
  project      = var.project_id
  display_name = "Health pipeline bucket size warning"
  combiner     = "OR"

  conditions {
    display_name = "Health bucket exceeds 1 GiB"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"gcs_bucket\"",
        "metric.type = \"storage.googleapis.com/storage/total_bytes\"",
        "resource.label.bucket_name = \"${google_storage_bucket.health.name}\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 1073741824
      duration        = "0s"

      aggregations {
        alignment_period   = "86400s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  notification_channels = var.notification_email == "" ? [] : [
    google_monitoring_notification_channel.email[0].name,
  ]

  alert_strategy {
    auto_close = "604800s"
  }

  depends_on = [
    google_storage_bucket.health,
    google_project_service.required["monitoring.googleapis.com"],
  ]
}

resource "google_monitoring_alert_policy" "fitbit_sync_stale" {
  project      = var.project_id
  display_name = "Fitbit has not synced for five days"
  combiner     = "OR"

  conditions {
    display_name = "Stale sync event emitted"

    condition_threshold {
      filter          = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.fitbit_sync_stale.name}\" AND resource.type = \"cloud_run_job\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = var.notification_email == "" ? [] : [
    google_monitoring_notification_channel.email[0].name,
  ]

  alert_strategy {
    auto_close = "604800s"
  }
}

resource "google_billing_budget" "project" {
  count = var.billing_account_id == "" ? 0 : 1

  billing_account = var.billing_account_id
  display_name    = "${var.name_prefix} monthly warning budget"

  budget_filter {
    projects = ["projects/${local.project_number}"]
  }

  amount {
    specified_amount {
      currency_code = var.billing_currency_code
      units         = tostring(var.monthly_budget_amount)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.9
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }

  dynamic "all_updates_rule" {
    for_each = var.notification_email == "" ? [] : [1]
    content {
      monitoring_notification_channels = [
        google_monitoring_notification_channel.email[0].name,
      ]
      disable_default_iam_recipients = false
    }
  }

  depends_on = [google_project_service.required["billingbudgets.googleapis.com"]]
}
