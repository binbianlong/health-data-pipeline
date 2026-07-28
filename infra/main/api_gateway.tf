resource "google_api_gateway_api" "public" {
  provider = google-beta
  count    = var.deploy_workloads ? 1 : 0

  project      = var.project_id
  api_id       = "${var.name_prefix}-public"
  display_name = "Health data pipeline public API"

  depends_on = [
    google_project_iam_member.cloud_build["roles/apigateway.admin"],
    google_project_service.required["apigateway.googleapis.com"],
    google_project_service.required["servicecontrol.googleapis.com"],
    google_project_service.required["servicemanagement.googleapis.com"],
  ]
}

resource "google_api_gateway_api_config" "public" {
  provider = google-beta
  count    = var.deploy_workloads ? 1 : 0

  project              = var.project_id
  api                  = google_api_gateway_api.public[0].api_id
  api_config_id_prefix = "${var.name_prefix}-"
  display_name         = "Health data pipeline public routes"

  gateway_config {
    backend_config {
      google_service_account = google_service_account.api_gateway.email
    }
  }

  openapi_documents {
    document {
      path = "openapi.yaml"
      contents = base64encode(templatefile("${path.module}/openapi.yaml.tftpl", {
        backend_url = local.service_url
      }))
    }
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    google_service_account_iam_member.cloud_build_runtime_act_as["gateway"],
  ]
}

resource "google_api_gateway_gateway" "public" {
  provider = google-beta
  count    = var.deploy_workloads ? 1 : 0

  project      = var.project_id
  region       = var.region
  gateway_id   = local.gateway_id
  api_config   = google_api_gateway_api_config.public[0].id
  display_name = "Health data pipeline public gateway"
}
