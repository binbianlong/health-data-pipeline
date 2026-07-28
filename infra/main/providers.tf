provider "google" {
  project               = var.project_id
  region                = var.region
  billing_project       = var.project_id
  user_project_override = true
}

data "google_project" "current" {
  project_id = var.project_id
}
