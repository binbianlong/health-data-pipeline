terraform {
  backend "gcs" {
    prefix = "health-data-pipeline/main"
  }
}
