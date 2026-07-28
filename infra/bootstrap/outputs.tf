output "state_bucket_name" {
  description = "Bucket to pass to terraform init -backend-config."
  value       = google_storage_bucket.terraform_state.name
}
