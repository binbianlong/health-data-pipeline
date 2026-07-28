config {
  call_module_type = "local"
  force            = false
}

plugin "google" {
  enabled = true
  version = "0.33.0"
  source  = "github.com/terraform-linters/tflint-ruleset-google"
}
