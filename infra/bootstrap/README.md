# Terraform bootstrap

This root creates only the private GCS bucket used by the main Terraform backend.

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Do not import this bucket into `infra/main`. Keep bootstrap state separate so the main backend never attempts to manage itself.
