# Main Terraform root

The GCS backend bucket must already exist from `../bootstrap`.

```bash
terraform init -backend-config="bucket=<STATE_BUCKET>"
terraform plan
terraform apply
```

The first apply uses `deploy_workloads=false`. It creates APIs, storage, secrets, service accounts, Artifact Registry, Cloud Tasks, monitoring primitives, and optional GitHub Workload Identity Federation. After an image is pushed, set `container_image` to an immutable Artifact Registry reference and `deploy_workloads=true`.

Secret values are deliberately absent from Terraform. Add versions with `gcloud secrets versions add` or through the OAuth setup flow.

If `billing_account_id` is set, the Terraform caller and Cloud Build service account need permission to manage budgets on that billing account. A budget only sends notifications; it does not cap charges.
