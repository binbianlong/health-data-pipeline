variable "project_id" {
  description = "Google Cloud project ID."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a valid Google Cloud project ID."
  }
}

variable "region" {
  description = "Region for all regional resources."
  type        = string
  default     = "us-central1"
}

variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "health-data-pipeline"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,39}$", var.name_prefix))
    error_message = "name_prefix must contain lowercase letters, numbers, and hyphens."
  }
}

variable "health_bucket_name" {
  description = "Globally unique private bucket for health data."
  type        = string

  validation {
    condition     = length(var.health_bucket_name) >= 3 && length(var.health_bucket_name) <= 63
    error_message = "health_bucket_name must be between 3 and 63 characters."
  }
}

variable "terraform_state_bucket_name" {
  description = "Bootstrap-created bucket. Main grants the Cloud Build account state access."
  type        = string
}

variable "container_image" {
  description = "Immutable Artifact Registry image reference used by Cloud Run."
  type        = string
  default     = ""
}

variable "deploy_workloads" {
  description = "Create Cloud Run and Scheduler after the first image has been pushed."
  type        = bool
  default     = false
}

variable "notion_data_source_id" {
  description = "Private Notion data source ID. This identifier is not the integration token."
  type        = string
  default     = ""
  sensitive   = true
}

variable "notification_email" {
  description = "Email address for Monitoring notifications. Empty disables the email channel."
  type        = string
  default     = ""
}

variable "billing_account_id" {
  description = "Billing account ID for an optional budget. Empty disables budget creation."
  type        = string
  default     = ""
}

variable "monthly_budget_amount" {
  description = "Optional monthly project budget in the billing account currency. It sends notifications but never stops services."
  type        = number
  default     = 5

  validation {
    condition     = var.monthly_budget_amount > 0 && floor(var.monthly_budget_amount) == var.monthly_budget_amount
    error_message = "monthly_budget_amount must be a positive whole number."
  }
}

variable "billing_currency_code" {
  description = "ISO 4217 currency code of the billing account."
  type        = string
  default     = "USD"

  validation {
    condition     = can(regex("^[A-Z]{3}$", var.billing_currency_code))
    error_message = "billing_currency_code must be a three-letter ISO 4217 code."
  }
}

variable "github_repository" {
  description = "Private GitHub repository in owner/name form. Empty disables GitHub WIF resources."
  type        = string
  default     = ""

  validation {
    condition     = var.github_repository == "" || can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must use owner/name form."
  }
}

variable "github_repository_id" {
  description = "Immutable numeric GitHub repository ID used in the WIF condition."
  type        = string
  default     = ""

  validation {
    condition     = var.github_repository_id == "" || can(regex("^[0-9]+$", var.github_repository_id))
    error_message = "github_repository_id must be an immutable numeric GitHub repository ID."
  }
}

variable "github_deploy_branch" {
  description = "Only this branch may exchange GitHub OIDC credentials for deployment."
  type        = string
  default     = "main"

  validation {
    condition     = can(regex("^[A-Za-z0-9._/-]+$", var.github_deploy_branch))
    error_message = "github_deploy_branch contains unsupported characters."
  }
}

variable "scheduler_paused" {
  description = "Maintenance mode: pause recovery scheduling, suppress its absence alert, and avoid new webhook wake-up tasks."
  type        = bool
  default     = true
}

variable "deletion_protection" {
  description = "Protect Cloud Run resources from accidental Terraform deletion."
  type        = bool
  default     = true
}
