from __future__ import annotations

import re
from pathlib import Path

from health_data_pipeline.google_health import data_type_spec
from health_data_pipeline.pipeline import SUPPORTED_DATA_TYPES

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_STORAGE_TERRAFORM = _PROJECT_ROOT / "infra" / "main" / "storage.tf"
_GITHUB_WIF_TERRAFORM = _PROJECT_ROOT / "infra" / "main" / "github_wif.tf"
_NORMALIZED_PREFIX = "blobs/normalized/record_kind="


def _lifecycle_prefixes(terraform: str, *, age: int) -> set[str]:
    condition = re.search(
        rf"condition\s*\{{\s*age\s*=\s*{age}\s*"
        r"matches_prefix\s*=\s*\[(?P<prefixes>.*?)\]\s*\}",
        terraform,
        flags=re.DOTALL,
    )
    assert condition is not None
    return set(re.findall(r'"([^"]+)"', condition.group("prefixes")))


def test_normalized_lifecycle_covers_every_generated_record_kind() -> None:
    terraform = _STORAGE_TERRAFORM.read_text(encoding="utf-8")
    generated_prefixes = {
        f"{_NORMALIZED_PREFIX}{data_type_spec(data_type).record_kind.upper()}/"
        for data_type in SUPPORTED_DATA_TYPES
    }
    ninety_day_prefixes = {
        prefix
        for prefix in _lifecycle_prefixes(terraform, age=90)
        if prefix.startswith(_NORMALIZED_PREFIX)
    }
    year_prefixes = {
        prefix
        for prefix in _lifecycle_prefixes(terraform, age=365)
        if prefix.startswith(_NORMALIZED_PREFIX)
    }

    assert generated_prefixes == {
        f"{_NORMALIZED_PREFIX}DAILY/",
        f"{_NORMALIZED_PREFIX}INTERVAL/",
        f"{_NORMALIZED_PREFIX}SAMPLE/",
        f"{_NORMALIZED_PREFIX}SESSION/",
    }
    assert ninety_day_prefixes == {
        f"{_NORMALIZED_PREFIX}INTERVAL/",
        f"{_NORMALIZED_PREFIX}SAMPLE/",
    }
    assert year_prefixes == {
        f"{_NORMALIZED_PREFIX}DAILY/",
        f"{_NORMALIZED_PREFIX}SESSION/",
    }
    assert generated_prefixes == ninety_day_prefixes | year_prefixes


def test_google_providers_override_the_user_adc_quota_project() -> None:
    for relative_path in (
        "infra/bootstrap/providers.tf",
        "infra/main/providers.tf",
    ):
        provider = (_PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
        assert "billing_project       = var.project_id" in provider
        assert "user_project_override = true" in provider

    main_provider = (_PROJECT_ROOT / "infra/main/providers.tf").read_text(encoding="utf-8")
    assert 'provider "google-beta"' in main_provider


def test_budget_currency_and_amount_flow_through_cloud_build() -> None:
    variables = (_PROJECT_ROOT / "infra/main/variables.tf").read_text(encoding="utf-8")
    monitoring = (_PROJECT_ROOT / "infra/main/monitoring.tf").read_text(encoding="utf-8")
    cloud_build = (_PROJECT_ROOT / "cloudbuild.yaml").read_text(encoding="utf-8")

    assert 'variable "billing_currency_code"' in variables
    assert 'variable "monthly_budget_amount"' in variables
    assert "currency_code = var.billing_currency_code" in monitoring
    assert "units         = tostring(var.monthly_budget_amount)" in monitoring
    assert '-var="billing_currency_code=${_BILLING_CURRENCY_CODE}"' in cloud_build
    assert '-var="monthly_budget_amount=${_MONTHLY_BUDGET_AMOUNT}"' in cloud_build
    assert cloud_build.count("- DOCKER_BUILDKIT=1") == 2


def test_deploy_identity_and_scheduler_start_are_fail_closed() -> None:
    service_accounts = (_PROJECT_ROOT / "infra/main/service_accounts.tf").read_text(
        encoding="utf-8"
    )
    workload_identity = (_PROJECT_ROOT / "infra/main/github_wif.tf").read_text(encoding="utf-8")
    scheduler = (_PROJECT_ROOT / "infra/main/scheduler.tf").read_text(encoding="utf-8")

    assert '"roles/iam.serviceAccountAdmin"' in service_accounts
    assert '"roles/iam.workloadIdentityPoolAdmin"' in service_accounts
    assert "assertion.repository_id == '${var.github_repository_id}'" in workload_identity
    assert '"attribute.repository_id" = "assertion.repository_id"' in workload_identity
    assert "paused           = var.scheduler_paused" in scheduler
    assert "enabled      = !var.scheduler_paused" in (
        _PROJECT_ROOT / "infra/main/monitoring.tf"
    ).read_text(encoding="utf-8")


def test_cloud_build_source_staging_is_explicit_and_least_privilege() -> None:
    storage = _STORAGE_TERRAFORM.read_text(encoding="utf-8")
    workflow = (_PROJECT_ROOT / ".github/workflows/deploy.yml").read_text(encoding="utf-8")
    workload_identity = _GITHUB_WIF_TERRAFORM.read_text(encoding="utf-8")

    assert 'resource "google_storage_bucket" "cloud_build_source"' in storage
    assert 'name                        = "${var.project_id}-cloud-build-source"' in storage
    assert "uniform_bucket_level_access = true" in storage
    assert 'public_access_prevention    = "enforced"' in storage
    assert 'matches_prefix = ["source/"]' in storage
    assert "age            = 7" in storage

    assert 'role   = "roles/storage.legacyBucketReader"' in storage
    assert 'role   = "roles/storage.objectCreator"' in storage
    assert 'role   = "roles/storage.objectViewer"' in storage
    assert "google_service_account.github_actions[0].email" in storage
    assert "member = local.cloud_build_member" in storage
    assert "roles/storage." not in workload_identity

    assert '--gcs-source-staging-dir="gs://${GCP_PROJECT_ID}-cloud-build-source/source"' in workflow


def test_firebase_hosting_exposes_only_the_required_routes() -> None:
    hosting = (_PROJECT_ROOT / "infra/main/firebase_hosting.tf").read_text(encoding="utf-8")
    cloud_run = (_PROJECT_ROOT / "infra/main/cloud_run.tf").read_text(encoding="utf-8")
    service_accounts = (_PROJECT_ROOT / "infra/main/service_accounts.tf").read_text(
        encoding="utf-8"
    )
    locals_tf = (_PROJECT_ROOT / "infra/main/locals.tf").read_text(encoding="utf-8")
    outputs = (_PROJECT_ROOT / "infra/main/outputs.tf").read_text(encoding="utf-8")

    for service in ("firebase.googleapis.com", "firebasehosting.googleapis.com"):
        assert f'"{service}"' in locals_tf

    assert 'resource "google_firebase_project" "default"' in hosting
    assert 'resource "google_firebase_hosting_site" "public"' in hosting
    assert 'resource "google_firebase_hosting_version" "public"' in hosting
    assert 'resource "google_firebase_hosting_release" "public"' in hosting
    assert 'deletion_policy = "ABANDON"' in hosting
    assert 'google_project_iam_member.cloud_build["roles/firebase.admin"]' in hosting

    expected_routes = {
        "/health",
        "/public/steps.svg",
        "/setup",
        "/setup/oauth/callback",
        "/setup/oauth/start",
        "/setup/session",
        "/webhooks/google-health",
    }
    assert set(re.findall(r'"(/[^"]+)"', hosting)) == expected_routes
    assert "/internal/" not in hosting
    assert "google_cloud_run_v2_service.api[0].name" in hosting

    assert '"roles/firebase.admin"' in service_accounts
    assert '"roles/apigateway.admin"' not in service_accounts
    assert "invoker_iam_disabled = true" in cloud_run
    assert 'member   = "allUsers"' not in cloud_run

    assert "PUBLIC_BASE_URL       = local.public_base_url" in locals_tf
    assert "CLOUD_RUN_SERVICE_URL = local.service_url" in locals_tf
    assert "google_firebase_hosting_site.public[0].default_url" in locals_tf
    assert 'output "public_base_url"' in outputs
    assert 'output "cloud_run_service_url"' in outputs


def test_oauth_callback_request_logging_exclusion_covers_public_frontends() -> None:
    logging = (_PROJECT_ROOT / "infra/main/logging.tf").read_text(encoding="utf-8")

    assert 'resource.type="cloud_run_revision"' in logging
    assert 'log_id("run.googleapis.com/requests")' in logging
    assert 'resource.type="firebase_domain"' in logging
    assert 'log_id("webrequests")' in logging
    assert "apigateway.googleapis.com" not in logging
    assert logging.count('httpRequest.requestUrl=~"/setup/oauth/callback([?].*)?$"') == 2
