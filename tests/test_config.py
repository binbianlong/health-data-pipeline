from __future__ import annotations

import pytest

from health_data_pipeline.config import AppConfig, ConfigurationError


def _required_env() -> dict[str, str]:
    return {
        "PROJECT_ID": "project",
        "HEALTH_BUCKET": "bucket",
        "SYNC_JOB_NAME": "sync-job",
        "TASKS_QUEUE": "queue",
        "TASK_CALLER_SERVICE_ACCOUNT": "tasks@example.iam.gserviceaccount.com",
        "PUBLIC_BASE_URL": "https://service.example/",
        "NOTION_DATA_SOURCE_ID": "notion-source",
    }


def test_config_loads_primary_names_and_derived_urls() -> None:
    config = AppConfig.from_env(_required_env())

    assert config.region == "us-central1"
    assert config.public_base_url == "https://service.example"
    assert config.cloud_run_service_url == "https://service.example"
    assert config.oauth_redirect_uri == ("https://service.example/setup/oauth/callback")
    assert config.jobs_run_url.endswith("/projects/project/locations/us-central1/jobs/sync-job:run")
    assert config.tasks_parent.endswith("/locations/us-central1/queues/queue")


def test_config_accepts_legacy_aliases_and_explicit_overrides() -> None:
    config = AppConfig.from_env(
        {
            "GOOGLE_CLOUD_PROJECT": "legacy-project",
            "GOOGLE_CLOUD_REGION": "asia-northeast1",
            "HEALTH_DATA_BUCKET": "legacy-bucket",
            "CLOUD_RUN_JOB_NAME": "legacy-job",
            "CLOUD_TASKS_QUEUE": "legacy-queue",
            "CLOUD_TASKS_SERVICE_ACCOUNT": "legacy@example.iam.gserviceaccount.com",
            "PUBLIC_BASE_URL": "https://public.example/",
            "CLOUD_RUN_SERVICE_URL": "https://legacy.example",
            "NOTION_DATA_SOURCE_ID": "source",
            "GOOGLE_OAUTH_REDIRECT_URI": "https://custom.example/callback",
            "WEBHOOK_MAX_BODY_BYTES": "2048",
        }
    )

    assert config.project_id == "legacy-project"
    assert config.region == "asia-northeast1"
    assert config.public_base_url == "https://public.example"
    assert config.cloud_run_service_url == "https://legacy.example"
    assert config.oauth_redirect_uri == "https://custom.example/callback"
    assert config.webhook_max_body_bytes == 2048


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"PROJECT_ID": ""}, "PROJECT_ID"),
        ({"PUBLIC_BASE_URL": "http://service.example"}, "https"),
        (
            {"CLOUD_RUN_SERVICE_URL": "http://internal.example"},
            "CLOUD_RUN_SERVICE_URL",
        ),
        ({"WEBHOOK_MAX_BODY_BYTES": "not-an-int"}, "integer"),
        ({"SETUP_SESSION_TTL_SECONDS": "0"}, "positive"),
    ],
)
def test_config_rejects_missing_or_invalid_values(
    updates: dict[str, str],
    message: str,
) -> None:
    env = _required_env()
    env.update(updates)

    with pytest.raises(ConfigurationError, match=message):
        AppConfig.from_env(env)
