"""Runtime configuration.

Only non-secret identifiers are read from the environment here.  OAuth client
secrets, refresh tokens, signing keys and Notion tokens are expected to be
provided by Secret Manager-backed dependencies at application startup.
"""

# ruff: noqa: S105

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


class ConfigurationError(ValueError):
    """Raised when required runtime configuration is missing or invalid."""


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"required environment variable is missing: {name}")
    return value


def _required_one_of(env: Mapping[str, str], *names: str) -> str:
    for name in names:
        value = env.get(name, "").strip()
        if value:
            return value
    joined = ", ".join(names)
    raise ConfigurationError(f"one required environment variable is missing: {joined}")


def _positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return value


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Non-secret settings shared by the service and the job."""

    project_id: str
    region: str
    health_bucket: str
    cloud_run_job: str
    cloud_tasks_queue: str
    cloud_tasks_service_account: str
    public_base_url: str
    cloud_run_service_url: str
    oauth_client_id: str
    oauth_redirect_uri: str
    notion_data_source_id: str
    health_api_base_url: str = "https://health.googleapis.com"
    oauth_authorization_url: str = "https://accounts.google.com/o/oauth2/v2/auth"
    oauth_token_url: str = "https://oauth2.googleapis.com/token"
    webhook_max_body_bytes: int = 1_048_576
    setup_session_ttl_seconds: int = 600
    task_bucket_minutes: int = 10

    @property
    def jobs_run_url(self) -> str:
        return (
            "https://run.googleapis.com/v2/projects/"
            f"{self.project_id}/locations/{self.region}/jobs/{self.cloud_run_job}:run"
        )

    @property
    def tasks_parent(self) -> str:
        return f"projects/{self.project_id}/locations/{self.region}/queues/{self.cloud_tasks_queue}"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AppConfig:
        source = os.environ if env is None else env
        region = source.get("REGION", source.get("GOOGLE_CLOUD_REGION", "us-central1")).strip()
        if not region:
            raise ConfigurationError("GOOGLE_CLOUD_REGION must not be empty")
        public_base_url = _required_one_of(
            source,
            "PUBLIC_BASE_URL",
            "CLOUD_RUN_SERVICE_URL",
        ).rstrip("/")
        cloud_run_service_url = _required_one_of(
            source,
            "CLOUD_RUN_SERVICE_URL",
            "PUBLIC_BASE_URL",
        ).rstrip("/")
        if not public_base_url.startswith("https://"):
            raise ConfigurationError("PUBLIC_BASE_URL must use https")
        if not cloud_run_service_url.startswith("https://"):
            raise ConfigurationError("CLOUD_RUN_SERVICE_URL must use https")

        return cls(
            project_id=_required_one_of(source, "PROJECT_ID", "GOOGLE_CLOUD_PROJECT"),
            region=region,
            health_bucket=_required_one_of(source, "HEALTH_BUCKET", "HEALTH_DATA_BUCKET"),
            cloud_run_job=_required_one_of(source, "SYNC_JOB_NAME", "CLOUD_RUN_JOB_NAME"),
            cloud_tasks_queue=_required_one_of(source, "TASKS_QUEUE", "CLOUD_TASKS_QUEUE"),
            cloud_tasks_service_account=_required_one_of(
                source,
                "TASK_CALLER_SERVICE_ACCOUNT",
                "CLOUD_TASKS_SERVICE_ACCOUNT",
            ),
            public_base_url=public_base_url,
            cloud_run_service_url=cloud_run_service_url,
            oauth_client_id=source.get("GOOGLE_OAUTH_CLIENT_ID", ""),
            oauth_redirect_uri=source.get(
                "GOOGLE_OAUTH_REDIRECT_URI",
                f"{public_base_url}/setup/oauth/callback",
            ),
            notion_data_source_id=_required(source, "NOTION_DATA_SOURCE_ID"),
            health_api_base_url=source.get(
                "GOOGLE_HEALTH_API_BASE_URL", "https://health.googleapis.com"
            ).rstrip("/"),
            oauth_authorization_url=source.get(
                "GOOGLE_OAUTH_AUTHORIZATION_URL",
                "https://accounts.google.com/o/oauth2/v2/auth",
            ),
            oauth_token_url=source.get(
                "GOOGLE_OAUTH_TOKEN_URL", "https://oauth2.googleapis.com/token"
            ),
            webhook_max_body_bytes=_positive_int(source, "WEBHOOK_MAX_BODY_BYTES", 1_048_576),
            setup_session_ttl_seconds=_positive_int(source, "SETUP_SESSION_TTL_SECONDS", 600),
            task_bucket_minutes=_positive_int(source, "TASK_BUCKET_MINUTES", 10),
        )
