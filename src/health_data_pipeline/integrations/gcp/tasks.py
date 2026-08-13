"""Cloud Tasks adapter for sync and materialized-view wake-ups."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from health_data_pipeline.sync.contracts import TaskEnqueueResult

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


class CloudTasksClient(Protocol):
    def create_task(self, *, request: Mapping[str, Any]) -> Any: ...


class CloudTaskConfig(Protocol):
    @property
    def tasks_parent(self) -> str: ...

    @property
    def task_bucket_minutes(self) -> int: ...

    @property
    def jobs_run_url(self) -> str: ...

    @property
    def cloud_tasks_service_account(self) -> str: ...

    @property
    def cloud_run_service_url(self) -> str: ...


def _safe_task_component(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "-", value)
    if not safe:
        raise ValueError("task name component is empty")
    return safe[:128]


def _task_bucket(value: datetime, minutes: int) -> tuple[str, datetime]:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("task bucket time must be timezone-aware")
    utc = value.astimezone(UTC)
    bucket_seconds = minutes * 60
    rounded_seconds = int(utc.timestamp()) // bucket_seconds * bucket_seconds
    rounded = datetime.fromtimestamp(rounded_seconds, tz=UTC)
    return (
        rounded.strftime("%Y%m%dT%H%MZ"),
        rounded + timedelta(minutes=minutes),
    )


class CloudTaskDispatcher:
    """Create direct Jobs API tasks and OIDC-authenticated view tasks."""

    def __init__(
        self,
        *,
        client: CloudTasksClient,
        config: CloudTaskConfig,
    ) -> None:
        self._client = client
        self._config = config

    def _create(
        self,
        task_id: str,
        http_request: Mapping[str, Any],
        *,
        schedule_at: datetime | None = None,
    ) -> TaskEnqueueResult:
        task_name = f"{self._config.tasks_parent}/tasks/{task_id}"
        task: dict[str, Any] = {
            "name": task_name,
            "http_request": dict(http_request),
        }
        if schedule_at is not None:
            task["schedule_time"] = schedule_at
        try:
            self._client.create_task(request={"parent": self._config.tasks_parent, "task": task})
        except AlreadyExists:
            return TaskEnqueueResult(task_name=task_name, created=False)
        return TaskEnqueueResult(task_name=task_name, created=True)

    def enqueue_sync_job(
        self,
        *,
        owner_hmac: str,
        ready_at: datetime,
    ) -> TaskEnqueueResult:
        """Coalesce persisted events and call ``jobs.run`` at a bucket boundary."""

        bucket, schedule_at = _task_bucket(ready_at, self._config.task_bucket_minutes)
        task_id = f"{_safe_task_component(owner_hmac)}-sync-{bucket}"
        return self._create(
            task_id,
            {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": self._config.jobs_run_url,
                "headers": {"Content-Type": "application/json"},
                "body": b"{}",
                "oauth_token": {
                    "service_account_email": self._config.cloud_tasks_service_account,
                    "scope": CLOUD_PLATFORM_SCOPE,
                },
            },
            schedule_at=schedule_at,
        )

    def enqueue_view_update(
        self,
        *,
        owner_hmac: str,
        generation: int,
        requested_at: datetime,
    ) -> TaskEnqueueResult:
        if generation < 1:
            raise ValueError("HEAD generation must be positive")
        bucket, _ = _task_bucket(
            requested_at,
            self._config.task_bucket_minutes,
        )
        task_id = f"{_safe_task_component(owner_hmac)}-views-{generation:020d}-{bucket}"
        body = json.dumps(
            {"requested_generation": generation},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return self._create(
            task_id,
            {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"{self._config.cloud_run_service_url}/internal/tasks/apply-views",
                "headers": {"Content-Type": "application/json"},
                "body": body,
                "oidc_token": {
                    "service_account_email": self._config.cloud_tasks_service_account,
                    "audience": self._config.cloud_run_service_url,
                },
            },
        )
