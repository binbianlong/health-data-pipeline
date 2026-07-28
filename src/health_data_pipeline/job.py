"""Cloud Tasks triggers and Cloud Run Job entry point."""

# ruff: noqa: PLW0603

from __future__ import annotations

import importlib
import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from health_data_pipeline.config import AppConfig

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


class TaskCreationError(RuntimeError):
    """A required Cloud Task could not be created."""


class CloudTasksClient(Protocol):
    def create_task(self, *, request: Mapping[str, Any]) -> Any: ...


@dataclass(frozen=True, slots=True)
class TaskEnqueueResult:
    task_name: str
    created: bool


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
        config: AppConfig,
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
        """Coalesce persisted events and call ``jobs.run`` at the next bucket boundary."""

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
                    "service_account_email": (self._config.cloud_tasks_service_account),
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
                "url": (f"{self._config.cloud_run_service_url}/internal/tasks/apply-views"),
                "headers": {"Content-Type": "application/json"},
                "body": body,
                "oidc_token": {
                    "service_account_email": (self._config.cloud_tasks_service_account),
                    "audience": self._config.cloud_run_service_url,
                },
            },
        )


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    committed: bool
    generation: int | None
    owner_hmac: str


class SyncPipeline(Protocol):
    def run(self) -> SyncOutcome: ...


class ViewTaskDispatcher(Protocol):
    def enqueue_view_update(
        self,
        *,
        owner_hmac: str,
        generation: int,
        requested_at: datetime,
    ) -> TaskEnqueueResult: ...


class SyncTaskDispatcher(Protocol):
    def enqueue_sync_job(
        self,
        *,
        owner_hmac: str,
        ready_at: datetime,
    ) -> TaskEnqueueResult: ...


class JobRunner:
    """Run one sync and recover materialized views from committed HEAD."""

    def __init__(
        self,
        *,
        pipeline: SyncPipeline,
        tasks: ViewTaskDispatcher,
        latest_generation: Callable[[], int],
        request_view_generation: Callable[[int], bool],
        sync_tasks: SyncTaskDispatcher | None = None,
        sync_recovery_needed: Callable[[], bool] | None = None,
        on_sync_recovery_error: Callable[[Exception], object] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        on_complete: Callable[[], object] | None = None,
    ) -> None:
        if (sync_tasks is None) is not (sync_recovery_needed is None):
            raise ValueError("sync_tasks and sync_recovery_needed must be configured together")
        self._pipeline = pipeline
        self._tasks = tasks
        self._latest_generation = latest_generation
        self._request_view_generation = request_view_generation
        self._sync_tasks = sync_tasks
        self._sync_recovery_needed = sync_recovery_needed
        self._on_sync_recovery_error = on_sync_recovery_error
        self._clock = clock
        self._on_complete = on_complete

    def _recover_pending_sync(self, owner_hmac: str) -> None:
        if self._sync_tasks is None or self._sync_recovery_needed is None:
            return
        try:
            if self._sync_recovery_needed():
                self._sync_tasks.enqueue_sync_job(
                    owner_hmac=owner_hmac,
                    ready_at=self._clock(),
                )
        except Exception as exc:
            # This is a wake-up optimization after the authoritative HEAD
            # commit. Durable events remain available to the hourly recovery.
            if self._on_sync_recovery_error is not None:
                self._on_sync_recovery_error(exc)

    def run(self) -> SyncOutcome:
        try:
            outcome = self._pipeline.run()
            try:
                if outcome.committed and outcome.generation is None:
                    raise RuntimeError("committed sync has no HEAD generation")
                generation = (
                    outcome.generation
                    if outcome.generation is not None
                    else self._latest_generation()
                )
                if (
                    generation is not None
                    and generation > 0
                    and self._request_view_generation(generation)
                ):
                    self._tasks.enqueue_view_update(
                        owner_hmac=outcome.owner_hmac,
                        generation=generation,
                        requested_at=self._clock(),
                    )
            finally:
                self._recover_pending_sync(outcome.owner_hmac)
            return outcome
        finally:
            if self._on_complete is not None:
                self._on_complete()


JobFactory = Callable[[], JobRunner]
_configured_factory: JobFactory | None = None


def configure_job_factory(factory: JobFactory) -> None:
    """Install the production composition root without importing it in tests."""

    global _configured_factory
    _configured_factory = factory


def _load_factory() -> JobFactory:
    if _configured_factory is not None:
        return _configured_factory
    dotted = os.environ.get(
        "HEALTH_PIPELINE_JOB_FACTORY",
        "health_data_pipeline.runtime:build_job_runner",
    )
    module_name, separator, attribute = dotted.partition(":")
    if not separator or not module_name or not attribute:
        raise RuntimeError("HEALTH_PIPELINE_JOB_FACTORY must be module:callable")
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute, None)
    if not callable(factory):
        raise TypeError(f"job factory is not callable: {dotted}")
    return cast(JobFactory, factory)


def main() -> None:
    """Console entry point used by ``python -m health_data_pipeline.job``."""

    runner = _load_factory()()
    runner.run()


if __name__ == "__main__":
    main()
