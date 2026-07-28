from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from health_data_pipeline.config import AppConfig
from health_data_pipeline.job import (
    CLOUD_PLATFORM_SCOPE,
    CloudTaskDispatcher,
    JobRunner,
    SyncOutcome,
    TaskEnqueueResult,
)
from health_data_pipeline.oauth import write_state
from health_data_pipeline.storage import InMemoryObjectStore
from health_data_pipeline.views import (
    NOTION_STATE_PATH,
    NotionSyncState,
    read_notion_sync_state,
    request_notion_generation,
)

NOW = datetime(2026, 7, 28, 12, 7, tzinfo=UTC)


def _config() -> AppConfig:
    return AppConfig(
        project_id="health-project",
        region="us-central1",
        health_bucket="health-bucket",
        cloud_run_job="health-sync",
        cloud_tasks_queue="health-tasks",
        cloud_tasks_service_account="task-caller@health-project.iam.gserviceaccount.com",
        cloud_run_service_url="https://health-service.example.run.app",
        oauth_client_id="client-id",
        oauth_redirect_uri="https://health-service.example.run.app/setup/oauth/callback",
        notion_data_source_id="notion-data-source",
    )


class FakeCloudTasksClient:
    def __init__(self) -> None:
        self.requests: list[Mapping[str, Any]] = []
        self._task_names: set[str] = set()

    def create_task(self, *, request: Mapping[str, Any]) -> object:
        self.requests.append(request)
        task = cast(Mapping[str, Any], request["task"])
        task_name = cast(str, task["name"])
        if task_name in self._task_names:
            exception_factory = cast(Callable[[str], Exception], AlreadyExists)
            raise exception_factory("task already exists")
        self._task_names.add(task_name)
        return object()


@dataclass
class StubPipeline:
    outcome: SyncOutcome
    events: list[str]

    def run(self) -> SyncOutcome:
        self.events.append("pipeline")
        return self.outcome


@dataclass
class FailingPipeline:
    events: list[str]

    def run(self) -> SyncOutcome:
        self.events.append("pipeline")
        raise RuntimeError("simulated pipeline failure")


@dataclass(frozen=True, slots=True)
class ViewTaskCall:
    owner_hmac: str
    generation: int
    requested_at: datetime
    desired_at_enqueue: int


class RecordingViewDispatcher:
    def __init__(
        self,
        store: InMemoryObjectStore,
        events: list[str],
    ) -> None:
        self._store = store
        self._events = events
        self.calls: list[ViewTaskCall] = []

    def enqueue_view_update(
        self,
        *,
        owner_hmac: str,
        generation: int,
        requested_at: datetime,
    ) -> TaskEnqueueResult:
        state, _ = read_notion_sync_state(self._store)
        self._events.append("enqueue")
        self.calls.append(
            ViewTaskCall(
                owner_hmac=owner_hmac,
                generation=generation,
                requested_at=requested_at,
                desired_at_enqueue=state.desired_generation,
            )
        )
        return TaskEnqueueResult(task_name=f"views-{generation}", created=True)


@dataclass(frozen=True, slots=True)
class SyncTaskCall:
    owner_hmac: str
    ready_at: datetime


class RecordingSyncDispatcher:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.calls: list[SyncTaskCall] = []

    def enqueue_sync_job(
        self,
        *,
        owner_hmac: str,
        ready_at: datetime,
    ) -> TaskEnqueueResult:
        self._events.append("recover")
        self.calls.append(SyncTaskCall(owner_hmac=owner_hmac, ready_at=ready_at))
        return TaskEnqueueResult(task_name="sync-recovery", created=True)


class FailingSyncDispatcher:
    def enqueue_sync_job(
        self,
        *,
        owner_hmac: str,
        ready_at: datetime,
    ) -> TaskEnqueueResult:
        del owner_hmac, ready_at
        raise RuntimeError("simulated sync recovery failure")


class FailingViewDispatcher:
    def __init__(self, store: InMemoryObjectStore) -> None:
        self._store = store

    def enqueue_view_update(
        self,
        *,
        owner_hmac: str,
        generation: int,
        requested_at: datetime,
    ) -> TaskEnqueueResult:
        del owner_hmac, requested_at
        assert read_notion_sync_state(self._store)[0].desired_generation == generation
        raise RuntimeError("simulated task creation failure")


@pytest.mark.parametrize(
    ("outcome", "latest_generation", "expected_generation", "latest_called"),
    [
        (
            SyncOutcome(committed=True, generation=7, owner_hmac="owner"),
            99,
            7,
            False,
        ),
        (
            SyncOutcome(committed=False, generation=8, owner_hmac="owner"),
            99,
            8,
            False,
        ),
        (
            SyncOutcome(committed=False, generation=None, owner_hmac="owner"),
            9,
            9,
            True,
        ),
    ],
)
def test_job_runner_persists_desired_before_view_recovery_with_or_without_commit(
    outcome: SyncOutcome,
    latest_generation: int,
    expected_generation: int,
    latest_called: bool,
) -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    events: list[str] = []
    dispatcher = RecordingViewDispatcher(store, events)

    def latest() -> int:
        events.append("latest")
        return latest_generation

    def request(generation: int) -> bool:
        events.append("desired")
        return request_notion_generation(store, generation)

    runner = JobRunner(
        pipeline=StubPipeline(outcome, events),
        tasks=dispatcher,
        latest_generation=latest,
        request_view_generation=request,
        clock=lambda: NOW,
    )

    assert runner.run() == outcome
    state, _ = read_notion_sync_state(store)
    assert state.desired_generation == expected_generation
    assert dispatcher.calls == [
        ViewTaskCall(
            owner_hmac="owner",
            generation=expected_generation,
            requested_at=NOW,
            desired_at_enqueue=expected_generation,
        )
    ]
    expected_events = (
        ["pipeline", "latest", "desired", "enqueue"]
        if latest_called
        else ["pipeline", "desired", "enqueue"]
    )
    assert events == expected_events


def test_job_runner_does_not_enqueue_an_applied_generation() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    applied = NotionSyncState(
        desired_generation=12,
        applied_generation=12,
        status="IDLE",
        applying_generation=None,
        updated_at="2026-07-28T12:00:00Z",
    )
    write_state(
        store,
        NOTION_STATE_PATH,
        applied.to_bytes(),
        if_generation_match=0,
    )
    events: list[str] = []
    dispatcher = RecordingViewDispatcher(store, events)
    runner = JobRunner(
        pipeline=StubPipeline(
            SyncOutcome(committed=False, generation=12, owner_hmac="owner"),
            events,
        ),
        tasks=dispatcher,
        latest_generation=lambda: 12,
        request_view_generation=lambda generation: request_notion_generation(
            store,
            generation,
        ),
        clock=lambda: NOW,
    )

    runner.run()

    assert dispatcher.calls == []
    assert read_notion_sync_state(store)[0] == applied


def test_job_runner_recovers_a_lost_task_for_desired_but_unapplied_head() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    pending = NotionSyncState(
        desired_generation=13,
        applied_generation=12,
        status="IDLE",
        applying_generation=None,
        updated_at="2026-07-28T12:00:00Z",
    )
    write_state(
        store,
        NOTION_STATE_PATH,
        pending.to_bytes(),
        if_generation_match=0,
    )
    events: list[str] = []
    dispatcher = RecordingViewDispatcher(store, events)
    runner = JobRunner(
        pipeline=StubPipeline(
            SyncOutcome(committed=False, generation=13, owner_hmac="owner"),
            events,
        ),
        tasks=dispatcher,
        latest_generation=lambda: 13,
        request_view_generation=lambda generation: request_notion_generation(
            store,
            generation,
        ),
        clock=lambda: NOW,
    )

    runner.run()

    assert dispatcher.calls[0].generation == 13
    assert dispatcher.calls[0].desired_at_enqueue == 13


def test_job_runner_recovers_after_task_creation_failed_post_desired_write() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)

    def request(generation: int) -> bool:
        return request_notion_generation(store, generation)

    first = JobRunner(
        pipeline=StubPipeline(
            SyncOutcome(committed=True, generation=14, owner_hmac="owner"),
            [],
        ),
        tasks=FailingViewDispatcher(store),
        latest_generation=lambda: 14,
        request_view_generation=request,
        clock=lambda: NOW,
    )

    with pytest.raises(RuntimeError, match="simulated task"):
        first.run()
    assert read_notion_sync_state(store)[0].desired_generation == 14

    events: list[str] = []
    recovered = RecordingViewDispatcher(store, events)
    second = JobRunner(
        pipeline=StubPipeline(
            SyncOutcome(committed=False, generation=14, owner_hmac="owner"),
            events,
        ),
        tasks=recovered,
        latest_generation=lambda: 14,
        request_view_generation=request,
        clock=lambda: NOW + timedelta(hours=1),
    )
    second.run()

    assert recovered.calls[0].generation == 14
    assert recovered.calls[0].desired_at_enqueue == 14


def test_job_runner_calls_completion_hook_after_success() -> None:
    events: list[str] = []
    outcome = SyncOutcome(committed=False, generation=None, owner_hmac="owner")
    runner = JobRunner(
        pipeline=StubPipeline(outcome, events),
        tasks=RecordingViewDispatcher(InMemoryObjectStore(clock=lambda: NOW), events),
        latest_generation=lambda: 0,
        request_view_generation=lambda _generation: False,
        on_complete=lambda: events.append("complete"),
    )

    assert runner.run() == outcome
    assert events == ["pipeline", "complete"]


def test_job_runner_calls_completion_hook_after_failure() -> None:
    events: list[str] = []
    runner = JobRunner(
        pipeline=FailingPipeline(events),
        tasks=RecordingViewDispatcher(InMemoryObjectStore(clock=lambda: NOW), events),
        latest_generation=lambda: 0,
        request_view_generation=lambda _generation: False,
        on_complete=lambda: events.append("complete"),
    )

    with pytest.raises(RuntimeError, match="simulated pipeline failure"):
        runner.run()
    assert events == ["pipeline", "complete"]


def test_job_runner_enqueues_pending_webhook_recovery_after_main_work() -> None:
    events: list[str] = []
    sync_dispatcher = RecordingSyncDispatcher(events)

    def pending() -> bool:
        events.append("pending")
        return True

    runner = JobRunner(
        pipeline=StubPipeline(
            SyncOutcome(committed=True, generation=15, owner_hmac="owner"),
            events,
        ),
        tasks=RecordingViewDispatcher(
            InMemoryObjectStore(clock=lambda: NOW),
            events,
        ),
        latest_generation=lambda: 15,
        request_view_generation=lambda _generation: False,
        sync_tasks=sync_dispatcher,
        sync_recovery_needed=pending,
        clock=lambda: NOW,
    )

    runner.run()

    assert events == ["pipeline", "pending", "recover"]
    assert sync_dispatcher.calls == [SyncTaskCall(owner_hmac="owner", ready_at=NOW)]


def test_job_runner_skips_sync_recovery_without_pending_webhook() -> None:
    events: list[str] = []
    sync_dispatcher = RecordingSyncDispatcher(events)
    runner = JobRunner(
        pipeline=StubPipeline(
            SyncOutcome(committed=False, generation=15, owner_hmac="owner"),
            events,
        ),
        tasks=RecordingViewDispatcher(
            InMemoryObjectStore(clock=lambda: NOW),
            events,
        ),
        latest_generation=lambda: 15,
        request_view_generation=lambda _generation: False,
        sync_tasks=sync_dispatcher,
        sync_recovery_needed=lambda: False,
        clock=lambda: NOW,
    )

    runner.run()

    assert sync_dispatcher.calls == []


def test_sync_recovery_failure_does_not_fail_a_successful_commit() -> None:
    events: list[str] = []
    errors: list[str] = []
    outcome = SyncOutcome(committed=True, generation=16, owner_hmac="owner")
    runner = JobRunner(
        pipeline=StubPipeline(outcome, events),
        tasks=RecordingViewDispatcher(
            InMemoryObjectStore(clock=lambda: NOW),
            events,
        ),
        latest_generation=lambda: 16,
        request_view_generation=lambda _generation: False,
        sync_tasks=FailingSyncDispatcher(),
        sync_recovery_needed=lambda: True,
        on_sync_recovery_error=lambda exc: errors.append(type(exc).__name__),
        clock=lambda: NOW,
    )

    assert runner.run() == outcome
    assert errors == ["RuntimeError"]


def test_job_runner_requires_both_sync_recovery_dependencies() -> None:
    with pytest.raises(ValueError, match="configured together"):
        JobRunner(
            pipeline=StubPipeline(
                SyncOutcome(committed=False, generation=None, owner_hmac="owner"),
                [],
            ),
            tasks=RecordingViewDispatcher(
                InMemoryObjectStore(clock=lambda: NOW),
                [],
            ),
            latest_generation=lambda: 0,
            request_view_generation=lambda _generation: False,
            sync_tasks=RecordingSyncDispatcher([]),
        )


def test_sync_job_task_runs_at_next_boundary_and_deduplicates_bucket() -> None:
    client = FakeCloudTasksClient()
    config = _config()
    dispatcher = CloudTaskDispatcher(client=client, config=config)

    first = dispatcher.enqueue_sync_job(
        owner_hmac="owner:hash",
        ready_at=NOW,
    )
    duplicate = dispatcher.enqueue_sync_job(
        owner_hmac="owner:hash",
        ready_at=NOW + timedelta(minutes=2),
    )
    next_bucket = dispatcher.enqueue_sync_job(
        owner_hmac="owner:hash",
        ready_at=NOW + timedelta(minutes=3),
    )

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.task_name == first.task_name
    assert first.task_name.endswith("/owner-hash-sync-20260728T1200Z")
    assert next_bucket.created is True
    assert next_bucket.task_name.endswith("/owner-hash-sync-20260728T1210Z")

    request = client.requests[0]
    assert request["parent"] == config.tasks_parent
    task = cast(Mapping[str, Any], request["task"])
    http = cast(Mapping[str, Any], task["http_request"])
    assert task["schedule_time"] == datetime(
        2026,
        7,
        28,
        12,
        10,
        tzinfo=UTC,
    )
    next_task = cast(Mapping[str, Any], client.requests[2]["task"])
    assert next_task["schedule_time"] == datetime(
        2026,
        7,
        28,
        12,
        20,
        tzinfo=UTC,
    )
    assert http == {
        "http_method": tasks_v2.HttpMethod.POST,
        "url": config.jobs_run_url,
        "headers": {"Content-Type": "application/json"},
        "body": b"{}",
        "oauth_token": {
            "service_account_email": config.cloud_tasks_service_account,
            "scope": CLOUD_PLATFORM_SCOPE,
        },
    }


def test_view_task_uses_oidc_generation_payload_and_bucket_deduplication() -> None:
    client = FakeCloudTasksClient()
    config = _config()
    dispatcher = CloudTaskDispatcher(client=client, config=config)

    first = dispatcher.enqueue_view_update(
        owner_hmac="owner",
        generation=42,
        requested_at=NOW,
    )
    duplicate = dispatcher.enqueue_view_update(
        owner_hmac="owner",
        generation=42,
        requested_at=NOW + timedelta(minutes=2),
    )

    assert first.created is True
    assert duplicate.created is False
    assert first.task_name == duplicate.task_name
    assert first.task_name.endswith("/owner-views-00000000000000000042-20260728T1200Z")

    task = cast(Mapping[str, Any], client.requests[0]["task"])
    http = cast(Mapping[str, Any], task["http_request"])
    assert http["url"] == f"{config.cloud_run_service_url}/internal/tasks/apply-views"
    assert http["http_method"] == tasks_v2.HttpMethod.POST
    assert json.loads(cast(bytes, http["body"])) == {"requested_generation": 42}
    assert http["oidc_token"] == {
        "service_account_email": config.cloud_tasks_service_account,
        "audience": config.cloud_run_service_url,
    }
    assert "oauth_token" not in http
