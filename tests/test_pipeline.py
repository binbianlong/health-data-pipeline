from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from health_data_pipeline.budget import ClassAMonthState, class_a_month_state_path
from health_data_pipeline.commit import CommitCoordinator
from health_data_pipeline.google_health import DataPointPage, FetchWindow
from health_data_pipeline.models import CommitKind, ContentReference
from health_data_pipeline.pipeline import (
    SUPPORTED_DATA_TYPES,
    BackfillChunk,
    IncrementalSyncPipeline,
    IngestedQuery,
    _advance_backfill_cursors_many,
    _backfill_budget_allows,
    _interval_dates,
    _reconciliation_fetch_start,
    _reopen_backfill_gaps,
    _select_backfill_chunks,
    _unique_manifest_references,
)
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import (
    ContentAddressedStore,
    HeadRepository,
    InMemoryObjectStore,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
SYNC_TIME = datetime(2026, 7, 28, 11, tzinfo=UTC)


def _civil_date(value: date) -> dict[str, Any]:
    return {
        "year": value.year,
        "month": value.month,
        "day": value.day,
    }


def _steps_point(value: date, count: int) -> Mapping[str, Any]:
    return {
        "steps": {
            "count": count,
            "interval": {
                "civilStartTime": {
                    "date": _civil_date(value),
                }
            },
        }
    }


class FakeHealth:
    def __init__(
        self,
        *,
        values: Mapping[date, int],
        data_points: Mapping[
            tuple[str, date],
            Sequence[Mapping[str, Any]],
        ]
        | None = None,
        sync_time: datetime | None = SYNC_TIME,
        fail_after: int | None = None,
    ) -> None:
        self.values = dict(values)
        self.data_points = {key: tuple(points) for key, points in (data_points or {}).items()}
        self.sync_time = sync_time
        self.fail_after = fail_after
        self.requests: list[tuple[str, FetchWindow]] = []

    def get_settings(self) -> Mapping[str, Any]:
        return {"timeZone": "Asia/Tokyo"}

    def latest_fitbit_sync_time(self) -> datetime | None:
        return self.sync_time

    def iter_data_point_pages(
        self,
        data_type: str,
        window: FetchWindow,
    ) -> Sequence[DataPointPage]:
        self.requests.append((data_type, window))
        if self.fail_after is not None and len(self.requests) > self.fail_after:
            raise RuntimeError("injected API failure")
        points: tuple[Mapping[str, Any], ...] = ()
        if data_type == "steps" and window.civil_start in self.values:
            assert window.civil_start is not None
            points = (
                _steps_point(
                    window.civil_start,
                    self.values[window.civil_start],
                ),
            )
        if window.civil_start is not None and window.civil_end is not None:
            points += tuple(
                point
                for (point_type, point_date), stored_points in sorted(
                    self.data_points.items(),
                    key=lambda item: item[0],
                )
                if point_type == data_type and window.civil_start <= point_date < window.civil_end
                for point in stored_points
            )
        return (
            DataPointPage(
                data_type=data_type,
                window=window,
                page_number=0,
                data_points=points,
                next_page_token=None,
                raw_payload={"dataPoints": list(points)},
            ),
        )


def _pipeline(
    store: InMemoryObjectStore,
    health: FakeHealth,
    *,
    now: datetime = NOW,
    backfill_ready: Callable[[int], bool] | None = None,
) -> tuple[
    IncrementalSyncPipeline,
    HeadRepository,
    SnapshotRepository,
]:
    content = ContentAddressedStore(store)
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(content)
    return (
        IncrementalSyncPipeline(
            store=store,
            content=content,
            heads=heads,
            snapshots=snapshots,
            commits=CommitCoordinator(heads=heads, snapshots=snapshots),
            health=health,
            owner_hmac="a" * 64,
            backfill_ready=backfill_ready,
            now=lambda: now,
        ),
        heads,
        snapshots,
    )


def _daily_rows(
    heads: HeadRepository,
    snapshots: SnapshotRepository,
    metric_date: date,
) -> list[dict[str, Any]]:
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    shard = snapshots.load_year_shard(root.state.daily_year_shards[str(metric_date.year)])
    revision = shard.daily_revisions[metric_date.isoformat()]
    return [json.loads(line) for line in snapshots.read_daily_revision(revision).splitlines()]


def test_initial_sync_commits_civil_days_and_calendar_periods() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    today = date(2026, 7, 28)
    yesterday = today - timedelta(days=1)
    health = FakeHealth(values={yesterday: 7_000, today: 8_000})
    pipeline, heads, snapshots = _pipeline(store, health)

    first = pipeline.run()

    assert first.committed
    assert first.generation == 1
    assert len(health.requests) == len(SUPPORTED_DATA_TYPES) * 2
    assert all(
        window.civil_end == window.civil_start + timedelta(days=1)
        for _, window in health.requests
        if window.civil_start is not None
    )
    rows = _daily_rows(heads, snapshots, today)
    observed = next(row for row in rows if row["period_kind"] == "DAY")
    weekly = next(row for row in rows if row["period_kind"] == "CALENDAR_WEEK")
    monthly = next(row for row in rows if row["period_kind"] == "CALENDAR_MONTH")
    assert observed["value_num"] == 8_000
    assert weekly["period_start"] == "2026-07-27"
    assert weekly["period_end"] == "2026-07-28"
    assert weekly["value_num"] == 7_500
    assert monthly["period_start"] == "2026-07-01"
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    assert root.state.data_type_cursors["steps"] == {
        "reconciled_through_date": "2026-07-28",
        "reconciled_at": "2026-07-28T12:00:00Z",
    }


def test_sync_advance_reconciles_from_last_successful_civil_cursor() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    first_pipeline, _heads, _snapshots = _pipeline(store, health)
    assert first_pipeline.run().committed
    initial_request_count = len(health.requests)

    later = datetime(2026, 8, 2, 12, tzinfo=UTC)
    health.sync_time = later - timedelta(hours=1)
    second_pipeline, heads, snapshots = _pipeline(store, health, now=later)
    outcome = second_pipeline.run()

    assert outcome.committed
    requests = health.requests[initial_request_count:]
    expected_dates = {date(2026, 7, 28) + timedelta(days=offset) for offset in range(6)}
    assert len(requests) == len(SUPPORTED_DATA_TYPES) * len(expected_dates)
    for data_type in SUPPORTED_DATA_TYPES:
        assert {
            window.civil_start for request_type, window in requests if request_type == data_type
        } == expected_dates
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    assert root.state.data_type_cursors["steps"] == {
        "reconciled_through_date": "2026-08-02",
        "reconciled_at": "2026-08-02T12:00:00Z",
    }


def test_each_idle_run_backfills_next_sleep_and_exercise_interval() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    pipeline, heads, snapshots = _pipeline(store, health)
    initial = pipeline.run()

    backfill = pipeline.run()
    request_count = len(health.requests)
    repeated = pipeline.run()

    assert initial.committed
    assert backfill.committed
    assert backfill.generation == 2
    assert repeated.committed
    assert repeated.generation == 3
    assert len(health.requests) == request_count + 2
    backfill_requests = health.requests[len(SUPPORTED_DATA_TYPES) * 2 : request_count]
    assert [
        (data_type, window.civil_start, window.civil_end) for data_type, window in backfill_requests
    ] == [
        ("sleep", date(2026, 6, 28), date(2026, 7, 29)),
        ("exercise", date(2026, 6, 28), date(2026, 7, 29)),
    ]
    assert [
        (data_type, window.civil_start, window.civil_end)
        for data_type, window in health.requests[request_count:]
    ] == [
        ("sleep", date(2026, 5, 28), date(2026, 6, 28)),
        ("exercise", date(2026, 5, 28), date(2026, 6, 28)),
    ]
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    assert root.change_set.commit_kind is CommitKind.BACKFILL
    assert (
        root.state.backfill_cursors["sleep"]
        == root.state.backfill_cursors["exercise"]
        == {
            "next_end_date": "2026-05-27",
            "complete": False,
        }
    )
    assert root.state.backfill_cursors["_schedule"] == {
        "last_completed_civil_date": "2026-07-28",
        "normalizer_version": "daily-v3",
        "next_data_type_index": 0,
        "policy_version": 2,
    }
    assert len(root.change_set.ingestion_manifests) == 2
    assert len(root.change_set.raw_replacements) == 2


def test_session_backfill_materializes_sleep_stages_and_exercise_minutes() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 10)
    health = FakeHealth(
        values={},
        data_points={
            ("sleep", metric_date): (
                {
                    "sleep": {
                        "interval": {
                            "endTime": "2026-07-09T23:00:00Z",
                            "endUtcOffset": "32400s",
                        },
                        "summary": {
                            "minutesAsleep": "420",
                            "stagesSummary": [
                                {"type": "REM", "minutes": "90"},
                                {"type": "DEEP", "minutes": "60"},
                                {"type": "LIGHT", "minutes": "270"},
                                {"type": "AWAKE", "minutes": "30"},
                            ],
                        },
                    }
                },
            ),
            ("exercise", metric_date): (
                {
                    "exercise": {
                        "interval": {
                            "startTime": "2026-07-10T00:00:00Z",
                            "startUtcOffset": "32400s",
                        },
                        "activeDuration": "1800s",
                    }
                },
            ),
        },
    )
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed

    backfill = pipeline.run()

    assert backfill.committed
    rows = _daily_rows(heads, snapshots, metric_date)
    observed = {
        row["metric_name"]: row
        for row in rows
        if row["period_kind"] == "DAY" and row["statistic"] == "observed"
    }
    assert {metric_name: row["value_num"] for metric_name, row in observed.items()} == {
        "exercise_minutes": 30,
        "sleep_awake_minutes": 30,
        "sleep_deep_minutes": 60,
        "sleep_light_minutes": 270,
        "sleep_minutes": 420,
        "sleep_rem_minutes": 90,
    }
    assert all(row["logic_version"] == "daily-v3" for row in observed.values())


def test_backfill_failure_leaves_cursor_and_head_unchanged() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    pipeline, heads, snapshots = _pipeline(store, health)
    initial = pipeline.run()
    health.fail_after = len(health.requests) + 1

    with pytest.raises(RuntimeError, match="injected API failure"):
        pipeline.run()

    assert initial.generation == 1
    head = heads.read().head
    assert head is not None
    assert head.generation == 1
    root = snapshots.load_root_snapshot(head.root_snapshot)
    assert root.state.backfill_cursors == {}


def test_backfill_pauses_at_class_a_notice_and_resumes_next_utc_month() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed
    initial_request_count = len(health.requests)
    state = ClassAMonthState(
        schema_version=1,
        month="2026-07",
        estimated_operations=3_000,
        updated_at="2026-07-28T12:00:00Z",
    )
    store.put(
        class_a_month_state_path("2026-07"),
        state.to_bytes(),
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )

    paused = pipeline.run()

    assert not paused.committed
    assert len(health.requests) == initial_request_count
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    assert root.state.backfill_cursors == {}

    august = datetime(2026, 8, 1, 12, tzinfo=UTC)
    august_pipeline, _, _ = _pipeline(store, health, now=august)
    resumed = august_pipeline.run()

    assert resumed.committed
    assert [data_type for data_type, _ in health.requests[initial_request_count:]] == [
        "sleep",
        "exercise",
    ]


def test_normalizer_upgrade_allows_only_one_bounded_correction_batch() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    state = ClassAMonthState(
        schema_version=1,
        month="2026-07",
        estimated_operations=3_894,
        updated_at="2026-07-28T12:00:00Z",
    )
    store.put(
        class_a_month_state_path("2026-07"),
        state.to_bytes(),
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )

    old_version: dict[str, Any] = {"_schedule": {"normalizer_version": "daily-v2"}}
    current_version: dict[str, Any] = {"_schedule": {"normalizer_version": "daily-v3"}}

    assert _backfill_budget_allows(store, old_version, now=NOW)
    assert not _backfill_budget_allows(store, current_version, now=NOW)
    assert not _backfill_budget_allows(store, {}, now=NOW)


def test_backfill_waits_for_notion_without_blocking_the_initial_sync() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    pipeline, _, _ = _pipeline(
        store,
        health,
        backfill_ready=lambda _generation: False,
    )

    initial = pipeline.run()
    initial_request_count = len(health.requests)
    idle = pipeline.run()

    assert initial.committed
    assert not idle.committed
    assert len(health.requests) == initial_request_count


def test_physical_replacement_scope_matches_the_exact_query_window() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    pipeline, heads, snapshots = _pipeline(store, health)

    assert pipeline.run().committed

    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    physical = next(
        replacement
        for replacement in root.change_set.raw_replacements
        if isinstance(replacement, dict) and replacement.get("data_type") == "steps"
    )
    assert physical == {
        "data_type": "steps",
        "kind": "PHYSICAL_WINDOW_REPLACE",
        "physical_start": "2026-07-26T15:00:00Z",
        "physical_end": "2026-07-27T15:00:00Z",
    }


def test_pending_webhook_is_processed_before_backfill() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={date(2026, 7, 25): 6_000})
    pipeline, heads, snapshots = _pipeline(store, health)
    pipeline.run()
    initial_request_count = len(health.requests)
    event = {
        "dataType": "steps",
        "intervals": [
            {
                "civilDateTimeInterval": {
                    "startDateTime": {"date": _civil_date(date(2026, 7, 25))},
                    "endDateTime": {"date": _civil_date(date(2026, 7, 25))},
                }
            }
        ],
    }
    store.put(
        "webhook-events/event-1.json",
        json.dumps(event).encode(),
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )

    assert pipeline.has_pending_webhooks()
    outcome = pipeline.run()

    assert outcome.committed
    assert not pipeline.has_pending_webhooks()
    assert health.requests[initial_request_count:] == [
        (
            "steps",
            FetchWindow(
                start=datetime(2026, 7, 24, 15, tzinfo=UTC),
                end=datetime(2026, 7, 25, 15, tzinfo=UTC),
                civil_start=date(2026, 7, 25),
                civil_end=date(2026, 7, 26),
            ),
        )
    ]
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    assert root.change_set.commit_kind is CommitKind.WEBHOOK
    assert root.state.backfill_cursors == {}
    assert root.state.data_type_cursors["steps"] == {
        "reconciled_through_date": "2026-07-28",
        "reconciled_at": "2026-07-28T12:00:00Z",
    }


def test_hrv_webhook_refetches_daily_summary_before_shared_metric_replacement() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 28)
    detail_key = ("heart-rate-variability", metric_date)
    daily_key = ("daily-heart-rate-variability", metric_date)
    health = FakeHealth(
        values={},
        data_points={
            detail_key: (_hrv_detail_point(metric_date, 30),),
            daily_key: (_daily_hrv_point(metric_date, 42),),
        },
    )
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed
    initial_request_count = len(health.requests)
    health.data_points[detail_key] = (_hrv_detail_point(metric_date, 35),)
    _store_webhook_event(
        store,
        data_type="heart-rate-variability",
        metric_date=metric_date,
    )

    outcome = pipeline.run()

    assert outcome.committed
    requests = health.requests[initial_request_count:]
    assert [(data_type, window.civil_start) for data_type, window in requests] == [
        ("heart-rate-variability", metric_date),
        ("daily-heart-rate-variability", metric_date),
    ]
    rows = _daily_rows(heads, snapshots, metric_date)
    day = next(
        row for row in rows if row["metric_name"] == "hrv_rmssd" and row["period_kind"] == "DAY"
    )
    weekly = next(
        row
        for row in rows
        if row["metric_name"] == "hrv_rmssd" and row["period_kind"] == "CALENDAR_WEEK"
    )
    monthly = next(
        row
        for row in rows
        if row["metric_name"] == "hrv_rmssd" and row["period_kind"] == "CALENDAR_MONTH"
    )
    assert day["value_num"] == 42
    assert day["source_data_types"] == ["daily-heart-rate-variability"]
    assert weekly["value_num"] == 42
    assert weekly["period_start"] == "2026-07-27"
    assert monthly["value_num"] == 42
    assert monthly["period_start"] == "2026-07-01"


def test_partial_detail_ingestion_cannot_replace_existing_daily_summary() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 28)
    health = FakeHealth(
        values={},
        data_points={
            ("heart-rate-variability", metric_date): (_hrv_detail_point(metric_date, 30),),
            ("daily-heart-rate-variability", metric_date): (_daily_hrv_point(metric_date, 42),),
        },
    )
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)

    merged = pipeline._merge_changed_observations(
        root,
        (
            IngestedQuery(
                data_type="heart-rate-variability",
                metric_date=metric_date,
                data_points=(_hrv_detail_point(metric_date, 35),),
                manifest=ContentReference(
                    uri="manifests/ingestion/detail-only.json.gz",
                    canonical_sha256="a" * 64,
                    object_sha256="b" * 64,
                ),
                replacement_scope={
                    "data_type": "heart-rate-variability",
                    "metric_date": metric_date.isoformat(),
                },
            ),
        ),
    )

    hrv = next(
        observation for observation in merged[metric_date] if observation.metric_name == "hrv_rmssd"
    )
    assert hrv.value == 42
    assert hrv.source_data_types == ("daily-heart-rate-variability",)


def test_daily_spo2_deletion_refetches_detail_before_shared_metric_replacement() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 28)
    detail_key = ("oxygen-saturation", metric_date)
    daily_key = ("daily-oxygen-saturation", metric_date)
    health = FakeHealth(
        values={},
        data_points={
            detail_key: (_spo2_detail_point(metric_date, 95),),
            daily_key: (_daily_spo2_point(metric_date, 97),),
        },
    )
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed
    initial_request_count = len(health.requests)
    health.data_points[daily_key] = ()
    _store_webhook_event(
        store,
        data_type="daily-oxygen-saturation",
        metric_date=metric_date,
    )

    outcome = pipeline.run()

    assert outcome.committed
    requests = health.requests[initial_request_count:]
    assert [(data_type, window.civil_start) for data_type, window in requests] == [
        ("oxygen-saturation", metric_date),
        ("daily-oxygen-saturation", metric_date),
    ]
    rows = _daily_rows(heads, snapshots, metric_date)
    day = next(row for row in rows if row["metric_name"] == "spo2" and row["period_kind"] == "DAY")
    assert day["value_num"] == 95
    assert day["source_data_types"] == ["oxygen-saturation"]


def test_new_normalizer_reopens_recent_sleep_and_exercise_together() -> None:
    today = date(2026, 7, 28)
    cursors: dict[str, Any] = {
        "_schedule": {
            "last_completed_civil_date": today.isoformat(),
            "normalizer_version": "daily-v1",
            "policy_version": 1,
        },
        "sleep": {"next_end_date": "2010-01-01", "complete": False},
        "exercise": {"next_end_date": "2008-12-31", "complete": True},
    }

    chunks = _select_backfill_chunks(cursors, today=today)

    assert [chunk.data_type for chunk in chunks] == ["sleep", "exercise"]
    assert chunks[0].dates == chunks[1].dates
    assert chunks[0].dates[0] == date(2026, 6, 28)
    assert chunks[0].dates[-1] == date(2026, 7, 28)
    advanced = _advance_backfill_cursors_many(cursors, chunks, completed_on=today)
    assert (
        advanced["sleep"]
        == advanced["exercise"]
        == {
            "next_end_date": "2026-06-27",
            "complete": False,
        }
    )
    assert advanced["_schedule"] == {
        "last_completed_civil_date": "2026-07-28",
        "normalizer_version": "daily-v3",
        "next_data_type_index": 0,
        "policy_version": 2,
    }


def test_completed_priority_backfill_resumes_legacy_round_robin() -> None:
    today = date(2026, 7, 28)
    cursors: dict[str, Any] = {
        "_schedule": {
            "last_completed_civil_date": "2026-07-27",
            "normalizer_version": "daily-v3",
            "next_data_type_index": 0,
            "policy_version": 2,
        },
        "sleep": {"next_end_date": "2008-12-31", "complete": True},
        "exercise": {"next_end_date": "2008-12-31", "complete": True},
        "steps": {"next_end_date": "2026-05-10", "complete": False},
    }

    chunks = _select_backfill_chunks(cursors, today=today)

    assert len(chunks) == 1
    assert chunks[0].data_type == "steps"
    assert chunks[0].dates[0] == date(2026, 4, 27)
    assert chunks[0].dates[-1] == date(2026, 5, 10)
    advanced = _advance_backfill_cursors_many(cursors, chunks, completed_on=today)
    assert advanced["steps"] == {
        "next_end_date": "2026-04-26",
        "complete": False,
    }
    assert advanced["sleep"] == cursors["sleep"]
    assert advanced["exercise"] == cursors["exercise"]
    schedule = advanced["_schedule"]
    assert isinstance(schedule, dict)
    assert schedule["next_data_type_index"] == 1


def test_legacy_high_volume_chunk_is_ingested_one_day_at_a_time() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    pipeline, _, _ = _pipeline(store, health)
    dates = tuple(date(2026, 7, 20) + timedelta(days=offset) for offset in range(3))

    pipeline._ingest_backfill_chunk(
        BackfillChunk(
            data_type="heart-rate",
            dates=dates,
            floor=date(2026, 4, 30),
            next_data_type_index=4,
        ),
        zone=ZoneInfo("Asia/Tokyo"),
        expected_generation=1,
    )

    assert [
        (data_type, window.civil_start, window.civil_end) for data_type, window in health.requests
    ] == [
        ("heart-rate", dates[0], dates[0] + timedelta(days=1)),
        ("heart-rate", dates[1], dates[1] + timedelta(days=1)),
        ("heart-rate", dates[2], dates[2] + timedelta(days=1)),
    ]


def test_foreground_reconciliation_is_bounded_and_reopens_older_rollup_gap() -> None:
    today = date(2026, 7, 28)
    requested_start = date(2026, 1, 1)

    recent_start = _reconciliation_fetch_start(
        "steps",
        requested_start=requested_start,
        today=today,
    )
    high_frequency_start = _reconciliation_fetch_start(
        "heart-rate",
        requested_start=requested_start,
        today=today,
    )
    assert recent_start == today - timedelta(days=89)
    assert high_frequency_start == today - timedelta(days=89)

    reopened = _reopen_backfill_gaps(
        {
            "steps": {"next_end_date": "2008-12-31", "complete": True},
            "_schedule": {"last_completed_civil_date": "2026-07-27"},
        },
        {"steps": recent_start - timedelta(days=1)},
    )
    assert reopened["steps"] == {
        "next_end_date": (recent_start - timedelta(days=1)).isoformat(),
        "complete": False,
    }
    assert reopened["_schedule"] == {"last_completed_civil_date": "2026-07-27"}


def test_manifest_references_are_deduplicated_by_uri() -> None:
    reference = ContentReference(
        uri="manifests/ingestion/example.json.gz",
        canonical_sha256="a" * 64,
        object_sha256="b" * 64,
    )

    assert _unique_manifest_references((reference, reference)) == (reference,)


def test_api_failure_keeps_head_uncommitted() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(
        values={date(2026, 7, 28): 8_000},
        fail_after=1,
    )
    pipeline, heads, _ = _pipeline(store, health)

    with pytest.raises(RuntimeError, match="injected API failure"):
        pipeline.run()

    assert heads.read().head is None
    assert tuple(store.list("state/leases/")) == ()


def test_physical_and_civil_webhook_ranges_keep_every_changed_date() -> None:
    dates = _interval_dates(
        {
            "physicalTimeInterval": {
                "startTime": "2026-07-26T15:00:00Z",
                "endTime": "2026-07-29T15:00:00Z",
            },
            "civilDateTimeInterval": {
                "startDateTime": {"date": _civil_date(date(2026, 7, 27))},
                "endDateTime": {"date": _civil_date(date(2026, 7, 30))},
            },
        },
        zone=ZoneInfo("Asia/Tokyo"),
    )

    assert dates == {
        date(2026, 7, 27),
        date(2026, 7, 28),
        date(2026, 7, 29),
        date(2026, 7, 30),
    }


def _hrv_detail_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "heartRateVariability": {
            "rootMeanSquareOfSuccessiveDifferencesMilliseconds": value,
            "sampleTime": {"civilTime": {"date": _civil_date(metric_date)}},
        }
    }


def _daily_hrv_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "dailyHeartRateVariability": {
            "averageHeartRateVariabilityMilliseconds": value,
            "date": _civil_date(metric_date),
        }
    }


def _spo2_detail_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "oxygenSaturation": {
            "percentage": value,
            "sampleTime": {"civilTime": {"date": _civil_date(metric_date)}},
        }
    }


def _daily_spo2_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "dailyOxygenSaturation": {
            "averagePercentage": value,
            "date": _civil_date(metric_date),
        }
    }


def _store_webhook_event(
    store: InMemoryObjectStore,
    *,
    data_type: str,
    metric_date: date,
) -> None:
    event = {
        "dataType": data_type,
        "intervals": [
            {
                "civilDateTimeInterval": {
                    "startDateTime": {"date": _civil_date(metric_date)},
                    "endDateTime": {"date": _civil_date(metric_date)},
                }
            }
        ],
    }
    store.put(
        f"webhook-events/{data_type}.json",
        json.dumps(event).encode(),
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )
