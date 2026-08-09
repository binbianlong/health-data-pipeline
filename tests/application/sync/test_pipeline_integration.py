from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from health_data_pipeline.budget import ClassAMonthState, class_a_month_state_path
from health_data_pipeline.commit import CommitCoordinator, CommitRequest
from health_data_pipeline.google_health import FetchWindow
from health_data_pipeline.models import CommitKind
from health_data_pipeline.pipeline import (
    SUPPORTED_DATA_TYPES,
    BackfillChunk,
    _backfill_budget_allows,
)
from health_data_pipeline.storage import (
    ContentAddressedStore,
    InMemoryObjectStore,
)
from tests.support.pipeline import (
    NOW,
    FakeHealth,
)
from tests.support.pipeline import (
    build_pipeline as _pipeline,
)
from tests.support.pipeline import (
    civil_date as _civil_date,
)
from tests.support.pipeline import (
    daily_hrv_point as _daily_hrv_point,
)
from tests.support.pipeline import (
    daily_rows as _daily_rows,
)
from tests.support.pipeline import (
    daily_spo2_point as _daily_spo2_point,
)
from tests.support.pipeline import (
    hrv_detail_point as _hrv_detail_point,
)
from tests.support.pipeline import (
    sleep_point as _sleep_point,
)
from tests.support.pipeline import (
    spo2_detail_point as _spo2_detail_point,
)
from tests.support.pipeline import (
    store_webhook_event as _store_webhook_event,
)


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
    observed = next(
        row
        for row in rows
        if row["record_kind"] == "METRIC"
        and row["metric_name"] == "steps"
        and row["period_kind"] == "DAY"
    )
    weekly = next(
        row
        for row in rows
        if row["record_kind"] == "METRIC"
        and row["metric_name"] == "steps"
        and row["period_kind"] == "CALENDAR_WEEK"
    )
    monthly = next(
        row
        for row in rows
        if row["record_kind"] == "METRIC"
        and row["metric_name"] == "steps"
        and row["period_kind"] == "CALENDAR_MONTH"
    )
    assert observed["value_num"] == 8_000
    assert weekly["period_start"] == "2026-07-27"
    assert weekly["period_end"] == "2026-07-28"
    assert weekly["value_num"] == 7_500
    assert monthly["period_start"] == "2026-07-01"
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    source_statuses = {
        row["dimension_key"]: row for row in rows if row["record_kind"] == "SOURCE_STATUS"
    }
    assert set(source_statuses) == set(SUPPORTED_DATA_TYPES)
    assert source_statuses["steps"]["value_text"] == "SUCCESS_WITH_DATA"
    assert source_statuses["steps"]["value_num"] == 1
    assert source_statuses["sleep"]["value_text"] == "SUCCESS_EMPTY"
    assert source_statuses["sleep"]["value_num"] == 0
    assert all(row["coverage_kind"] == "NOT_APPLICABLE" for row in source_statuses.values())
    missing_sleep = next(
        row
        for row in rows
        if row["record_kind"] == "METRIC"
        and row["metric_name"] == "sleep_minutes"
        and row["period_kind"] == "DAY"
    )
    assert missing_sleep["value_num"] is None
    assert missing_sleep["missing_reason"] == "NO_SOURCE_VALUE"
    assert missing_sleep["data_status"] == "MISSING"
    assert root.state.data_type_cursors["steps"] == {
        "reconciled_through_date": "2026-07-28",
        "reconciled_at": "2026-07-28T12:00:00Z",
    }
    content = ContentAddressedStore(store)
    manifests = [
        json.loads(content.read(reference)) for reference in root.change_set.ingestion_manifests
    ]
    steps_manifest = next(
        manifest
        for manifest in manifests
        if manifest["data_type"] == "steps"
        and manifest["query_window"]["civil_start"] == today.isoformat()
    )
    assert steps_manifest["schema_version"] == 3
    assert steps_manifest["schema_version_daily"] == 3
    assert steps_manifest["normalizer_version"] == "daily-v5"
    assert "normalized_source" not in steps_manifest
    assert "blobs/flattened/" in steps_manifest["flattened_source"]["uri"]
    assert steps_manifest["source_stream_row_count"] == 1
    assert steps_manifest["flattened_source_row_count"] == 1
    assert steps_manifest["duplicate_source_row_count"] == 0
    assert steps_manifest["source_query_statuses"][today.isoformat()] == {
        "status": "SUCCESS_WITH_DATA",
        "source_count": 1,
        "normalized_count": 1,
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
        "normalizer_version": "daily-v5",
        "next_data_type_index": 0,
        "policy_version": 2,
    }
    assert len(root.change_set.ingestion_manifests) == 2
    assert len(root.change_set.raw_replacements) == 2


def test_session_backfill_materializes_sleep_stages_and_exercise_minutes() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 10)
    sleep = _sleep_point(
        metric_date,
        data_point_name="users/me/dataPoints/sleep-main",
        start_time="2026-07-09T16:00:00Z",
        end_time="2026-07-09T23:00:00Z",
        minutes_asleep=420,
        main_sleep=True,
        nap=False,
    )
    sleep["sleep"]["summary"] = {
        "minutesAsleep": "420",
        "stagesSummary": [
            {"type": "REM", "minutes": "90"},
            {"type": "DEEP", "minutes": "60"},
            {"type": "LIGHT", "minutes": "270"},
            {"type": "AWAKE", "minutes": "30"},
        ],
    }
    health = FakeHealth(
        values={},
        data_points={
            ("sleep", metric_date): (sleep,),
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
        if row["record_kind"] == "METRIC"
        and row["period_kind"] == "DAY"
        and row["statistic"] == "observed"
    }
    assert {metric_name: row["value_num"] for metric_name, row in observed.items()} == {
        "exercise_minutes": 30,
        "sleep_awake_minutes": 30,
        "sleep_deep_minutes": 60,
        "sleep_light_minutes": 270,
        "sleep_minutes": 420,
        "sleep_rem_minutes": 90,
    }
    assert all(row["logic_version"] == "daily-v5" for row in observed.values())
    sessions = [row for row in rows if row.get("record_kind") == "SLEEP_SESSION"]
    assert len(sessions) == 1
    assert sessions[0]["session_start_time"] == "2026-07-09T16:00:00Z"
    assert sessions[0]["session_end_time"] == "2026-07-09T23:00:00Z"
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    shard = snapshots.load_year_shard(root.state.daily_year_shards["2026"])
    assert shard.daily_revisions[metric_date.isoformat()].schema_version == 3


def test_sleep_uses_main_session_for_totals_and_retains_main_and_nap_boundaries() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 28)
    main = _sleep_point(
        metric_date,
        data_point_name="users/me/dataPoints/main",
        start_time="2026-07-27T14:30:00Z",
        end_time="2026-07-27T22:30:00Z",
        minutes_asleep=420,
        main_sleep=True,
        nap=False,
    )
    nap = _sleep_point(
        metric_date,
        data_point_name="users/me/dataPoints/nap",
        start_time="2026-07-28T05:00:00Z",
        end_time="2026-07-28T05:30:00Z",
        minutes_asleep=25,
        main_sleep=False,
        nap=True,
    )
    health = FakeHealth(
        values={},
        data_points={("sleep", metric_date): (main, nap)},
    )
    pipeline, heads, snapshots = _pipeline(store, health)

    assert pipeline.run().committed

    rows = _daily_rows(heads, snapshots, metric_date)
    sleep_total = next(
        row for row in rows if row["metric_name"] == "sleep_minutes" and row["period_kind"] == "DAY"
    )
    sessions = [row for row in rows if row.get("record_kind") == "SLEEP_SESSION"]
    assert sleep_total["value_num"] == 420
    assert len(sessions) == 2
    assert {row["session_is_main"] for row in sessions} == {False, True}
    assert {row["session_is_nap"] for row in sessions} == {False, True}
    assert all("users/me" not in str(row["dimension_key"]) for row in sessions)


def test_sleep_boundary_only_correction_changes_the_daily_revision() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 28)
    point = _sleep_point(
        metric_date,
        data_point_name="users/me/dataPoints/main",
        start_time="2026-07-27T14:30:00Z",
        end_time="2026-07-27T22:30:00Z",
        minutes_asleep=420,
        main_sleep=True,
        nap=False,
    )
    health = FakeHealth(values={}, data_points={("sleep", metric_date): (point,)})
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed
    first_head = heads.read().head
    assert first_head is not None
    first_root = snapshots.load_root_snapshot(first_head.root_snapshot)
    first_shard = snapshots.load_year_shard(first_root.state.daily_year_shards["2026"])
    first_revision = first_shard.daily_revisions[metric_date.isoformat()]

    health.data_points[("sleep", metric_date)] = (
        _sleep_point(
            metric_date,
            data_point_name="users/me/dataPoints/main",
            start_time="2026-07-27T14:35:00Z",
            end_time="2026-07-27T22:30:00Z",
            minutes_asleep=420,
            main_sleep=True,
            nap=False,
        ),
    )
    _store_webhook_event(store, data_type="sleep", metric_date=metric_date)

    assert pipeline.run().committed

    second_head = heads.read().head
    assert second_head is not None
    second_root = snapshots.load_root_snapshot(second_head.root_snapshot)
    second_shard = snapshots.load_year_shard(second_root.state.daily_year_shards["2026"])
    second_revision = second_shard.daily_revisions[metric_date.isoformat()]
    assert second_revision.canonical_sha256 != first_revision.canonical_sha256
    assert second_root.change_set.changed_metric_dates == (metric_date,)


def test_non_sleep_recalculation_preserves_committed_sleep_sessions() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 28)
    point = _sleep_point(
        metric_date,
        data_point_name="users/me/dataPoints/main",
        start_time="2026-07-27T14:30:00Z",
        end_time="2026-07-27T22:30:00Z",
        minutes_asleep=420,
        main_sleep=True,
        nap=False,
    )
    health = FakeHealth(
        values={metric_date: 8_000},
        data_points={("sleep", metric_date): (point,)},
    )
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed
    before = [
        row
        for row in _daily_rows(heads, snapshots, metric_date)
        if row.get("record_kind") == "SLEEP_SESSION"
    ]

    health.values[metric_date] = 8_100
    _store_webhook_event(store, data_type="steps", metric_date=metric_date)
    assert pipeline.run().committed

    after = [
        row
        for row in _daily_rows(heads, snapshots, metric_date)
        if row.get("record_kind") == "SLEEP_SESSION"
    ]
    assert after == before


def test_empty_sleep_replacement_removes_stale_sessions_but_keeps_other_metrics() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    metric_date = date(2026, 7, 28)
    health = FakeHealth(
        values={metric_date: 8_000},
        data_points={
            ("sleep", metric_date): (
                _sleep_point(
                    metric_date,
                    data_point_name="users/me/dataPoints/main",
                    start_time="2026-07-27T14:30:00Z",
                    end_time="2026-07-27T22:30:00Z",
                    minutes_asleep=420,
                    main_sleep=True,
                    nap=False,
                ),
            )
        },
    )
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed
    health.data_points[("sleep", metric_date)] = ()
    _store_webhook_event(store, data_type="sleep", metric_date=metric_date)

    assert pipeline.run().committed

    rows = _daily_rows(heads, snapshots, metric_date)
    assert not any(row.get("record_kind") == "SLEEP_SESSION" for row in rows)
    sleep_total = next(
        row
        for row in rows
        if row.get("record_kind") == "METRIC"
        and row.get("metric_name") == "sleep_minutes"
        and row.get("period_kind") == "DAY"
    )
    assert sleep_total["value_num"] is None
    assert sleep_total["missing_reason"] == "NO_SOURCE_VALUE"
    assert any(row.get("metric_name") == "steps" for row in rows)


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
    current_version: dict[str, Any] = {"_schedule": {"normalizer_version": "daily-v5"}}

    assert _backfill_budget_allows(store, old_version, now=NOW)
    assert not _backfill_budget_allows(store, current_version, now=NOW)
    assert not _backfill_budget_allows(store, {}, now=NOW)


def test_normalizer_upgrade_commits_once_then_returns_to_backfill_pause() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    pipeline, heads, snapshots = _pipeline(store, health)
    assert pipeline.run().committed

    coordinator = CommitCoordinator(heads=heads, snapshots=snapshots)
    coordinator.commit(
        CommitRequest(
            daily_rows={},
            commit_kind=CommitKind.REBUILD,
            created_at=NOW,
            backfill_cursors={
                "_schedule": {
                    "normalizer_version": "daily-v2",
                }
            },
            preserve_fitbit_last_sync_time=True,
        ),
        expected_base=heads.read(),
    )
    store.put(
        class_a_month_state_path("2026-07"),
        ClassAMonthState(
            schema_version=1,
            month="2026-07",
            estimated_operations=4_034,
            updated_at="2026-07-28T12:00:00Z",
        ).to_bytes(),
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )
    initial_request_count = len(health.requests)

    correction = pipeline.run()
    corrected_request_count = len(health.requests)
    paused = pipeline.run()

    assert correction.committed
    assert not paused.committed
    assert corrected_request_count == initial_request_count + 2
    assert len(health.requests) == corrected_request_count
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    schedule = root.state.backfill_cursors["_schedule"]
    assert isinstance(schedule, dict)
    assert schedule["normalizer_version"] == "daily-v5"


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
