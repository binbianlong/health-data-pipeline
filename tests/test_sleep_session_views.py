from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

from health_data_pipeline.commit import CommitCoordinator, CommitRequest
from health_data_pipeline.models import CommitKind
from health_data_pipeline.notion_adapter import SnapshotViewSource
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import ContentAddressedStore, HeadRepository, InMemoryObjectStore
from health_data_pipeline.views import DailyView, SleepSessionView, render_notion_body


def _daily_view(
    *,
    sessions: tuple[SleepSessionView, ...] = (),
    available: bool,
) -> DailyView:
    return DailyView(
        metric_date=date(2026, 11, 1),
        source_generation=7,
        data_status="CANONICAL_SYNCED",
        fitbit_last_sync_time=datetime(2026, 11, 1, 12, tzinfo=UTC),
        metrics=(),
        sleep_sessions=sessions,
        sleep_sessions_available=available,
    )


def test_notion_separates_main_sleep_and_nap_with_edge_offsets() -> None:
    main = SleepSessionView(
        start_time=datetime(
            2026,
            10,
            31,
            23,
            30,
            tzinfo=timezone(timedelta(hours=2)),
        ),
        end_time=datetime(
            2026,
            11,
            1,
            7,
            30,
            tzinfo=timezone(timedelta(hours=1)),
        ),
        is_main=True,
        is_nap=False,
        minutes_asleep=480,
    )
    nap = SleepSessionView(
        start_time=datetime(
            2026,
            11,
            1,
            14,
            5,
            tzinfo=timezone(timedelta(hours=1)),
        ),
        end_time=datetime(
            2026,
            11,
            1,
            14,
            35,
            tzinfo=timezone(timedelta(hours=1)),
        ),
        is_main=False,
        is_nap=True,
        minutes_asleep=25,
    )

    body = render_notion_body(_daily_view(sessions=(nap, main), available=True))

    assert (
        "- 主睡眠: 就寝時刻 2026-10-31T23:30+02:00 / "
        "起床時刻 2026-11-01T07:30+01:00 / 睡眠時間: 480分"
    ) in body
    assert (
        "- 昼寝 1: 就寝時刻 2026-11-01T14:05+01:00 / "
        "起床時刻 2026-11-01T14:35+01:00 / 睡眠時間: 25分"
    ) in body


def test_notion_does_not_duplicate_main_sleep_that_is_also_marked_as_nap() -> None:
    ambiguous = SleepSessionView(
        start_time=datetime(2026, 10, 31, 23, tzinfo=UTC),
        end_time=datetime(2026, 11, 1, 7, tzinfo=UTC),
        is_main=True,
        is_nap=True,
        minutes_asleep=420,
    )

    body = render_notion_body(_daily_view(sessions=(ambiguous,), available=True))

    assert body.count("2026-10-31T23:00+00:00") == 1
    assert "- 主睡眠:" in body
    assert "- 昼寝 1:" not in body
    assert "昼寝: なし" in body


def test_notion_distinguishes_unavailable_sessions_from_no_naps() -> None:
    unavailable = render_notion_body(_daily_view(available=False))
    main_only = render_notion_body(
        _daily_view(
            sessions=(
                SleepSessionView(
                    start_time=datetime(2026, 10, 31, 23, tzinfo=UTC),
                    end_time=datetime(2026, 11, 1, 7, tzinfo=UTC),
                    is_main=True,
                    is_nap=False,
                ),
            ),
            available=True,
        )
    )

    assert "主睡眠: 未取得" in unavailable
    assert "昼寝: 未取得" in unavailable
    assert "- 主睡眠:" in main_only
    assert "昼寝: なし" in main_only
    assert "昼寝: 未取得" not in main_only


def _v2_common(*, record_kind: str, metric_name: str) -> dict[str, object]:
    return {
        "record_kind": record_kind,
        "metric_name": metric_name,
        "statistic": "observed",
        "period_kind": "DAY",
        "dimension_key": None,
        "value_num": None,
        "value_text": None,
        "unit": "count",
        "aggregation": "daily_value",
        "period_start": "2026-11-01",
        "period_end": "2026-11-01",
        "sample_count": 1,
        "expected_count": 1,
        "coverage_ratio": 1,
        "data_status": "CANONICAL_SYNCED",
        "quality_flags": [],
        "source_data_types": [],
        "fitbit_last_sync_time": "2026-11-01T12:00:00+00:00",
        "time_zone": "Europe/Paris",
        "physical_day_start": "2026-10-31T23:00:00+00:00",
        "physical_day_end": "2026-11-01T23:00:00+00:00",
        "logic_version": "daily-v4",
        "session_start_time": None,
        "session_end_time": None,
        "session_start_utc_offset_seconds": None,
        "session_end_utc_offset_seconds": None,
        "session_is_main": None,
        "session_is_nap": None,
    }


def test_snapshot_v2_decodes_sleep_session_without_exposing_it_as_metric() -> None:
    store = InMemoryObjectStore()
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(ContentAddressedStore(store))
    coordinator = CommitCoordinator(heads=heads, snapshots=snapshots)
    metric_row = {
        **_v2_common(record_kind="METRIC", metric_name="steps"),
        "value_num": 8_000,
    }
    session_row = {
        **_v2_common(
            record_kind="SLEEP_SESSION",
            metric_name="sleep_session_minutes_asleep",
        ),
        "period_kind": "SESSION",
        "dimension_key": "sleep:0123456789abcdef",
        "value_num": 480,
        "unit": "min",
        "aggregation": "session_value",
        "source_data_types": ["sleep"],
        "session_start_time": "2026-10-31T21:30:00+00:00",
        "session_end_time": "2026-11-01T06:30:00+00:00",
        "session_start_utc_offset_seconds": 7_200,
        "session_end_utc_offset_seconds": 3_600,
        "session_is_main": True,
        "session_is_nap": False,
    }
    coordinator.commit(
        CommitRequest(
            daily_rows={date(2026, 11, 1): (metric_row, session_row)},
            commit_kind=CommitKind.SCHEDULED,
            created_at=datetime(2026, 11, 1, 12, tzinfo=UTC),
            fitbit_last_sync_time=datetime(2026, 11, 1, 12, tzinfo=UTC),
            time_zone="Europe/Paris",
            daily_schema_version=2,
        ),
        expected_base=heads.read(),
    )

    source = SnapshotViewSource(heads=heads, snapshots=snapshots)
    [view] = source.views_between(after_generation=0, through_generation=1)

    assert [metric.metric_name for metric in view.metrics] == ["steps"]
    assert view.sleep_sessions_available
    assert len(view.sleep_sessions) == 1
    session = view.sleep_sessions[0]
    assert session.start_time.isoformat(timespec="minutes") == "2026-10-31T23:30+02:00"
    assert session.end_time.isoformat(timespec="minutes") == "2026-11-01T07:30+01:00"
    assert session.is_main
    assert not session.is_nap
    assert session.minutes_asleep == 480
