from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from health_data_pipeline.application.sync.planning import (
    _advance_backfill_cursors_many,
    _interval_dates,
    _reconciliation_fetch_start,
    _reopen_backfill_gaps,
    _select_backfill_chunks,
    _time_zone,
)
from tests.support.pipeline import civil_date


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
        "normalizer_version": "daily-v5",
        "next_data_type_index": 0,
        "policy_version": 2,
    }


def test_completed_priority_backfill_resumes_legacy_round_robin() -> None:
    today = date(2026, 7, 28)
    cursors: dict[str, Any] = {
        "_schedule": {
            "last_completed_civil_date": "2026-07-27",
            "normalizer_version": "daily-v5",
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


def test_time_zone_records_whether_google_settings_or_default_was_used() -> None:
    assert _time_zone({}, "Asia/Tokyo") == ("Asia/Tokyo", "CONFIG_DEFAULT")
    assert _time_zone({"timeZone": "America/New_York"}, "Asia/Tokyo") == (
        "America/New_York",
        "GOOGLE_HEALTH_SETTINGS",
    )


def test_physical_and_civil_webhook_ranges_keep_every_changed_date() -> None:
    dates = _interval_dates(
        {
            "physicalTimeInterval": {
                "startTime": "2026-07-26T15:00:00Z",
                "endTime": "2026-07-29T15:00:00Z",
            },
            "civilDateTimeInterval": {
                "startDateTime": {"date": civil_date(date(2026, 7, 27))},
                "endDateTime": {"date": civil_date(date(2026, 7, 30))},
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
