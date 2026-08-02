import hashlib
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from health_data_pipeline.sleep_sessions import (
    SleepSessionValidationError,
    normalize_sleep_sessions,
    select_main_sleep_points,
)


def _point(
    name: str,
    *,
    start: str,
    end: str,
    start_offset: str = "32400s",
    end_offset: str = "32400s",
    civil_end: date | None = date(2026, 8, 2),
    minutes: str | None = "420",
    main: bool | None = None,
    nap: bool | None = None,
    update_time: str | None = None,
) -> dict[str, Any]:
    interval: dict[str, Any] = {
        "startTime": start,
        "endTime": end,
        "startUtcOffset": start_offset,
        "endUtcOffset": end_offset,
    }
    if civil_end is not None:
        interval["civilEndTime"] = {
            "date": {
                "year": civil_end.year,
                "month": civil_end.month,
                "day": civil_end.day,
            }
        }
    metadata: dict[str, bool] = {}
    if main is not None:
        metadata["mainSleep"] = main
    if nap is not None:
        metadata["nap"] = nap
    sleep: dict[str, Any] = {"interval": interval, "metadata": metadata}
    if minutes is not None:
        sleep["summary"] = {"minutesAsleep": minutes}
    if update_time is not None:
        sleep["updateTime"] = update_time
    return {"dataPointName": name, "sleep": sleep}


def test_normalizes_times_offsets_identity_and_flags() -> None:
    main = _point(
        "users/me/dataTypes/sleep/dataPoints/main",
        start="2026-08-01T23:30:00+09:00",
        end="2026-08-02T07:00:00+09:00",
        main=True,
        nap=True,
    )
    nap = _point(
        "users/me/dataTypes/sleep/dataPoints/nap",
        start="2026-08-02T04:00:00Z",
        end="2026-08-02T04:30:00Z",
        minutes="25.5",
        nap=True,
    )

    sessions = normalize_sleep_sessions([nap, main])[date(2026, 8, 2)]

    assert len(sessions) == 2
    normalized_main = next(session for session in sessions if session.is_main)
    assert normalized_main.start_time == datetime(2026, 8, 1, 14, 30, tzinfo=UTC)
    assert normalized_main.end_time == datetime(2026, 8, 1, 22, 0, tzinfo=UTC)
    assert normalized_main.start_utc_offset_seconds == 32_400
    assert normalized_main.end_utc_offset_seconds == 32_400
    assert normalized_main.is_nap
    assert not normalized_main.main_inferred
    assert normalized_main.minutes_asleep == Decimal(420)
    expected_hash = hashlib.sha256(str(main["dataPointName"]).encode()).hexdigest()
    assert normalized_main.dimension_key == f"sleep:{expected_hash}"
    normalized_nap = next(session for session in sessions if not session.is_main)
    assert normalized_nap.is_nap
    assert normalized_nap.minutes_asleep == Decimal("25.5")


def test_civil_end_date_wins_over_physical_date_and_offset() -> None:
    point = _point(
        "civil-date",
        start="2026-08-01T15:00:00Z",
        end="2026-08-01T23:00:00Z",
        civil_end=date(2026, 8, 3),
        main=True,
    )

    assert tuple(normalize_sleep_sessions([point])) == (date(2026, 8, 3),)


def test_physical_end_plus_offset_is_metric_date_fallback() -> None:
    point = _point(
        "physical-date",
        start="2026-08-01T16:00:00Z",
        end="2026-08-02T16:00:00Z",
        civil_end=None,
        main=True,
    )

    assert tuple(normalize_sleep_sessions([point])) == (date(2026, 8, 3),)


@pytest.mark.parametrize(
    "change",
    [
        {"dataPointName": ""},
        {"sleep": {"interval": {}}},
        {"sleep": {"interval": {"startTime": "not-a-time"}}},
    ],
)
def test_ignores_incomplete_sleep_points(change: Mapping[str, Any]) -> None:
    point = _point(
        "invalid",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        main=True,
    )
    point.update(change)

    assert normalize_sleep_sessions([{"steps": {"count": 1}}, point]) == {}


def test_revision_with_latest_update_time_wins() -> None:
    older = _point(
        "revised",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T21:00:00Z",
        minutes="400",
        update_time="2026-08-02T00:00:00Z",
    )
    latest = _point(
        "revised",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="450",
        update_time="2026-08-02T01:00:00Z",
    )

    session = normalize_sleep_sessions([latest, older])[date(2026, 8, 2)][0]

    assert session.end_time == datetime(2026, 8, 1, 22, 0, tzinfo=UTC)
    assert session.minutes_asleep == Decimal(450)


def test_identical_duplicates_do_not_require_update_time() -> None:
    point = _point(
        "duplicate",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        main=True,
    )

    assert normalize_sleep_sessions([point, dict(point)]) == normalize_sleep_sessions([point])


def test_different_revisions_without_update_time_fail_closed() -> None:
    first = _point(
        "ambiguous",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T21:00:00Z",
    )
    second = _point(
        "ambiguous",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
    )

    with pytest.raises(SleepSessionValidationError, match="require a valid updateTime"):
        normalize_sleep_sessions([first, second])


def test_conflicting_equal_latest_revisions_fail_closed() -> None:
    first = _point(
        "ambiguous-latest",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T21:00:00Z",
        update_time="2026-08-02T01:00:00Z",
    )
    second = _point(
        "ambiguous-latest",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        update_time="2026-08-02T01:00:00Z",
    )

    with pytest.raises(SleepSessionValidationError, match="same latest updateTime"):
        select_main_sleep_points([first, second])


def test_selects_one_explicit_main_deterministically_and_demotes_extra() -> None:
    short_main = _point(
        "short-main",
        start="2026-08-01T17:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="280",
        main=True,
    )
    long_main = _point(
        "long-main",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="430",
        main=True,
    )

    selected = select_main_sleep_points([short_main, long_main])
    sessions = normalize_sleep_sessions([short_main, long_main])[date(2026, 8, 2)]

    assert selected == (long_main,)
    assert sum(session.is_main for session in sessions) == 1
    assert not any(session.main_inferred for session in sessions)


def test_falls_back_to_longest_non_nap_and_marks_inference() -> None:
    short = _point(
        "short",
        start="2026-08-01T18:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="230",
    )
    long = _point(
        "long",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="430",
    )
    longer_nap = _point(
        "longer-nap",
        start="2026-08-01T12:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="500",
        nap=True,
    )

    selected = select_main_sleep_points([longer_nap, short, long])
    sessions = normalize_sleep_sessions([longer_nap, short, long])[date(2026, 8, 2)]

    assert selected == (long,)
    inferred = next(session for session in sessions if session.main_inferred)
    assert not inferred.is_main
    assert not inferred.is_nap


def test_falls_back_to_longest_when_every_session_is_a_nap() -> None:
    short_nap = _point(
        "short-nap",
        start="2026-08-01T20:00:00Z",
        end="2026-08-01T21:00:00Z",
        minutes="50",
        nap=True,
    )
    long_nap = _point(
        "long-nap",
        start="2026-08-01T19:00:00Z",
        end="2026-08-01T21:00:00Z",
        minutes="100",
        nap=True,
    )

    assert select_main_sleep_points([short_nap, long_nap]) == (long_nap,)
    session = next(
        item
        for item in normalize_sleep_sessions([short_nap, long_nap])[date(2026, 8, 2)]
        if item.main_inferred
    )
    assert session.is_nap


def test_rejected_nap_stage_status_is_treated_as_nap() -> None:
    rejected_nap = _point(
        "rejected-nap",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="430",
        nap=False,
    )
    rejected_nap["sleep"]["metadata"]["stagesStatus"] = "SLEEP_STAGES_STATUS_REJECTED_NAP"
    ordinary_sleep = _point(
        "ordinary",
        start="2026-08-01T18:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="200",
    )

    assert select_main_sleep_points([rejected_nap, ordinary_sleep]) == (ordinary_sleep,)
    rejected_session = next(
        session
        for session in normalize_sleep_sessions([rejected_nap, ordinary_sleep])[date(2026, 8, 2)]
        if session.dimension_key.endswith(hashlib.sha256(b"rejected-nap").hexdigest())
    )
    assert rejected_session.is_nap


def test_main_selection_accepts_summary_only_points_without_identity_or_start() -> None:
    fallback = {
        "sleep": {
            "interval": {"civilEndTime": {"date": {"year": 2026, "month": 8, "day": 2}}},
            "metadata": {"nap": False},
            "summary": {"minutesAsleep": "500"},
        }
    }
    explicit_main = {
        "sleep": {
            "interval": {"civilEndTime": {"date": {"year": 2026, "month": 8, "day": 2}}},
            "metadata": {"mainSleep": True},
            "summary": {"minutesAsleep": "400"},
        }
    }

    assert select_main_sleep_points([fallback, explicit_main]) == (explicit_main,)
    assert normalize_sleep_sessions([fallback, explicit_main]) == {}


def test_main_selection_uses_physical_end_date_for_minimal_point() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-08-02T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {"minutesAsleep": "400"},
        }
    }

    assert select_main_sleep_points([point]) == (point,)


def test_rejects_utc_offset_of_exactly_twenty_four_hours() -> None:
    point = _point(
        "invalid-offset",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        end_offset="86400s",
        civil_end=None,
    )

    assert normalize_sleep_sessions([point]) == {}
    assert select_main_sleep_points([point]) == ()


def test_main_selection_is_stable_for_equal_lengths() -> None:
    first = _point(
        "a",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="400",
    )
    second = _point(
        "b",
        start="2026-08-01T14:00:00Z",
        end="2026-08-01T22:00:00Z",
        minutes="400",
    )

    assert select_main_sleep_points([first, second]) == select_main_sleep_points([second, first])
