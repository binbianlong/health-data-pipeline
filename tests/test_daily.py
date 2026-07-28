from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from health_data_pipeline.daily import (
    INSUFFICIENT_DATA,
    DayContext,
    Observation,
    affected_metric_dates,
    build_daily_rows,
)


def _observation(day: date, value: int | None) -> Observation:
    return Observation(
        metric_date=day,
        metric_name="steps",
        value=None if value is None else Decimal(value),
        unit="count",
        aggregation="daily_sum",
        source_data_types=("steps",),
    )


def _context(day: date) -> DayContext:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return DayContext(
        metric_date=day,
        fitbit_last_sync_time=start + timedelta(days=1),
        time_zone="UTC",
        physical_day_start=start,
        physical_day_end=start + timedelta(days=1),
    )


def test_affected_dates_propagate_to_month_end_and_week_end() -> None:
    changed = date(2026, 1, 31)
    result = affected_metric_dates([changed], today=date(2026, 2, 15))
    assert result[0] == changed
    assert result[-1] == date(2026, 2, 1)
    assert len(result) == 2


def test_calendar_week_mean_runs_monday_through_current_day() -> None:
    current_day = date(2026, 1, 8)  # Thursday
    history = {
        date(2026, 1, 5): [_observation(date(2026, 1, 5), 10)],
        date(2026, 1, 6): [_observation(date(2026, 1, 6), 20)],
        date(2026, 1, 7): [_observation(date(2026, 1, 7), 30)],
    }
    rows = build_daily_rows(_context(current_day), [_observation(current_day, 40)], history)
    weekly = next(row for row in rows if row["period_kind"] == "CALENDAR_WEEK")
    assert weekly["value_num"] == Decimal(25)
    assert weekly["sample_count"] == 4
    assert weekly["period_start"] == date(2026, 1, 5)
    assert weekly["period_end"] == current_day


def test_sunday_finalizes_seven_day_calendar_week() -> None:
    sunday = date(2026, 1, 11)
    monday = date(2026, 1, 5)
    history = {
        monday + timedelta(days=offset): [_observation(monday + timedelta(days=offset), offset + 1)]
        for offset in range(6)
    }
    rows = build_daily_rows(_context(sunday), [_observation(sunday, 7)], history)
    weekly = next(row for row in rows if row["period_kind"] == "CALENDAR_WEEK")
    assert weekly["value_num"] == Decimal(4)
    assert weekly["sample_count"] == 7
    assert weekly["expected_count"] == 7
    assert weekly["period_start"] == monday
    assert weekly["period_end"] == sunday


@pytest.mark.parametrize(
    ("monday", "sunday"),
    [
        (date(2025, 12, 29), date(2026, 1, 4)),
        (date(2026, 3, 30), date(2026, 4, 5)),
    ],
)
def test_calendar_week_keeps_monday_sunday_bounds_across_year_and_month(
    monday: date,
    sunday: date,
) -> None:
    history = {
        monday + timedelta(days=offset): [_observation(monday + timedelta(days=offset), offset + 1)]
        for offset in range(6)
    }

    rows = build_daily_rows(_context(sunday), [_observation(sunday, 7)], history)
    weekly = next(row for row in rows if row["period_kind"] == "CALENDAR_WEEK")

    assert monday.weekday() == 0
    assert sunday.weekday() == 6
    assert weekly["period_start"] == monday
    assert weekly["period_end"] == sunday
    assert weekly["value_num"] == Decimal(4)
    assert weekly["sample_count"] == 7
    assert weekly["expected_count"] == 7


def test_monday_does_not_include_previous_week() -> None:
    monday = date(2026, 1, 12)
    history = {monday - timedelta(days=1): [_observation(monday - timedelta(days=1), 1000)]}
    rows = build_daily_rows(_context(monday), [_observation(monday, 10)], history)
    weekly = next(row for row in rows if row["period_kind"] == "CALENDAR_WEEK")
    assert weekly["value_num"] == Decimal(10)
    assert weekly["sample_count"] == 1
    assert weekly["expected_count"] == 1
    assert weekly["period_start"] == monday


def test_changed_monday_propagates_only_through_its_sunday_across_year() -> None:
    monday = date(2025, 12, 29)
    result = affected_metric_dates([monday], today=date(2026, 2, 1))
    assert date(2026, 1, 4) in result
    assert date(2026, 1, 5) not in result
    assert date(2025, 12, 31) in result


@pytest.mark.parametrize(
    ("changed", "week_end"),
    [
        (date(2025, 12, 31), date(2026, 1, 4)),
        (date(2026, 3, 31), date(2026, 4, 5)),
    ],
)
def test_past_correction_stops_at_week_sunday_across_boundaries(
    changed: date,
    week_end: date,
) -> None:
    result = affected_metric_dates(
        [changed],
        today=week_end + timedelta(days=30),
    )
    expected = tuple(
        changed + timedelta(days=offset) for offset in range((week_end - changed).days + 1)
    )

    assert result == expected
    assert week_end + timedelta(days=1) not in result


def test_missing_week_value_is_not_zero() -> None:
    current_day = date(2026, 1, 8)
    rows = build_daily_rows(_context(current_day), [_observation(current_day, None)], {})
    weekly = next(row for row in rows if row["period_kind"] == "CALENDAR_WEEK")
    assert weekly["value_num"] is None
    assert weekly["data_status"] == INSUFFICIENT_DATA


def test_daily_rows_do_not_contain_commit_metadata() -> None:
    current_day = date(2026, 1, 8)
    row = build_daily_rows(_context(current_day), [_observation(current_day, 100)], {})[0]
    assert "generation" not in row
    assert "created_at" not in row


def test_calendar_month_mean_is_month_to_date() -> None:
    current_day = date(2026, 2, 3)
    history = {
        date(2026, 2, 1): [_observation(date(2026, 2, 1), 10)],
        date(2026, 2, 2): [_observation(date(2026, 2, 2), 20)],
        date(2026, 1, 31): [_observation(date(2026, 1, 31), 1000)],
    }
    rows = build_daily_rows(_context(current_day), [_observation(current_day, 30)], history)
    monthly = next(row for row in rows if row["period_kind"] == "CALENDAR_MONTH")
    assert monthly["value_num"] == Decimal(20)
    assert monthly["period_start"] == date(2026, 2, 1)
    assert monthly["period_end"] == current_day
