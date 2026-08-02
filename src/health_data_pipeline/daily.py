"""Pure daily metric calculations.

The module deliberately contains no cloud or API dependencies.  Inputs are
already-normalized daily observations and outputs match the long-form schema
stored below ``daily-canonical``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final

from health_data_pipeline.sleep_sessions import SleepSession

INSUFFICIENT_DATA: Final = "INSUFFICIENT_DATA"
DECEMBER: Final = 12


@dataclass(frozen=True, slots=True)
class Observation:
    """One normalized metric for a civil day."""

    metric_date: date
    metric_name: str
    value: Decimal | None
    unit: str
    aggregation: str
    data_status: str = "CANONICAL_SYNCED"
    sample_count: int = 1
    expected_count: int = 1
    quality_flags: tuple[str, ...] = ()
    source_data_types: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DayContext:
    """Metadata that is part of the metric meaning, not commit metadata."""

    metric_date: date
    fitbit_last_sync_time: datetime
    time_zone: str
    physical_day_start: datetime
    physical_day_end: datetime
    logic_version: str = "daily-v1"


def affected_metric_dates(
    changed_dates: Iterable[date],
    *,
    today: date,
) -> tuple[date, ...]:
    """Return dates affected by calendar-week and calendar-month means."""

    affected: set[date] = set()
    for changed in changed_dates:
        week_end = changed + timedelta(days=6 - changed.weekday())
        next_month = (
            changed.replace(year=changed.year + 1, month=1, day=1)
            if changed.month == DECEMBER
            else changed.replace(month=changed.month + 1, day=1)
        )
        month_end = next_month - timedelta(days=1)
        end = min(max(week_end, month_end), today)
        current = changed
        while current <= end:
            if current <= week_end or current.month == changed.month:
                affected.add(current)
            current += timedelta(days=1)
    return tuple(sorted(affected))


def build_daily_rows(
    context: DayContext,
    observations: Sequence[Observation],
    history: Mapping[date, Sequence[Observation]],
) -> list[dict[str, Any]]:
    """Build observed, calendar-week, and calendar-month long-form rows.

    Calendar means include the current day and use only available non-missing
    observations. Missing observations never become numeric zero.
    """

    current = {
        item.metric_name: item for item in observations if item.metric_date == context.metric_date
    }
    rows: list[dict[str, Any]] = []
    for metric_name in sorted(current):
        observation = current[metric_name]
        rows.append(_observed_row(context, observation))
        week_start = context.metric_date - timedelta(days=context.metric_date.weekday())
        weekly_values = _baseline_values(
            metric_name,
            week_start,
            context.metric_date + timedelta(days=1),
            {
                **history,
                context.metric_date: observations,
            },
        )
        rows.append(
            _mean_row(
                context,
                observation,
                period_kind="CALENDAR_WEEK",
                minimum_days=1,
                baseline=weekly_values,
                period_start=week_start,
                period_end=context.metric_date,
                expected_count=context.metric_date.weekday() + 1,
            )
        )
        month_start = context.metric_date.replace(day=1)
        month_values = _baseline_values(
            metric_name,
            month_start,
            context.metric_date + timedelta(days=1),
            {
                **history,
                context.metric_date: observations,
            },
        )
        rows.append(
            _mean_row(
                context,
                observation,
                period_kind="CALENDAR_MONTH",
                minimum_days=1,
                baseline=month_values,
                period_start=month_start,
                period_end=context.metric_date,
                expected_count=context.metric_date.day,
            )
        )
    return sorted(rows, key=_row_sort_key)


def build_sleep_session_rows(
    context: DayContext,
    sessions: Sequence[SleepSession],
    *,
    data_status: str,
) -> list[dict[str, Any]]:
    """Build one typed long-form v2 row for each validated sleep session."""

    rows: list[dict[str, Any]] = []
    for session in sessions:
        if session.metric_date != context.metric_date:
            raise ValueError("sleep session metric date does not match its daily revision")
        has_minutes = session.minutes_asleep is not None
        rows.append(
            {
                "record_kind": "SLEEP_SESSION",
                "metric_name": "sleep_session",
                "statistic": "observed",
                "period_kind": "SESSION",
                "dimension_key": session.dimension_key,
                "value_num": session.minutes_asleep,
                "value_text": None,
                "unit": "min",
                "aggregation": "session_value",
                "period_start": context.metric_date,
                "period_end": context.metric_date,
                "sample_count": 1 if has_minutes else 0,
                "expected_count": 1,
                "coverage_ratio": Decimal(1) if has_minutes else Decimal(0),
                "data_status": data_status,
                "quality_flags": ["MAIN_SLEEP_INFERRED"] if session.main_inferred else [],
                "source_data_types": ["sleep"],
                "fitbit_last_sync_time": context.fitbit_last_sync_time,
                "time_zone": context.time_zone,
                "physical_day_start": context.physical_day_start,
                "physical_day_end": context.physical_day_end,
                "logic_version": context.logic_version,
                "session_start_time": session.start_time,
                "session_end_time": session.end_time,
                "session_start_utc_offset_seconds": session.start_utc_offset_seconds,
                "session_end_utc_offset_seconds": session.end_utc_offset_seconds,
                "session_is_main": session.is_main,
                "session_is_nap": session.is_nap,
            }
        )
    return sorted(rows, key=_row_sort_key)


def _baseline_values(
    metric_name: str,
    start: date,
    end_exclusive: date,
    history: Mapping[date, Sequence[Observation]],
) -> list[Decimal]:
    values: list[Decimal] = []
    cursor = start
    while cursor < end_exclusive:
        matching = next(
            (
                item
                for item in history.get(cursor, ())
                if item.metric_name == metric_name and item.value is not None
            ),
            None,
        )
        if matching is not None:
            values.append(matching.value)  # type: ignore[arg-type]
        cursor += timedelta(days=1)
    return values


def _observed_row(context: DayContext, observation: Observation) -> dict[str, Any]:
    coverage = _coverage(observation.sample_count, observation.expected_count)
    return _base_row(
        context,
        observation,
        statistic="observed",
        period_kind="DAY",
        value=observation.value,
        period_start=context.metric_date,
        period_end=context.metric_date,
        sample_count=observation.sample_count,
        expected_count=observation.expected_count,
        coverage_ratio=coverage,
        data_status=observation.data_status,
    )


def _mean_row(
    context: DayContext,
    observation: Observation,
    *,
    period_kind: str,
    minimum_days: int,
    baseline: Sequence[Decimal],
    period_start: date,
    period_end: date,
    expected_count: int,
) -> dict[str, Any]:
    enough_data = len(baseline) >= minimum_days
    value = sum(baseline, Decimal(0)) / len(baseline) if enough_data else None
    return _base_row(
        context,
        observation,
        statistic="mean",
        period_kind=period_kind,
        value=value,
        period_start=period_start,
        period_end=period_end,
        sample_count=len(baseline),
        expected_count=expected_count,
        coverage_ratio=_coverage(len(baseline), expected_count),
        data_status=observation.data_status if enough_data else INSUFFICIENT_DATA,
    )


def _base_row(
    context: DayContext,
    observation: Observation,
    *,
    statistic: str,
    period_kind: str,
    value: Decimal | None,
    period_start: date,
    period_end: date,
    sample_count: int,
    expected_count: int,
    coverage_ratio: Decimal,
    data_status: str,
) -> dict[str, Any]:
    return {
        "record_kind": "METRIC",
        "metric_name": observation.metric_name,
        "statistic": statistic,
        "period_kind": period_kind,
        "dimension_key": None,
        "value_num": value,
        "value_text": None,
        "unit": observation.unit,
        "aggregation": observation.aggregation,
        "period_start": period_start,
        "period_end": period_end,
        "sample_count": sample_count,
        "expected_count": expected_count,
        "coverage_ratio": coverage_ratio,
        "data_status": data_status,
        "quality_flags": list(observation.quality_flags),
        "source_data_types": list(observation.source_data_types),
        "fitbit_last_sync_time": context.fitbit_last_sync_time,
        "time_zone": context.time_zone,
        "physical_day_start": context.physical_day_start,
        "physical_day_end": context.physical_day_end,
        "logic_version": context.logic_version,
        "session_start_time": None,
        "session_end_time": None,
        "session_start_utc_offset_seconds": None,
        "session_end_utc_offset_seconds": None,
        "session_is_main": None,
        "session_is_nap": None,
    }


def _coverage(sample_count: int, expected_count: int) -> Decimal:
    if expected_count <= 0:
        return Decimal(0)
    return min(Decimal(sample_count) / Decimal(expected_count), Decimal(1))


def _row_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row["metric_name"]),
        str(row["statistic"]),
        str(row["period_kind"]),
        str(row.get("dimension_key") or ""),
    )
