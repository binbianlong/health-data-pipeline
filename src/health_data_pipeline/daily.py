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

from health_data_pipeline.metrics import CoverageKind
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
    expected_count: int | None = 1
    coverage_kind: CoverageKind = "OBSERVATION_COUNT"
    missing_reason: str | None = None
    quality_flags: tuple[str, ...] = ()
    source_data_types: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DayContext:
    """Metadata that is part of the metric meaning, not commit metadata."""

    metric_date: date
    fitbit_last_sync_time: datetime | None
    time_zone: str
    physical_day_start: datetime
    physical_day_end: datetime
    time_zone_source: str = "GOOGLE_HEALTH_SETTINGS"
    logic_version: str = "daily-v1"


@dataclass(frozen=True, slots=True)
class SourceQueryStatus:
    """Outcome of one successful, committed source query for a civil day."""

    data_type: str
    status: str
    source_count: int
    normalized_count: int


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
        weekly_observations = _baseline_observations(
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
                baseline=weekly_observations,
                period_start=week_start,
                period_end=context.metric_date,
                expected_count=context.metric_date.weekday() + 1,
            )
        )
        month_start = context.metric_date.replace(day=1)
        month_observations = _baseline_observations(
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
                baseline=month_observations,
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
    """Build one typed envelope and long-form summary rows per sleep session."""

    rows: list[dict[str, Any]] = []
    for session in sessions:
        if session.metric_date != context.metric_date:
            raise ValueError("sleep session metric date does not match its daily revision")
        has_minutes = session.minutes_asleep is not None
        quality_flags = ["MAIN_SLEEP_INFERRED"] if session.main_inferred else []
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
                "expected_count": None,
                "coverage_ratio": None,
                "coverage_kind": "NOT_APPLICABLE",
                "data_status": data_status,
                "missing_reason": None if has_minutes else "SESSION_VALUE_UNAVAILABLE",
                "quality_flags": quality_flags,
                "source_data_types": ["sleep"],
                "fitbit_last_sync_time": context.fitbit_last_sync_time,
                "time_zone": context.time_zone,
                "time_zone_source": context.time_zone_source,
                "physical_day_start": context.physical_day_start,
                "physical_day_end": context.physical_day_end,
                "logic_version": context.logic_version,
                "session_start_time": session.start_time,
                "session_end_time": session.end_time,
                "session_start_utc_offset_seconds": session.start_utc_offset_seconds,
                "session_end_utc_offset_seconds": session.end_utc_offset_seconds,
                "session_is_main": session.is_main,
                "session_is_nap": session.is_nap,
                "session_role": session.role.value,
                "session_role_source": session.role_source.value,
                "session_processed": session.processed,
                "session_stages_status": session.stages_status,
                "session_manually_edited": session.manually_edited,
                "session_sleep_type": session.sleep_type,
                "session_create_time": session.create_time,
                "session_update_time": session.update_time,
            }
        )
        summary_values = {
            "sleep_session_minutes_asleep": session.minutes_asleep,
            "sleep_session_minutes_in_sleep_period": session.minutes_in_sleep_period,
            "sleep_session_minutes_awake": session.minutes_awake,
            "sleep_session_minutes_to_fall_asleep": session.minutes_to_fall_asleep,
            "sleep_session_minutes_after_wake_up": session.minutes_after_wake_up,
        }
        rows.extend(
            _sleep_session_metric_row(
                context,
                session.dimension_key,
                metric_name,
                value,
                unit="min",
                data_status=data_status,
                quality_flags=quality_flags,
            )
            for metric_name, value in summary_values.items()
            if value is not None
        )
        for stage in session.stage_summaries:
            stage_name = stage.stage_type.lower()
            if stage.minutes is not None:
                rows.append(
                    _sleep_session_metric_row(
                        context,
                        session.dimension_key,
                        f"sleep_session_stage_{stage_name}_minutes",
                        stage.minutes,
                        unit="min",
                        data_status=data_status,
                        quality_flags=quality_flags,
                    )
                )
            if stage.count is not None:
                rows.append(
                    _sleep_session_metric_row(
                        context,
                        session.dimension_key,
                        f"sleep_session_stage_{stage_name}_count",
                        Decimal(stage.count),
                        unit="count",
                        data_status=data_status,
                        quality_flags=quality_flags,
                    )
                )
    return sorted(rows, key=_row_sort_key)


def _sleep_session_metric_row(
    context: DayContext,
    dimension_key: str,
    metric_name: str,
    value: Decimal,
    *,
    unit: str,
    data_status: str,
    quality_flags: Sequence[str],
) -> dict[str, Any]:
    """Serialize one session summary value without duplicating session metadata."""

    observation = Observation(
        metric_date=context.metric_date,
        metric_name=metric_name,
        value=value,
        unit=unit,
        aggregation="session_value",
        data_status=data_status,
        coverage_kind="NOT_APPLICABLE",
        quality_flags=tuple(quality_flags),
        source_data_types=("sleep",),
    )
    row = _base_row(
        context,
        observation,
        statistic="observed",
        period_kind="SESSION",
        value=value,
        period_start=context.metric_date,
        period_end=context.metric_date,
        sample_count=1,
        expected_count=None,
        coverage_ratio=None,
        coverage_kind="NOT_APPLICABLE",
        data_status=data_status,
        missing_reason=None,
    )
    row["dimension_key"] = dimension_key
    return row


def build_source_status_rows(
    context: DayContext,
    statuses: Sequence[SourceQueryStatus],
    *,
    data_status: str,
) -> list[dict[str, Any]]:
    """Build typed source-query rows used to distinguish empty from unqueried data."""

    rows: list[dict[str, Any]] = []
    for status in statuses:
        if status.source_count < 0 or status.normalized_count < 0:
            raise ValueError("source query counts must not be negative")
        rows.append(
            {
                "record_kind": "SOURCE_STATUS",
                "metric_name": "source_status",
                "statistic": "observed",
                "period_kind": "DAY",
                "dimension_key": status.data_type,
                "value_num": status.source_count,
                "value_text": status.status,
                "unit": "records",
                "aggregation": "query_result",
                "period_start": context.metric_date,
                "period_end": context.metric_date,
                "sample_count": status.normalized_count,
                "expected_count": None,
                "coverage_ratio": None,
                "coverage_kind": "NOT_APPLICABLE",
                "data_status": data_status,
                "missing_reason": None,
                "quality_flags": [],
                "source_data_types": [status.data_type],
                "fitbit_last_sync_time": context.fitbit_last_sync_time,
                "time_zone": context.time_zone,
                "time_zone_source": context.time_zone_source,
                "physical_day_start": context.physical_day_start,
                "physical_day_end": context.physical_day_end,
                "logic_version": context.logic_version,
                "session_start_time": None,
                "session_end_time": None,
                "session_start_utc_offset_seconds": None,
                "session_end_utc_offset_seconds": None,
                "session_is_main": None,
                "session_is_nap": None,
                "session_role": None,
                "session_role_source": None,
                "session_processed": None,
                "session_stages_status": None,
                "session_manually_edited": None,
                "session_sleep_type": None,
                "session_create_time": None,
                "session_update_time": None,
            }
        )
    return sorted(rows, key=_row_sort_key)


def _baseline_observations(
    metric_name: str,
    start: date,
    end_exclusive: date,
    history: Mapping[date, Sequence[Observation]],
) -> list[Observation]:
    observations: list[Observation] = []
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
            observations.append(matching)
        cursor += timedelta(days=1)
    return observations


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
        coverage_kind=observation.coverage_kind,
        data_status=observation.data_status,
        missing_reason=observation.missing_reason,
    )


def _mean_row(
    context: DayContext,
    observation: Observation,
    *,
    period_kind: str,
    minimum_days: int,
    baseline: Sequence[Observation],
    period_start: date,
    period_end: date,
    expected_count: int,
) -> dict[str, Any]:
    enough_data = len(baseline) >= minimum_days
    values = tuple(item.value for item in baseline if item.value is not None)
    value = sum(values, Decimal(0)) / len(values) if enough_data else None
    quality_flags = {flag for item in baseline for flag in item.quality_flags}
    if len(baseline) < expected_count:
        quality_flags.add("PARTIAL_COVERAGE")
    source_data_types = {source for item in baseline for source in item.source_data_types}
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
        coverage_kind="CALENDAR_DAYS",
        data_status=_aggregate_data_status(baseline) if enough_data else INSUFFICIENT_DATA,
        missing_reason=None if enough_data else observation.missing_reason,
        quality_flags=tuple(sorted(quality_flags)),
        source_data_types=tuple(sorted(source_data_types)),
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
    expected_count: int | None,
    coverage_ratio: Decimal | None,
    coverage_kind: CoverageKind,
    data_status: str,
    missing_reason: str | None,
    quality_flags: Sequence[str] | None = None,
    source_data_types: Sequence[str] | None = None,
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
        "coverage_kind": coverage_kind,
        "data_status": data_status,
        "missing_reason": missing_reason,
        "quality_flags": list(
            observation.quality_flags if quality_flags is None else quality_flags
        ),
        "source_data_types": list(
            observation.source_data_types if source_data_types is None else source_data_types
        ),
        "fitbit_last_sync_time": context.fitbit_last_sync_time,
        "time_zone": context.time_zone,
        "time_zone_source": context.time_zone_source,
        "physical_day_start": context.physical_day_start,
        "physical_day_end": context.physical_day_end,
        "logic_version": context.logic_version,
        "session_start_time": None,
        "session_end_time": None,
        "session_start_utc_offset_seconds": None,
        "session_end_utc_offset_seconds": None,
        "session_is_main": None,
        "session_is_nap": None,
        "session_role": None,
        "session_role_source": None,
        "session_processed": None,
        "session_stages_status": None,
        "session_manually_edited": None,
        "session_sleep_type": None,
        "session_create_time": None,
        "session_update_time": None,
    }


def _coverage(sample_count: int, expected_count: int | None) -> Decimal | None:
    if expected_count is None or expected_count <= 0:
        return None
    return min(Decimal(sample_count) / Decimal(expected_count), Decimal(1))


def _aggregate_data_status(observations: Sequence[Observation]) -> str:
    priority = {
        "CANONICAL_SYNCED": 0,
        "PROVISIONAL": 1,
        "CANONICAL_STALE": 2,
        "MISSING": 3,
        "INSUFFICIENT_DATA": 4,
    }
    return max(
        (item.data_status for item in observations),
        key=lambda status: (priority.get(status, 5), status),
        default=INSUFFICIENT_DATA,
    )


def _row_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row["record_kind"]),
        str(row["metric_name"]),
        str(row["statistic"]),
        str(row["period_kind"]),
        str(row.get("dimension_key") or ""),
    )
