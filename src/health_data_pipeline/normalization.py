"""Normalize Google Health API v4 union data points into daily observations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from typing import Any, Literal

from health_data_pipeline.daily import Observation

Reducer = Literal["sum", "mean"]


@dataclass(frozen=True, slots=True)
class MetricExtractor:
    data_field: str
    metric_name: str
    value_path: tuple[str, ...]
    unit: str
    aggregation: str
    reducer: Reducer
    scale: Decimal = Decimal(1)


EXTRACTORS: tuple[MetricExtractor, ...] = (
    MetricExtractor("steps", "steps", ("count",), "count", "daily_sum", "sum"),
    MetricExtractor(
        "distance",
        "distance",
        ("millimeters",),
        "m",
        "daily_sum",
        "sum",
        Decimal("0.001"),
    ),
    MetricExtractor(
        "activeZoneMinutes",
        "active_zone_minutes",
        ("activeZoneMinutes",),
        "min",
        "daily_sum",
        "sum",
    ),
    MetricExtractor(
        "heartRate",
        "heart_rate",
        ("beatsPerMinute",),
        "bpm",
        "daily_mean",
        "mean",
    ),
    MetricExtractor(
        "dailyRestingHeartRate",
        "resting_heart_rate",
        ("beatsPerMinute",),
        "bpm",
        "daily_value",
        "mean",
    ),
    MetricExtractor(
        "dailyHeartRateVariability",
        "hrv_rmssd",
        ("averageHeartRateVariabilityMilliseconds",),
        "ms",
        "daily_value",
        "mean",
    ),
    MetricExtractor(
        "heartRateVariability",
        "hrv_rmssd",
        ("rootMeanSquareOfSuccessiveDifferencesMilliseconds",),
        "ms",
        "daily_mean",
        "mean",
    ),
    MetricExtractor(
        "dailyOxygenSaturation",
        "spo2",
        ("averagePercentage",),
        "%",
        "daily_value",
        "mean",
    ),
    MetricExtractor(
        "oxygenSaturation",
        "spo2",
        ("percentage",),
        "%",
        "daily_mean",
        "mean",
    ),
    MetricExtractor(
        "respiratoryRateSleepSummary",
        "sleep_respiratory_rate",
        ("fullSleepStats", "breathsPerMinute"),
        "breaths/min",
        "daily_mean",
        "mean",
    ),
    MetricExtractor(
        "dailyRespiratoryRate",
        "sleep_respiratory_rate",
        ("breathsPerMinute",),
        "breaths/min",
        "daily_value",
        "mean",
    ),
    MetricExtractor(
        "sleep",
        "sleep_minutes",
        ("summary", "minutesAsleep"),
        "min",
        "daily_sum",
        "sum",
    ),
    MetricExtractor(
        "exercise",
        "exercise_minutes",
        ("activeDuration",),
        "min",
        "daily_sum",
        "sum",
    ),
)

_SLEEP_STAGE_METRICS: Mapping[str, str] = {
    "REM": "sleep_rem_minutes",
    "DEEP": "sleep_deep_minutes",
    "LIGHT": "sleep_light_minutes",
    "AWAKE": "sleep_awake_minutes",
}
_SLEEPING_STAGE_METRICS = (
    "sleep_rem_minutes",
    "sleep_deep_minutes",
    "sleep_light_minutes",
)


def normalize_daily_observations(
    data_points: Sequence[Mapping[str, Any]],
) -> dict[date, list[Observation]]:
    """Extract supported daily metrics while preserving unknown raw fields elsewhere."""

    values: dict[
        tuple[date, str, str, str, Reducer],
        list[Decimal],
    ] = defaultdict(list)
    sources: dict[tuple[date, str], set[str]] = defaultdict(set)
    incomplete_sleep_stage_dates: set[date] = set()

    by_field: dict[str, list[MetricExtractor]] = defaultdict(list)
    for extractor in EXTRACTORS:
        by_field[extractor.data_field].append(extractor)

    for point in data_points:
        field = next((name for name in by_field if isinstance(point.get(name), Mapping)), None)
        if field is None:
            continue
        payload = point[field]
        if not isinstance(payload, Mapping):
            continue
        metric_date = _metric_date(field, payload)
        if metric_date is None:
            continue
        additional_metrics = _additional_daily_metrics(field, payload)
        incomplete_sleep_stage_dates.update(
            _incomplete_sleep_stage_date(field, metric_date, additional_metrics)
        )
        primary_extractors = (
            extractor
            for extractor in by_field[field]
            if extractor.metric_name not in additional_metrics
        )
        for extractor in primary_extractors:
            raw_value = _nested(payload, extractor.value_path)
            numeric = _decimal_value(raw_value, duration=field == "exercise")
            if numeric is None:
                continue
            key = (
                metric_date,
                extractor.metric_name,
                extractor.unit,
                extractor.aggregation,
                extractor.reducer,
            )
            values[key].append(numeric * extractor.scale)
            sources[(metric_date, extractor.metric_name)].add(_kebab_case(field))
        for metric_name, minutes in additional_metrics.items():
            key = (
                metric_date,
                metric_name,
                "min",
                "daily_sum",
                "sum",
            )
            values[key].append(minutes)
            sources[(metric_date, metric_name)].add(_kebab_case(field))

    result: dict[date, list[Observation]] = defaultdict(list)
    for (metric_date, metric_name, unit, aggregation, reducer), samples in values.items():
        value = sum(samples, Decimal(0))
        if reducer == "mean":
            value /= Decimal(len(samples))
        result[metric_date].append(
            Observation(
                metric_date=metric_date,
                metric_name=metric_name,
                value=value,
                unit=unit,
                aggregation=aggregation,
                sample_count=len(samples),
                expected_count=len(samples),
                source_data_types=tuple(sorted(sources[(metric_date, metric_name)])),
            )
        )
    resolved: dict[date, list[Observation]] = {}
    for day, items in result.items():
        resolved[day] = _resolve_daily_observations(
            items,
            omit_sleeping_stages=day in incomplete_sleep_stage_dates,
        )
    return resolved


def _incomplete_sleep_stage_date(
    field: str,
    metric_date: date,
    additional_metrics: Mapping[str, Decimal],
) -> tuple[date, ...]:
    if field != "sleep":
        return ()
    if all(metric_name in additional_metrics for metric_name in _SLEEPING_STAGE_METRICS):
        return ()
    return (metric_date,)


def _resolve_daily_observations(
    items: Sequence[Observation],
    *,
    omit_sleeping_stages: bool,
) -> list[Observation]:
    selected = (
        [item for item in items if item.metric_name not in _SLEEPING_STAGE_METRICS]
        if omit_sleeping_stages
        else items
    )
    by_metric: dict[str, Observation] = {}
    for item in sorted(
        selected,
        key=lambda observation: (
            _aggregation_priority(observation.aggregation),
            observation.metric_name,
        ),
    ):
        by_metric[item.metric_name] = item
    return [by_metric[name] for name in sorted(by_metric)]


def _metric_date(field: str, payload: Mapping[str, Any]) -> date | None:
    if field == "sleep":
        civil = _date_from_path(payload, ("interval", "civilEndTime", "date"))
        return civil or _physical_interval_date(
            payload,
            time_field="endTime",
            offset_field="endUtcOffset",
        )
    if field == "exercise":
        civil = _date_from_path(payload, ("interval", "civilStartTime", "date"))
        return civil or _physical_interval_date(
            payload,
            time_field="startTime",
            offset_field="startUtcOffset",
        )
    direct = _date_from_path(payload, ("date",))
    if direct is not None:
        return direct
    sample = _date_from_path(payload, ("sampleTime", "civilTime", "date"))
    if sample is not None:
        return sample
    return _date_from_path(payload, ("interval", "civilStartTime", "date"))


def _date_from_path(payload: Mapping[str, Any], path: tuple[str, ...]) -> date | None:
    value = _nested(payload, path)
    if not isinstance(value, Mapping):
        return None
    try:
        return date(int(value["year"]), int(value["month"]), int(value["day"]))
    except (KeyError, TypeError, ValueError):
        return None


def _physical_interval_date(
    payload: Mapping[str, Any],
    *,
    time_field: str,
    offset_field: str,
) -> date | None:
    interval = payload.get("interval")
    if not isinstance(interval, Mapping):
        return None
    timestamp = _rfc3339_datetime(interval.get(time_field))
    if timestamp is None:
        return None

    offset_seconds = _protobuf_duration_seconds(interval.get(offset_field))
    if offset_seconds is None or abs(offset_seconds) > Decimal(24 * 60 * 60):
        return None
    utc_timestamp = timestamp.astimezone(UTC)
    return (utc_timestamp + timedelta(seconds=int(offset_seconds))).date()


def _rfc3339_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _protobuf_duration_seconds(value: Any) -> Decimal | None:
    if not isinstance(value, str) or not value.endswith("s"):
        return None
    try:
        seconds = Decimal(value[:-1])
    except (InvalidOperation, ValueError):
        return None
    return seconds if seconds == seconds.to_integral_value() else None


def _additional_daily_metrics(
    field: str,
    payload: Mapping[str, Any],
) -> dict[str, Decimal]:
    return _sleep_stage_minutes(payload) if field == "sleep" else {}


def _sleep_stage_minutes(payload: Mapping[str, Any]) -> dict[str, Decimal]:
    summary = _nested(payload, ("summary", "stagesSummary"))
    summarized = _summarized_sleep_stage_minutes(summary)
    detailed = _detailed_sleep_stage_minutes(payload.get("stages"))
    if detailed and _sleep_stage_totals_match_summary(payload, detailed):
        result = {
            **detailed,
            "sleep_minutes": sum(
                (detailed.get(metric_name, Decimal(0)) for metric_name in _SLEEPING_STAGE_METRICS),
                Decimal(0),
            ),
        }
    elif summarized and _sleep_stage_totals_match_summary(payload, summarized):
        result = summarized
    else:
        result = {}
    if result:
        for metric_name in (*_SLEEPING_STAGE_METRICS, "sleep_awake_minutes"):
            result.setdefault(metric_name, Decimal(0))
    minutes_awake = _decimal_value(
        _nested(payload, ("summary", "minutesAwake")),
        duration=False,
    )
    if "sleep_awake_minutes" not in result and minutes_awake is not None and minutes_awake >= 0:
        result["sleep_awake_minutes"] = minutes_awake
    return result


def _summarized_sleep_stage_minutes(value: Any) -> dict[str, Decimal]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return {}
    candidates: dict[str, set[Decimal]] = defaultdict(set)
    for item in value:
        if not isinstance(item, Mapping):
            continue
        metric_name = _sleep_stage_metric(item.get("type"))
        minutes = _decimal_value(item.get("minutes"), duration=False)
        if metric_name is None or minutes is None or minutes < 0:
            continue
        candidates[metric_name].add(minutes)
    if any(len(minutes) != 1 for minutes in candidates.values()):
        return {}
    return {metric_name: next(iter(minutes)) for metric_name, minutes in candidates.items()}


def _detailed_sleep_stage_minutes(value: Any) -> dict[str, Decimal]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return {}
    intervals: list[tuple[datetime, datetime, str]] = []
    seen_intervals: set[tuple[datetime, datetime, str]] = set()
    if not value:
        return {}
    for item in value:
        if not isinstance(item, Mapping):
            return {}
        metric_name = _sleep_stage_metric(item.get("type"))
        start = _rfc3339_datetime(item.get("startTime"))
        end = _rfc3339_datetime(item.get("endTime"))
        if metric_name is None or start is None or end is None or end <= start:
            return {}
        interval = (start, end, metric_name)
        if interval not in seen_intervals:
            seen_intervals.add(interval)
            intervals.append(interval)

    intervals.sort(key=lambda interval: (interval[0], interval[1], interval[2]))
    if any(current[0] < previous[1] for previous, current in pairwise(intervals)):
        return {}

    result: dict[str, Decimal] = defaultdict(Decimal)
    for start, end, metric_name in intervals:
        seconds = Decimal(str((end - start).total_seconds()))
        result[metric_name] += seconds / Decimal(60)
    return dict(result)


def _sleep_stage_totals_match_summary(
    payload: Mapping[str, Any],
    stages: Mapping[str, Decimal],
) -> bool:
    """Reject partial or duplicated stage totals before publishing daily metrics."""

    tolerance = Decimal(1)
    reported_asleep = _decimal_value(
        _nested(payload, ("summary", "minutesAsleep")),
        duration=False,
    )
    computed_asleep = sum(
        (stages.get(metric_name, Decimal(0)) for metric_name in _SLEEPING_STAGE_METRICS),
        Decimal(0),
    )
    if reported_asleep is not None and abs(computed_asleep - reported_asleep) > tolerance:
        return False

    reported_awake = _decimal_value(
        _nested(payload, ("summary", "minutesAwake")),
        duration=False,
    )
    computed_awake = stages.get("sleep_awake_minutes", Decimal(0))
    if reported_awake is not None and abs(computed_awake - reported_awake) > tolerance:
        return False

    reported_period = _decimal_value(
        _nested(payload, ("summary", "minutesInSleepPeriod")),
        duration=False,
    )
    if reported_period is not None:
        computed_period = computed_asleep + computed_awake
        if abs(computed_period - reported_period) > tolerance:
            return False
    return True


def _sleep_stage_metric(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.upper().removeprefix("SLEEP_STAGE_TYPE_")
    return _SLEEP_STAGE_METRICS.get(normalized)


def _nested(payload: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = payload
    for key in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _decimal_value(value: Any, *, duration: bool) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    is_duration = duration and isinstance(value, str) and value.endswith("s")
    if is_duration:
        value = value[:-1]
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return numeric / Decimal(60) if is_duration else numeric


def _kebab_case(value: str) -> str:
    chars: list[str] = []
    for char in value:
        if char.isupper():
            chars.extend(("-", char.lower()))
        else:
            chars.append(char)
    return "".join(chars)


def _aggregation_priority(value: str) -> int:
    """Prefer an API-provided daily summary over detail-derived values."""

    if value == "daily_value":
        return 3
    if value in {"daily_sum", "daily_mean"}:
        return 2
    return 1
