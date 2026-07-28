"""Normalize Google Health API v4 union data points into daily observations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
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


def normalize_daily_observations(
    data_points: Sequence[Mapping[str, Any]],
) -> dict[date, list[Observation]]:
    """Extract supported daily metrics while preserving unknown raw fields elsewhere."""

    values: dict[
        tuple[date, str, str, str, Reducer],
        list[Decimal],
    ] = defaultdict(list)
    sources: dict[tuple[date, str], set[str]] = defaultdict(set)

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
        for extractor in by_field[field]:
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
        by_metric: dict[str, Observation] = {}
        for item in sorted(
            items,
            key=lambda observation: (
                _aggregation_priority(observation.aggregation),
                observation.metric_name,
            ),
        ):
            by_metric[item.metric_name] = item
        resolved[day] = [by_metric[name] for name in sorted(by_metric)]
    return resolved


def _metric_date(field: str, payload: Mapping[str, Any]) -> date | None:
    if field == "sleep":
        return _date_from_path(payload, ("interval", "civilEndTime", "date"))
    if field == "exercise":
        return _date_from_path(payload, ("interval", "civilStartTime", "date"))
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
