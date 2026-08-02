"""Stable metric catalog shared by normalization, storage, and views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

CoverageKind = Literal[
    "UNKNOWN",
    "OBSERVATION_COUNT",
    "CALENDAR_DAYS",
    "NOT_APPLICABLE",
]


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    """Meaning and presentation metadata for one canonical daily metric."""

    name: str
    unit: str
    aggregation: str
    source_data_types: tuple[str, ...]
    notion_label: str
    coverage_kind: CoverageKind = "UNKNOWN"


METRIC_DEFINITIONS: Final[tuple[MetricDefinition, ...]] = (
    MetricDefinition(
        "sleep_minutes", "min", "daily_sum", ("sleep",), "睡眠時間", "OBSERVATION_COUNT"
    ),
    MetricDefinition(
        "sleep_rem_minutes", "min", "daily_sum", ("sleep",), "REM睡眠", "OBSERVATION_COUNT"
    ),
    MetricDefinition(
        "sleep_deep_minutes", "min", "daily_sum", ("sleep",), "深い睡眠", "OBSERVATION_COUNT"
    ),
    MetricDefinition(
        "sleep_light_minutes", "min", "daily_sum", ("sleep",), "浅い睡眠", "OBSERVATION_COUNT"
    ),
    MetricDefinition(
        "sleep_awake_minutes", "min", "daily_sum", ("sleep",), "覚醒時間", "OBSERVATION_COUNT"
    ),
    MetricDefinition(
        "resting_heart_rate",
        "bpm",
        "daily_value",
        ("daily-resting-heart-rate",),
        "安静時心拍数",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "hrv_rmssd",
        "ms",
        "daily_value",
        ("daily-heart-rate-variability", "heart-rate-variability"),
        "HRV (RMSSD)",
    ),
    MetricDefinition(
        "spo2",
        "%",
        "daily_value",
        ("daily-oxygen-saturation", "oxygen-saturation"),
        "SpO2",
    ),
    MetricDefinition(
        "sleep_respiratory_rate",
        "breaths/min",
        "daily_value",
        ("respiratory-rate-sleep-summary",),
        "睡眠時呼吸数",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition("heart_rate", "bpm", "daily_mean", ("heart-rate",), "平均心拍数"),
    MetricDefinition("steps", "count", "daily_sum", ("steps",), "歩数"),
    MetricDefinition("distance", "m", "daily_sum", ("distance",), "距離"),
    MetricDefinition(
        "active_zone_minutes",
        "min",
        "daily_sum",
        ("active-zone-minutes",),
        "Active Zone Minutes",
    ),
    MetricDefinition("exercise_minutes", "min", "daily_sum", ("exercise",), "運動時間"),
)

METRICS_BY_NAME: Final = {definition.name: definition for definition in METRIC_DEFINITIONS}
METRICS_BY_DATA_TYPE: Final = {
    data_type: frozenset(
        definition.name
        for definition in METRIC_DEFINITIONS
        if data_type in definition.source_data_types
    )
    for data_type in {
        data_type for definition in METRIC_DEFINITIONS for data_type in definition.source_data_types
    }
}


def metric_definition(metric_name: str) -> MetricDefinition:
    """Return the registered definition or fail before persisting an unknown metric."""

    try:
        return METRICS_BY_NAME[metric_name]
    except KeyError as exc:
        raise ValueError(f"unknown metric name: {metric_name}") from exc
