"""Stable metric catalog shared by normalization, storage, and views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

from health_data_pipeline.data_types import METRIC_SOURCE_DATA_TYPES
from health_data_pipeline.data_types import (
    METRICS_BY_DATA_TYPE as _CATALOG_METRICS_BY_DATA_TYPE,
)

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
        "sleep_minutes",
        "min",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["sleep_minutes"],
        "睡眠時間",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "sleep_rem_minutes",
        "min",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["sleep_rem_minutes"],
        "REM睡眠",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "sleep_deep_minutes",
        "min",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["sleep_deep_minutes"],
        "深い睡眠",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "sleep_light_minutes",
        "min",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["sleep_light_minutes"],
        "浅い睡眠",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "sleep_awake_minutes",
        "min",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["sleep_awake_minutes"],
        "覚醒時間",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "resting_heart_rate",
        "bpm",
        "daily_value",
        METRIC_SOURCE_DATA_TYPES["resting_heart_rate"],
        "安静時心拍数",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "hrv_rmssd",
        "ms",
        "daily_value",
        METRIC_SOURCE_DATA_TYPES["hrv_rmssd"],
        "HRV (RMSSD)",
    ),
    MetricDefinition(
        "spo2",
        "%",
        "daily_value",
        METRIC_SOURCE_DATA_TYPES["spo2"],
        "SpO2",
    ),
    MetricDefinition(
        "sleep_respiratory_rate",
        "breaths/min",
        "daily_value",
        METRIC_SOURCE_DATA_TYPES["sleep_respiratory_rate"],
        "睡眠時呼吸数",
        "OBSERVATION_COUNT",
    ),
    MetricDefinition(
        "heart_rate",
        "bpm",
        "daily_mean",
        METRIC_SOURCE_DATA_TYPES["heart_rate"],
        "平均心拍数",
    ),
    MetricDefinition(
        "steps",
        "count",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["steps"],
        "歩数",
    ),
    MetricDefinition(
        "distance",
        "m",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["distance"],
        "距離",
    ),
    MetricDefinition(
        "active_zone_minutes",
        "min",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["active_zone_minutes"],
        "Active Zone Minutes",
    ),
    MetricDefinition(
        "exercise_minutes",
        "min",
        "daily_sum",
        METRIC_SOURCE_DATA_TYPES["exercise_minutes"],
        "運動時間",
    ),
)

METRICS_BY_NAME: Final = {definition.name: definition for definition in METRIC_DEFINITIONS}
METRICS_BY_DATA_TYPE: Final = _CATALOG_METRICS_BY_DATA_TYPE


def metric_definition(metric_name: str) -> MetricDefinition:
    """Return the registered definition or fail before persisting an unknown metric."""

    try:
        return METRICS_BY_NAME[metric_name]
    except KeyError as exc:
        raise ValueError(f"unknown metric name: {metric_name}") from exc
