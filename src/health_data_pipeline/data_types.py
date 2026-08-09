"""Shared catalog for health data-source behavior and canonical metric bindings."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Final, Literal

type RecordKind = Literal["daily", "interval", "sample", "session"]

SHORT_WINDOW = timedelta(days=14)
DEFAULT_WINDOW = timedelta(days=90)
DEFAULT_PAGE_SIZE = 10_000
SMALL_PAGE_SIZE = 25


@dataclass(frozen=True, slots=True)
class DataTypeSpec:
    """Fetch, retention-policy, and canonical-metric contract for one data type."""

    name: str
    max_window: timedelta
    page_size: int
    record_kind: RecordKind
    filter_field: str
    metric_names: tuple[str, ...] = ()
    webhook_supported: bool = False
    backfill_priority: bool = False
    high_frequency: bool = False
    metric_source_priority: int = 0


DATA_TYPE_SPECS: Final[tuple[DataTypeSpec, ...]] = (
    DataTypeSpec(
        name="steps",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="interval",
        filter_field="steps.interval.start_time",
        metric_names=("steps",),
        webhook_supported=True,
    ),
    DataTypeSpec(
        name="distance",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="interval",
        filter_field="distance.interval.start_time",
        metric_names=("distance",),
        webhook_supported=True,
    ),
    DataTypeSpec(
        name="active-zone-minutes",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="interval",
        filter_field="active_zone_minutes.interval.start_time",
        metric_names=("active_zone_minutes",),
        webhook_supported=True,
    ),
    DataTypeSpec(
        name="heart-rate",
        max_window=SHORT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="sample",
        filter_field="heart_rate.sample_time.physical_time",
        metric_names=("heart_rate",),
        webhook_supported=True,
        high_frequency=True,
    ),
    DataTypeSpec(
        name="daily-resting-heart-rate",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="daily",
        filter_field="daily_resting_heart_rate.date",
        metric_names=("resting_heart_rate",),
        webhook_supported=True,
        metric_source_priority=1,
    ),
    DataTypeSpec(
        name="heart-rate-variability",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="sample",
        filter_field="heart_rate_variability.sample_time.physical_time",
        metric_names=("hrv_rmssd",),
        webhook_supported=True,
        high_frequency=True,
    ),
    DataTypeSpec(
        name="daily-heart-rate-variability",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="daily",
        filter_field="daily_heart_rate_variability.date",
        metric_names=("hrv_rmssd",),
        webhook_supported=True,
        metric_source_priority=1,
    ),
    DataTypeSpec(
        name="oxygen-saturation",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="sample",
        filter_field="oxygen_saturation.sample_time.physical_time",
        metric_names=("spo2",),
        high_frequency=True,
    ),
    DataTypeSpec(
        name="daily-oxygen-saturation",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="daily",
        filter_field="daily_oxygen_saturation.date",
        metric_names=("spo2",),
        webhook_supported=True,
        metric_source_priority=1,
    ),
    DataTypeSpec(
        name="sleep",
        max_window=DEFAULT_WINDOW,
        page_size=SMALL_PAGE_SIZE,
        record_kind="session",
        filter_field="sleep.interval.civil_end_time",
        metric_names=(
            "sleep_minutes",
            "sleep_rem_minutes",
            "sleep_deep_minutes",
            "sleep_light_minutes",
            "sleep_awake_minutes",
        ),
        webhook_supported=True,
        backfill_priority=True,
    ),
    DataTypeSpec(
        name="respiratory-rate-sleep-summary",
        max_window=DEFAULT_WINDOW,
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="sample",
        filter_field="respiratory_rate_sleep_summary.sample_time.physical_time",
        metric_names=("sleep_respiratory_rate",),
        webhook_supported=True,
    ),
    DataTypeSpec(
        name="exercise",
        max_window=DEFAULT_WINDOW,
        page_size=SMALL_PAGE_SIZE,
        record_kind="session",
        filter_field="exercise.interval.civil_start_time",
        metric_names=("exercise_minutes",),
        webhook_supported=True,
        backfill_priority=True,
    ),
)

DATA_TYPE_SPECS_BY_NAME: Final[dict[str, DataTypeSpec]] = {
    spec.name: spec for spec in DATA_TYPE_SPECS
}
TARGET_DATA_TYPES: Final[tuple[str, ...]] = tuple(spec.name for spec in DATA_TYPE_SPECS)
# Compatibility name used by the existing pipeline API.
SUPPORTED_DATA_TYPES: Final[tuple[str, ...]] = TARGET_DATA_TYPES
WEBHOOK_SUPPORTED_DATA_TYPES: Final[tuple[str, ...]] = tuple(
    spec.name for spec in DATA_TYPE_SPECS if spec.webhook_supported
)
PRIORITY_BACKFILL_DATA_TYPES: Final[tuple[str, ...]] = tuple(
    spec.name for spec in DATA_TYPE_SPECS if spec.backfill_priority
)
HIGH_FREQUENCY_DATA_TYPES: Final[frozenset[str]] = frozenset(
    spec.name for spec in DATA_TYPE_SPECS if spec.high_frequency
)
METRICS_BY_DATA_TYPE: Final[dict[str, frozenset[str]]] = {
    spec.name: frozenset(spec.metric_names) for spec in DATA_TYPE_SPECS
}

_METRIC_NAMES: Final[tuple[str, ...]] = tuple(
    dict.fromkeys(metric_name for spec in DATA_TYPE_SPECS for metric_name in spec.metric_names)
)
METRIC_SOURCE_DATA_TYPES: Final[dict[str, tuple[str, ...]]] = {
    metric_name: tuple(
        spec.name
        for spec in sorted(
            DATA_TYPE_SPECS,
            key=lambda candidate: -candidate.metric_source_priority,
        )
        if metric_name in spec.metric_names
    )
    for metric_name in _METRIC_NAMES
}

_COMPATIBILITY_SHORT_WINDOW_TYPES: Final = frozenset(
    {
        "active-minutes",
        "calories-in-heart-rate-zone",
        "total-calories",
    }
)


def data_type_spec(data_type: str) -> DataTypeSpec:
    """Return a catalog entry, preserving the legacy fallback for unknown types."""

    catalog_entry = DATA_TYPE_SPECS_BY_NAME.get(data_type)
    if catalog_entry is not None:
        return catalog_entry
    return DataTypeSpec(
        name=data_type,
        max_window=(
            SHORT_WINDOW if data_type in _COMPATIBILITY_SHORT_WINDOW_TYPES else DEFAULT_WINDOW
        ),
        page_size=DEFAULT_PAGE_SIZE,
        record_kind="interval",
        filter_field=f"{data_type.replace('-', '_')}.interval.start_time",
    )
