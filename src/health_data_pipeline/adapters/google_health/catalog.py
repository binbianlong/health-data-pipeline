"""Compatibility facade for the shared health data-type catalog."""

from health_data_pipeline.data_types import (
    DATA_TYPE_SPECS,
    DATA_TYPE_SPECS_BY_NAME,
    DEFAULT_PAGE_SIZE,
    DEFAULT_WINDOW,
    HIGH_FREQUENCY_DATA_TYPES,
    METRIC_SOURCE_DATA_TYPES,
    METRICS_BY_DATA_TYPE,
    PRIORITY_BACKFILL_DATA_TYPES,
    SHORT_WINDOW,
    SMALL_PAGE_SIZE,
    SUPPORTED_DATA_TYPES,
    TARGET_DATA_TYPES,
    WEBHOOK_SUPPORTED_DATA_TYPES,
    DataTypeSpec,
    RecordKind,
    data_type_spec,
)

__all__ = [
    "DATA_TYPE_SPECS",
    "DATA_TYPE_SPECS_BY_NAME",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_WINDOW",
    "HIGH_FREQUENCY_DATA_TYPES",
    "METRICS_BY_DATA_TYPE",
    "METRIC_SOURCE_DATA_TYPES",
    "PRIORITY_BACKFILL_DATA_TYPES",
    "SHORT_WINDOW",
    "SMALL_PAGE_SIZE",
    "SUPPORTED_DATA_TYPES",
    "TARGET_DATA_TYPES",
    "WEBHOOK_SUPPORTED_DATA_TYPES",
    "DataTypeSpec",
    "RecordKind",
    "data_type_spec",
]
