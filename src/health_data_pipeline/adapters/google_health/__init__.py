"""Google Health integration contracts."""

from health_data_pipeline.adapters.google_health.catalog import (
    DATA_TYPE_SPECS,
    DATA_TYPE_SPECS_BY_NAME,
    HIGH_FREQUENCY_DATA_TYPES,
    METRIC_SOURCE_DATA_TYPES,
    METRICS_BY_DATA_TYPE,
    PRIORITY_BACKFILL_DATA_TYPES,
    SUPPORTED_DATA_TYPES,
    TARGET_DATA_TYPES,
    WEBHOOK_SUPPORTED_DATA_TYPES,
    DataTypeSpec,
    data_type_spec,
)

__all__ = [
    "DATA_TYPE_SPECS",
    "DATA_TYPE_SPECS_BY_NAME",
    "HIGH_FREQUENCY_DATA_TYPES",
    "METRICS_BY_DATA_TYPE",
    "METRIC_SOURCE_DATA_TYPES",
    "PRIORITY_BACKFILL_DATA_TYPES",
    "SUPPORTED_DATA_TYPES",
    "TARGET_DATA_TYPES",
    "WEBHOOK_SUPPORTED_DATA_TYPES",
    "DataTypeSpec",
    "data_type_spec",
]
