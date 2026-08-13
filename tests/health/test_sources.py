from __future__ import annotations

from datetime import timedelta

from health_data_pipeline.health.metrics import METRIC_DEFINITIONS
from health_data_pipeline.health.sources import (
    DATA_TYPE_SPECS,
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


def test_catalog_preserves_target_order_and_policy_groups() -> None:
    assert TARGET_DATA_TYPES == (
        "steps",
        "distance",
        "active-zone-minutes",
        "heart-rate",
        "daily-resting-heart-rate",
        "heart-rate-variability",
        "daily-heart-rate-variability",
        "oxygen-saturation",
        "daily-oxygen-saturation",
        "sleep",
        "respiratory-rate-sleep-summary",
        "exercise",
    )
    assert SUPPORTED_DATA_TYPES == TARGET_DATA_TYPES
    assert tuple(spec.name for spec in DATA_TYPE_SPECS) == TARGET_DATA_TYPES
    assert PRIORITY_BACKFILL_DATA_TYPES == ("sleep", "exercise")
    assert (
        frozenset({"heart-rate", "heart-rate-variability", "oxygen-saturation"})
        == HIGH_FREQUENCY_DATA_TYPES
    )
    assert (
        tuple(data_type for data_type in TARGET_DATA_TYPES if data_type != "oxygen-saturation")
        == WEBHOOK_SUPPORTED_DATA_TYPES
    )


def test_catalog_exposes_fetch_contracts() -> None:
    assert data_type_spec("heart-rate") == DataTypeSpec(
        name="heart-rate",
        max_window=timedelta(days=14),
        page_size=10_000,
        record_kind="sample",
        filter_field="heart_rate.sample_time.physical_time",
        metric_names=("heart_rate",),
        webhook_supported=True,
        high_frequency=True,
    )
    assert data_type_spec("sleep") == DataTypeSpec(
        name="sleep",
        max_window=timedelta(days=90),
        page_size=25,
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
    )
    daily = data_type_spec("daily-heart-rate-variability")
    assert daily.record_kind == "daily"
    assert daily.filter_field == "daily_heart_rate_variability.date"
    assert daily.metric_source_priority == 1


def test_metric_bindings_are_derived_in_both_directions() -> None:
    assert METRICS_BY_DATA_TYPE["sleep"] == frozenset(
        {
            "sleep_minutes",
            "sleep_rem_minutes",
            "sleep_deep_minutes",
            "sleep_light_minutes",
            "sleep_awake_minutes",
        }
    )
    assert METRIC_SOURCE_DATA_TYPES["hrv_rmssd"] == (
        "daily-heart-rate-variability",
        "heart-rate-variability",
    )
    assert METRIC_SOURCE_DATA_TYPES["spo2"] == (
        "daily-oxygen-saturation",
        "oxygen-saturation",
    )
    definitions = {definition.name: definition for definition in METRIC_DEFINITIONS}
    assert set(definitions) == set(METRIC_SOURCE_DATA_TYPES)
    for metric_name, source_data_types in METRIC_SOURCE_DATA_TYPES.items():
        assert definitions[metric_name].source_data_types == source_data_types
        assert all(
            metric_name in METRICS_BY_DATA_TYPE[data_type] for data_type in source_data_types
        )


def test_unknown_data_type_uses_the_documented_fallback() -> None:
    short = data_type_spec("active-minutes")
    assert short.max_window == timedelta(days=14)
    assert short.record_kind == "interval"
    assert short.filter_field == "active_minutes.interval.start_time"

    future = data_type_spec("future-data")
    assert future.max_window == timedelta(days=90)
    assert future.page_size == 10_000
    assert future.record_kind == "interval"
    assert future.filter_field == "future_data.interval.start_time"
    assert future.metric_names == ()
