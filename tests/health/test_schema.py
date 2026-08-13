from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from health_data_pipeline.health.schema import (
    DailySchemaValidationError,
    validate_daily_row,
)
from health_data_pipeline.state.objects import ContentAddressedStore, InMemoryObjectStore
from health_data_pipeline.state.snapshots import SnapshotRepository, SnapshotValidationError

METRIC_DATE = date(2026, 8, 2)
DAY_START = datetime(2026, 8, 1, 15, tzinfo=UTC)
DAY_END = datetime(2026, 8, 2, 15, tzinfo=UTC)
SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas" / "daily_canonical"


def _v1_row() -> dict[str, object]:
    return {
        "metric_name": "steps",
        "statistic": "observed",
        "period_kind": "DAY",
        "dimension_key": None,
        "value_num": Decimal(8_432),
        "value_text": None,
        "unit": "count",
        "aggregation": "daily_sum",
        "period_start": METRIC_DATE,
        "period_end": METRIC_DATE,
        "sample_count": 1,
        "expected_count": 1,
        "coverage_ratio": Decimal(1),
        "data_status": "CANONICAL_SYNCED",
        "quality_flags": [],
        "source_data_types": ["steps"],
        "fitbit_last_sync_time": DAY_END,
        "time_zone": "Asia/Tokyo",
        "physical_day_start": DAY_START,
        "physical_day_end": DAY_END,
        "logic_version": "daily-v4",
    }


def _v2_metric_row() -> dict[str, object]:
    return {
        **_v1_row(),
        "record_kind": "METRIC",
        "session_start_time": None,
        "session_end_time": None,
        "session_start_utc_offset_seconds": None,
        "session_end_utc_offset_seconds": None,
        "session_is_main": None,
        "session_is_nap": None,
    }


def _v3_metric_row() -> dict[str, object]:
    return {
        **_v2_metric_row(),
        "missing_reason": None,
        "coverage_kind": "OBSERVATION_COUNT",
        "time_zone_source": "GOOGLE_HEALTH_SETTINGS",
        "session_role": None,
        "session_role_source": None,
        "session_processed": None,
        "session_manually_edited": None,
        "session_stages_status": None,
        "session_sleep_type": None,
        "session_create_time": None,
        "session_update_time": None,
    }


def _v3_sleep_session_row() -> dict[str, object]:
    return {
        **_v3_metric_row(),
        "record_kind": "SLEEP_SESSION",
        "metric_name": "sleep_session",
        "period_kind": "SESSION",
        "dimension_key": "sleep:0123456789abcdef",
        "value_num": Decimal("420.5"),
        "unit": "min",
        "aggregation": "session_value",
        "expected_count": None,
        "coverage_ratio": None,
        "coverage_kind": "NOT_APPLICABLE",
        "source_data_types": ["sleep"],
        "session_start_time": datetime(2026, 8, 1, 14, tzinfo=UTC),
        "session_end_time": datetime(2026, 8, 1, 22, tzinfo=UTC),
        "session_start_utc_offset_seconds": 32_400,
        "session_end_utc_offset_seconds": 32_400,
        "session_is_main": False,
        "session_is_nap": False,
        "session_role": "MAIN",
        "session_role_source": "INFERRED",
        "session_processed": True,
        "session_manually_edited": False,
        "session_stages_status": "COMPLETE",
        "session_sleep_type": "STAGES",
        "session_create_time": datetime(2026, 8, 1, 22, 5, tzinfo=UTC),
        "session_update_time": datetime(2026, 8, 1, 22, 10, tzinfo=UTC),
    }


def _v3_source_status_row() -> dict[str, object]:
    return {
        **_v3_metric_row(),
        "record_kind": "SOURCE_STATUS",
        "metric_name": "source_status",
        "dimension_key": "sleep",
        "source_data_types": ["sleep"],
        "value_num": Decimal(2),
        "value_text": "SUCCESS_WITH_DATA",
        "unit": "records",
        "aggregation": "query_result",
        "expected_count": None,
        "coverage_ratio": None,
        "coverage_kind": "NOT_APPLICABLE",
        "time_zone_source": "LEGACY_UNKNOWN",
    }


def test_v1_and_v2_are_strict_version_specific_contracts() -> None:
    validate_daily_row(_v1_row(), schema_version=1, metric_date=METRIC_DATE)
    validate_daily_row(_v2_metric_row(), schema_version=2, metric_date=METRIC_DATE)

    with pytest.raises(DailySchemaValidationError, match="does not match schema v1"):
        validate_daily_row(
            {**_v1_row(), "record_kind": "METRIC"},
            schema_version=1,
            metric_date=METRIC_DATE,
        )
    incomplete = _v2_metric_row()
    incomplete.pop("session_is_nap")
    with pytest.raises(DailySchemaValidationError, match="session_is_nap"):
        validate_daily_row(incomplete, schema_version=2, metric_date=METRIC_DATE)


@pytest.mark.parametrize(
    "row_factory",
    [_v3_metric_row, _v3_sleep_session_row, _v3_source_status_row],
)
def test_v3_accepts_each_tagged_record_kind(
    row_factory: Callable[[], dict[str, object]],
) -> None:
    row = row_factory()
    validate_daily_row(row, schema_version=3, metric_date=METRIC_DATE)


def test_v3_metric_can_be_scoped_to_a_sleep_session() -> None:
    row = {
        **_v3_metric_row(),
        "metric_name": "sleep_stage_deep",
        "period_kind": "SESSION",
        "dimension_key": "sleep:0123456789abcdef",
    }
    validate_daily_row(row, schema_version=3, metric_date=METRIC_DATE)


@pytest.mark.parametrize(
    "change",
    [
        {"data_status": "MISSING", "value_num": Decimal(1), "missing_reason": None},
        {"data_status": "MISSING", "value_num": None, "missing_reason": None},
    ],
)
def test_v3_missing_metric_requires_null_values_and_reason(
    change: dict[str, object],
) -> None:
    with pytest.raises(DailySchemaValidationError, match="MISSING metrics require"):
        validate_daily_row(
            {**_v3_metric_row(), **change},
            schema_version=3,
            metric_date=METRIC_DATE,
        )


@pytest.mark.parametrize(
    "data_status",
    [
        "PROVISIONAL",
        "CANONICAL_SYNCED",
        "CANONICAL_STALE",
        "MISSING",
        "INSUFFICIENT_DATA",
    ],
)
def test_v3_source_status_allows_normal_data_statuses(data_status: str) -> None:
    row = {**_v3_source_status_row(), "data_status": data_status}
    validate_daily_row(row, schema_version=3, metric_date=METRIC_DATE)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"session_is_main": False}, "session_is_main"),
        ({"period_kind": "CALENDAR_WEEK"}, "CALENDAR_DAYS"),
        ({"period_end": date(2026, 8, 3)}, "period_end must match"),
    ],
)
def test_v3_metric_rejects_cross_kind_and_partition_inconsistency(
    change: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(DailySchemaValidationError, match=message):
        validate_daily_row(
            {**_v3_metric_row(), **change},
            schema_version=3,
            metric_date=METRIC_DATE,
        )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"session_start_time": None}, "session_start_time"),
        (
            {
                "session_start_time": datetime(2026, 8, 1, 23, tzinfo=UTC),
                "session_end_time": datetime(2026, 8, 1, 22, tzinfo=UTC),
            },
            "must be before",
        ),
        ({"session_role": "NAP", "session_is_nap": False}, "NAP role"),
        ({"value_num": Decimal(-1)}, "non-negative"),
    ],
)
def test_v3_sleep_session_requires_boundaries_and_classification(
    change: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(DailySchemaValidationError, match=message):
        validate_daily_row(
            {**_v3_sleep_session_row(), **change},
            schema_version=3,
            metric_date=METRIC_DATE,
        )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"value_text": "NO_DATA"}, "value_text"),
        ({"value_num": Decimal("1.5")}, "record count"),
        ({"session_role": "UNCLASSIFIED"}, "session_role"),
        ({"period_start": date(2026, 8, 1)}, "exactly one day"),
        ({"source_data_types": ["steps"]}, "must match dimension_key"),
        ({"value_text": "SUCCESS_EMPTY"}, "SUCCESS_EMPTY"),
        ({"value_num": Decimal(0)}, "SUCCESS_WITH_DATA"),
        ({"value_text": "SOURCE_PROCESSING", "value_num": Decimal(0)}, "SOURCE_PROCESSING"),
        ({"value_text": "INVALID_SOURCE", "sample_count": 2}, "INVALID_SOURCE"),
    ],
)
def test_v3_source_status_enforces_query_result_shape(
    change: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(DailySchemaValidationError, match=message):
        validate_daily_row(
            {**_v3_source_status_row(), **change},
            schema_version=3,
            metric_date=METRIC_DATE,
        )


def test_daily_publication_rejects_unsupported_version_and_publishes_v3() -> None:
    repository = SnapshotRepository(ContentAddressedStore(InMemoryObjectStore()))
    with pytest.raises(SnapshotValidationError, match="unsupported daily schema version"):
        repository.publish_daily_revision(
            METRIC_DATE,
            [],
            source_generation=1,
            schema_version=4,
        )

    reference = repository.publish_daily_revision(
        METRIC_DATE,
        [_v3_metric_row(), _v3_source_status_row()],
        source_generation=1,
        schema_version=3,
    )
    assert reference.schema_version == 3
    assert "schema_version=3/metric_date=2026-08-02" in reference.uri


def test_v3_schema_artifacts_expose_required_discriminants_and_types() -> None:
    json_schema = json.loads((SCHEMA_DIR / "v3.schema.json").read_text(encoding="utf-8"))
    bigquery_schema = {
        field["name"]: field
        for field in json.loads((SCHEMA_DIR / "v3.bigquery.json").read_text(encoding="utf-8"))
    }

    assert json_schema["properties"]["record_kind"]["enum"] == [
        "METRIC",
        "SLEEP_SESSION",
        "SOURCE_STATUS",
    ]
    assert json_schema["properties"]["time_zone_source"]["enum"] == [
        "GOOGLE_HEALTH_SETTINGS",
        "CONFIG_DEFAULT",
        "LEGACY_UNKNOWN",
    ]
    assert len(json_schema["oneOf"]) == 3
    assert "missing_reason" in json_schema["required"]
    assert bigquery_schema["value_num"]["type"] == "BIGNUMERIC"
    assert bigquery_schema["expected_count"]["mode"] == "NULLABLE"
    assert bigquery_schema["session_update_time"]["type"] == "TIMESTAMP"
