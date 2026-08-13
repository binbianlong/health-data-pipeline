"""Explicit upgrades for rows preserved while a daily revision is recalculated."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from health_data_pipeline.health.schema import (
    DAILY_SCHEMA_V1,
    DAILY_SCHEMA_V2,
    DAILY_SCHEMA_V3,
    require_supported_daily_schema,
    validate_daily_row,
)


@dataclass(frozen=True, slots=True)
class DailyRevisionRows:
    """Decoded rows together with the schema declared by their revision."""

    schema_version: int
    rows: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        require_supported_daily_schema(self.schema_version)


def upgrade_preserved_sleep_rows(
    revision: DailyRevisionRows,
    *,
    metric_date: date,
) -> tuple[Mapping[str, object], ...]:
    """Return v3 sleep rows that may survive an unrelated source recalculation."""

    if revision.schema_version == DAILY_SCHEMA_V1:
        return ()

    upgraded: list[Mapping[str, object]] = []
    for row in revision.rows:
        if _is_session_metric(row):
            candidate = (
                _upgrade_v2_session_metric(row)
                if revision.schema_version == DAILY_SCHEMA_V2
                else dict(row)
            )
        elif row.get("record_kind") == "SLEEP_SESSION":
            candidate = (
                _upgrade_v2_session_envelope(row)
                if revision.schema_version == DAILY_SCHEMA_V2
                else dict(row)
            )
        else:
            continue
        validate_daily_row(candidate, schema_version=DAILY_SCHEMA_V3, metric_date=metric_date)
        upgraded.append(candidate)
    return tuple(upgraded)


def _is_session_metric(row: Mapping[str, Any]) -> bool:
    return (
        row.get("record_kind") == "METRIC"
        and row.get("period_kind") == "SESSION"
        and row.get("dimension_key") is not None
    )


def _upgrade_v2_session_metric(row: Mapping[str, Any]) -> dict[str, object]:
    missing_reason = row.get("missing_reason")
    if missing_reason is None and row.get("data_status") == "MISSING":
        missing_reason = "LEGACY_VALUE_UNAVAILABLE"
    return {
        **row,
        "expected_count": None,
        "coverage_ratio": None,
        "coverage_kind": "NOT_APPLICABLE",
        "missing_reason": missing_reason,
        "time_zone_source": "LEGACY_UNKNOWN",
        "session_role": None,
        "session_role_source": None,
        "session_processed": None,
        "session_manually_edited": None,
        "session_stages_status": None,
        "session_sleep_type": None,
        "session_create_time": None,
        "session_update_time": None,
    }


def _upgrade_v2_session_envelope(row: Mapping[str, Any]) -> dict[str, object]:
    flags = tuple(str(value) for value in row.get("quality_flags", ()))
    is_main = row.get("session_is_main") is True
    is_nap = row.get("session_is_nap") is True
    inferred = "MAIN_SLEEP_INFERRED" in flags
    if is_main or inferred:
        role = "MAIN"
        role_source = "INFERRED" if inferred else "SOURCE_FLAG"
    elif is_nap:
        role = "NAP"
        # v2 combined an explicit nap flag with REJECTED_NAP inference.
        role_source = "UNCLASSIFIED"
    else:
        role = "UNCLASSIFIED"
        role_source = "UNCLASSIFIED"
    return {
        **row,
        "expected_count": None,
        "coverage_ratio": None,
        "coverage_kind": "NOT_APPLICABLE",
        "missing_reason": (
            None if row.get("value_num") is not None else "SESSION_VALUE_UNAVAILABLE"
        ),
        "time_zone_source": "LEGACY_UNKNOWN",
        "session_role": role,
        "session_role_source": role_source,
        "session_processed": None,
        "session_manually_edited": None,
        "session_stages_status": None,
        "session_sleep_type": None,
        "session_create_time": None,
        "session_update_time": None,
    }
