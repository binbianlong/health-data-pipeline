"""Typed application models shared by materialized-view ports."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


class ViewUpdateError(RuntimeError):
    """A materialized view could not be safely updated."""


@dataclass(frozen=True, slots=True)
class DailyMetric:
    metric_name: str
    statistic: str
    value: int | float | None
    unit: str | None
    status: str
    weekly_average: float | None = None
    monthly_average: float | None = None
    coverage: float | None = None
    weekly_coverage: float | None = None
    monthly_coverage: float | None = None
    quality_flags: tuple[str, ...] = ()
    missing_reason: str | None = None
    coverage_kind: str = "UNKNOWN"
    weekly_status: str | None = None
    monthly_status: str | None = None
    weekly_quality_flags: tuple[str, ...] = ()
    monthly_quality_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SleepSessionView:
    """One committed sleep interval rendered with its edge-specific offsets."""

    start_time: datetime
    end_time: datetime
    is_main: bool
    is_nap: bool
    minutes_asleep: int | float | None = None
    role_source: str | None = None
    processed: bool | None = None
    stages_status: str | None = None
    manually_edited: bool | None = None


@dataclass(frozen=True, slots=True)
class DailyView:
    metric_date: date
    source_generation: int
    data_status: str
    fitbit_last_sync_time: datetime | None
    metrics: tuple[DailyMetric, ...]
    time_zone: str | None = None
    time_zone_source: str | None = None
    physical_day_start: datetime | None = None
    physical_day_end: datetime | None = None
    sleep_sessions: tuple[SleepSessionView, ...] = ()
    sleep_sessions_available: bool = False
    sleep_source_status: str | None = None


@dataclass(frozen=True, slots=True)
class NotionPage:
    page_id: str
    source_generation: int
    content_sha256: str | None


@dataclass(frozen=True, slots=True)
class NotionSyncState:
    desired_generation: int
    applied_generation: int
    status: str
    applying_generation: int | None
    updated_at: str

    @classmethod
    def from_bytes(cls, data: bytes) -> NotionSyncState:
        try:
            return cls(**json.loads(data))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ViewUpdateError("invalid Notion sync state") from exc

    def to_bytes(self) -> bytes:
        return canonical_view_json(asdict(self))


@dataclass(frozen=True, slots=True)
class NotionPageState:
    """Durable page correspondence retained while the page is managed."""

    metric_date: str
    page_id: str
    source_generation: int
    content_sha256: str
    updated_at: str

    @classmethod
    def from_bytes(cls, data: bytes) -> NotionPageState:
        try:
            return cls(**json.loads(data))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ViewUpdateError("invalid Notion page state") from exc

    def to_bytes(self) -> bytes:
        return canonical_view_json(asdict(self))


@dataclass(frozen=True, slots=True)
class StepSample:
    metric_date: date
    steps: int | None


def canonical_view_json(value: Mapping[str, Any]) -> bytes:
    """Encode deterministic state and content-hash inputs."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
