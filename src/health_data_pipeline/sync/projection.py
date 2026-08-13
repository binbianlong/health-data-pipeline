"""Projection of ingested source rows into canonical daily revision rows."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, cast
from zoneinfo import ZoneInfo

from health_data_pipeline.health.daily import (
    DayContext,
    Observation,
    SourceQueryStatus,
    build_daily_rows,
    build_sleep_session_rows,
    build_source_status_rows,
)
from health_data_pipeline.health.metrics import (
    METRIC_DEFINITIONS,
    METRICS_BY_DATA_TYPE,
    CoverageKind,
)
from health_data_pipeline.health.normalization import normalize_daily_observations
from health_data_pipeline.health.schema import DAILY_SCHEMA_V3
from health_data_pipeline.health.sleep import SleepSession, normalize_sleep_sessions
from health_data_pipeline.state.models import RootSnapshot
from health_data_pipeline.state.snapshots import SnapshotRepository
from health_data_pipeline.sync.ingestion import IngestedQuery
from health_data_pipeline.sync.migrations import (
    DailyRevisionRows,
    upgrade_preserved_sleep_rows,
)


class DailyProjector:
    """Read the committed revision and build complete v3 replacements for affected days."""

    def __init__(self, *, snapshots: SnapshotRepository, normalizer_version: str) -> None:
        self._snapshots = snapshots
        self._normalizer_version = normalizer_version

    def merge_changed_observations(
        self,
        root: RootSnapshot | None,
        ingested: Sequence[IngestedQuery],
        *,
        source_statuses: Mapping[date, Sequence[SourceQueryStatus]],
    ) -> dict[date, tuple[Observation, ...]]:
        by_date_and_type: dict[tuple[date, str], list[Observation]] = defaultdict(list)
        for item in ingested:
            normalized = normalize_daily_observations(item.data_points)
            for metric_date, observations in normalized.items():
                by_date_and_type[(metric_date, item.data_type)].extend(observations)

        result: dict[date, tuple[Observation, ...]] = {}
        queried_dates = {item.metric_date for item in ingested}
        queried_dates.update(metric_date for metric_date, _ in by_date_and_type)
        for metric_date in queried_dates:
            existing = {
                observation.metric_name: observation
                for observation in self.observations_for_date(root, metric_date)
            }
            for item in (query for query in ingested if query.metric_date == metric_date):
                for metric_name in METRICS_BY_DATA_TYPE[item.data_type]:
                    current = existing.get(metric_name)
                    if current is not None and item.data_type in current.source_data_types:
                        existing.pop(metric_name)
            candidates = [
                observation
                for (day, _), observations in by_date_and_type.items()
                if day == metric_date
                for observation in observations
            ]
            for observation in sorted(candidates, key=_observation_priority):
                current = existing.get(observation.metric_name)
                if current is None or _observation_priority(observation) >= _observation_priority(
                    current
                ):
                    existing[observation.metric_name] = observation
            _add_missing_observations(
                existing,
                metric_date=metric_date,
                source_statuses=source_statuses.get(metric_date, ()),
            )
            result[metric_date] = tuple(existing[name] for name in sorted(existing))
        return result

    def merge_changed_source_statuses(
        self,
        root: RootSnapshot | None,
        ingested: Sequence[IngestedQuery],
    ) -> dict[date, tuple[SourceQueryStatus, ...]]:
        queried_dates = {item.metric_date for item in ingested}
        result: dict[date, tuple[SourceQueryStatus, ...]] = {}
        for metric_date in sorted(queried_dates):
            statuses = {
                status.data_type: status
                for status in self.source_statuses_for_date(root, metric_date)
            }
            for item in (query for query in ingested if query.metric_date == metric_date):
                statuses[item.data_type] = item.source_status
            result[metric_date] = tuple(statuses[name] for name in sorted(statuses))
        return result

    def merge_changed_sleep_sessions(
        self,
        ingested: Sequence[IngestedQuery],
    ) -> dict[date, tuple[SleepSession, ...]]:
        points_by_date: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
        queried_dates: set[date] = set()
        for item in ingested:
            if item.data_type != "sleep":
                continue
            queried_dates.add(item.metric_date)
            points_by_date[item.metric_date].extend(item.data_points)

        result: dict[date, tuple[SleepSession, ...]] = {}
        for metric_date in sorted(queried_dates):
            normalized = normalize_sleep_sessions(tuple(points_by_date[metric_date]))
            unexpected_dates = set(normalized).difference({metric_date})
            if unexpected_dates:
                raise ValueError("sleep session falls outside its replacement day")
            result[metric_date] = normalized.get(metric_date, ())
        return result

    def recalculate_rows(
        self,
        root: RootSnapshot | None,
        targets: Sequence[date],
        changed: Mapping[date, Sequence[Observation]],
        changed_sleep_sessions: Mapping[date, Sequence[SleepSession]],
        changed_source_statuses: Mapping[date, Sequence[SourceQueryStatus]],
        *,
        zone: ZoneInfo,
        time_zone_source: str,
        now: datetime,
        fitbit_last_sync: datetime | None,
    ) -> dict[date, Sequence[Mapping[str, object]] | None]:
        if not targets:
            return {}
        earliest = min(
            min(target - timedelta(days=target.weekday()), target.replace(day=1))
            for target in targets
        )
        latest = max(targets)
        history: dict[date, Sequence[Observation]] = {}
        cursor = earliest
        while cursor <= latest:
            history[cursor] = (
                changed[cursor] if cursor in changed else self.observations_for_date(root, cursor)
            )
            cursor += timedelta(days=1)

        rows: dict[date, Sequence[Mapping[str, object]] | None] = {}
        for metric_date in targets:
            observations = tuple(
                replace(
                    observation,
                    data_status=(
                        observation.data_status
                        if observation.value is None
                        else data_status(
                            metric_date,
                            zone=zone,
                            now=now,
                            fitbit_last_sync=fitbit_last_sync,
                        )
                    ),
                )
                for observation in history.get(metric_date, ())
            )
            history[metric_date] = observations
            start = datetime.combine(metric_date, time.min, tzinfo=zone).astimezone(UTC)
            end = datetime.combine(
                metric_date + timedelta(days=1),
                time.min,
                tzinfo=zone,
            ).astimezone(UTC)
            context = DayContext(
                metric_date=metric_date,
                fitbit_last_sync_time=fitbit_last_sync,
                time_zone=zone.key,
                physical_day_start=start,
                physical_day_end=end,
                time_zone_source=time_zone_source,
                logic_version=self._normalizer_version,
            )
            metric_rows = build_daily_rows(context, observations, history) if observations else []
            session_rows = (
                list(
                    build_sleep_session_rows(
                        context,
                        changed_sleep_sessions[metric_date],
                        data_status=data_status(
                            metric_date,
                            zone=zone,
                            now=now,
                            fitbit_last_sync=fitbit_last_sync,
                        ),
                    )
                )
                if metric_date in changed_sleep_sessions
                else list(self.sleep_session_rows_for_date(root, metric_date))
            )
            source_statuses = (
                tuple(changed_source_statuses[metric_date])
                if metric_date in changed_source_statuses
                else self.source_statuses_for_date(root, metric_date)
            )
            source_rows = build_source_status_rows(
                context,
                source_statuses,
                data_status=data_status(
                    metric_date,
                    zone=zone,
                    now=now,
                    fitbit_last_sync=fitbit_last_sync,
                ),
            )
            combined_rows = [*metric_rows, *session_rows, *source_rows]
            if combined_rows:
                rows[metric_date] = combined_rows
            elif metric_date in changed or metric_date in changed_sleep_sessions:
                rows[metric_date] = None
        return rows

    def sleep_session_rows_for_date(
        self,
        root: RootSnapshot | None,
        metric_date: date,
    ) -> tuple[Mapping[str, object], ...]:
        revision = self.daily_revision_for_date(root, metric_date)
        if revision is None:
            return ()
        return upgrade_preserved_sleep_rows(revision, metric_date=metric_date)

    def source_statuses_for_date(
        self,
        root: RootSnapshot | None,
        metric_date: date,
    ) -> tuple[SourceQueryStatus, ...]:
        revision = self.daily_revision_for_date(root, metric_date)
        if revision is None or revision.schema_version < DAILY_SCHEMA_V3:
            return ()
        result: list[SourceQueryStatus] = []
        for row in revision.rows:
            if row.get("record_kind") != "SOURCE_STATUS":
                continue
            data_type = row.get("dimension_key")
            status = row.get("value_text")
            if not isinstance(data_type, str) or not isinstance(status, str):
                raise TypeError("source status row has invalid identity fields")
            result.append(
                SourceQueryStatus(
                    data_type=data_type,
                    status=status,
                    source_count=_nonnegative_int(row.get("value_num"), 0),
                    normalized_count=_nonnegative_int(row.get("sample_count"), 0),
                )
            )
        return tuple(sorted(result, key=lambda item: item.data_type))

    def observations_for_date(
        self,
        root: RootSnapshot | None,
        metric_date: date,
    ) -> tuple[Observation, ...]:
        revision = self.daily_revision_for_date(root, metric_date)
        if revision is None:
            return ()
        result: list[Observation] = []
        for row in revision.rows:
            if (
                row.get("period_kind") != "DAY"
                or row.get("statistic") != "observed"
                or row.get("dimension_key") is not None
                or row.get("record_kind", "METRIC") != "METRIC"
            ):
                continue
            value = row.get("value_num")
            result.append(
                Observation(
                    metric_date=metric_date,
                    metric_name=str(row["metric_name"]),
                    value=None if value is None else Decimal(str(value)),
                    unit=str(row.get("unit", "")),
                    aggregation=str(row.get("aggregation", "daily_value")),
                    data_status=str(row.get("data_status", "CANONICAL_SYNCED")),
                    sample_count=_nonnegative_int(row.get("sample_count"), 0),
                    expected_count=_optional_nonnegative_int(row.get("expected_count")),
                    coverage_kind=_coverage_kind(row.get("coverage_kind")),
                    missing_reason=(
                        str(row["missing_reason"])
                        if row.get("missing_reason") is not None
                        else None
                    ),
                    quality_flags=tuple(str(item) for item in row.get("quality_flags", ())),
                    source_data_types=tuple(str(item) for item in row.get("source_data_types", ())),
                )
            )
        return tuple(result)

    def daily_revision_for_date(
        self,
        root: RootSnapshot | None,
        metric_date: date,
    ) -> DailyRevisionRows | None:
        if root is None:
            return None
        shard_reference = root.state.daily_year_shards.get(f"{metric_date.year:04d}")
        if shard_reference is None:
            return None
        shard = self._snapshots.load_year_shard(shard_reference)
        reference = shard.daily_revisions.get(metric_date.isoformat())
        if reference is None:
            return None
        payload = self._snapshots.read_daily_revision(reference, metric_date=metric_date)
        rows: list[Mapping[str, Any]] = []
        for line in payload.splitlines():
            row = json.loads(line, parse_float=Decimal)
            if not isinstance(row, Mapping):
                raise TypeError("daily revision row is not an object")
            rows.append(cast(Mapping[str, Any], row))
        return DailyRevisionRows(schema_version=reference.schema_version, rows=tuple(rows))


def data_status(
    metric_date: date,
    *,
    zone: ZoneInfo,
    now: datetime,
    fitbit_last_sync: datetime | None,
) -> str:
    day_end = datetime.combine(
        metric_date + timedelta(days=1),
        time.min,
        tzinfo=zone,
    ).astimezone(UTC)
    finalization = datetime.combine(
        metric_date + timedelta(days=1),
        time(4, 5),
        tzinfo=zone,
    ).astimezone(UTC)
    if now < finalization:
        return "PROVISIONAL"
    if fitbit_last_sync is not None and fitbit_last_sync >= day_end:
        return "CANONICAL_SYNCED"
    return "CANONICAL_STALE"


def _observation_priority(value: Observation) -> tuple[int, str]:
    daily = any(source.startswith("daily-") for source in value.source_data_types)
    return (1 if daily else 0, value.metric_name)


def _add_missing_observations(
    observations: dict[str, Observation],
    *,
    metric_date: date,
    source_statuses: Sequence[SourceQueryStatus],
) -> None:
    statuses = {status.data_type: status.status for status in source_statuses}
    for definition in METRIC_DEFINITIONS:
        if definition.name in observations:
            continue
        queried = tuple(source for source in definition.source_data_types if source in statuses)
        if not queried:
            continue
        status_values = {statuses[source] for source in queried}
        if "SOURCE_PROCESSING" in status_values:
            missing_reason = "SOURCE_PROCESSING"
        elif "INVALID_SOURCE" in status_values:
            missing_reason = "INVALID_SOURCE"
        elif len(queried) < len(definition.source_data_types):
            missing_reason = "NOT_QUERIED"
        else:
            missing_reason = "NO_SOURCE_VALUE"
        observations[definition.name] = Observation(
            metric_date=metric_date,
            metric_name=definition.name,
            value=None,
            unit=definition.unit,
            aggregation=definition.aggregation,
            data_status="MISSING",
            sample_count=0,
            expected_count=None,
            coverage_kind="UNKNOWN",
            missing_reason=missing_reason,
            quality_flags=(f"MISSING_{missing_reason}",),
            source_data_types=tuple(sorted(queried)),
        )


def _nonnegative_int(value: Any, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    if isinstance(value, (float, Decimal)) and value >= 0 and int(value) == value:
        return int(value)
    return default


def _optional_nonnegative_int(value: Any) -> int | None:
    if value is None:
        return None
    parsed = _nonnegative_int(value, -1)
    return parsed if parsed >= 0 else None


def _coverage_kind(value: Any) -> CoverageKind:
    if value in {
        "UNKNOWN",
        "OBSERVATION_COUNT",
        "CALENDAR_DAYS",
        "NOT_APPLICABLE",
    }:
        return cast(CoverageKind, value)
    return "OBSERVATION_COUNT"
