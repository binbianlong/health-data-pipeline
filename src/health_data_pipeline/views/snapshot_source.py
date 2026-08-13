"""Project committed snapshot rows into Notion and steps application models."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, cast
from zoneinfo import ZoneInfo

from health_data_pipeline.state.head import HeadRepository
from health_data_pipeline.state.models import RootSnapshot
from health_data_pipeline.state.objects import ObjectNotFound
from health_data_pipeline.state.snapshots import SnapshotRepository
from health_data_pipeline.views.models import (
    DailyMetric,
    DailyView,
    SleepSessionView,
    StepSample,
)


class SnapshotViewSource:
    """Materialize Notion rows and step samples from a committed Snapshot."""

    def __init__(
        self,
        *,
        heads: HeadRepository,
        snapshots: SnapshotRepository,
    ) -> None:
        self._heads = heads
        self._snapshots = snapshots

    def latest_generation(self) -> int:
        current = self._heads.read().head
        return 0 if current is None else current.generation

    def views_between(
        self,
        *,
        after_generation: int,
        through_generation: int,
    ) -> Sequence[DailyView]:
        """Materialize every date changed in the unapplied generation range.

        Each date is rendered from the target Snapshot, so repeated changes in
        intermediate generations collapse to one final, monotonic write.
        """

        root = self._root_for_generation(through_generation)
        dates: set[date]
        if after_generation < 0 or after_generation >= through_generation:
            if after_generation == through_generation:
                return ()
            raise ValueError("invalid generation range")
        if after_generation == 0:
            dates = set(self._available_dates(root))
        else:
            dates = set()
            cursor = root
            while cursor.generation > after_generation:
                dates.update(cursor.change_set.changed_metric_dates)
                if cursor.generation == after_generation + 1:
                    break
                if cursor.parent_snapshot is None:
                    dates.update(self._available_dates(root))
                    break
                try:
                    cursor = self._snapshots.load_root_snapshot(cursor.parent_snapshot)
                except ObjectNotFound:
                    # A pruned audit chain must degrade to a full current-state
                    # reconciliation instead of silently dropping dates.
                    dates.update(self._available_dates(root))
                    break
        return tuple(self._daily_view(root, metric_date) for metric_date in sorted(dates))

    def for_generation(self, generation: int) -> Sequence[StepSample]:
        root = self._root_for_generation(generation)
        commit_date = root.change_set.created_at.astimezone(ZoneInfo(root.state.time_zone)).date()
        # The public profile is a once-daily view of completed civil days.
        # Excluding the commit date prevents partial intraday totals from
        # appearing while Fitbit is still updating that day.
        last = date.fromordinal(commit_date.toordinal() - 1)
        target_dates = tuple(
            date.fromordinal(last.toordinal() - offset) for offset in range(6, -1, -1)
        )
        return tuple(
            StepSample(
                metric_date=metric_date,
                steps=self._step_count(root, metric_date),
            )
            for metric_date in target_dates
        )

    def observations_for_date(
        self,
        root: RootSnapshot,
        metric_date: date,
    ) -> tuple[Mapping[str, Any], ...]:
        return tuple(
            row
            for row in self._daily_rows(root, metric_date)
            if row.get("period_kind") == "DAY"
            and row.get("statistic") == "observed"
            and row.get("dimension_key") is None
            and row.get("record_kind", "METRIC") == "METRIC"
        )

    def _root_for_generation(self, generation: int) -> RootSnapshot:
        current = self._heads.read().head
        if current is None:
            raise ValueError("no committed HEAD exists")
        if generation < 1 or generation > current.generation:
            raise ValueError("requested generation is outside the HEAD chain")
        root = self._snapshots.load_root_snapshot(current.root_snapshot)
        while root.generation > generation:
            if root.parent_snapshot is None:
                raise ValueError("Snapshot chain ended before requested generation")
            root = self._snapshots.load_root_snapshot(root.parent_snapshot)
        if root.generation != generation:
            raise ValueError("Snapshot chain does not contain requested generation")
        return root

    def _available_dates(self, root: RootSnapshot) -> tuple[date, ...]:
        result: list[date] = []
        for reference in root.state.daily_year_shards.values():
            shard = self._snapshots.load_year_shard(reference)
            result.extend(date.fromisoformat(value) for value in shard.daily_revisions)
        return tuple(sorted(result))

    def _daily_rows(
        self,
        root: RootSnapshot,
        metric_date: date,
    ) -> tuple[Mapping[str, Any], ...]:
        shard_reference = root.state.daily_year_shards.get(f"{metric_date.year:04d}")
        if shard_reference is None:
            return ()
        shard = self._snapshots.load_year_shard(shard_reference)
        revision = shard.daily_revisions.get(metric_date.isoformat())
        if revision is None:
            return ()
        payload = self._snapshots.read_daily_revision(revision, metric_date=metric_date)
        rows: list[Mapping[str, Any]] = []
        for line in payload.splitlines():
            value = json.loads(line, parse_float=Decimal)
            if not isinstance(value, Mapping):
                raise TypeError("daily revision row is not an object")
            rows.append(cast(Mapping[str, Any], value))
        return tuple(rows)

    def _daily_view(self, root: RootSnapshot, metric_date: date) -> DailyView:
        rows = self._daily_rows(root, metric_date)
        observed: dict[str, Mapping[str, Any]] = {}
        weekly: dict[str, Mapping[str, Any]] = {}
        monthly: dict[str, Mapping[str, Any]] = {}
        session_rows_list: list[Mapping[str, Any]] = []
        sleep_source_status: str | None = None
        sleep_source_status_seen = False
        for row in rows:
            record_kind = row.get("record_kind", "METRIC")
            if record_kind == "SLEEP_SESSION":
                session_rows_list.append(row)
                continue
            if record_kind == "SOURCE_STATUS":
                if not sleep_source_status_seen and row.get("dimension_key") == "sleep":
                    sleep_source_status = _optional_string(row.get("value_text"))
                    sleep_source_status_seen = True
                continue
            if record_kind != "METRIC" or row.get("dimension_key") is not None:
                continue

            statistic = row.get("statistic")
            period_kind = row.get("period_kind")
            if statistic == "observed" and period_kind == "DAY":
                observed[str(row["metric_name"])] = row
            elif statistic == "mean" and period_kind == "CALENDAR_WEEK":
                weekly[str(row["metric_name"])] = row
            elif statistic == "mean" and period_kind == "CALENDAR_MONTH":
                monthly[str(row["metric_name"])] = row

        session_rows = tuple(session_rows_list)
        sleep_sessions = tuple(_sleep_session_view(row) for row in session_rows)
        metrics = tuple(
            DailyMetric(
                metric_name=name,
                statistic="value",
                value=_optional_number(row.get("value_num")),
                unit=_optional_string(row.get("unit")),
                status=str(row.get("data_status", "UNKNOWN")),
                weekly_average=_optional_number(weekly.get(name, {}).get("value_num")),
                monthly_average=_optional_number(monthly.get(name, {}).get("value_num")),
                coverage=(
                    _optional_number(row.get("coverage_ratio")) if "coverage_kind" in row else None
                ),
                weekly_coverage=(
                    _optional_number(weekly.get(name, {}).get("coverage_ratio"))
                    if "coverage_kind" in weekly.get(name, {})
                    else None
                ),
                monthly_coverage=(
                    _optional_number(monthly.get(name, {}).get("coverage_ratio"))
                    if "coverage_kind" in monthly.get(name, {})
                    else None
                ),
                quality_flags=tuple(str(value) for value in row.get("quality_flags", ())),
                missing_reason=_optional_string(row.get("missing_reason")),
                coverage_kind=str(row.get("coverage_kind", "UNKNOWN")),
                weekly_status=_optional_string(weekly.get(name, {}).get("data_status")),
                monthly_status=_optional_string(monthly.get(name, {}).get("data_status")),
                weekly_quality_flags=tuple(
                    str(value) for value in weekly.get(name, {}).get("quality_flags", ())
                ),
                monthly_quality_flags=tuple(
                    str(value) for value in monthly.get(name, {}).get("quality_flags", ())
                ),
            )
            for name, row in sorted(observed.items())
        )
        statuses = {metric.status for metric in metrics}
        statuses.update(str(row.get("data_status", "UNKNOWN")) for row in session_rows)
        data_status = (
            next(iter(statuses)) if len(statuses) == 1 else ("MISSING" if not statuses else "MIXED")
        )
        context_row = next(iter(observed.values()), None)
        if context_row is None:
            context_row = session_rows[0] if session_rows else (rows[0] if rows else None)
        return DailyView(
            metric_date=metric_date,
            source_generation=root.generation,
            data_status=data_status,
            fitbit_last_sync_time=root.state.fitbit_last_sync_time,
            metrics=metrics,
            time_zone=(
                _optional_string(context_row.get("time_zone"))
                if context_row is not None
                else root.state.time_zone
            ),
            time_zone_source=(
                _optional_string(context_row.get("time_zone_source"))
                if context_row is not None
                else None
            ),
            physical_day_start=(
                _optional_aware_datetime(context_row.get("physical_day_start"))
                if context_row is not None
                else None
            ),
            physical_day_end=(
                _optional_aware_datetime(context_row.get("physical_day_end"))
                if context_row is not None
                else None
            ),
            sleep_sessions=sleep_sessions,
            sleep_sessions_available=bool(session_rows),
            sleep_source_status=sleep_source_status,
        )

    def _step_count(self, root: RootSnapshot, metric_date: date) -> int | None:
        for row in self._daily_rows(root, metric_date):
            if (
                row.get("metric_name") == "steps"
                and row.get("statistic") == "observed"
                and row.get("period_kind") == "DAY"
                and row.get("dimension_key") is None
                and row.get("record_kind", "METRIC") == "METRIC"
            ):
                value = _optional_number(row.get("value_num"))
                return None if value is None else round(value)
        return None


def _sleep_session_view(row: Mapping[str, Any]) -> SleepSessionView:
    start = _required_aware_datetime(row.get("session_start_time"), "session_start_time")
    end = _required_aware_datetime(row.get("session_end_time"), "session_end_time")
    if end <= start:
        raise ValueError("sleep session end must be after start")
    start_offset = _required_utc_offset(row.get("session_start_utc_offset_seconds"))
    end_offset = _required_utc_offset(row.get("session_end_utc_offset_seconds"))
    is_main = row.get("session_is_main")
    is_nap = row.get("session_is_nap")
    if not isinstance(is_main, bool) or not isinstance(is_nap, bool):
        raise TypeError("sleep session classification must be boolean")
    role = _optional_string(row.get("session_role"))
    return SleepSessionView(
        start_time=start.astimezone(timezone(timedelta(seconds=start_offset))),
        end_time=end.astimezone(timezone(timedelta(seconds=end_offset))),
        is_main=(
            role == "MAIN" or is_main or "MAIN_SLEEP_INFERRED" in row.get("quality_flags", ())
        ),
        is_nap=role == "NAP" or is_nap,
        minutes_asleep=_optional_number(row.get("value_num")),
        role_source=_optional_string(row.get("session_role_source")),
        processed=_optional_bool(row.get("session_processed")),
        stages_status=_optional_string(row.get("session_stages_status")),
        manually_edited=_optional_bool(row.get("session_manually_edited")),
    )


def _required_aware_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"sleep session {field} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"sleep session {field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"sleep session {field} must be timezone-aware")
    return parsed


def _required_utc_offset(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("sleep session UTC offset must be an integer")
    if abs(value) > 18 * 60 * 60:
        raise ValueError("sleep session UTC offset is outside the supported range")
    return cast(int, value)


def _optional_number(value: Any) -> int | float | None:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return cast(int | float, value)


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_aware_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None
