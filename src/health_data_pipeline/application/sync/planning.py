"""Pure acquisition planning for webhooks, reconciliation, and backfill."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import JsonValue

from health_data_pipeline import data_types as health_catalog
from health_data_pipeline.models import RootSnapshot

SUPPORTED_DATA_TYPES = health_catalog.SUPPORTED_DATA_TYPES
_BACKFILL_BATCH_DAYS = 31
_LEGACY_BACKFILL_BATCH_DAYS = 14
_BACKFILL_EARLIEST_DATE = date(2009, 1, 1)
_BACKFILL_HIGH_FREQUENCY_DAYS = 90
_RECONCILIATION_MAX_DAYS = 90
_BACKFILL_TIME_ZONE = ZoneInfo("Asia/Tokyo")
_BACKFILL_META_KEY = "_schedule"
_BACKFILL_POLICY_VERSION = 2
_NORMALIZER_VERSION = "daily-v5"
_PRIORITY_BACKFILL_DATA_TYPES = health_catalog.PRIORITY_BACKFILL_DATA_TYPES
_HIGH_FREQUENCY_DATA_TYPES = health_catalog.HIGH_FREQUENCY_DATA_TYPES


@dataclass(frozen=True, slots=True)
class PendingWebhook:
    """One durable webhook and all civil dates it may have changed."""

    object_name: str
    data_type: str
    dates: tuple[date, ...]


@dataclass(frozen=True, slots=True)
class BackfillChunk:
    """One bounded historical acquisition interval."""

    data_type: str
    dates: tuple[date, ...]
    floor: date
    next_data_type_index: int | None = None


def _parse_webhook_event(
    object_name: str,
    data: bytes,
    *,
    zone: ZoneInfo,
) -> PendingWebhook | None:
    try:
        value = json.loads(data)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, Mapping):
        return None
    raw_event = value.get("data", value.get("event", value))
    if not isinstance(raw_event, Mapping):
        return None
    data_type = raw_event.get("dataType")
    if not isinstance(data_type, str):
        return None
    dates: set[date] = set()
    raw_intervals = raw_event.get("intervals", ())
    if isinstance(raw_intervals, Sequence) and not isinstance(
        raw_intervals, (str, bytes, bytearray)
    ):
        for interval in raw_intervals:
            if not isinstance(interval, Mapping):
                continue
            dates.update(_interval_dates(interval, zone=zone))
    return PendingWebhook(
        object_name=object_name,
        data_type=data_type,
        dates=tuple(sorted(dates)),
    )


def _select_backfill_chunk(
    cursors: Mapping[str, JsonValue],
    *,
    today: date,
) -> BackfillChunk | None:
    """Return the first chunk selected by the current multi-type policy."""

    chunks = _select_backfill_chunks(cursors, today=today)
    return chunks[0] if chunks else None


def _select_backfill_chunks(
    cursors: Mapping[str, JsonValue],
    *,
    today: date,
) -> tuple[BackfillChunk, ...]:
    """Select the same recent-first 31-day interval for sleep and exercise.

    The two session types share one HEAD commit, so one historical day cannot
    become current for sleep without the matching exercise replacement. The
    Class A monthly gate is evaluated separately before this selector runs.
    """

    schedule = cursors.get(_BACKFILL_META_KEY)
    schedule_mapping = schedule if isinstance(schedule, Mapping) else {}
    migration_pending = (
        schedule_mapping.get("policy_version") != _BACKFILL_POLICY_VERSION
        or schedule_mapping.get("normalizer_version") != _NORMALIZER_VERSION
    )
    newest_default = today
    floor = _BACKFILL_EARLIEST_DATE
    next_ends: list[date] = []
    all_complete = True
    for data_type in _PRIORITY_BACKFILL_DATA_TYPES:
        cursor = cursors.get(data_type)
        cursor_mapping = cursor if isinstance(cursor, Mapping) else {}
        complete = cursor_mapping.get("complete") is True
        all_complete = all_complete and complete
        if migration_pending:
            next_end = newest_default
        else:
            next_end = _iso_date(cursor_mapping.get("next_end_date")) or newest_default
        next_end = min(next_end, newest_default)
        if not complete or migration_pending:
            next_ends.append(next_end)
    if all_complete and not migration_pending:
        if schedule_mapping.get("last_completed_civil_date") == today.isoformat():
            return ()
        legacy = _select_legacy_backfill_chunk(
            cursors,
            schedule=schedule_mapping,
            today=today,
        )
        return () if legacy is None else (legacy,)

    # Re-fetching an already-covered partner interval is safe and ensures a
    # migrated or previously divergent cursor never skips either session type.
    common_end = max(next_ends, default=newest_default)
    if common_end < floor:
        return ()
    common_start = max(
        floor,
        common_end - timedelta(days=_BACKFILL_BATCH_DAYS - 1),
    )
    dates = tuple(_inclusive_dates(common_start, common_end))
    return tuple(
        BackfillChunk(
            data_type=data_type,
            dates=dates,
            floor=floor,
        )
        for data_type in _PRIORITY_BACKFILL_DATA_TYPES
    )


def _select_legacy_backfill_chunk(
    cursors: Mapping[str, JsonValue],
    *,
    schedule: Mapping[str, object],
    today: date,
) -> BackfillChunk | None:
    """Continue the former round-robin after priority session history completes."""

    raw_index = schedule.get("next_data_type_index", 0)
    start_index = (
        raw_index
        if isinstance(raw_index, int)
        and not isinstance(raw_index, bool)
        and 0 <= raw_index < len(SUPPORTED_DATA_TYPES)
        else 0
    )
    newest_default = today
    for offset in range(len(SUPPORTED_DATA_TYPES)):
        index = (start_index + offset) % len(SUPPORTED_DATA_TYPES)
        data_type = SUPPORTED_DATA_TYPES[index]
        floor = _backfill_floor(data_type, today=today)
        cursor = cursors.get(data_type)
        cursor_mapping = cursor if isinstance(cursor, Mapping) else {}
        if cursor_mapping.get("complete") is True:
            continue
        next_end = _iso_date(cursor_mapping.get("next_end_date")) or newest_default
        next_end = min(next_end, newest_default)
        if next_end < floor:
            continue
        chunk_start = max(
            floor,
            next_end - timedelta(days=_LEGACY_BACKFILL_BATCH_DAYS - 1),
        )
        return BackfillChunk(
            data_type=data_type,
            dates=tuple(_inclusive_dates(chunk_start, next_end)),
            floor=floor,
            next_data_type_index=(index + 1) % len(SUPPORTED_DATA_TYPES),
        )
    return None


def _advance_backfill_cursors(
    cursors: Mapping[str, JsonValue],
    chunk: BackfillChunk,
    *,
    completed_on: date,
) -> dict[str, JsonValue]:
    return _advance_backfill_cursors_many(
        cursors,
        (chunk,),
        completed_on=completed_on,
    )


def _advance_backfill_cursors_many(
    cursors: Mapping[str, JsonValue],
    chunks: Sequence[BackfillChunk],
    *,
    completed_on: date,
) -> dict[str, JsonValue]:
    if not chunks:
        raise ValueError("backfill cursor advancement requires at least one chunk")
    result = dict(cursors)
    for chunk in chunks:
        oldest = min(chunk.dates)
        complete = oldest <= chunk.floor
        result[chunk.data_type] = cast(
            JsonValue,
            {
                "next_end_date": (oldest - timedelta(days=1)).isoformat(),
                "complete": complete,
            },
        )
    result[_BACKFILL_META_KEY] = cast(
        JsonValue,
        {
            "last_completed_civil_date": completed_on.isoformat(),
            "normalizer_version": _NORMALIZER_VERSION,
            "next_data_type_index": next(
                (
                    chunk.next_data_type_index
                    for chunk in chunks
                    if chunk.next_data_type_index is not None
                ),
                _backfill_schedule_index(cursors),
            ),
            "policy_version": _BACKFILL_POLICY_VERSION,
        },
    )
    return result


def _backfill_schedule_index(cursors: Mapping[str, JsonValue]) -> int:
    raw_schedule = cursors.get(_BACKFILL_META_KEY)
    schedule = raw_schedule if isinstance(raw_schedule, Mapping) else {}
    raw_index = schedule.get("next_data_type_index", 0)
    if (
        isinstance(raw_index, int)
        and not isinstance(raw_index, bool)
        and 0 <= raw_index < len(SUPPORTED_DATA_TYPES)
    ):
        return raw_index
    return 0


def _backfill_floor(data_type: str, *, today: date) -> date:
    if data_type in _HIGH_FREQUENCY_DATA_TYPES:
        return today - timedelta(days=_BACKFILL_HIGH_FREQUENCY_DAYS - 1)
    return _BACKFILL_EARLIEST_DATE


def _plan_fetch_dates(
    root: RootSnapshot | None,
    pending: Sequence[PendingWebhook],
    *,
    sync_advanced: bool,
    today: date,
    zone: ZoneInfo,
    backfill_chunks: Sequence[BackfillChunk],
) -> tuple[dict[str, set[date]], set[str], dict[str, date]]:
    fetch_dates: dict[str, set[date]] = defaultdict(set)
    for event in pending:
        if event.data_type in health_catalog.METRICS_BY_DATA_TYPE:
            event_dates = event.dates or (today,)
            for data_type in _webhook_replacement_data_types(event.data_type):
                fetch_dates[data_type].update(event_dates)
    reconciled_data_types: set[str] = set()
    truncated: dict[str, date] = {}
    if sync_advanced or root is None:
        for data_type in SUPPORTED_DATA_TYPES:
            requested_start = _reconciliation_start_date(
                root,
                data_type,
                today=today,
                zone=zone,
            )
            fetch_start = _reconciliation_fetch_start(
                data_type,
                requested_start=requested_start,
                today=today,
            )
            fetch_dates[data_type].update(_inclusive_dates(fetch_start, today))
            reconciled_data_types.add(data_type)
            if data_type not in _HIGH_FREQUENCY_DATA_TYPES and requested_start < fetch_start:
                truncated[data_type] = fetch_start - timedelta(days=1)
    elif backfill_chunks:
        for chunk in backfill_chunks:
            fetch_dates[chunk.data_type].update(chunk.dates)
    return fetch_dates, reconciled_data_types, truncated


def _webhook_replacement_data_types(data_type: str) -> tuple[str, ...]:
    """Return every API representation needed to replace webhook metrics safely.

    HRV and SpO2 each have detail and daily-summary data types which normalize to
    one metric. Re-fetching only the notified representation could discard the
    still-current sibling value, so both representations share one replacement
    set for the affected civil dates.
    """

    metrics = health_catalog.METRICS_BY_DATA_TYPE.get(data_type, frozenset())
    return tuple(
        candidate
        for candidate in SUPPORTED_DATA_TYPES
        if metrics & health_catalog.METRICS_BY_DATA_TYPE[candidate]
    )


def _reconciliation_start_date(
    root: RootSnapshot | None,
    data_type: str,
    *,
    today: date,
    zone: ZoneInfo,
) -> date:
    """Use the last successful civil cursor, with Fitbit sync as migration fallback."""

    if root is None:
        return today - timedelta(days=1)
    cursor = root.state.data_type_cursors.get(data_type)
    cursor_mapping = cursor if isinstance(cursor, Mapping) else {}
    cursor_date = _iso_date(cursor_mapping.get("reconciled_through_date"))
    if cursor_date is None:
        cursor_date = _timestamp_civil_date(
            cursor_mapping.get("reconciled_at", cursor_mapping.get("reconciled_through")),
            zone=zone,
        )
    if cursor_date is None and root.state.fitbit_last_sync_time is not None:
        cursor_date = root.state.fitbit_last_sync_time.astimezone(zone).date()
    return min(cursor_date or (today - timedelta(days=1)), today)


def _reconciliation_fetch_start(
    data_type: str,
    *,
    requested_start: date,
    today: date,
) -> date:
    """Bound foreground reconciliation while preserving the 90-day detail guarantee."""

    return max(
        min(requested_start, today),
        today - timedelta(days=_RECONCILIATION_MAX_DAYS - 1),
        _backfill_floor(data_type, today=today),
    )


def _reopen_backfill_gaps(
    cursors: Mapping[str, JsonValue],
    gap_ends: Mapping[str, date],
) -> dict[str, JsonValue]:
    """Return truncated non-high-frequency ranges to the normal backfill rotation."""

    result = dict(cursors)
    for data_type, gap_end in gap_ends.items():
        result[data_type] = cast(
            JsonValue,
            {
                "next_end_date": gap_end.isoformat(),
                "complete": False,
            },
        )
    return result


def _next_backfill_cursors(
    root: RootSnapshot | None,
    backfill_chunks: Sequence[BackfillChunk],
    truncated_reconciliation: Mapping[str, date],
    *,
    completed_on: date,
) -> Mapping[str, JsonValue] | None:
    if backfill_chunks and root is not None:
        return _advance_backfill_cursors_many(
            root.state.backfill_cursors,
            backfill_chunks,
            completed_on=completed_on,
        )
    if truncated_reconciliation:
        previous = root.state.backfill_cursors if root is not None else {}
        return _reopen_backfill_gaps(previous, truncated_reconciliation)
    return None


def _inclusive_dates(start: date, end: date) -> Iterable[date]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def _iso_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _timestamp_civil_date(value: Any, *, zone: ZoneInfo) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(zone).date()


def _interval_dates(
    value: Mapping[str, Any],
    *,
    zone: ZoneInfo,
) -> set[date]:
    result: set[date] = set()
    civil = value.get("civilDateTimeInterval")
    if isinstance(civil, Mapping):
        civil_start = _civil_edge_date(civil.get("startDateTime"))
        civil_end = _civil_edge_date(civil.get("endDateTime"))
        result.update(_date_range(civil_start, civil_end))
    physical = value.get("physicalTimeInterval")
    if isinstance(physical, Mapping):
        physical_start = _physical_edge_time(physical.get("startTime"))
        physical_end = _physical_edge_time(physical.get("endTime"))
        if physical_start is not None:
            local_start = physical_start.astimezone(zone).date()
            local_end = (
                (physical_end - timedelta(microseconds=1)).astimezone(zone).date()
                if physical_end is not None and physical_end > physical_start
                else local_start
            )
            result.update(_date_range(local_start, local_end))
    return result


def _civil_edge_date(value: Any) -> date | None:
    if not isinstance(value, Mapping):
        return None
    raw_date = value.get("date")
    if not isinstance(raw_date, Mapping):
        return None
    try:
        return date(
            int(raw_date["year"]),
            int(raw_date["month"]),
            int(raw_date["day"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _physical_edge_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _date_range(start: date | None, end: date | None) -> set[date]:
    if start is None:
        return set()
    final = start if end is None or end < start else end
    values: set[date] = set()
    cursor = start
    while cursor <= final:
        values.add(cursor)
        cursor += timedelta(days=1)
    return values


def _time_zone(settings: Mapping[str, Any], default: str) -> tuple[str, str]:
    value = settings.get("timeZone")
    if not isinstance(value, str):
        return default, "CONFIG_DEFAULT"
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError:
        return default, "CONFIG_DEFAULT"
    return value, "GOOGLE_HEALTH_SETTINGS"
