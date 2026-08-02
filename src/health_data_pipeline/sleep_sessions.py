"""Resolve Google Health sleep data into one aggregate source and durable sessions."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

_MAX_UTC_OFFSET_SECONDS = 18 * 60 * 60
_KNOWN_SLEEP_STAGES = frozenset({"AWAKE", "LIGHT", "DEEP", "REM", "ASLEEP", "RESTLESS"})


class SleepSessionValidationError(ValueError):
    """A set of reconciled sleep points cannot be resolved deterministically."""


class SleepSessionRole(StrEnum):
    """Resolved role of one materializable sleep session."""

    MAIN = "MAIN"
    NAP = "NAP"
    UNCLASSIFIED = "UNCLASSIFIED"


class SleepSessionRoleSource(StrEnum):
    """How the resolved sleep-session role was obtained."""

    SOURCE_FLAG = "SOURCE_FLAG"
    INFERRED = "INFERRED"
    UNCLASSIFIED = "UNCLASSIFIED"


class SleepResolutionDiagnosticCode(StrEnum):
    """Stable diagnostic codes available to ingestion and persistence layers."""

    INCOMPLETE_EXPLICIT_MAIN_IGNORED = "INCOMPLETE_EXPLICIT_MAIN_IGNORED"
    INCOMPLETE_SLEEP_SESSION_IGNORED = "INCOMPLETE_SLEEP_SESSION_IGNORED"
    LEGACY_SLEEP_SUMMARY_FALLBACK = "LEGACY_SLEEP_SUMMARY_FALLBACK"
    MAIN_SLEEP_INFERRED = "MAIN_SLEEP_INFERRED"


@dataclass(frozen=True, slots=True)
class SleepStageSummary:
    """Source-reported duration and segment count for one known sleep stage."""

    stage_type: str
    minutes: Decimal | None
    count: int | None


@dataclass(frozen=True, slots=True)
class SleepSession:
    """One valid sleep interval, with physical times normalized to UTC.

    The legacy ``is_main``, ``is_nap`` and ``main_inferred`` fields retain their
    existing meaning. ``role`` is the complete resolved classification: an
    inferred main therefore has ``role=MAIN``, ``is_main=False`` and
    ``main_inferred=True``.
    """

    dimension_key: str
    metric_date: date
    start_time: datetime
    end_time: datetime
    start_utc_offset_seconds: int
    end_utc_offset_seconds: int
    is_main: bool
    is_nap: bool
    main_inferred: bool
    minutes_asleep: Decimal | None
    role: SleepSessionRole = SleepSessionRole.UNCLASSIFIED
    role_source: SleepSessionRoleSource = SleepSessionRoleSource.UNCLASSIFIED
    processed: bool | None = None
    stages_status: str | None = None
    manually_edited: bool | None = None
    sleep_type: str | None = None
    create_time: datetime | None = None
    update_time: datetime | None = None
    minutes_in_sleep_period: Decimal | None = None
    minutes_awake: Decimal | None = None
    minutes_to_fall_asleep: Decimal | None = None
    minutes_after_wake_up: Decimal | None = None
    stage_summaries: tuple[SleepStageSummary, ...] = ()


@dataclass(frozen=True, slots=True)
class SleepResolutionDiagnostic:
    """One non-fatal resolution decision suitable for a manifest or quality flag."""

    metric_date: date
    code: SleepResolutionDiagnosticCode
    source_identity: str | None = None


@dataclass(frozen=True, slots=True)
class SleepDayResolution:
    """The shared aggregate and persistence decision for one civil end date."""

    metric_date: date
    aggregate_point: Mapping[str, Any] | None
    sessions: tuple[SleepSession, ...]
    quality_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SleepResolution:
    """Shared, deterministic result consumed by aggregate and session adapters."""

    days: tuple[SleepDayResolution, ...]
    diagnostics: tuple[SleepResolutionDiagnostic, ...] = ()

    @property
    def aggregate_points(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(day.aggregate_point for day in self.days if day.aggregate_point is not None)

    @property
    def sessions_by_date(self) -> dict[date, tuple[SleepSession, ...]]:
        return {day.metric_date: day.sessions for day in self.days if day.sessions}

    @property
    def quality_flags_by_date(self) -> dict[date, tuple[str, ...]]:
        return {day.metric_date: day.quality_flags for day in self.days if day.quality_flags}


@dataclass(frozen=True, slots=True)
class _Candidate:
    point: Mapping[str, Any]
    dimension_key: str
    identity: str
    metric_date: date
    start_time: datetime
    end_time: datetime
    start_utc_offset_seconds: int
    end_utc_offset_seconds: int
    explicit_main: bool
    is_nap: bool
    nap_inferred: bool
    minutes_asleep: Decimal | None
    processed: bool | None
    stages_status: str | None
    manually_edited: bool | None
    sleep_type: str | None
    create_time: datetime | None
    update_time: datetime | None
    minutes_in_sleep_period: Decimal | None
    minutes_awake: Decimal | None
    minutes_to_fall_asleep: Decimal | None
    minutes_after_wake_up: Decimal | None
    stage_summaries: tuple[SleepStageSummary, ...]
    selected_main: bool = False
    main_inferred: bool = False


@dataclass(frozen=True, slots=True)
class _AggregateCandidate:
    point: Mapping[str, Any]
    metric_date: date
    start_time: datetime | None
    end_time: datetime | None
    explicit_main: bool
    is_nap: bool
    minutes_asleep: Decimal | None
    identity: str


def resolve_sleep_data(points: Sequence[Mapping[str, Any]]) -> SleepResolution:
    """Resolve the same main source for daily aggregates and durable sessions.

    Materializable sessions always take precedence. A summary-only legacy point
    is eligible only on a date with no materializable session, preventing an
    incomplete explicit-main point from driving aggregates while a different
    session is persisted as main.
    """

    resolved_named = _resolve_revisions(points)
    selection_points = _with_anonymous_points(points, resolved_named)
    aggregate_candidates = _parse_aggregate_candidates(selection_points)
    materializable = _parse_candidates(resolved_named)

    aggregate_by_date: dict[date, list[_AggregateCandidate]] = defaultdict(list)
    for aggregate_candidate in aggregate_candidates:
        aggregate_by_date[aggregate_candidate.metric_date].append(aggregate_candidate)
    sessions_by_date: dict[date, list[_Candidate]] = defaultdict(list)
    for session_candidate in materializable:
        sessions_by_date[session_candidate.metric_date].append(session_candidate)

    days: list[SleepDayResolution] = []
    diagnostics: list[SleepResolutionDiagnostic] = []
    for metric_date in sorted(set(aggregate_by_date) | set(sessions_by_date)):
        complete = sessions_by_date.get(metric_date, [])
        all_candidates = aggregate_by_date.get(metric_date, [])
        flags: set[str] = set()
        if complete:
            selected = _select_main_candidates(complete)
            main = next(candidate for candidate in selected if candidate.selected_main)
            sessions = tuple(
                sorted((_to_sleep_session(item) for item in selected), key=_session_sort_key)
            )
            complete_identities = {candidate.identity for candidate in complete}
            incomplete = [
                candidate
                for candidate in all_candidates
                if candidate.identity not in complete_identities
            ]
            for candidate in incomplete:
                code = (
                    SleepResolutionDiagnosticCode.INCOMPLETE_EXPLICIT_MAIN_IGNORED
                    if candidate.explicit_main
                    else SleepResolutionDiagnosticCode.INCOMPLETE_SLEEP_SESSION_IGNORED
                )
                flags.add(code.value)
                diagnostics.append(SleepResolutionDiagnostic(metric_date, code, candidate.identity))
            if main.main_inferred:
                flags.add(SleepResolutionDiagnosticCode.MAIN_SLEEP_INFERRED.value)
                diagnostics.append(
                    SleepResolutionDiagnostic(
                        metric_date,
                        SleepResolutionDiagnosticCode.MAIN_SLEEP_INFERRED,
                        main.identity,
                    )
                )
            days.append(
                SleepDayResolution(
                    metric_date=metric_date,
                    aggregate_point=main.point,
                    sessions=sessions,
                    quality_flags=tuple(sorted(flags)),
                )
            )
            continue

        if not all_candidates:
            continue
        aggregate_main = _select_aggregate_main(all_candidates)
        flags.add(SleepResolutionDiagnosticCode.LEGACY_SLEEP_SUMMARY_FALLBACK.value)
        diagnostics.append(
            SleepResolutionDiagnostic(
                metric_date,
                SleepResolutionDiagnosticCode.LEGACY_SLEEP_SUMMARY_FALLBACK,
                aggregate_main.identity,
            )
        )
        if not aggregate_main.explicit_main:
            flags.add(SleepResolutionDiagnosticCode.MAIN_SLEEP_INFERRED.value)
            diagnostics.append(
                SleepResolutionDiagnostic(
                    metric_date,
                    SleepResolutionDiagnosticCode.MAIN_SLEEP_INFERRED,
                    aggregate_main.identity,
                )
            )
        days.append(
            SleepDayResolution(
                metric_date=metric_date,
                aggregate_point=aggregate_main.point,
                sessions=(),
                quality_flags=tuple(sorted(flags)),
            )
        )

    return SleepResolution(days=tuple(days), diagnostics=tuple(diagnostics))


def reconcile_sleep_points(
    points: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    """Return the deterministic current revision of each sleep source point.

    API page payloads remain immutable elsewhere.  This flattened working set
    keeps only the latest named revision and removes exact anonymous pagination
    overlap so historical source revisions are not materialized twice.
    """

    named = _resolve_revisions(points)
    anonymous_by_signature: dict[bytes, Mapping[str, Any]] = {}
    for point in points:
        if not isinstance(point.get("sleep"), Mapping):
            continue
        data_point_name = point.get("dataPointName")
        if isinstance(data_point_name, str) and data_point_name:
            continue
        signature = _complete_point_signature(point)
        anonymous_by_signature.setdefault(signature, point)
    return tuple(
        sorted(
            (*named, *anonymous_by_signature.values()),
            key=_complete_point_signature,
        )
    )


def normalize_sleep_sessions(
    points: Sequence[Mapping[str, Any]],
    *,
    resolution: SleepResolution | None = None,
) -> dict[date, tuple[SleepSession, ...]]:
    """Return valid, reconciled sleep sessions grouped by civil end date."""

    resolved = resolution or resolve_sleep_data(points)
    return resolved.sessions_by_date


def select_main_sleep_points(
    points: Sequence[Mapping[str, Any]],
    *,
    resolution: SleepResolution | None = None,
) -> tuple[Mapping[str, Any], ...]:
    """Return the aggregate point chosen by the shared sleep resolver per date."""

    resolved = resolution or resolve_sleep_data(points)
    return resolved.aggregate_points


def _with_anonymous_points(
    points: Sequence[Mapping[str, Any]],
    named: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    anonymous = [
        point
        for point in points
        if isinstance(point.get("sleep"), Mapping)
        and (not isinstance(point.get("dataPointName"), str) or not point["dataPointName"])
    ]
    return (*named, *sorted(anonymous, key=_complete_point_signature))


def _resolve_revisions(
    points: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    by_name: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for point in points:
        if not isinstance(point.get("sleep"), Mapping):
            continue
        data_point_name = point.get("dataPointName")
        if not isinstance(data_point_name, str) or not data_point_name:
            continue
        by_name[data_point_name].append(point)

    return tuple(_resolve_named_revisions(by_name[name]) for name in sorted(by_name))


def _resolve_named_revisions(
    points: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    by_payload: dict[bytes, list[Mapping[str, Any]]] = defaultdict(list)
    for point in points:
        by_payload[_sleep_payload_signature(point)].append(point)

    if len(by_payload) == 1:
        timestamped = [(_update_time(point), point) for point in points]
        available_times = [updated_at for updated_at, _ in timestamped if updated_at is not None]
        if available_times:
            latest_time = max(available_times)
            return min(
                (point for updated_at, point in timestamped if updated_at == latest_time),
                key=_complete_point_signature,
            )
        return min(points, key=_complete_point_signature)

    revisions: list[tuple[datetime, bytes, Mapping[str, Any]]] = []
    for payload_signature, matching in by_payload.items():
        for point in matching:
            updated_at = _update_time(point)
            if updated_at is None:
                raise SleepSessionValidationError(
                    "different revisions of a sleep data point require a valid updateTime"
                )
            revisions.append((updated_at, payload_signature, point))

    latest_time = max(updated_at for updated_at, _, _ in revisions)
    latest = [item for item in revisions if item[0] == latest_time]
    latest_payloads = {payload_signature for _, payload_signature, _ in latest}
    if len(latest_payloads) != 1:
        raise SleepSessionValidationError(
            "different sleep data point revisions have the same latest updateTime"
        )
    return min((point for _, _, point in latest), key=_complete_point_signature)


def _parse_candidates(points: Sequence[Mapping[str, Any]]) -> tuple[_Candidate, ...]:
    return tuple(
        candidate for point in points if (candidate := _parse_candidate(point)) is not None
    )


def _parse_aggregate_candidates(
    points: Sequence[Mapping[str, Any]],
) -> tuple[_AggregateCandidate, ...]:
    return tuple(
        candidate
        for point in points
        if (candidate := _parse_aggregate_candidate(point)) is not None
    )


def _parse_aggregate_candidate(point: Mapping[str, Any]) -> _AggregateCandidate | None:
    sleep = point.get("sleep")
    if not isinstance(sleep, Mapping):
        return None
    interval = sleep.get("interval")
    if not isinstance(interval, Mapping):
        return None
    end_time = _aware_datetime(interval.get("endTime"))
    end_offset = _utc_offset_seconds(interval.get("endUtcOffset"))
    metric_date = _civil_end_date(interval)
    if metric_date is None and end_time is not None and end_offset is not None:
        metric_date = (end_time + timedelta(seconds=end_offset)).date()
    if metric_date is None:
        return None
    metadata = sleep.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    summary = sleep.get("summary")
    summary = summary if isinstance(summary, Mapping) else {}
    return _AggregateCandidate(
        point=point,
        metric_date=metric_date,
        start_time=_aware_datetime(interval.get("startTime")),
        end_time=end_time,
        explicit_main=metadata.get("mainSleep") is True,
        is_nap=_is_nap(metadata)[0],
        minutes_asleep=_nonnegative_decimal(summary.get("minutesAsleep")),
        identity=_selection_identity(point),
    )


def _parse_candidate(point: Mapping[str, Any]) -> _Candidate | None:
    data_point_name = point.get("dataPointName")
    sleep = point.get("sleep")
    if not isinstance(data_point_name, str) or not data_point_name:
        return None
    if not isinstance(sleep, Mapping):
        return None
    interval = sleep.get("interval")
    if not isinstance(interval, Mapping):
        return None

    start_time = _aware_datetime(interval.get("startTime"))
    end_time = _aware_datetime(interval.get("endTime"))
    start_offset = _utc_offset_seconds(interval.get("startUtcOffset"))
    end_offset = _utc_offset_seconds(interval.get("endUtcOffset"))
    if (
        start_time is None
        or end_time is None
        or start_offset is None
        or end_offset is None
        or end_time <= start_time
    ):
        return None

    metric_date = _civil_end_date(interval)
    if metric_date is None:
        metric_date = (end_time + timedelta(seconds=end_offset)).date()
    metadata = sleep.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    summary = sleep.get("summary")
    summary = summary if isinstance(summary, Mapping) else {}
    is_nap, nap_inferred = _is_nap(metadata)
    identity = _dimension_key(data_point_name)
    return _Candidate(
        point=point,
        dimension_key=identity,
        identity=identity,
        metric_date=metric_date,
        start_time=start_time,
        end_time=end_time,
        start_utc_offset_seconds=start_offset,
        end_utc_offset_seconds=end_offset,
        explicit_main=metadata.get("mainSleep") is True,
        is_nap=is_nap,
        nap_inferred=nap_inferred,
        minutes_asleep=_nonnegative_decimal(summary.get("minutesAsleep")),
        processed=_optional_bool(metadata.get("processed")),
        stages_status=_optional_string(metadata.get("stagesStatus")),
        manually_edited=_optional_bool(metadata.get("manuallyEdited")),
        sleep_type=_optional_string(sleep.get("type")),
        create_time=_source_time(point, sleep, "createTime"),
        update_time=_source_time(point, sleep, "updateTime"),
        minutes_in_sleep_period=_nonnegative_decimal(summary.get("minutesInSleepPeriod")),
        minutes_awake=_nonnegative_decimal(summary.get("minutesAwake")),
        minutes_to_fall_asleep=_nonnegative_decimal(summary.get("minutesToFallAsleep")),
        minutes_after_wake_up=_nonnegative_decimal(summary.get("minutesAfterWakeUp")),
        stage_summaries=_stage_summaries(summary.get("stagesSummary")),
    )


def _select_main_candidates(candidates: Sequence[_Candidate]) -> tuple[_Candidate, ...]:
    explicit = [candidate for candidate in candidates if candidate.explicit_main]
    inferred = not explicit
    pool = (
        explicit
        or [candidate for candidate in candidates if not candidate.is_nap]
        or list(candidates)
    )
    main = min(pool, key=_main_selection_key)
    return tuple(
        replace(
            candidate,
            selected_main=candidate is main,
            main_inferred=candidate is main and inferred,
        )
        for candidate in candidates
    )


def _select_aggregate_main(candidates: Sequence[_AggregateCandidate]) -> _AggregateCandidate:
    explicit = [candidate for candidate in candidates if candidate.explicit_main]
    pool = (
        explicit
        or [candidate for candidate in candidates if not candidate.is_nap]
        or list(candidates)
    )
    return min(pool, key=_aggregate_selection_key)


def _to_sleep_session(candidate: _Candidate) -> SleepSession:
    if candidate.selected_main:
        role = SleepSessionRole.MAIN
        role_source = (
            SleepSessionRoleSource.INFERRED
            if candidate.main_inferred
            else SleepSessionRoleSource.SOURCE_FLAG
        )
    elif candidate.is_nap:
        role = SleepSessionRole.NAP
        role_source = (
            SleepSessionRoleSource.INFERRED
            if candidate.nap_inferred
            else SleepSessionRoleSource.SOURCE_FLAG
        )
    else:
        role = SleepSessionRole.UNCLASSIFIED
        role_source = SleepSessionRoleSource.UNCLASSIFIED
    return SleepSession(
        dimension_key=candidate.dimension_key,
        metric_date=candidate.metric_date,
        start_time=candidate.start_time,
        end_time=candidate.end_time,
        start_utc_offset_seconds=candidate.start_utc_offset_seconds,
        end_utc_offset_seconds=candidate.end_utc_offset_seconds,
        is_main=candidate.selected_main and candidate.explicit_main,
        is_nap=candidate.is_nap,
        main_inferred=candidate.main_inferred,
        minutes_asleep=candidate.minutes_asleep,
        role=role,
        role_source=role_source,
        processed=candidate.processed,
        stages_status=candidate.stages_status,
        manually_edited=candidate.manually_edited,
        sleep_type=candidate.sleep_type,
        create_time=candidate.create_time,
        update_time=candidate.update_time,
        minutes_in_sleep_period=candidate.minutes_in_sleep_period,
        minutes_awake=candidate.minutes_awake,
        minutes_to_fall_asleep=candidate.minutes_to_fall_asleep,
        minutes_after_wake_up=candidate.minutes_after_wake_up,
        stage_summaries=candidate.stage_summaries,
    )


def _main_selection_key(candidate: _Candidate) -> tuple[Decimal, datetime, datetime, str]:
    effective_minutes = (
        candidate.minutes_asleep
        if candidate.minutes_asleep is not None
        else _physical_minutes(candidate.start_time, candidate.end_time)
    )
    return (-effective_minutes, candidate.start_time, candidate.end_time, candidate.identity)


def _aggregate_selection_key(
    candidate: _AggregateCandidate,
) -> tuple[Decimal, datetime, datetime, str]:
    physical_minutes = (
        _physical_minutes(candidate.start_time, candidate.end_time)
        if candidate.start_time is not None
        and candidate.end_time is not None
        and candidate.end_time > candidate.start_time
        else Decimal(0)
    )
    effective_minutes = (
        candidate.minutes_asleep if candidate.minutes_asleep is not None else physical_minutes
    )
    latest = datetime.max.replace(tzinfo=UTC)
    return (
        -effective_minutes,
        candidate.start_time or latest,
        candidate.end_time or latest,
        candidate.identity,
    )


def _physical_minutes(start: datetime, end: datetime) -> Decimal:
    return Decimal(str((end - start).total_seconds())) / Decimal(60)


def _session_sort_key(session: SleepSession) -> tuple[datetime, datetime, str]:
    return (session.start_time, session.end_time, session.dimension_key)


def _dimension_key(data_point_name: str) -> str:
    digest = hashlib.sha256(data_point_name.encode("utf-8")).hexdigest()
    return f"sleep:{digest}"


def _selection_identity(point: Mapping[str, Any]) -> str:
    data_point_name = point.get("dataPointName")
    if isinstance(data_point_name, str) and data_point_name:
        return _dimension_key(data_point_name)
    return f"anonymous:{hashlib.sha256(_complete_point_signature(point)).hexdigest()}"


def _civil_end_date(interval: Mapping[str, Any]) -> date | None:
    civil = interval.get("civilEndTime")
    if not isinstance(civil, Mapping):
        return None
    value = civil.get("date")
    if not isinstance(value, Mapping):
        return None
    try:
        return date(int(value["year"]), int(value["month"]), int(value["day"]))
    except (KeyError, TypeError, ValueError):
        return None


def _aware_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _utc_offset_seconds(value: Any) -> int | None:
    if not isinstance(value, str) or not value.endswith("s"):
        return None
    try:
        seconds = Decimal(value[:-1])
    except (InvalidOperation, ValueError):
        return None
    if seconds != seconds.to_integral_value() or abs(seconds) > _MAX_UTC_OFFSET_SECONDS:
        return None
    return int(seconds)


def _nonnegative_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() and number >= 0 else None


def _nonnegative_int(value: Any) -> int | None:
    number = _nonnegative_decimal(value)
    if number is None or number != number.to_integral_value():
        return None
    return int(number)


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _is_nap(metadata: Mapping[str, Any]) -> tuple[bool, bool]:
    if metadata.get("nap") is True:
        return True, False
    stages_status = metadata.get("stagesStatus")
    if not isinstance(stages_status, str):
        return False, False
    normalized = stages_status.upper()
    for prefix in ("SLEEP_STAGES_STATUS_", "SLEEP_STAGE_STATUS_"):
        normalized = normalized.removeprefix(prefix)
    return normalized == "REJECTED_NAP", normalized == "REJECTED_NAP"


def _stage_summaries(value: Any) -> tuple[SleepStageSummary, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    candidates: dict[str, set[tuple[Decimal | None, int | None]]] = defaultdict(set)
    for item in value:
        if not isinstance(item, Mapping):
            continue
        stage_type = _known_stage_type(item.get("type"))
        minutes = _nonnegative_decimal(item.get("minutes"))
        count = _nonnegative_int(item.get("count"))
        if stage_type is None or (minutes is None and count is None):
            continue
        candidates[stage_type].add((minutes, count))
    return tuple(
        SleepStageSummary(stage_type, minutes, count)
        for stage_type in sorted(candidates)
        if len(candidates[stage_type]) == 1
        for minutes, count in candidates[stage_type]
    )


def _known_stage_type(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.upper().removeprefix("SLEEP_STAGE_TYPE_")
    return normalized if normalized in _KNOWN_SLEEP_STAGES else None


def _source_time(
    point: Mapping[str, Any],
    sleep: Mapping[str, Any],
    field: str,
) -> datetime | None:
    nested = sleep.get(field)
    return _aware_datetime(nested if nested is not None else point.get(field))


def _update_time(point: Mapping[str, Any]) -> datetime | None:
    sleep = point.get("sleep")
    if not isinstance(sleep, Mapping):
        return _aware_datetime(point.get("updateTime"))
    return _source_time(point, sleep, "updateTime")


def _sleep_payload_signature(point: Mapping[str, Any]) -> bytes:
    sleep = point.get("sleep")
    if not isinstance(sleep, Mapping):  # pragma: no cover - guarded by caller
        raise SleepSessionValidationError("sleep payload must be an object")
    payload = {key: value for key, value in sleep.items() if key != "updateTime"}
    return _json_signature(payload)


def _complete_point_signature(point: Mapping[str, Any]) -> bytes:
    return _json_signature(point)


def _json_signature(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SleepSessionValidationError(
            "sleep data point must contain valid JSON values"
        ) from exc
