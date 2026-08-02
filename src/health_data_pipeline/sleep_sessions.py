"""Normalize Google Health sleep sessions without losing their time boundaries."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

_MAX_UTC_OFFSET_SECONDS = 24 * 60 * 60


class SleepSessionValidationError(ValueError):
    """A set of reconciled sleep points cannot be resolved deterministically."""


@dataclass(frozen=True, slots=True)
class SleepSession:
    """One valid sleep interval, with physical times normalized to UTC.

    ``is_main`` identifies the one explicitly marked main sleep selected for the
    metric date. ``main_inferred`` identifies the deterministic fallback when
    Google Health did not mark any session as main. ``is_nap`` preserves the API
    nap flag even when the same malformed source session is also selected as main.
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


@dataclass(frozen=True, slots=True)
class _Candidate:
    point: Mapping[str, Any]
    dimension_key: str
    metric_date: date
    start_time: datetime
    end_time: datetime
    start_utc_offset_seconds: int
    end_utc_offset_seconds: int
    explicit_main: bool
    is_nap: bool
    minutes_asleep: Decimal | None
    selected_main: bool = False
    main_inferred: bool = False


@dataclass(frozen=True, slots=True)
class _MainPointCandidate:
    point: Mapping[str, Any]
    metric_date: date
    start_time: datetime | None
    end_time: datetime | None
    explicit_main: bool
    is_nap: bool
    minutes_asleep: Decimal | None
    identity: str


def normalize_sleep_sessions(
    points: Sequence[Mapping[str, Any]],
) -> dict[date, tuple[SleepSession, ...]]:
    """Return valid, reconciled sleep sessions grouped by civil end date.

    Non-sleep and incomplete points are ignored. Conflicting revisions of the
    same ``dataPointName`` fail closed when their ordering is ambiguous.
    """

    selected = _select_main_candidates(_parse_candidates(_resolve_revisions(points)))
    grouped: dict[date, list[SleepSession]] = defaultdict(list)
    for candidate in selected:
        grouped[candidate.metric_date].append(
            SleepSession(
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
            )
        )
    return {
        metric_date: tuple(sorted(sessions, key=_session_sort_key))
        for metric_date, sessions in sorted(grouped.items())
    }


def select_main_sleep_points(
    points: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    """Select exactly one main sleep source point per valid metric date.

    An explicit ``metadata.mainSleep`` wins. Without one, the longest non-nap
    session wins; if every session is a nap, the longest session wins. Ties are
    broken by physical interval and the hashed data-point identity.
    """

    candidates = _parse_main_point_candidates(_resolve_selection_points(points))
    selected: list[_MainPointCandidate] = []
    by_date: dict[date, list[_MainPointCandidate]] = defaultdict(list)
    for candidate in candidates:
        by_date[candidate.metric_date].append(candidate)
    for metric_date in sorted(by_date):
        sessions = by_date[metric_date]
        pool = [candidate for candidate in sessions if candidate.explicit_main]
        if not pool:
            pool = [candidate for candidate in sessions if not candidate.is_nap]
        if not pool:
            pool = sessions
        selected.append(min(pool, key=_main_point_selection_key))
    return tuple(candidate.point for candidate in selected)


def _resolve_selection_points(
    points: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    named = _resolve_revisions(points)
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
    candidates: list[_Candidate] = []
    for point in points:
        candidate = _parse_candidate(point)
        if candidate is not None:
            candidates.append(candidate)
    return tuple(candidates)


def _parse_main_point_candidates(
    points: Sequence[Mapping[str, Any]],
) -> tuple[_MainPointCandidate, ...]:
    candidates: list[_MainPointCandidate] = []
    for point in points:
        sleep = point.get("sleep")
        if not isinstance(sleep, Mapping):
            continue
        interval = sleep.get("interval")
        if not isinstance(interval, Mapping):
            continue
        end_time = _aware_datetime(interval.get("endTime"))
        end_offset = _utc_offset_seconds(interval.get("endUtcOffset"))
        metric_date = _civil_end_date(interval)
        if metric_date is None and end_time is not None and end_offset is not None:
            metric_date = (end_time + timedelta(seconds=end_offset)).date()
        if metric_date is None:
            continue
        metadata = sleep.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        summary = sleep.get("summary")
        summary = summary if isinstance(summary, Mapping) else {}
        candidates.append(
            _MainPointCandidate(
                point=point,
                metric_date=metric_date,
                start_time=_aware_datetime(interval.get("startTime")),
                end_time=end_time,
                explicit_main=metadata.get("mainSleep") is True,
                is_nap=_is_nap(metadata),
                minutes_asleep=_nonnegative_decimal(summary.get("minutesAsleep")),
                identity=_selection_identity(point),
            )
        )
    return tuple(candidates)


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
    return _Candidate(
        point=point,
        dimension_key=_dimension_key(data_point_name),
        metric_date=metric_date,
        start_time=start_time,
        end_time=end_time,
        start_utc_offset_seconds=start_offset,
        end_utc_offset_seconds=end_offset,
        explicit_main=metadata.get("mainSleep") is True,
        is_nap=_is_nap(metadata),
        minutes_asleep=_nonnegative_decimal(summary.get("minutesAsleep")),
    )


def _select_main_candidates(candidates: Sequence[_Candidate]) -> tuple[_Candidate, ...]:
    by_date: dict[date, list[_Candidate]] = defaultdict(list)
    for candidate in candidates:
        by_date[candidate.metric_date].append(candidate)

    selected: list[_Candidate] = []
    for metric_date in sorted(by_date):
        sessions = by_date[metric_date]
        explicit = [candidate for candidate in sessions if candidate.explicit_main]
        inferred = not explicit
        pool = explicit
        if not pool:
            pool = [candidate for candidate in sessions if not candidate.is_nap]
        if not pool:
            pool = sessions
        main = min(pool, key=_main_selection_key)
        selected.extend(
            replace(
                candidate,
                selected_main=candidate is main,
                main_inferred=candidate is main and inferred,
            )
            for candidate in sessions
        )
    return tuple(selected)


def _main_selection_key(candidate: _Candidate) -> tuple[Decimal, datetime, datetime, str]:
    physical_minutes = Decimal(str((candidate.end_time - candidate.start_time).total_seconds()))
    physical_minutes /= Decimal(60)
    effective_minutes = (
        candidate.minutes_asleep if candidate.minutes_asleep is not None else physical_minutes
    )
    return (-effective_minutes, candidate.start_time, candidate.end_time, candidate.dimension_key)


def _session_sort_key(session: SleepSession) -> tuple[datetime, datetime, str]:
    return (session.start_time, session.end_time, session.dimension_key)


def _main_point_selection_key(
    candidate: _MainPointCandidate,
) -> tuple[Decimal, datetime, datetime, str]:
    physical_minutes = Decimal(0)
    if (
        candidate.start_time is not None
        and candidate.end_time is not None
        and candidate.end_time > candidate.start_time
    ):
        physical_minutes = Decimal(
            str((candidate.end_time - candidate.start_time).total_seconds())
        ) / Decimal(60)
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
    if seconds != seconds.to_integral_value() or abs(seconds) >= _MAX_UTC_OFFSET_SECONDS:
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


def _is_nap(metadata: Mapping[str, Any]) -> bool:
    if metadata.get("nap") is True:
        return True
    stages_status = metadata.get("stagesStatus")
    if not isinstance(stages_status, str):
        return False
    normalized = stages_status.upper()
    for prefix in ("SLEEP_STAGES_STATUS_", "SLEEP_STAGE_STATUS_"):
        normalized = normalized.removeprefix(prefix)
    return normalized == "REJECTED_NAP"


def _update_time(point: Mapping[str, Any]) -> datetime | None:
    sleep = point.get("sleep")
    nested = sleep.get("updateTime") if isinstance(sleep, Mapping) else None
    return _aware_datetime(nested if nested is not None else point.get("updateTime"))


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
