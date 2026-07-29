"""Soft Cloud Storage Class A operation policy."""

from __future__ import annotations

import json
import math
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from health_data_pipeline.storage import (
    GCS_LIST_PAGE_SIZE,
    ObjectNotFound,
    ObjectStore,
    PreconditionFailed,
    StoredObject,
)

CLASS_A_STATE_PREFIX = "state/budget/class-a"
CLASS_A_NOTICE_THRESHOLD = 3_000
CLASS_A_WARNING_THRESHOLD = 4_000
CLASS_A_BACKFILL_PAUSE_THRESHOLD = CLASS_A_NOTICE_THRESHOLD
DEFAULT_CAS_ATTEMPTS = 4


class WorkKind(StrEnum):
    CURRENT_SYNC = "CURRENT_SYNC"
    VIEW_RECOVERY = "VIEW_RECOVERY"
    BACKFILL = "BACKFILL"
    GARBAGE_COLLECTION = "GARBAGE_COLLECTION"
    REBUILD = "REBUILD"


class WarningLevel(StrEnum):
    NORMAL = "NORMAL"
    NOTICE = "NOTICE"
    WARNING = "WARNING"


@dataclass(frozen=True, slots=True)
class ClassABudgetDecision:
    estimated_operations: int
    warning_level: WarningLevel

    def allows(self, work: WorkKind) -> bool:
        """Keep current data flowing while pausing optional historical work."""

        return not (
            work is WorkKind.BACKFILL
            and self.estimated_operations >= CLASS_A_BACKFILL_PAUSE_THRESHOLD
        )


@dataclass(frozen=True, slots=True)
class ClassAMonthState:
    schema_version: int
    month: str
    estimated_operations: int
    updated_at: str

    @classmethod
    def from_bytes(cls, data: bytes) -> ClassAMonthState:
        try:
            value: object = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ClassAUsageStateError("invalid Class A monthly state") from exc
        if not isinstance(value, dict):
            raise ClassAUsageStateError("Class A monthly state must be an object")
        expected_keys = {
            "estimated_operations",
            "month",
            "schema_version",
            "updated_at",
        }
        if set(value) != expected_keys:
            raise ClassAUsageStateError("unexpected Class A monthly state fields")
        schema_version = value["schema_version"]
        month = value["month"]
        estimated_operations = value["estimated_operations"]
        updated_at = value["updated_at"]
        if not isinstance(schema_version, int) or isinstance(schema_version, bool):
            raise ClassAUsageStateError("invalid Class A state schema")
        if schema_version != 1:
            raise ClassAUsageStateError("unsupported Class A state schema")
        if not isinstance(month, str):
            raise ClassAUsageStateError("invalid Class A state month")
        try:
            class_a_month_state_path(month)
        except ValueError as exc:
            raise ClassAUsageStateError("invalid Class A state month") from exc
        if not isinstance(estimated_operations, int) or isinstance(
            estimated_operations,
            bool,
        ):
            raise ClassAUsageStateError("invalid Class A operation count")
        if estimated_operations < 0:
            raise ClassAUsageStateError("negative Class A operation count")
        if not isinstance(updated_at, str):
            raise ClassAUsageStateError("invalid Class A update timestamp")
        return cls(
            schema_version=schema_version,
            month=month,
            estimated_operations=estimated_operations,
            updated_at=updated_at,
        )

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "estimated_operations": self.estimated_operations,
                "month": self.month,
                "schema_version": self.schema_version,
                "updated_at": self.updated_at,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class ClassAUsageUpdate:
    month: str
    previous_operations: int
    estimated_operations: int
    job_operations: int
    state_write_attempts: int
    emitted_levels: tuple[WarningLevel, ...]


class ClassAUsageStateError(RuntimeError):
    """The monthly estimate could not be safely updated."""


class ClassACountingObjectStore:
    """Count estimated GCS Class A attempts while delegating storage behavior."""

    def __init__(self, store: ObjectStore) -> None:
        self._store = store
        self._pending_operations = 0
        self._lock = threading.Lock()

    def get(self, object_name: str) -> StoredObject:
        return self._store.get(object_name)

    def put(
        self,
        object_name: str,
        data: bytes,
        *,
        if_generation_match: int,
        metadata: Mapping[str, str],
        content_type: str,
        cache_control: str,
    ) -> StoredObject:
        self._increment()
        return self._store.put(
            object_name,
            data,
            if_generation_match=if_generation_match,
            metadata=metadata,
            content_type=content_type,
            cache_control=cache_control,
        )

    def list(self, prefix: str) -> Iterable[StoredObject]:
        self._increment()
        objects = tuple(self._store.list(prefix))
        additional_pages = _estimated_list_pages(len(objects)) - 1
        if additional_pages:
            self._increment(additional_pages)
        return objects

    def list_names(self, prefix: str) -> Iterable[str]:
        self._increment()
        names = tuple(self._store.list_names(prefix))
        additional_pages = _estimated_list_pages(len(names)) - 1
        if additional_pages:
            self._increment(additional_pages)
        return names

    def delete(self, object_name: str, *, if_generation_match: int) -> None:
        self._store.delete(
            object_name,
            if_generation_match=if_generation_match,
        )

    def pending_operations(self) -> int:
        with self._lock:
            return self._pending_operations

    def acknowledge(self, operations: int) -> None:
        if operations < 0:
            raise ValueError("operations must be non-negative")
        with self._lock:
            if operations > self._pending_operations:
                raise ValueError("cannot acknowledge uncounted Class A operations")
            self._pending_operations -= operations

    def _increment(self, operations: int = 1) -> None:
        if operations < 1:
            raise ValueError("operations must be positive")
        with self._lock:
            self._pending_operations += operations


def _estimated_list_pages(item_count: int) -> int:
    """Match the explicit GCS adapter page size, including an empty response."""

    if item_count < 0:
        raise ValueError("item_count must be non-negative")
    return max(1, math.ceil(item_count / GCS_LIST_PAGE_SIZE))


ClassAEventEmitter = Callable[[str, Mapping[str, object]], None]


class ClassAUsageReporter:
    """CAS-add one Job's estimate to a UTC calendar-month state object."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        counter: ClassACountingObjectStore,
        emit: ClassAEventEmitter,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_attempts: int = DEFAULT_CAS_ATTEMPTS,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._store = store
        self._counter = counter
        self._emit = emit
        self._clock = clock
        self._max_attempts = max_attempts

    def flush(self) -> ClassAUsageUpdate | None:
        pending = self._counter.pending_operations()
        if pending == 0:
            return None
        now = _aware_utc(self._clock())
        month = now.strftime("%Y-%m")
        path = class_a_month_state_path(month)
        state_write_attempts = 0

        for _ in range(self._max_attempts):
            previous, object_generation = self._read_month(path, month, now)
            state_write_attempts += 1
            job_operations = pending + state_write_attempts
            estimated_operations = previous.estimated_operations + job_operations
            updated = ClassAMonthState(
                schema_version=1,
                month=month,
                estimated_operations=estimated_operations,
                updated_at=_timestamp(now),
            )
            try:
                self._store.put(
                    path,
                    updated.to_bytes(),
                    if_generation_match=object_generation,
                    metadata={"schema": "class-a-month-v1"},
                    content_type="application/json",
                    cache_control="no-store",
                )
            except PreconditionFailed:
                continue

            self._counter.acknowledge(pending)
            emitted_levels = self._emit_crossings(
                month=month,
                previous_operations=previous.estimated_operations,
                estimated_operations=estimated_operations,
                job_operations=job_operations,
            )
            return ClassAUsageUpdate(
                month=month,
                previous_operations=previous.estimated_operations,
                estimated_operations=estimated_operations,
                job_operations=job_operations,
                state_write_attempts=state_write_attempts,
                emitted_levels=emitted_levels,
            )
        raise ClassAUsageStateError("could not CAS-update Class A monthly state")

    def _read_month(
        self,
        path: str,
        month: str,
        now: datetime,
    ) -> tuple[ClassAMonthState, int]:
        try:
            stored = self._store.get(path)
        except ObjectNotFound:
            return (
                ClassAMonthState(
                    schema_version=1,
                    month=month,
                    estimated_operations=0,
                    updated_at=_timestamp(now),
                ),
                0,
            )
        state = ClassAMonthState.from_bytes(stored.data)
        if state.month != month:
            raise ClassAUsageStateError("Class A state month does not match its path")
        return state, stored.generation

    def _emit_crossings(
        self,
        *,
        month: str,
        previous_operations: int,
        estimated_operations: int,
        job_operations: int,
    ) -> tuple[WarningLevel, ...]:
        emitted: list[WarningLevel] = []
        thresholds = (
            (CLASS_A_NOTICE_THRESHOLD, WarningLevel.NOTICE),
            (CLASS_A_WARNING_THRESHOLD, WarningLevel.WARNING),
        )
        for threshold, level in thresholds:
            if previous_operations < threshold <= estimated_operations:
                self._emit(
                    "class_a_warning",
                    {
                        "estimated_operations": estimated_operations,
                        "job_estimated_operations": job_operations,
                        "month": month,
                        "threshold": threshold,
                        "warning_level": level.value,
                    },
                )
                emitted.append(level)
        return tuple(emitted)


def class_a_budget_decision(estimated_operations: int) -> ClassABudgetDecision:
    if estimated_operations < 0:
        raise ValueError("estimated operations must not be negative")
    if estimated_operations >= CLASS_A_WARNING_THRESHOLD:
        level = WarningLevel.WARNING
    elif estimated_operations >= CLASS_A_NOTICE_THRESHOLD:
        level = WarningLevel.NOTICE
    else:
        level = WarningLevel.NORMAL
    return ClassABudgetDecision(estimated_operations, level)


def class_a_backfill_allowed(
    store: ObjectStore,
    *,
    now: datetime,
) -> bool:
    """Return whether optional backfill may start in the current UTC month.

    A missing month starts at zero. Invalid accounting state fails closed for
    backfill only; current synchronization does not call this gate.
    """

    current = _aware_utc(now)
    month = current.strftime("%Y-%m")
    try:
        stored = store.get(class_a_month_state_path(month))
    except ObjectNotFound:
        return True
    try:
        state = ClassAMonthState.from_bytes(stored.data)
    except ClassAUsageStateError:
        return False
    if state.month != month:
        return False
    return class_a_budget_decision(state.estimated_operations).allows(WorkKind.BACKFILL)


def class_a_month_state_path(month: str) -> str:
    try:
        parsed = datetime.strptime(month, "%Y-%m").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError("month must use YYYY-MM") from exc
    if parsed.strftime("%Y-%m") != month:
        raise ValueError("month must use canonical YYYY-MM")
    return f"{CLASS_A_STATE_PREFIX}/month={month}.json"


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Class A clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
