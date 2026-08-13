from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime

import pytest

from health_data_pipeline.ops.class_a import (
    CLASS_A_STATE_PREFIX,
    ClassACountingObjectStore,
    ClassAMonthState,
    ClassAUsageReporter,
    WarningLevel,
    WorkKind,
    class_a_backfill_allowed,
    class_a_budget_decision,
    class_a_month_state_path,
    class_a_normalizer_upgrade_allowed,
)
from health_data_pipeline.state.objects import (
    InMemoryObjectStore,
    ObjectStore,
    PreconditionFailed,
    StoredObject,
)

NOW = datetime(2026, 7, 28, 12, 7, tzinfo=UTC)
MONTH = "2026-07"
MONTH_PATH = class_a_month_state_path(MONTH)


def _put(
    store: ObjectStore,
    object_name: str,
    data: bytes = b"data",
    *,
    if_generation_match: int = 0,
) -> StoredObject:
    return store.put(
        object_name,
        data,
        if_generation_match=if_generation_match,
        metadata={},
        content_type="application/octet-stream",
        cache_control="no-store",
    )


def _write_month_state(
    store: ObjectStore,
    estimated_operations: int,
) -> StoredObject:
    state = ClassAMonthState(
        schema_version=1,
        month=MONTH,
        estimated_operations=estimated_operations,
        updated_at="2026-07-28T12:00:00Z",
    )
    return store.put(
        MONTH_PATH,
        state.to_bytes(),
        if_generation_match=0,
        metadata={"schema": "class-a-month-v1"},
        content_type="application/json",
        cache_control="no-store",
    )


def _read_month_state(store: ObjectStore) -> ClassAMonthState:
    return ClassAMonthState.from_bytes(store.get(MONTH_PATH).data)


class ConflictOnceObjectStore:
    """Fail the first Class A state CAS attempt without doing a write."""

    def __init__(self, delegate: ObjectStore) -> None:
        self._delegate = delegate
        self.conflicted = False

    def get(self, object_name: str) -> StoredObject:
        return self._delegate.get(object_name)

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
        if object_name.startswith(CLASS_A_STATE_PREFIX) and not self.conflicted:
            self.conflicted = True
            raise PreconditionFailed(
                object_name,
                expected_generation=if_generation_match,
                actual_generation=None,
            )
        return self._delegate.put(
            object_name,
            data,
            if_generation_match=if_generation_match,
            metadata=metadata,
            content_type=content_type,
            cache_control=cache_control,
        )

    def list(self, prefix: str) -> Iterable[StoredObject]:
        return self._delegate.list(prefix)

    def list_names(self, prefix: str) -> Iterable[str]:
        return self._delegate.list_names(prefix)

    def delete(self, object_name: str, *, if_generation_match: int) -> None:
        self._delegate.delete(
            object_name,
            if_generation_match=if_generation_match,
        )


def test_counting_store_counts_put_and_list_attempts_only() -> None:
    raw = InMemoryObjectStore(clock=lambda: NOW)
    store = ClassACountingObjectStore(raw)

    created = _put(store, "raw/example.json")
    store.get(created.name)
    tuple(store.list("raw/"))
    with pytest.raises(PreconditionFailed):
        _put(store, created.name)
    store.delete(created.name, if_generation_match=created.generation)

    assert store.pending_operations() == 3


def test_counting_store_counts_each_explicit_gcs_list_page() -> None:
    raw = InMemoryObjectStore(clock=lambda: NOW)
    for index in range(1_001):
        _put(raw, f"webhook-events/{index:04d}.json")
    store = ClassACountingObjectStore(raw)

    assert len(tuple(store.list_names("webhook-events/"))) == 1_001
    assert store.pending_operations() == 2


def test_reporter_adds_job_operations_and_its_own_state_write() -> None:
    raw = InMemoryObjectStore(clock=lambda: NOW)
    store = ClassACountingObjectStore(raw)
    events: list[tuple[str, Mapping[str, object]]] = []
    reporter = ClassAUsageReporter(
        store=raw,
        counter=store,
        emit=lambda event, payload: events.append((event, payload)),
        clock=lambda: NOW,
    )
    _put(store, "raw/first.json")
    tuple(store.list("raw/"))

    update = reporter.flush()

    assert update is not None
    assert update.previous_operations == 0
    assert update.job_operations == 3
    assert update.state_write_attempts == 1
    assert update.estimated_operations == 3
    assert _read_month_state(raw).estimated_operations == 3
    assert store.pending_operations() == 0
    assert events == []
    assert reporter.flush() is None


def test_reporter_counts_failed_state_cas_attempt_before_retry() -> None:
    raw = InMemoryObjectStore(clock=lambda: NOW)
    store = ClassACountingObjectStore(raw)
    reporter = ClassAUsageReporter(
        store=ConflictOnceObjectStore(raw),
        counter=store,
        emit=lambda _event, _payload: None,
        clock=lambda: NOW,
    )
    _put(store, "raw/first.json")

    update = reporter.flush()

    assert update is not None
    assert update.state_write_attempts == 2
    assert update.job_operations == 3
    assert _read_month_state(raw).estimated_operations == 3
    assert store.pending_operations() == 0


def test_reporter_emits_notice_only_when_crossing_three_thousand() -> None:
    raw = InMemoryObjectStore(clock=lambda: NOW)
    _write_month_state(raw, 2_998)
    store = ClassACountingObjectStore(raw)
    events: list[tuple[str, Mapping[str, object]]] = []
    reporter = ClassAUsageReporter(
        store=raw,
        counter=store,
        emit=lambda event, payload: events.append((event, payload)),
        clock=lambda: NOW,
    )
    _put(store, "raw/first.json")

    first = reporter.flush()
    _put(store, "raw/second.json")
    second = reporter.flush()

    assert first is not None
    assert first.emitted_levels == (WarningLevel.NOTICE,)
    assert second is not None
    assert second.emitted_levels == ()
    assert [event for event, _payload in events] == ["class_a_warning"]
    assert events[0][1] == {
        "estimated_operations": 3_000,
        "job_estimated_operations": 2,
        "month": MONTH,
        "threshold": 3_000,
        "warning_level": "NOTICE",
    }


def test_reporter_emits_warning_only_when_crossing_four_thousand() -> None:
    raw = InMemoryObjectStore(clock=lambda: NOW)
    _write_month_state(raw, 3_998)
    store = ClassACountingObjectStore(raw)
    events: list[tuple[str, Mapping[str, object]]] = []
    reporter = ClassAUsageReporter(
        store=raw,
        counter=store,
        emit=lambda event, payload: events.append((event, payload)),
        clock=lambda: NOW,
    )
    _put(store, "raw/first.json")

    update = reporter.flush()

    assert update is not None
    assert update.estimated_operations == 4_000
    assert update.emitted_levels == (WarningLevel.WARNING,)
    assert events == [
        (
            "class_a_warning",
            {
                "estimated_operations": 4_000,
                "job_estimated_operations": 2,
                "month": MONTH,
                "threshold": 4_000,
                "warning_level": "WARNING",
            },
        )
    ]


def test_four_thousand_is_warning_not_collection_stop() -> None:
    decision = class_a_budget_decision(4_000)
    assert decision.warning_level is WarningLevel.WARNING
    assert decision.allows(WorkKind.CURRENT_SYNC)
    assert decision.allows(WorkKind.VIEW_RECOVERY)


def test_notice_pauses_only_optional_backfill() -> None:
    below = class_a_budget_decision(2_999)
    notice = class_a_budget_decision(3_000)
    decision = class_a_budget_decision(4_000)

    assert below.allows(WorkKind.BACKFILL)
    assert not notice.allows(WorkKind.BACKFILL)
    assert not decision.allows(WorkKind.BACKFILL)
    assert decision.allows(WorkKind.GARBAGE_COLLECTION)
    assert decision.allows(WorkKind.REBUILD)


def test_backfill_gate_uses_only_current_utc_month_state() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    assert class_a_backfill_allowed(store, now=NOW)
    _write_month_state(store, 3_000)

    assert not class_a_backfill_allowed(store, now=NOW)
    assert class_a_backfill_allowed(
        store,
        now=datetime(2026, 8, 1, tzinfo=UTC),
    )


def test_normalizer_upgrade_gate_keeps_seven_hundred_fifty_operation_margin() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    assert class_a_normalizer_upgrade_allowed(store, now=NOW)

    _write_month_state(store, 4_249)
    assert class_a_normalizer_upgrade_allowed(store, now=NOW)

    stored = store.get(MONTH_PATH)
    store.put(
        MONTH_PATH,
        ClassAMonthState(
            schema_version=1,
            month="2026-07",
            estimated_operations=4_250,
            updated_at="2026-07-28T12:00:00Z",
        ).to_bytes(),
        if_generation_match=stored.generation,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )
    assert not class_a_normalizer_upgrade_allowed(store, now=NOW)
