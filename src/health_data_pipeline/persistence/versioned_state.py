"""CAS-protected JSON state stored through the shared object-store boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from health_data_pipeline.storage import ObjectNotFound, ObjectStore


@dataclass(frozen=True, slots=True)
class VersionedObject:
    """Application state bytes paired with their object generation."""

    data: bytes
    generation: int


def utc_timestamp() -> str:
    """Return a canonical UTC timestamp for persisted application state."""

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def read_state(store: ObjectStore, path: str) -> VersionedObject | None:
    """Read optional application state without weakening missing-object semantics."""

    try:
        stored = store.get(path)
    except ObjectNotFound:
        return None
    return VersionedObject(data=stored.data, generation=stored.generation)


def write_state(
    store: ObjectStore,
    path: str,
    data: bytes,
    *,
    if_generation_match: int,
) -> int:
    """Write application state with a mandatory generation precondition."""

    stored = store.put(
        path,
        data,
        if_generation_match=if_generation_match,
        metadata={"schema": "application-state-json"},
        content_type="application/json",
        cache_control="no-store",
    )
    return stored.generation
