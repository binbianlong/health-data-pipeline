"""Future reachability-based garbage collection planning.

The v1 runtime does not list or delete canonical objects. This pure planner
keeps the retention policy testable for a later, explicitly enabled GC job.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class StoredObject:
    uri: str
    created_at: datetime

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class GcPlan:
    delete: tuple[str, ...]
    retained: tuple[str, ...]
    too_young: tuple[str, ...]


def plan_reachability_gc(
    objects: Iterable[StoredObject],
    *,
    retention_references: Iterable[str],
    audit_references: Iterable[str],
    now: datetime,
    orphan_grace: timedelta = timedelta(days=30),
) -> GcPlan:
    """Plan deletions without allowing audit references to pin payloads."""

    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if orphan_grace < timedelta(0):
        raise ValueError("orphan grace must not be negative")

    retained_refs = frozenset(retention_references)
    # Materialize for validation and to make the deliberate non-pinning policy
    # visible to callers and static analysis.
    frozenset(audit_references)

    delete: list[str] = []
    retained: list[str] = []
    too_young: list[str] = []
    for stored in sorted(objects, key=lambda item: item.uri):
        if stored.uri in retained_refs:
            retained.append(stored.uri)
        elif now - stored.created_at < orphan_grace:
            too_young.append(stored.uri)
        else:
            delete.append(stored.uri)
    return GcPlan(tuple(delete), tuple(retained), tuple(too_young))
