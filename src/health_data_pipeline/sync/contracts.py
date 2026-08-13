"""Infrastructure-neutral contracts shared by sync orchestration and job runners."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class FetchWindow:
    """Physical and optional civil interval requested from a source adapter."""

    start: datetime
    end: datetime
    civil_start: date | None = None
    civil_end: date | None = None

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("fetch window datetimes must be timezone-aware")
        if self.start >= self.end:
            raise ValueError("fetch window must satisfy start < end")
        if (self.civil_start is None) != (self.civil_end is None):
            raise ValueError("civil window boundaries must be provided together")
        if (
            self.civil_start is not None
            and self.civil_end is not None
            and self.civil_start >= self.civil_end
        ):
            raise ValueError("civil window must satisfy start < end")


@dataclass(frozen=True, slots=True)
class DataPointPage:
    """One source page and the exact interval used to retrieve it."""

    data_type: str
    window: FetchWindow
    page_number: int
    data_points: tuple[Mapping[str, Any], ...]
    next_page_token: str | None
    raw_payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    """Result of one synchronization attempt."""

    committed: bool
    generation: int | None
    owner_hmac: str


class SyncPipeline(Protocol):
    """A synchronization use case runnable by the job application."""

    def run(self) -> SyncOutcome: ...


@dataclass(frozen=True, slots=True)
class TaskEnqueueResult:
    """Provider-independent outcome of enqueueing an application task."""

    task_name: str
    created: bool


class ViewTaskDispatcher(Protocol):
    """Schedule recovery of materialized views after a committed sync."""

    def enqueue_view_update(
        self,
        *,
        owner_hmac: str,
        generation: int,
        requested_at: datetime,
    ) -> TaskEnqueueResult: ...


class SyncTaskDispatcher(Protocol):
    """Schedule recovery of durable pending synchronization work."""

    def enqueue_sync_job(
        self,
        *,
        owner_hmac: str,
        ready_at: datetime,
    ) -> TaskEnqueueResult: ...
