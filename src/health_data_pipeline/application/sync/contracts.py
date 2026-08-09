"""Infrastructure-neutral contracts shared by sync orchestration and job runners."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


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
