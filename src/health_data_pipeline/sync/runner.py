"""Cloud Run Job orchestration after one sync attempt."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from health_data_pipeline.sync.contracts import (
    SyncOutcome,
    SyncPipeline,
    SyncTaskDispatcher,
    ViewTaskDispatcher,
)


class JobRunner:
    """Run one sync and recover materialized views from committed HEAD."""

    def __init__(
        self,
        *,
        pipeline: SyncPipeline,
        tasks: ViewTaskDispatcher,
        latest_generation: Callable[[], int],
        request_view_generation: Callable[[int], bool],
        sync_tasks: SyncTaskDispatcher | None = None,
        sync_recovery_needed: Callable[[], bool] | None = None,
        on_sync_recovery_error: Callable[[Exception], object] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        on_complete: Callable[[], object] | None = None,
    ) -> None:
        if (sync_tasks is None) is not (sync_recovery_needed is None):
            raise ValueError("sync_tasks and sync_recovery_needed must be configured together")
        self._pipeline = pipeline
        self._tasks = tasks
        self._latest_generation = latest_generation
        self._request_view_generation = request_view_generation
        self._sync_tasks = sync_tasks
        self._sync_recovery_needed = sync_recovery_needed
        self._on_sync_recovery_error = on_sync_recovery_error
        self._clock = clock
        self._on_complete = on_complete

    def _recover_pending_sync(self, owner_hmac: str) -> None:
        if self._sync_tasks is None or self._sync_recovery_needed is None:
            return
        try:
            if self._sync_recovery_needed():
                self._sync_tasks.enqueue_sync_job(
                    owner_hmac=owner_hmac,
                    ready_at=self._clock(),
                )
        except Exception as exc:
            # This is a wake-up optimization after the authoritative HEAD
            # commit. Durable events remain available to scheduled recovery.
            if self._on_sync_recovery_error is not None:
                self._on_sync_recovery_error(exc)

    def run(self) -> SyncOutcome:
        try:
            outcome = self._pipeline.run()
            try:
                if outcome.committed and outcome.generation is None:
                    raise RuntimeError("committed sync has no HEAD generation")
                generation = (
                    outcome.generation
                    if outcome.generation is not None
                    else self._latest_generation()
                )
                if (
                    generation is not None
                    and generation > 0
                    and self._request_view_generation(generation)
                ):
                    self._tasks.enqueue_view_update(
                        owner_hmac=outcome.owner_hmac,
                        generation=generation,
                        requested_at=self._clock(),
                    )
            finally:
                self._recover_pending_sync(outcome.owner_hmac)
            return outcome
        finally:
            if self._on_complete is not None:
                self._on_complete()
