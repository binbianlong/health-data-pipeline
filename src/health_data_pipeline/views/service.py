"""Monotonic application services for Notion and public view updates."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

from health_data_pipeline.state.objects import ObjectStore, PreconditionFailed
from health_data_pipeline.state.versioned import (
    read_state,
    utc_timestamp,
    write_state,
)
from health_data_pipeline.views.contracts import (
    DailyViewSource,
    NotionGateway,
    NotionRenderer,
    ProtocolStepSamples,
    StepsPublisher,
)
from health_data_pipeline.views.models import (
    NotionPage,
    NotionPageState,
    NotionSyncState,
    ViewUpdateError,
)

NOTION_STATE_PATH = "state/notion/sync.json"
NOTION_PAGE_STATE_PREFIX = "state/notion/pages"


def read_notion_sync_state(
    store: ObjectStore,
) -> tuple[NotionSyncState, int]:
    current = read_state(store, NOTION_STATE_PATH)
    if current is None:
        return (
            NotionSyncState(
                desired_generation=0,
                applied_generation=0,
                status="IDLE",
                applying_generation=None,
                updated_at=utc_timestamp(),
            ),
            0,
        )
    return NotionSyncState.from_bytes(current.data), current.generation


def request_notion_generation(
    store: ObjectStore,
    generation: int,
    *,
    max_attempts: int = 4,
) -> bool:
    """Persist a desired generation before a wake-up task is created.

    The return value says whether the materialized view still needs a wake-up,
    including when a prior task was lost after desired_generation was saved.
    """

    if generation < 1:
        raise ValueError("generation must be positive")
    for _ in range(max_attempts):
        state, object_generation = read_notion_sync_state(store)
        if state.applied_generation >= generation:
            return False
        if state.desired_generation >= generation:
            return True
        updated = replace(
            state,
            desired_generation=generation,
            updated_at=utc_timestamp(),
        )
        try:
            write_state(
                store,
                NOTION_STATE_PATH,
                updated.to_bytes(),
                if_generation_match=object_generation,
            )
        except PreconditionFailed:
            continue
        return True
    raise ViewUpdateError("could not advance desired Notion generation")


class NotionViewService:
    """Apply the newest HEAD generation without allowing page regression."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        source: DailyViewSource,
        notion: NotionGateway,
        renderer: NotionRenderer,
    ) -> None:
        self._store = store
        self._source = source
        self._notion = notion
        self._renderer = renderer

    def _read_state(self) -> tuple[NotionSyncState, int]:
        return read_notion_sync_state(self._store)

    def request_generation(self, generation: int) -> bool:
        """Monotonically raise desired_generation.

        A CAS conflict is safe to retry from the caller or the next recovery job.
        """

        return request_notion_generation(self._store, generation)

    def apply_latest(self, requested_generation: int | None = None) -> int:
        """Read HEAD at execution time and apply exactly that newer generation."""

        latest = self._source.latest_generation()
        state, object_generation = self._read_state()
        target = max(latest, state.desired_generation)
        # The payload is only a wake-up hint; it must never pull the target back.
        if requested_generation is not None:
            target = max(target, min(requested_generation, latest))
        if target <= state.applied_generation:
            return state.applied_generation

        applying = replace(
            state,
            desired_generation=max(state.desired_generation, target),
            status="APPLYING",
            applying_generation=target,
            updated_at=utc_timestamp(),
        )
        applying_object_generation = write_state(
            self._store,
            NOTION_STATE_PATH,
            applying.to_bytes(),
            if_generation_match=object_generation,
        )

        for view in self._source.views_between(
            after_generation=state.applied_generation,
            through_generation=target,
        ):
            if view.source_generation != target:
                raise ViewUpdateError("daily view generation does not match target")
            body = self._renderer.render_body(view)
            content_hash = self._renderer.content_hash(view, body)
            existing = self._notion.find_daily_page(view.metric_date)
            if existing is not None and existing.source_generation > target:
                self._record_page_state(view.metric_date, existing)
                continue
            if (
                existing is not None
                and existing.source_generation == target
                and existing.content_sha256 == content_hash
            ):
                self._record_page_state(view.metric_date, existing)
                continue
            page_id = self._notion.upsert_daily_page(
                existing_page_id=None if existing is None else existing.page_id,
                view=view,
                body=body,
                content_sha256=content_hash,
            )
            self._record_page_state(
                view.metric_date,
                NotionPage(
                    page_id=page_id,
                    source_generation=target,
                    content_sha256=content_hash,
                ),
            )

        completed = replace(
            applying,
            applied_generation=target,
            status="IDLE",
            applying_generation=None,
            updated_at=utc_timestamp(),
        )
        write_state(
            self._store,
            NOTION_STATE_PATH,
            completed.to_bytes(),
            if_generation_match=applying_object_generation,
        )
        return target

    def _record_page_state(self, metric_date: date, page: NotionPage) -> None:
        """Persist page identity after Notion's generation marker is committed."""

        if page.content_sha256 is None:
            return
        path = (
            f"{NOTION_PAGE_STATE_PREFIX}/year={metric_date.year:04d}/{metric_date.isoformat()}.json"
        )
        current = read_state(self._store, path)
        if current is not None:
            previous = NotionPageState.from_bytes(current.data)
            if previous.source_generation > page.source_generation:
                return
            if (
                previous.source_generation == page.source_generation
                and previous.page_id == page.page_id
                and previous.content_sha256 == page.content_sha256
            ):
                return
            expected_generation = current.generation
        else:
            expected_generation = 0
        state = NotionPageState(
            metric_date=metric_date.isoformat(),
            page_id=page.page_id,
            source_generation=page.source_generation,
            content_sha256=page.content_sha256,
            updated_at=utc_timestamp(),
        )
        try:
            write_state(
                self._store,
                path,
                state.to_bytes(),
                if_generation_match=expected_generation,
            )
        except PreconditionFailed:
            # A concurrent, newer writer is authoritative. The next scheduled
            # reconciliation also repairs a missing mapping.
            latest = read_state(self._store, path)
            if latest is None:
                raise
            recorded = NotionPageState.from_bytes(latest.data)
            if recorded.source_generation < page.source_generation:
                raise


class ViewApplicationService:
    """Single entry point used by the Cloud Tasks view writer."""

    def __init__(
        self,
        *,
        notion: NotionViewService,
        steps: StepsPublisher,
        step_samples: ProtocolStepSamples,
    ) -> None:
        self._notion = notion
        self._steps = steps
        self._step_samples = step_samples

    def apply_latest(self, requested_generation: int | None = None) -> int:
        generation = self._notion.apply_latest(requested_generation)
        self._steps.publish(
            generation=generation,
            samples=self._step_samples.for_generation(generation),
        )
        return generation
