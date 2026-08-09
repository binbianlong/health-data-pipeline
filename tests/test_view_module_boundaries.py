from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime

import pytest

from health_data_pipeline.adapters.notion.gateway import NotionDataSourceGateway
from health_data_pipeline.application.views import service as view_service
from health_data_pipeline.application.views.models import (
    DailyView,
    NotionPage,
    NotionPageState,
    NotionSyncState,
    StepSample,
    ViewUpdateError,
)
from health_data_pipeline.application.views.service import (
    NotionViewService as ApplicationNotionViewService,
)
from health_data_pipeline.application.views.service import request_notion_generation
from health_data_pipeline.notion_adapter import (
    NotionDataSourceGateway as LegacyNotionDataSourceGateway,
)
from health_data_pipeline.notion_adapter import SnapshotViewSource as LegacySnapshotViewSource
from health_data_pipeline.oauth import read_state as oauth_read_state
from health_data_pipeline.oauth import write_state as oauth_write_state
from health_data_pipeline.persistence.versioned_state import read_state, write_state
from health_data_pipeline.presentation.snapshot_views import SnapshotViewSource
from health_data_pipeline.presentation.steps_svg import (
    STEPS_HEAD_PATH,
    StepsPointer,
    StepsSvgService,
    render_steps_svg,
)
from health_data_pipeline.storage import InMemoryObjectStore, PreconditionFailed
from health_data_pipeline.views import NotionViewService as LegacyNotionViewService
from health_data_pipeline.views import StepsSvgService as LegacyStepsSvgService


class OneViewSource:
    def __init__(self, view: DailyView) -> None:
        self._view = view

    def latest_generation(self) -> int:
        return self._view.source_generation

    def views_between(
        self,
        *,
        after_generation: int,
        through_generation: int,
    ) -> tuple[DailyView, ...]:
        assert after_generation == 0
        assert through_generation == self._view.source_generation
        return (self._view,)


class RecordingNotion:
    def __init__(self) -> None:
        self.body: str | None = None
        self.content_sha256: str | None = None

    def find_daily_page(self, metric_date: date) -> NotionPage | None:
        del metric_date
        return None

    def upsert_daily_page(
        self,
        *,
        existing_page_id: str | None,
        view: DailyView,
        body: str,
        content_sha256: str,
    ) -> str:
        del existing_page_id, view
        self.body = body
        self.content_sha256 = content_sha256
        return "page-id"


class StubRenderer:
    def render_body(self, view: DailyView) -> str:
        return f"custom:{view.metric_date.isoformat()}"

    def content_hash(self, view: DailyView, body: str) -> str:
        del view
        return f"hash:{body}"


def test_application_service_depends_on_renderer_contract() -> None:
    view = DailyView(
        metric_date=date(2026, 8, 9),
        source_generation=1,
        data_status="CANONICAL_SYNCED",
        fitbit_last_sync_time=datetime(2026, 8, 9, tzinfo=UTC),
        metrics=(),
    )
    notion = RecordingNotion()
    service = ApplicationNotionViewService(
        store=InMemoryObjectStore(),
        source=OneViewSource(view),
        notion=notion,
        renderer=StubRenderer(),
    )

    assert service.request_generation(1)
    assert service.apply_latest() == 1
    assert notion.body == "custom:2026-08-09"
    assert notion.content_sha256 == "hash:custom:2026-08-09"


def test_legacy_facades_keep_runtime_imports_compatible() -> None:
    assert LegacyNotionDataSourceGateway is NotionDataSourceGateway
    assert LegacySnapshotViewSource is SnapshotViewSource
    assert LegacyStepsSvgService is StepsSvgService
    assert issubclass(LegacyNotionViewService, ApplicationNotionViewService)


def test_oauth_reexports_shared_versioned_state_helpers() -> None:
    assert oauth_read_state is read_state
    assert oauth_write_state is write_state

    store = InMemoryObjectStore()
    generation = oauth_write_state(
        store,
        "state/test.json",
        b"{}",
        if_generation_match=0,
    )

    current = read_state(store, "state/test.json")
    assert current is not None
    assert current.generation == generation
    assert current.data == b"{}"


@pytest.mark.parametrize(
    "decoder",
    [NotionSyncState.from_bytes, NotionPageState.from_bytes, StepsPointer.from_bytes],
)
def test_view_state_decoders_reject_invalid_json(decoder: Callable[[bytes], object]) -> None:
    with pytest.raises(ViewUpdateError):
        decoder(b"{")


def test_notion_generation_rejects_invalid_input_and_exhausted_cas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryObjectStore()
    with pytest.raises(ValueError, match="positive"):
        request_notion_generation(store, 0)

    def conflict(*args: object, **kwargs: object) -> int:
        del args, kwargs
        raise PreconditionFailed(
            "state/notion/sync.json",
            expected_generation=0,
            actual_generation=1,
        )

    monkeypatch.setattr(view_service, "write_state", conflict)
    with pytest.raises(ViewUpdateError, match="advance desired"):
        request_notion_generation(store, 1, max_attempts=1)


def test_steps_presentation_rejects_invalid_input_and_missing_pointer() -> None:
    sample = StepSample(metric_date=date(2026, 8, 9), steps=1)
    with pytest.raises(ValueError, match="exactly seven"):
        render_steps_svg((sample,))
    with pytest.raises(ValueError, match="exactly seven"):
        StepsSvgService(InMemoryObjectStore()).publish(generation=1, samples=(sample,))
    with pytest.raises(ViewUpdateError, match="has not been published"):
        StepsSvgService(InMemoryObjectStore()).read_current()


def test_steps_presentation_rejects_invalid_completed_date() -> None:
    store = InMemoryObjectStore()
    pointer = StepsPointer(
        generation=1,
        svg_uri="public/steps/legacy.svg",
        content_sha256="legacy",
        updated_at="2026-08-09T00:00:00Z",
        through_date="invalid",
    )
    store.put(
        STEPS_HEAD_PATH,
        pointer.to_bytes(),
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )
    samples = tuple(StepSample(metric_date=date(2026, 8, day), steps=day) for day in range(3, 10))

    with pytest.raises(ViewUpdateError, match="through_date"):
        StepsSvgService(store).publish(generation=2, samples=samples)
