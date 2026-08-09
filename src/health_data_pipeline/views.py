"""Compatibility facade for the modular materialized-view implementation."""

from __future__ import annotations

from health_data_pipeline.application.views.contracts import (
    DailyViewSource,
    NotionGateway,
    NotionRenderer,
    ProtocolStepSamples,
    StepsPublisher,
)
from health_data_pipeline.application.views.models import (
    DailyMetric,
    DailyView,
    NotionPage,
    NotionPageState,
    NotionSyncState,
    SleepSessionView,
    StepSample,
    ViewUpdateError,
)
from health_data_pipeline.application.views.service import (
    NOTION_PAGE_STATE_PREFIX,
    NOTION_STATE_PATH,
    ViewApplicationService,
    read_notion_sync_state,
    request_notion_generation,
)
from health_data_pipeline.application.views.service import (
    NotionViewService as _ApplicationNotionViewService,
)
from health_data_pipeline.presentation.notion_renderer import (
    DefaultNotionRenderer,
    notion_content_hash,
    render_notion_body,
)
from health_data_pipeline.presentation.steps_svg import (
    STEPS_HEAD_PATH,
    STEPS_SVG_CANONICALIZATION,
    StepsPointer,
    StepsSvgService,
    hmac_compare,
    render_steps_svg,
)
from health_data_pipeline.storage import ObjectStore


class NotionViewService(_ApplicationNotionViewService):
    """Backward-compatible service with the default presentation renderer."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        source: DailyViewSource,
        notion: NotionGateway,
        renderer: NotionRenderer | None = None,
    ) -> None:
        super().__init__(
            store=store,
            source=source,
            notion=notion,
            renderer=renderer or DefaultNotionRenderer(),
        )


__all__ = [
    "NOTION_PAGE_STATE_PREFIX",
    "NOTION_STATE_PATH",
    "STEPS_HEAD_PATH",
    "STEPS_SVG_CANONICALIZATION",
    "DailyMetric",
    "DailyView",
    "DailyViewSource",
    "DefaultNotionRenderer",
    "NotionGateway",
    "NotionPage",
    "NotionPageState",
    "NotionRenderer",
    "NotionSyncState",
    "NotionViewService",
    "ProtocolStepSamples",
    "SleepSessionView",
    "StepSample",
    "StepsPointer",
    "StepsPublisher",
    "StepsSvgService",
    "ViewApplicationService",
    "ViewUpdateError",
    "hmac_compare",
    "notion_content_hash",
    "read_notion_sync_state",
    "render_notion_body",
    "render_steps_svg",
    "request_notion_generation",
]
