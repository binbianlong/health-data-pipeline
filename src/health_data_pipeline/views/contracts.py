"""Ports consumed by the materialized-view application services."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Protocol

from health_data_pipeline.views.models import (
    DailyView,
    NotionPage,
    StepSample,
)


class NotionGateway(Protocol):
    def find_daily_page(self, metric_date: date) -> NotionPage | None: ...

    def upsert_daily_page(
        self,
        *,
        existing_page_id: str | None,
        view: DailyView,
        body: str,
        content_sha256: str,
    ) -> str: ...


class DailyViewSource(Protocol):
    def latest_generation(self) -> int: ...

    def views_between(
        self,
        *,
        after_generation: int,
        through_generation: int,
    ) -> Sequence[DailyView]: ...


class NotionRenderer(Protocol):
    def render_body(self, view: DailyView) -> str: ...

    def content_hash(self, view: DailyView, body: str) -> str: ...


class ProtocolStepSamples(Protocol):
    def for_generation(self, generation: int) -> Sequence[StepSample]: ...


class StepsPublisher(Protocol):
    def publish(self, *, generation: int, samples: Sequence[StepSample]) -> bool: ...
