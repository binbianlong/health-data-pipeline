"""Monotonic Notion and public steps-SVG materialized views."""

# ruff: noqa: B904, PLC0415, PLR2004

from __future__ import annotations

import hashlib
import html
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime
from typing import Any, Protocol

from health_data_pipeline.oauth import read_state, write_state
from health_data_pipeline.storage import (
    ObjectNotFound,
    ObjectStore,
    PreconditionFailed,
)

NOTION_STATE_PATH = "state/notion/sync.json"
NOTION_PAGE_STATE_PREFIX = "state/notion/pages"
STEPS_HEAD_PATH = "state/views/steps-head.json"


class ViewUpdateError(RuntimeError):
    """A materialized view could not be safely updated."""


@dataclass(frozen=True, slots=True)
class DailyMetric:
    metric_name: str
    statistic: str
    value: int | float | None
    unit: str | None
    status: str
    weekly_average: float | None = None
    monthly_average: float | None = None
    coverage: float | None = None
    weekly_coverage: float | None = None
    monthly_coverage: float | None = None
    quality_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DailyView:
    metric_date: date
    source_generation: int
    data_status: str
    fitbit_last_sync_time: datetime | None
    metrics: tuple[DailyMetric, ...]


@dataclass(frozen=True, slots=True)
class NotionPage:
    page_id: str
    source_generation: int
    content_sha256: str | None


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


@dataclass(frozen=True, slots=True)
class NotionSyncState:
    desired_generation: int
    applied_generation: int
    status: str
    applying_generation: int | None
    updated_at: str

    @classmethod
    def from_bytes(cls, data: bytes) -> NotionSyncState:
        try:
            return cls(**json.loads(data))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ViewUpdateError("invalid Notion sync state") from exc

    def to_bytes(self) -> bytes:
        return _canonical_json(asdict(self))


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
                updated_at=_now_timestamp(),
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
            updated_at=_now_timestamp(),
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


@dataclass(frozen=True, slots=True)
class NotionPageState:
    """Durable page correspondence retained while the page is managed."""

    metric_date: str
    page_id: str
    source_generation: int
    content_sha256: str
    updated_at: str

    @classmethod
    def from_bytes(cls, data: bytes) -> NotionPageState:
        try:
            return cls(**json.loads(data))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ViewUpdateError("invalid Notion page state") from exc

    def to_bytes(self) -> bytes:
        return _canonical_json(asdict(self))


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _now_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _format_value(value: int | float | None) -> str:
    if value is None:
        return "欠損"
    if isinstance(value, float):
        return format(value, ".6g")
    return str(value)


def _format_coverage(value: float | None) -> str:
    return "unknown" if value is None else f"{value * 100:.1f}%"


def render_notion_body(view: DailyView) -> str:
    """Render a stable, compact table intended for ChatGPT consumption."""

    lines = [
        f"Date: {view.metric_date.isoformat()}",
        f"Data status: {view.data_status}",
        f"Source HEAD generation: {view.source_generation}",
        "Fitbit last sync time: "
        + (
            view.fitbit_last_sync_time.isoformat()
            if view.fitbit_last_sync_time is not None
            else "unknown"
        ),
        "",
        (
            "| Metric | Value | Unit | Calendar week avg "
            "(Mon-date; final Sun) | Week coverage | "
            "Calendar month avg (1st-date) | Month coverage | Status | Daily coverage |"
        ),
        "|---|---:|---|---:|---:|---:|---:|---|---:|",
    ]
    for metric in sorted(view.metrics, key=lambda item: (item.metric_name, item.statistic)):
        label = (
            metric.metric_name
            if metric.statistic == "value"
            else f"{metric.metric_name} ({metric.statistic})"
        )
        coverage = _format_coverage(metric.coverage)
        flags = ",".join(sorted(metric.quality_flags))
        status = metric.status if not flags else f"{metric.status}; {flags}"
        lines.append(
            "| "
            + " | ".join(
                (
                    label,
                    _format_value(metric.value),
                    metric.unit or "",
                    _format_value(metric.weekly_average),
                    _format_coverage(metric.weekly_coverage),
                    _format_value(metric.monthly_average),
                    _format_coverage(metric.monthly_coverage),
                    status,
                    coverage,
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def notion_content_hash(view: DailyView, body: str) -> str:
    """Hash only deterministic view content, never a write timestamp."""

    payload = {
        "metric_date": view.metric_date.isoformat(),
        "source_generation": view.source_generation,
        "data_status": view.data_status,
        "fitbit_last_sync_time": (
            view.fitbit_last_sync_time.isoformat() if view.fitbit_last_sync_time else None
        ),
        "body": body,
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


class NotionViewService:
    """Apply the newest HEAD generation without allowing page regression."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        source: DailyViewSource,
        notion: NotionGateway,
    ) -> None:
        self._store = store
        self._source = source
        self._notion = notion

    def _read_state(self) -> tuple[NotionSyncState, int]:
        return read_notion_sync_state(self._store)

    def request_generation(self, generation: int) -> bool:
        """Monotonically raise desired_generation.

        A CAS conflict is safe to retry from the caller or the next hourly job.
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
            updated_at=_now_timestamp(),
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
            body = render_notion_body(view)
            content_hash = notion_content_hash(view, body)
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
            updated_at=_now_timestamp(),
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
            updated_at=_now_timestamp(),
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


@dataclass(frozen=True, slots=True)
class StepSample:
    metric_date: date
    steps: int | None


@dataclass(frozen=True, slots=True)
class StepsPointer:
    generation: int
    svg_uri: str
    content_sha256: str
    updated_at: str

    @classmethod
    def from_bytes(cls, data: bytes) -> StepsPointer:
        try:
            return cls(**json.loads(data))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ViewUpdateError("invalid steps pointer") from exc

    def to_bytes(self) -> bytes:
        return _canonical_json(asdict(self))


def render_steps_svg(samples: Sequence[StepSample]) -> bytes:
    """Render deterministic, dependency-free SVG for exactly seven days."""

    ordered = tuple(sorted(samples, key=lambda sample: sample.metric_date))
    if len(ordered) != 7:
        raise ValueError("steps SVG requires exactly seven daily samples")
    width = 700
    height = 220
    plot_top = 30
    plot_height = 135
    bar_width = 56
    gap = 36
    maximum = max((sample.steps or 0 for sample in ordered), default=0) or 1
    parts = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
            f'height="{height}" viewBox="0 0 {width} {height}" '
            'role="img" aria-label="Steps over the last 7 days">'
        ),
        "<style>"
        "text{font-family:system-ui,sans-serif;fill:#4493f8}"
        ".bar{fill:#238636}.missing{fill:#656d76}"
        "</style>",
    ]
    for index, sample in enumerate(ordered):
        value = sample.steps
        numeric = value or 0
        bar_height = round(plot_height * numeric / maximum, 2)
        x = 32 + index * (bar_width + gap)
        y = plot_top + plot_height - bar_height
        css_class = "missing" if value is None else "bar"
        label = "—" if value is None else f"{value:,}"
        parts.extend(
            (
                (
                    f'<rect class="{css_class}" x="{x}" y="{y}" '
                    f'width="{bar_width}" height="{bar_height}" rx="5"/>'
                ),
                (
                    f'<text x="{x + bar_width / 2}" y="{plot_top + plot_height + 18}" '
                    f'font-size="12" text-anchor="middle">'
                    f"{html.escape(sample.metric_date.strftime('%m/%d'))}</text>"
                ),
                (
                    f'<text x="{x + bar_width / 2}" y="{max(18, y - 5)}" '
                    f'font-size="11" text-anchor="middle">{html.escape(label)}</text>'
                ),
            )
        )
    parts.append("</svg>")
    return "".join(parts).encode("utf-8")


class StepsSvgService:
    """Publish immutable SVG objects and a monotonically advancing pointer."""

    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    def publish(self, *, generation: int, samples: Sequence[StepSample]) -> bool:
        svg = render_steps_svg(samples)
        digest = hashlib.sha256(svg).hexdigest()
        uri = f"public/steps/canonicalization=fhcj-1/sha256/{digest[:2]}/{digest}.svg"
        try:
            self._store.put(
                uri,
                svg,
                if_generation_match=0,
                metadata={"content-sha256": digest},
                content_type="image/svg+xml; charset=utf-8",
                cache_control="public, max-age=31536000, immutable",
            )
        except PreconditionFailed:
            try:
                existing_svg = self._store.get(uri)
            except ObjectNotFound as exc:
                raise ViewUpdateError(
                    "content-addressed SVG disappeared after create conflict"
                ) from exc
            if existing_svg.data != svg:
                raise ViewUpdateError("content-addressed SVG object mismatch")

        current = read_state(self._store, STEPS_HEAD_PATH)
        if current is not None:
            pointer = StepsPointer.from_bytes(current.data)
            if generation <= pointer.generation:
                return False
            expected_generation = current.generation
        else:
            expected_generation = 0

        pointer = StepsPointer(
            generation=generation,
            svg_uri=uri,
            content_sha256=digest,
            updated_at=_now_timestamp(),
        )
        write_state(
            self._store,
            STEPS_HEAD_PATH,
            pointer.to_bytes(),
            if_generation_match=expected_generation,
        )
        return True

    def read_current(self) -> tuple[bytes, int]:
        current = read_state(self._store, STEPS_HEAD_PATH)
        if current is None:
            raise ViewUpdateError("steps SVG has not been published")
        pointer = StepsPointer.from_bytes(current.data)
        try:
            svg = self._store.get(pointer.svg_uri)
        except ObjectNotFound as exc:
            raise ViewUpdateError("steps SVG pointer target is missing") from exc
        if not svg.data:
            raise ViewUpdateError("steps SVG pointer target is empty")
        digest = hashlib.sha256(svg.data).hexdigest()
        if not hmac_compare(digest, pointer.content_sha256):
            raise ViewUpdateError("steps SVG content hash mismatch")
        return svg.data, pointer.generation


def hmac_compare(left: str, right: str) -> bool:
    # Kept local to avoid leaking timing differences when the pointer is corrupt.
    import hmac

    return hmac.compare_digest(left, right)


class ViewApplicationService:
    """Single entry point used by the Cloud Tasks view writer."""

    def __init__(
        self,
        *,
        notion: NotionViewService,
        steps: StepsSvgService,
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


class ProtocolStepSamples(Protocol):
    def for_generation(self, generation: int) -> Sequence[StepSample]: ...
