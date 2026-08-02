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

from health_data_pipeline.metrics import METRIC_DEFINITIONS
from health_data_pipeline.oauth import read_state, write_state
from health_data_pipeline.storage import (
    ObjectNotFound,
    ObjectStore,
    PreconditionFailed,
)

NOTION_STATE_PATH = "state/notion/sync.json"
NOTION_PAGE_STATE_PREFIX = "state/notion/pages"
STEPS_HEAD_PATH = "state/views/steps-head.json"
STEPS_SVG_CANONICALIZATION = "fhcj-3"

_NOTION_METRIC_LABELS = {
    definition.name: definition.notion_label for definition in METRIC_DEFINITIONS
}
_NOTION_METRIC_UNITS = {definition.name: definition.unit for definition in METRIC_DEFINITIONS}
_NOTION_METRIC_ORDER = {
    metric_name: index for index, metric_name in enumerate(_NOTION_METRIC_LABELS)
}


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
    missing_reason: str | None = None
    coverage_kind: str = "UNKNOWN"
    weekly_status: str | None = None
    monthly_status: str | None = None
    weekly_quality_flags: tuple[str, ...] = ()
    monthly_quality_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SleepSessionView:
    """One committed sleep interval rendered with its edge-specific offsets."""

    start_time: datetime
    end_time: datetime
    is_main: bool
    is_nap: bool
    minutes_asleep: int | float | None = None
    role_source: str | None = None
    processed: bool | None = None
    stages_status: str | None = None
    manually_edited: bool | None = None


@dataclass(frozen=True, slots=True)
class DailyView:
    metric_date: date
    source_generation: int
    data_status: str
    fitbit_last_sync_time: datetime | None
    metrics: tuple[DailyMetric, ...]
    time_zone: str | None = None
    time_zone_source: str | None = None
    physical_day_start: datetime | None = None
    physical_day_end: datetime | None = None
    sleep_sessions: tuple[SleepSessionView, ...] = ()
    sleep_sessions_available: bool = False
    sleep_source_status: str | None = None


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


def _notion_metric_sort_key(metric: DailyMetric) -> tuple[int, int | str, str]:
    order = _NOTION_METRIC_ORDER.get(metric.metric_name)
    if order is not None:
        return (0, order, metric.statistic)
    return (1, metric.metric_name, metric.statistic)


def _format_session_time(value: datetime) -> str:
    return value.isoformat(timespec="minutes")


def _format_sleep_session(label: str, session: SleepSessionView) -> str:
    duration = (
        ""
        if session.minutes_asleep is None
        else f" / 睡眠時間: {_format_value(session.minutes_asleep)}分"
    )
    details: list[str] = []
    if session.role_source == "INFERRED":
        details.append("主睡眠判定: 推定")
    if session.processed is False:
        details.append("Fitbit処理中")
    if session.stages_status:
        details.append(f"睡眠ステージ状態: {session.stages_status}")
    if session.manually_edited is True:
        details.append("手動編集あり")
    suffix = "" if not details else " / " + " / ".join(details)
    return (
        f"- {label}: 就寝時刻 {_format_session_time(session.start_time)} / "
        f"起床時刻 {_format_session_time(session.end_time)}{duration}{suffix}"
    )


def _sleep_session_lines(view: DailyView) -> list[str]:
    if not view.sleep_sessions_available:
        if view.sleep_source_status == "SUCCESS_EMPTY":
            return ["## 睡眠セッション", "主睡眠: 記録なし", "昼寝: なし", ""]
        if view.sleep_source_status == "SOURCE_PROCESSING":
            return ["## 睡眠セッション", "主睡眠: Fitbit処理中", "昼寝: 判定待ち", ""]
        return ["## 睡眠セッション", "主睡眠: 未取得", "昼寝: 未取得", ""]

    ordered = tuple(sorted(view.sleep_sessions, key=lambda item: (item.start_time, item.end_time)))
    main = tuple(session for session in ordered if session.is_main)
    naps = tuple(session for session in ordered if session.is_nap and not session.is_main)
    unknown = tuple(session for session in ordered if not session.is_main and not session.is_nap)
    lines = ["## 睡眠セッション"]
    if main:
        lines.extend(
            _format_sleep_session(
                "主睡眠" if len(main) == 1 else f"主睡眠 {index}",
                session,
            )
            for index, session in enumerate(main, start=1)
        )
    else:
        lines.append("主睡眠: 未取得")
    if naps:
        lines.extend(
            _format_sleep_session(f"昼寝 {index}", session)
            for index, session in enumerate(naps, start=1)
        )
    else:
        lines.append("昼寝: なし")
    lines.extend(
        _format_sleep_session(f"未分類の睡眠 {index}", session)
        for index, session in enumerate(unknown, start=1)
    )
    lines.append("")
    return lines


def render_notion_body(view: DailyView) -> str:
    """Render a stable, compact table intended for ChatGPT consumption."""

    metrics = list(view.metrics)
    present = {(metric.metric_name, metric.statistic) for metric in metrics}
    metrics.extend(
        DailyMetric(
            metric_name=metric_name,
            statistic="value",
            value=None,
            unit=_NOTION_METRIC_UNITS[metric_name],
            status="MISSING",
            missing_reason="NOT_QUERIED",
        )
        for metric_name in _NOTION_METRIC_LABELS
        if (metric_name, "value") not in present
    )

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
        f"Time zone: {view.time_zone or 'unknown'}",
        f"Time zone source: {view.time_zone_source or 'unknown'}",
        "Physical day: "
        + (
            f"{view.physical_day_start.isoformat()} / {view.physical_day_end.isoformat()}"
            if view.physical_day_start is not None and view.physical_day_end is not None
            else "unknown"
        ),
        "",
        *_sleep_session_lines(view),
        (
            "| Metric | Value | Unit | Calendar week avg "
            "(Mon-date; final Sun) | Week coverage | Week status | "
            "Calendar month avg (1st-date) | Month coverage | Month status | "
            "Daily status | Daily coverage |"
        ),
        "|---|---:|---|---:|---:|---|---:|---:|---|---|---:|",
    ]
    for metric in sorted(metrics, key=_notion_metric_sort_key):
        metric_label = _NOTION_METRIC_LABELS.get(metric.metric_name, metric.metric_name)
        label = (
            metric_label if metric.statistic == "value" else f"{metric_label} ({metric.statistic})"
        )
        coverage = f"{_format_coverage(metric.coverage)} ({metric.coverage_kind})"
        flags = ",".join(sorted(metric.quality_flags))
        missing = "" if metric.missing_reason is None else f"; {metric.missing_reason}"
        status = metric.status + missing if not flags else f"{metric.status}{missing}; {flags}"
        weekly_flags = ",".join(sorted(metric.weekly_quality_flags))
        weekly_status = metric.weekly_status or "unknown"
        if weekly_flags:
            weekly_status += f"; {weekly_flags}"
        monthly_flags = ",".join(sorted(metric.monthly_quality_flags))
        monthly_status = metric.monthly_status or "unknown"
        if monthly_flags:
            monthly_status += f"; {monthly_flags}"
        lines.append(
            "| "
            + " | ".join(
                (
                    label,
                    _format_value(metric.value),
                    metric.unit or "",
                    _format_value(metric.weekly_average),
                    _format_coverage(metric.weekly_coverage),
                    weekly_status,
                    _format_value(metric.monthly_average),
                    _format_coverage(metric.monthly_coverage),
                    monthly_status,
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
    through_date: str | None = None

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
    bar_width = 40
    gap = 52
    maximum = max((sample.steps or 0 for sample in ordered), default=0) or 1
    parts = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
            f'height="{height}" viewBox="0 0 {width} {height}" '
            'role="img" aria-label="Steps over the last 7 days">'
        ),
        "<style>"
        'text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans",'
        "Helvetica,Arial,sans-serif;fill:#f0f6fc}"
        ".bar{fill:#84e17a}.missing{fill:#656d76}"
        "</style>",
    ]
    for index, sample in enumerate(ordered):
        value = sample.steps
        numeric = value or 0
        bar_height = round(plot_height * numeric / maximum, 2)
        x = 54 + index * (bar_width + gap)
        y = plot_top + plot_height - bar_height
        css_class = "missing" if value is None else "bar"
        label = "—" if value is None else f"{value:,}"
        parts.extend(
            (
                (
                    f'<rect class="{css_class}" x="{x}" y="{y}" '
                    f'width="{bar_width}" height="{bar_height}"/>'
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
    """Publish one immutable profile SVG for each completed civil day."""

    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    def publish(self, *, generation: int, samples: Sequence[StepSample]) -> bool:
        ordered = tuple(sorted(samples, key=lambda sample: sample.metric_date))
        if len(ordered) != 7:
            raise ValueError("steps SVG requires exactly seven daily samples")
        through_date = ordered[-1].metric_date

        current = read_state(self._store, STEPS_HEAD_PATH)
        if current is not None:
            pointer = StepsPointer.from_bytes(current.data)
            if generation < pointer.generation:
                return False
            current_rendering_prefix = (
                f"public/steps/canonicalization={STEPS_SVG_CANONICALIZATION}/"
            )
            rendering_is_current = pointer.svg_uri.startswith(current_rendering_prefix)
            # A legacy pointer without a completed-day cursor may migrate once
            # at the same generation. Its replacement follows the date guard.
            if pointer.through_date is not None:
                try:
                    published_through = date.fromisoformat(pointer.through_date)
                except ValueError as exc:
                    raise ViewUpdateError("invalid steps pointer through_date") from exc
                if rendering_is_current and generation <= pointer.generation:
                    return False
                if rendering_is_current and through_date <= published_through:
                    return False
                if not rendering_is_current and through_date < published_through:
                    return False
            expected_generation = current.generation
        else:
            expected_generation = 0

        svg = render_steps_svg(ordered)
        digest = hashlib.sha256(svg).hexdigest()
        uri = (
            f"public/steps/canonicalization={STEPS_SVG_CANONICALIZATION}/"
            f"sha256/{digest[:2]}/{digest}.svg"
        )
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

        pointer = StepsPointer(
            generation=generation,
            svg_uri=uri,
            content_sha256=digest,
            updated_at=_now_timestamp(),
            through_date=through_date.isoformat(),
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
