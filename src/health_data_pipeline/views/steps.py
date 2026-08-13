"""Deterministic publication of the public seven-day steps SVG."""

from __future__ import annotations

import hashlib
import hmac
import html
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date

from health_data_pipeline.state.objects import ObjectNotFound, ObjectStore, PreconditionFailed
from health_data_pipeline.state.versioned import (
    read_state,
    utc_timestamp,
    write_state,
)
from health_data_pipeline.views.models import (
    StepSample,
    ViewUpdateError,
    canonical_view_json,
)

STEPS_HEAD_PATH = "state/views/steps-head.json"
STEPS_SVG_CANONICALIZATION = "fhcj-3"
STEPS_SAMPLE_DAYS = 7


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
        return canonical_view_json(asdict(self))


@dataclass(frozen=True, slots=True)
class _RenderedStepsSvg:
    data: bytes
    content_sha256: str
    uri: str


def _validated_samples(
    samples: Sequence[StepSample],
) -> tuple[tuple[StepSample, ...], date]:
    ordered = tuple(sorted(samples, key=lambda sample: sample.metric_date))
    if len(ordered) != STEPS_SAMPLE_DAYS:
        raise ValueError("steps SVG requires exactly seven daily samples")
    return ordered, ordered[-1].metric_date


def render_steps_svg(samples: Sequence[StepSample]) -> bytes:
    """Render deterministic, dependency-free SVG for exactly seven days."""

    ordered = tuple(sorted(samples, key=lambda sample: sample.metric_date))
    if len(ordered) != STEPS_SAMPLE_DAYS:
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
        "Helvetica,Arial,sans-serif;fill:#8b949e}"
        ".bar{fill:#45A12B}.missing{fill:#656d76}"
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


def _rendered_steps_svg(samples: Sequence[StepSample]) -> _RenderedStepsSvg:
    data = render_steps_svg(samples)
    digest = hashlib.sha256(data).hexdigest()
    return _RenderedStepsSvg(
        data=data,
        content_sha256=digest,
        uri=(
            f"public/steps/canonicalization={STEPS_SVG_CANONICALIZATION}/"
            f"sha256/{digest[:2]}/{digest}.svg"
        ),
    )


class StepsSvgService:
    """Publish one immutable profile SVG for each completed civil day."""

    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    def publish(self, *, generation: int, samples: Sequence[StepSample]) -> bool:
        ordered, through_date = _validated_samples(samples)
        expected_generation = self._expected_head_generation(
            generation=generation,
            through_date=through_date,
        )
        if expected_generation is None:
            return False
        rendering = _rendered_steps_svg(ordered)
        self._publish_rendering(
            generation=generation,
            through_date=through_date,
            rendering=rendering,
            expected_generation=expected_generation,
        )
        return True

    def _expected_head_generation(
        self,
        *,
        generation: int,
        through_date: date,
    ) -> int | None:
        current = read_state(self._store, STEPS_HEAD_PATH)
        if current is None:
            return 0
        pointer = StepsPointer.from_bytes(current.data)
        if generation < pointer.generation:
            return None
        current_rendering_prefix = f"public/steps/canonicalization={STEPS_SVG_CANONICALIZATION}/"
        rendering_is_current = pointer.svg_uri.startswith(current_rendering_prefix)
        if not self._allows_publication(
            pointer=pointer,
            generation=generation,
            through_date=through_date,
            rendering_is_current=rendering_is_current,
        ):
            return None
        return current.generation

    @classmethod
    def _allows_publication(
        cls,
        *,
        pointer: StepsPointer,
        generation: int,
        through_date: date,
        rendering_is_current: bool,
    ) -> bool:
        # A legacy pointer without a completed-day cursor may migrate once
        # at the same generation. Its replacement follows the date guard.
        if pointer.through_date is None:
            return True
        published_through = cls._published_through_date(pointer.through_date)
        if rendering_is_current:
            return generation > pointer.generation and through_date > published_through
        return through_date >= published_through

    @staticmethod
    def _published_through_date(value: str) -> date:
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ViewUpdateError("invalid steps pointer through_date") from exc

    def _publish_rendering(
        self,
        *,
        generation: int,
        through_date: date,
        rendering: _RenderedStepsSvg,
        expected_generation: int,
    ) -> None:
        self._put_content_addressed_svg(rendering)
        pointer = StepsPointer(
            generation=generation,
            svg_uri=rendering.uri,
            content_sha256=rendering.content_sha256,
            updated_at=utc_timestamp(),
            through_date=through_date.isoformat(),
        )
        write_state(
            self._store,
            STEPS_HEAD_PATH,
            pointer.to_bytes(),
            if_generation_match=expected_generation,
        )

    def _put_content_addressed_svg(self, rendering: _RenderedStepsSvg) -> None:
        try:
            self._store.put(
                rendering.uri,
                rendering.data,
                if_generation_match=0,
                metadata={"content-sha256": rendering.content_sha256},
                content_type="image/svg+xml; charset=utf-8",
                cache_control="public, max-age=31536000, immutable",
            )
        except PreconditionFailed:
            try:
                existing_svg = self._store.get(rendering.uri)
            except ObjectNotFound as exc:
                raise ViewUpdateError(
                    "content-addressed SVG disappeared after create conflict"
                ) from exc
            if existing_svg.data != rendering.data:
                raise ViewUpdateError("content-addressed SVG object mismatch") from None

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
    """Compare pointer hashes without leaking timing differences."""

    return hmac.compare_digest(left, right)
