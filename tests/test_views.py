from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from health_data_pipeline.commit import CommitCoordinator, CommitRequest
from health_data_pipeline.models import CommitKind
from health_data_pipeline.notion_adapter import SnapshotViewSource
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import ContentAddressedStore, HeadRepository, InMemoryObjectStore
from health_data_pipeline.views import (
    DailyMetric,
    DailyView,
    NotionPage,
    NotionViewService,
    StepSample,
    StepsPointer,
    StepsSvgService,
    render_notion_body,
)


class FakeSource:
    def __init__(self, generation: int, views: list[DailyView]) -> None:
        self.generation = generation
        self.views = views

    def latest_generation(self) -> int:
        return self.generation

    def views_between(
        self,
        *,
        after_generation: int,
        through_generation: int,
    ) -> list[DailyView]:
        assert after_generation < through_generation
        assert through_generation == self.generation
        return self.views


class FakeNotion:
    def __init__(self) -> None:
        self.pages: dict[date, NotionPage] = {}
        self.writes: list[tuple[DailyView, str]] = []

    def find_daily_page(self, metric_date: date) -> NotionPage | None:
        return self.pages.get(metric_date)

    def upsert_daily_page(
        self,
        *,
        existing_page_id: str | None,
        view: DailyView,
        body: str,
        content_sha256: str,
    ) -> str:
        page_id = existing_page_id or f"page-{view.metric_date}"
        self.pages[view.metric_date] = NotionPage(
            page_id=page_id,
            source_generation=view.source_generation,
            content_sha256=content_sha256,
        )
        self.writes.append((view, body))
        return page_id


def _daily(generation: int = 7) -> DailyView:
    return DailyView(
        metric_date=date(2026, 7, 28),
        source_generation=generation,
        data_status="CANONICAL_SYNCED",
        fitbit_last_sync_time=datetime(2026, 7, 29, tzinfo=UTC),
        metrics=(
            DailyMetric(
                metric_name="steps",
                statistic="value",
                value=8000,
                unit="count",
                status="OK",
                weekly_average=7500,
                monthly_average=7200,
                coverage=1.0,
            ),
        ),
    )


def test_notion_applies_latest_once_and_uses_month_average() -> None:
    store = InMemoryObjectStore()
    notion = FakeNotion()
    service = NotionViewService(
        store=store,
        source=FakeSource(7, [_daily()]),
        notion=notion,
    )

    assert service.apply_latest(requested_generation=3) == 7
    assert service.apply_latest(requested_generation=7) == 7

    assert len(notion.writes) == 1
    assert "Calendar month avg" in notion.writes[0][1]
    assert "Calendar week avg (Mon-date; final Sun)" in notion.writes[0][1]
    assert "7200" in render_notion_body(_daily())
    assert store.get("state/notion/pages/year=2026/2026-07-28.json").data


def test_notion_renders_requested_metrics_in_fixed_japanese_order() -> None:
    requested = (
        ("sleep_minutes", "睡眠時間"),
        ("sleep_rem_minutes", "REM睡眠"),
        ("sleep_deep_minutes", "深い睡眠"),
        ("sleep_light_minutes", "浅い睡眠"),
        ("sleep_awake_minutes", "覚醒時間"),
        ("exercise_minutes", "運動時間"),
    )
    view = DailyView(
        metric_date=date(2026, 7, 28),
        source_generation=7,
        data_status="CANONICAL_SYNCED",
        fitbit_last_sync_time=datetime(2026, 7, 29, tzinfo=UTC),
        metrics=(
            *(
                DailyMetric(
                    metric_name=metric_name,
                    statistic="value",
                    value=index,
                    unit="min",
                    status="OK",
                )
                for index, (metric_name, _) in reversed(tuple(enumerate(requested, start=1)))
            ),
            DailyMetric(
                metric_name="steps",
                statistic="value",
                value=8_000,
                unit="count",
                status="OK",
            ),
        ),
    )

    body = render_notion_body(view)
    positions = [body.index(f"| {label} |") for _, label in requested]

    assert positions == sorted(positions)
    assert body.index("| 運動時間 |") < body.index("| steps |")
    for metric_name, label in requested:
        assert metric_name not in body
        assert label in body


def test_notion_marks_unavailable_requested_metrics_as_missing() -> None:
    body = render_notion_body(_daily())

    for label in ("睡眠時間", "REM睡眠", "深い睡眠", "浅い睡眠", "覚醒時間", "運動時間"):
        assert f"| {label} | 欠損 | min |" in body
    assert body.count("| MISSING |") == 6


def test_notion_never_overwrites_a_newer_page() -> None:
    store = InMemoryObjectStore()
    notion = FakeNotion()
    notion.pages[date(2026, 7, 28)] = NotionPage(
        page_id="newer", source_generation=8, content_sha256="new"
    )
    service = NotionViewService(
        store=store,
        source=FakeSource(7, [_daily()]),
        notion=notion,
    )

    service.apply_latest(7)

    assert notion.writes == []
    assert notion.pages[date(2026, 7, 28)].source_generation == 8


def test_steps_svg_pointer_is_content_addressed_and_monotonic() -> None:
    store = InMemoryObjectStore()
    service = StepsSvgService(store)
    samples = [StepSample(date(2026, 7, day), day * 1000) for day in range(22, 29)]

    assert service.publish(generation=9, samples=samples)
    assert not service.publish(generation=8, samples=samples)
    svg, generation = service.read_current()

    pointer = StepsPointer.from_bytes(store.get("state/views/steps-head.json").data)
    assert generation == 9
    assert pointer.generation == 9
    assert pointer.content_sha256 in pointer.svg_uri
    assert b"7" in svg
    assert b"health" not in svg.lower()
    assert b"Steps over the last 7 days" in svg
    assert b"#4493f8" in svg
    assert b"#238636" in svg
    assert b'fill="#fff"' not in svg
    assert "直近7日間の歩数".encode() not in svg


def _step_rows(value: int) -> list[dict[str, object]]:
    return [
        {
            "metric_name": "steps",
            "statistic": "observed",
            "period_kind": "DAY",
            "dimension_key": None,
            "value_num": Decimal(value),
            "unit": "count",
            "data_status": "CANONICAL_SYNCED",
        }
    ]


def test_snapshot_source_collects_all_dates_across_generation_gap() -> None:
    store = InMemoryObjectStore()
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(ContentAddressedStore(store))
    commits = CommitCoordinator(heads=heads, snapshots=snapshots)
    first_date = date(2026, 7, 27)
    second_date = date(2026, 7, 28)
    for rows in (
        {first_date: _step_rows(1_000)},
        {first_date: _step_rows(2_000)},
        {second_date: _step_rows(3_000)},
        {first_date: _step_rows(4_000)},
    ):
        commits.commit(
            CommitRequest(
                daily_rows=rows,
                commit_kind=CommitKind.SCHEDULED,
                created_at=datetime(2026, 7, 28, tzinfo=UTC),
                time_zone="Asia/Tokyo",
            ),
            expected_base=heads.read(),
        )

    source = SnapshotViewSource(heads=heads, snapshots=snapshots)
    views = source.views_between(
        after_generation=1,
        through_generation=4,
    )

    assert [view.metric_date for view in views] == [first_date, second_date]
    assert {view.source_generation for view in views} == {4}
    first_steps = next(
        metric.value
        for view in views
        if view.metric_date == first_date
        for metric in view.metrics
        if metric.metric_name == "steps"
    )
    assert first_steps == 4_000
