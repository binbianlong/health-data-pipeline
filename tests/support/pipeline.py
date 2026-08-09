"""Focused factories shared by synchronization application tests."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from health_data_pipeline.application.sync.ingestion import SourceIngestor
from health_data_pipeline.application.sync.projection import DailyProjector
from health_data_pipeline.commit import CommitCoordinator
from health_data_pipeline.google_health import DataPointPage, FetchWindow
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import (
    ContentAddressedStore,
    HeadRepository,
    InMemoryObjectStore,
)

if TYPE_CHECKING:
    from health_data_pipeline.pipeline import IncrementalSyncPipeline

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
SYNC_TIME = datetime(2026, 7, 28, 11, tzinfo=UTC)
NORMALIZER_VERSION = "daily-v5"
DAILY_SCHEMA_VERSION = 3


def civil_date(value: date) -> dict[str, Any]:
    return {
        "year": value.year,
        "month": value.month,
        "day": value.day,
    }


def steps_point(value: date, count: int) -> Mapping[str, Any]:
    return {
        "steps": {
            "count": count,
            "interval": {
                "civilStartTime": {
                    "date": civil_date(value),
                }
            },
        }
    }


def sleep_point(
    metric_date: date,
    *,
    data_point_name: str,
    start_time: str,
    end_time: str,
    minutes_asleep: int,
    main_sleep: bool,
    nap: bool,
) -> Mapping[str, Any]:
    return {
        "dataPointName": data_point_name,
        "sleep": {
            "interval": {
                "civilEndTime": {"date": civil_date(metric_date)},
                "startTime": start_time,
                "startUtcOffset": "32400s",
                "endTime": end_time,
                "endUtcOffset": "32400s",
            },
            "metadata": {"mainSleep": main_sleep, "nap": nap},
            "summary": {"minutesAsleep": str(minutes_asleep)},
            "updateTime": "2026-07-28T11:00:00Z",
        },
    }


def hrv_detail_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "heartRateVariability": {
            "rootMeanSquareOfSuccessiveDifferencesMilliseconds": value,
            "sampleTime": {"civilTime": {"date": civil_date(metric_date)}},
        }
    }


def daily_hrv_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "dailyHeartRateVariability": {
            "averageHeartRateVariabilityMilliseconds": value,
            "date": civil_date(metric_date),
        }
    }


def spo2_detail_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "oxygenSaturation": {
            "percentage": value,
            "sampleTime": {"civilTime": {"date": civil_date(metric_date)}},
        }
    }


def daily_spo2_point(metric_date: date, value: int) -> Mapping[str, Any]:
    return {
        "dailyOxygenSaturation": {
            "averagePercentage": value,
            "date": civil_date(metric_date),
        }
    }


class FakeHealth:
    def __init__(
        self,
        *,
        values: Mapping[date, int],
        data_points: Mapping[
            tuple[str, date],
            Sequence[Mapping[str, Any]],
        ]
        | None = None,
        sync_time: datetime | None = SYNC_TIME,
        fail_after: int | None = None,
    ) -> None:
        self.values = dict(values)
        self.data_points = {key: tuple(points) for key, points in (data_points or {}).items()}
        self.sync_time = sync_time
        self.fail_after = fail_after
        self.requests: list[tuple[str, FetchWindow]] = []

    def get_settings(self) -> Mapping[str, Any]:
        return {"timeZone": "Asia/Tokyo"}

    def latest_fitbit_sync_time(self) -> datetime | None:
        return self.sync_time

    def iter_data_point_pages(
        self,
        data_type: str,
        window: FetchWindow,
    ) -> Sequence[DataPointPage]:
        self.requests.append((data_type, window))
        if self.fail_after is not None and len(self.requests) > self.fail_after:
            raise RuntimeError("injected API failure")
        points: tuple[Mapping[str, Any], ...] = ()
        if data_type == "steps" and window.civil_start in self.values:
            assert window.civil_start is not None
            points = (
                steps_point(
                    window.civil_start,
                    self.values[window.civil_start],
                ),
            )
        if window.civil_start is not None and window.civil_end is not None:
            points += tuple(
                point
                for (point_type, point_date), stored_points in sorted(
                    self.data_points.items(),
                    key=lambda item: item[0],
                )
                if point_type == data_type and window.civil_start <= point_date < window.civil_end
                for point in stored_points
            )
        return (
            DataPointPage(
                data_type=data_type,
                window=window,
                page_number=0,
                data_points=points,
                next_page_token=None,
                raw_payload={"dataPoints": list(points)},
            ),
        )


def build_pipeline(
    store: InMemoryObjectStore,
    health: FakeHealth,
    *,
    now: datetime = NOW,
    backfill_ready: Callable[[int], bool] | None = None,
) -> tuple[
    IncrementalSyncPipeline,
    HeadRepository,
    SnapshotRepository,
]:
    from health_data_pipeline.pipeline import IncrementalSyncPipeline  # noqa: PLC0415

    content = ContentAddressedStore(store)
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(content)
    return (
        IncrementalSyncPipeline(
            store=store,
            content=content,
            heads=heads,
            snapshots=snapshots,
            commits=CommitCoordinator(heads=heads, snapshots=snapshots),
            health=health,
            owner_hmac="a" * 64,
            backfill_ready=backfill_ready,
            now=lambda: now,
        ),
        heads,
        snapshots,
    )


def build_ingestor(store: InMemoryObjectStore, health: FakeHealth) -> SourceIngestor:
    return SourceIngestor(
        health=health,
        content=ContentAddressedStore(store),
        daily_schema_version=DAILY_SCHEMA_VERSION,
        normalizer_version=NORMALIZER_VERSION,
    )


def build_projector(snapshots: SnapshotRepository) -> DailyProjector:
    return DailyProjector(
        snapshots=snapshots,
        normalizer_version=NORMALIZER_VERSION,
    )


def daily_rows(
    heads: HeadRepository,
    snapshots: SnapshotRepository,
    metric_date: date,
) -> list[dict[str, Any]]:
    head = heads.read().head
    assert head is not None
    root = snapshots.load_root_snapshot(head.root_snapshot)
    shard = snapshots.load_year_shard(root.state.daily_year_shards[str(metric_date.year)])
    revision = shard.daily_revisions[metric_date.isoformat()]
    return [json.loads(line) for line in snapshots.read_daily_revision(revision).splitlines()]


def store_webhook_event(
    store: InMemoryObjectStore,
    *,
    data_type: str,
    metric_date: date,
) -> None:
    event = {
        "dataType": data_type,
        "intervals": [
            {
                "civilDateTimeInterval": {
                    "startDateTime": {"date": civil_date(metric_date)},
                    "endDateTime": {"date": civil_date(metric_date)},
                }
            }
        ],
    }
    store.put(
        f"webhook-events/{data_type}.json",
        json.dumps(event).encode(),
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )
