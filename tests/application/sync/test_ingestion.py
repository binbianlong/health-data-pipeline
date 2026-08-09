from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from health_data_pipeline.application.sync.ingestion import (
    deduplicate_data_points,
    unique_manifest_references,
)
from health_data_pipeline.google_health import DataPointPage, FetchWindow
from health_data_pipeline.models import ContentReference
from health_data_pipeline.storage import ContentAddressedStore, InMemoryObjectStore
from tests.support.pipeline import (
    NOW,
    FakeHealth,
    build_ingestor,
    respiratory_rate_point,
    sleep_point,
    steps_point,
)


def test_physical_replacement_scope_matches_the_exact_query_window() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(values={})
    ingestor = build_ingestor(store, health)

    (query,) = ingestor.ingest_dates(
        "steps",
        (date(2026, 7, 27),),
        zone=ZoneInfo("Asia/Tokyo"),
        expected_generation=1,
    )

    assert query.replacement_scope == {
        "data_type": "steps",
        "kind": "PHYSICAL_WINDOW_REPLACE",
        "physical_start": "2026-07-26T15:00:00Z",
        "physical_end": "2026-07-27T15:00:00Z",
    }


def test_ingestor_heartbeats_after_each_api_page() -> None:
    events: list[str] = []

    class PagedHealth:
        def iter_data_point_pages(
            self,
            data_type: str,
            window: FetchWindow,
        ) -> Iterable[DataPointPage]:
            for page_number in range(2):
                events.append(f"page-{page_number}")
                yield DataPointPage(
                    data_type=data_type,
                    window=window,
                    page_number=page_number,
                    data_points=(),
                    next_page_token="next" if page_number == 0 else None,
                    raw_payload={"dataPoints": [], "page": page_number},
                )

    store = InMemoryObjectStore(clock=lambda: NOW)
    ingestor = build_ingestor(store, PagedHealth())

    ingestor.ingest_dates(
        "steps",
        (date(2026, 7, 28),),
        zone=ZoneInfo("Asia/Tokyo"),
        expected_generation=1,
        heartbeat=lambda: events.append("heartbeat"),
    )

    assert events == ["page-0", "heartbeat", "page-1", "heartbeat"]


def test_ingestor_stops_before_the_next_page_when_heartbeat_fails() -> None:
    fetched_pages: list[int] = []

    class PagedHealth:
        def iter_data_point_pages(
            self,
            data_type: str,
            window: FetchWindow,
        ) -> Iterable[DataPointPage]:
            for page_number in range(2):
                fetched_pages.append(page_number)
                yield DataPointPage(
                    data_type=data_type,
                    window=window,
                    page_number=page_number,
                    data_points=(),
                    next_page_token="next" if page_number == 0 else None,
                    raw_payload={"dataPoints": [], "page": page_number},
                )

    store = InMemoryObjectStore(clock=lambda: NOW)
    ingestor = build_ingestor(store, PagedHealth())

    def reject_heartbeat() -> None:
        raise RuntimeError("lease ownership was lost")

    with pytest.raises(RuntimeError, match="ownership was lost"):
        ingestor.ingest_dates(
            "steps",
            (date(2026, 7, 28),),
            zone=ZoneInfo("Asia/Tokyo"),
            expected_generation=1,
            heartbeat=reject_heartbeat,
        )

    assert fetched_pages == [0]


def test_manifest_references_are_deduplicated_by_uri() -> None:
    reference = ContentReference(
        uri="manifests/ingestion/example.json.gz",
        canonical_sha256="a" * 64,
        object_sha256="b" * 64,
    )

    assert unique_manifest_references((reference, reference)) == (reference,)


def test_named_non_sleep_data_points_drop_exact_page_overlap() -> None:
    metric_date = date(2026, 7, 28)
    point = {
        "dataPointName": "users/me/dataPoints/steps-1",
        **steps_point(metric_date, 8_000),
    }

    selected, duplicate_count = deduplicate_data_points(
        "steps",
        (point, json.loads(json.dumps(point))),
    )

    assert selected == (point,)
    assert duplicate_count == 1


def test_anonymous_interval_data_points_reject_ambiguous_exact_duplicate() -> None:
    point = steps_point(date(2026, 7, 28), 8_000)

    with pytest.raises(ValueError, match="ambiguous duplicate"):
        deduplicate_data_points(
            "steps",
            (point, json.loads(json.dumps(point))),
        )


def test_named_interval_data_points_reject_conflicting_payloads() -> None:
    metric_date = date(2026, 7, 28)
    first = {
        "dataPointName": "users/me/dataPoints/steps-1",
        **steps_point(metric_date, 8_000),
    }
    conflicting = {
        "dataPointName": "users/me/dataPoints/steps-1",
        **steps_point(metric_date, 8_001),
    }

    with pytest.raises(
        ValueError,
        match="reconciled data point identity has conflicting payloads",
    ):
        deduplicate_data_points("steps", (first, conflicting))


def test_reused_sample_name_keeps_observations_at_distinct_times() -> None:
    metric_date = date(2026, 7, 28)
    first = respiratory_rate_point(metric_date, hour=1, breaths_per_minute=15)
    second = respiratory_rate_point(metric_date, hour=6, breaths_per_minute=16)

    selected, duplicate_count = deduplicate_data_points(
        "respiratory-rate-sleep-summary",
        (first, second),
    )

    assert selected == (first, second)
    assert duplicate_count == 0


def test_production_respiratory_shape_keeps_distinct_sleep_samples() -> None:
    shared_name = (
        "users/health-user/dataTypes/respiratory-rate-sleep-summary/dataPoints/shared-summary"
    )
    first = {
        "dataPointName": shared_name,
        "respiratoryRateSleepSummary": {
            "sampleTime": {
                "physicalTime": "2026-07-27T22:12:00Z",
                "utcOffset": "32400s",
                "civilTime": {
                    "date": {"year": 2026, "month": 7, "day": 28},
                    "time": {"hours": 7, "minutes": 12},
                },
            },
            "deepSleepStats": {
                "breathsPerMinute": 14.5,
                "standardDeviation": 1.0,
                "signalToNoise": 8.0,
            },
            "lightSleepStats": {
                "breathsPerMinute": 15.0,
                "standardDeviation": 1.1,
                "signalToNoise": 7.5,
            },
            "remSleepStats": {
                "breathsPerMinute": 15.5,
                "standardDeviation": 1.2,
                "signalToNoise": 7.0,
            },
            "fullSleepStats": {
                "breathsPerMinute": 15.0,
                "standardDeviation": 1.1,
                "signalToNoise": 7.5,
            },
        },
    }
    second: dict[str, Any] = json.loads(json.dumps(first))
    second_summary = second["respiratoryRateSleepSummary"]
    second_summary["sampleTime"] = {
        "physicalTime": "2026-07-28T04:43:00Z",
        "utcOffset": "32400s",
        "civilTime": {
            "date": {"year": 2026, "month": 7, "day": 28},
            "time": {"hours": 13, "minutes": 43},
        },
    }
    second_summary["lightSleepStats"]["breathsPerMinute"] = 15.2
    second_summary["remSleepStats"]["signalToNoise"] = 7.2

    selected, duplicate_count = deduplicate_data_points(
        "respiratory-rate-sleep-summary",
        (first, second, json.loads(json.dumps(first))),
    )

    assert selected == (first, second)
    assert duplicate_count == 1


def test_reused_sample_identity_rejects_different_values_at_the_same_time() -> None:
    metric_date = date(2026, 7, 28)
    first = respiratory_rate_point(metric_date, hour=1, breaths_per_minute=15)
    second = respiratory_rate_point(metric_date, hour=1, breaths_per_minute=16)

    with pytest.raises(ValueError, match="sample identity has conflicting payloads"):
        deduplicate_data_points(
            "respiratory-rate-sleep-summary",
            (first, second),
        )


def test_reused_sample_name_drops_only_an_exact_clone() -> None:
    point = respiratory_rate_point(
        date(2026, 7, 28),
        hour=1,
        breaths_per_minute=15,
    )

    selected, duplicate_count = deduplicate_data_points(
        "respiratory-rate-sleep-summary",
        (point, json.loads(json.dumps(point))),
    )

    assert selected == (point,)
    assert duplicate_count == 1


def test_anonymous_sample_drops_only_an_exact_clone() -> None:
    point = dict(
        respiratory_rate_point(
            date(2026, 7, 28),
            hour=1,
            breaths_per_minute=15,
        )
    )
    point.pop("dataPointName")

    selected, duplicate_count = deduplicate_data_points(
        "respiratory-rate-sleep-summary",
        (point, json.loads(json.dumps(point))),
    )

    assert selected == (point,)
    assert duplicate_count == 1


def test_anonymous_sample_rejects_conflicting_payload_at_the_same_time() -> None:
    first = dict(
        respiratory_rate_point(
            date(2026, 7, 28),
            hour=1,
            breaths_per_minute=15,
        )
    )
    second = dict(
        respiratory_rate_point(
            date(2026, 7, 28),
            hour=1,
            breaths_per_minute=16,
        )
    )
    first.pop("dataPointName")
    second.pop("dataPointName")

    with pytest.raises(ValueError, match="sample identity has conflicting payloads"):
        deduplicate_data_points(
            "respiratory-rate-sleep-summary",
            (first, second),
        )


def test_ingestor_commits_reused_sample_name_as_two_source_rows() -> None:
    metric_date = date(2026, 7, 28)
    points = (
        respiratory_rate_point(metric_date, hour=1, breaths_per_minute=15),
        respiratory_rate_point(metric_date, hour=6, breaths_per_minute=16),
    )
    store = InMemoryObjectStore(clock=lambda: NOW)
    health = FakeHealth(
        values={},
        data_points={("respiratory-rate-sleep-summary", metric_date): points},
    )

    (query,) = build_ingestor(store, health).ingest_dates(
        "respiratory-rate-sleep-summary",
        (metric_date,),
        zone=ZoneInfo("Asia/Tokyo"),
        expected_generation=1,
    )
    manifest = json.loads(ContentAddressedStore(store).read(query.manifest))

    assert query.data_points == points
    assert query.source_status.source_count == 2
    assert query.source_status.normalized_count == 1
    assert manifest["source_stream_row_count"] == 2
    assert manifest["flattened_source_row_count"] == 2
    assert manifest["duplicate_source_row_count"] == 0


def test_sleep_named_revisions_keep_the_latest_update() -> None:
    metric_date = date(2026, 7, 28)
    older: dict[str, Any] = json.loads(
        json.dumps(
            sleep_point(
                metric_date,
                data_point_name="users/me/dataPoints/sleep-1",
                start_time="2026-07-27T14:30:00Z",
                end_time="2026-07-27T22:30:00Z",
                minutes_asleep=400,
                main_sleep=True,
                nap=False,
            )
        )
    )
    newer: dict[str, Any] = json.loads(
        json.dumps(
            sleep_point(
                metric_date,
                data_point_name="users/me/dataPoints/sleep-1",
                start_time="2026-07-27T14:30:00Z",
                end_time="2026-07-27T22:30:00Z",
                minutes_asleep=420,
                main_sleep=True,
                nap=False,
            )
        )
    )
    older["sleep"]["updateTime"] = "2026-07-28T10:00:00Z"
    newer["sleep"]["updateTime"] = "2026-07-28T11:00:00Z"

    selected, duplicate_count = deduplicate_data_points(
        "sleep",
        (older, newer),
    )

    assert selected == (newer,)
    assert duplicate_count == 1


def test_sleep_anonymous_exact_page_overlap_is_reconciled_once() -> None:
    point: dict[str, Any] = json.loads(
        json.dumps(
            sleep_point(
                date(2026, 7, 28),
                data_point_name="users/me/dataPoints/sleep-1",
                start_time="2026-07-27T14:30:00Z",
                end_time="2026-07-27T22:30:00Z",
                minutes_asleep=420,
                main_sleep=True,
                nap=False,
            )
        )
    )
    point.pop("dataPointName")

    selected, duplicate_count = deduplicate_data_points(
        "sleep",
        (point, json.loads(json.dumps(point))),
    )

    assert selected == (point,)
    assert duplicate_count == 1
