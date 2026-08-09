from __future__ import annotations

import json
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from health_data_pipeline.application.sync.ingestion import (
    deduplicate_data_points,
    unique_manifest_references,
)
from health_data_pipeline.models import ContentReference
from health_data_pipeline.storage import InMemoryObjectStore
from tests.support.pipeline import (
    NOW,
    FakeHealth,
    build_ingestor,
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


def test_named_non_sleep_data_points_reject_conflicting_page_overlap() -> None:
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
