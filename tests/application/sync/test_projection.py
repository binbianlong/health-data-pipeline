from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from health_data_pipeline.application.sync.ingestion import IngestedQuery
from health_data_pipeline.application.sync.migrations import (
    DailyRevisionRows,
    upgrade_preserved_sleep_rows,
)
from health_data_pipeline.application.sync.projection import DailyProjector
from health_data_pipeline.commit import CommitCoordinator, CommitRequest
from health_data_pipeline.daily import DayContext, Observation, SourceQueryStatus, build_daily_rows
from health_data_pipeline.models import CommitKind, ContentReference
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import (
    ContentAddressedStore,
    HeadRepository,
    InMemoryObjectStore,
)
from tests.support.pipeline import NORMALIZER_VERSION, NOW, hrv_detail_point

METRIC_DATE = date(2026, 8, 2)
DAY_START = datetime(2026, 8, 1, 15, tzinfo=UTC)
DAY_END = datetime(2026, 8, 2, 15, tzinfo=UTC)


def _v2_sleep_session(*, is_main: bool = False, is_nap: bool = True) -> dict[str, object]:
    return {
        "record_kind": "SLEEP_SESSION",
        "metric_name": "sleep_session",
        "statistic": "observed",
        "period_kind": "SESSION",
        "dimension_key": "sleep:legacy",
        "value_num": Decimal(45),
        "value_text": None,
        "unit": "min",
        "aggregation": "session_value",
        "period_start": METRIC_DATE,
        "period_end": METRIC_DATE,
        "sample_count": 1,
        "expected_count": 1,
        "coverage_ratio": Decimal(1),
        "data_status": "CANONICAL_SYNCED",
        "quality_flags": [],
        "source_data_types": ["sleep"],
        "fitbit_last_sync_time": DAY_END,
        "time_zone": "Asia/Tokyo",
        "physical_day_start": DAY_START,
        "physical_day_end": DAY_END,
        "logic_version": "daily-v4",
        "session_start_time": datetime(2026, 8, 2, 3, tzinfo=UTC),
        "session_end_time": datetime(2026, 8, 2, 3, 45, tzinfo=UTC),
        "session_start_utc_offset_seconds": 32_400,
        "session_end_utc_offset_seconds": 32_400,
        "session_is_main": is_main,
        "session_is_nap": is_nap,
    }


def test_v2_nap_is_upgraded_without_inventing_its_provenance() -> None:
    upgraded = upgrade_preserved_sleep_rows(
        DailyRevisionRows(schema_version=2, rows=(_v2_sleep_session(),)),
        metric_date=METRIC_DATE,
    )

    assert upgraded[0]["session_role"] == "NAP"
    assert upgraded[0]["session_role_source"] == "UNCLASSIFIED"
    assert upgraded[0]["coverage_kind"] == "NOT_APPLICABLE"
    assert upgraded[0]["time_zone_source"] == "LEGACY_UNKNOWN"


def test_v1_has_no_preservable_sleep_rows() -> None:
    assert (
        upgrade_preserved_sleep_rows(
            DailyRevisionRows(schema_version=1, rows=({},)),
            metric_date=METRIC_DATE,
        )
        == ()
    )


def test_partial_detail_ingestion_cannot_replace_existing_daily_summary() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(ContentAddressedStore(store))
    metric_date = date(2026, 7, 28)
    daily_summary = Observation(
        metric_date=metric_date,
        metric_name="hrv_rmssd",
        value=Decimal(42),
        unit="ms",
        aggregation="daily_value",
        source_data_types=("daily-heart-rate-variability",),
    )
    context = DayContext(
        metric_date=metric_date,
        fitbit_last_sync_time=NOW,
        time_zone="Asia/Tokyo",
        physical_day_start=datetime(2026, 7, 27, 15, tzinfo=UTC),
        physical_day_end=datetime(2026, 7, 28, 15, tzinfo=UTC),
        logic_version=NORMALIZER_VERSION,
    )
    committed = CommitCoordinator(heads=heads, snapshots=snapshots).commit(
        CommitRequest(
            daily_rows={
                metric_date: build_daily_rows(
                    context,
                    (daily_summary,),
                    {metric_date: (daily_summary,)},
                )
            },
            commit_kind=CommitKind.SCHEDULED,
            created_at=NOW,
            daily_schema_version=3,
        ),
        expected_base=heads.read(),
    )
    detail_status = SourceQueryStatus(
        data_type="heart-rate-variability",
        status="SUCCESS_WITH_DATA",
        source_count=1,
        normalized_count=1,
    )

    merged = DailyProjector(
        snapshots=snapshots,
        normalizer_version=NORMALIZER_VERSION,
    ).merge_changed_observations(
        committed.root_snapshot,
        (
            IngestedQuery(
                data_type="heart-rate-variability",
                metric_date=metric_date,
                data_points=(hrv_detail_point(metric_date, 35),),
                manifest=ContentReference(
                    uri="manifests/ingestion/detail-only.json.gz",
                    canonical_sha256="a" * 64,
                    object_sha256="b" * 64,
                ),
                replacement_scope={
                    "data_type": "heart-rate-variability",
                    "metric_date": metric_date.isoformat(),
                },
                source_status=detail_status,
            ),
        ),
        source_statuses={metric_date: (detail_status,)},
    )

    hrv = next(
        observation for observation in merged[metric_date] if observation.metric_name == "hrv_rmssd"
    )
    assert hrv.value == 42
    assert hrv.source_data_types == ("daily-heart-rate-variability",)
