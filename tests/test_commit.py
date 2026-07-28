from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from health_data_pipeline.commit import CommitCoordinator, CommitRequest
from health_data_pipeline.models import CommitKind
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import (
    ContentAddressedStore,
    HeadRepository,
    InMemoryObjectStore,
    PreconditionFailed,
)

NOW = datetime(2026, 7, 28, tzinfo=UTC)


def _rows(value: int) -> list[dict[str, object]]:
    return [
        {
            "metric_name": "steps",
            "statistic": "observed",
            "period_kind": "DAY",
            "dimension_key": None,
            "value_num": Decimal(value),
            "period_start": date(2026, 7, 28),
            "period_end": date(2026, 7, 28),
        }
    ]


def _coordinator() -> tuple[CommitCoordinator, HeadRepository, SnapshotRepository]:
    store = InMemoryObjectStore(clock=lambda: NOW)
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(ContentAddressedStore(store))
    return CommitCoordinator(heads=heads, snapshots=snapshots), heads, snapshots


def test_genesis_commit_builds_head_root_and_year_shard() -> None:
    coordinator, heads, snapshots = _coordinator()
    result = coordinator.commit(
        CommitRequest(
            daily_rows={date(2026, 7, 28): _rows(8432)},
            commit_kind=CommitKind.SCHEDULED,
            created_at=NOW,
            time_zone="UTC",
        ),
        expected_base=heads.read(),
    )
    assert result.head_version.head is not None
    assert result.head_version.head.generation == 1
    root = snapshots.load_root_snapshot(result.head_version.head.root_snapshot)
    shard = snapshots.load_year_shard(root.state.daily_year_shards["2026"])
    assert "2026-07-28" in shard.daily_revisions
    assert root.change_set.commit_kind is CommitKind.INITIAL


def test_same_daily_content_reuses_year_shard() -> None:
    coordinator, heads, snapshots = _coordinator()
    first = coordinator.commit(
        CommitRequest(
            daily_rows={date(2026, 7, 28): _rows(8432)},
            commit_kind=CommitKind.SCHEDULED,
            created_at=NOW,
            time_zone="UTC",
        ),
        expected_base=heads.read(),
    )
    first_year = first.root_snapshot.state.daily_year_shards["2026"]
    second = coordinator.commit(
        CommitRequest(
            daily_rows={date(2026, 7, 28): _rows(8432)},
            commit_kind=CommitKind.SCHEDULED,
            created_at=NOW,
            webhook_cursor={"bucket": "next"},
            time_zone="UTC",
        ),
        expected_base=heads.read(),
    )
    assert second.root_snapshot.state.daily_year_shards["2026"] == first_year
    assert second.changed_metric_dates == ()
    current = heads.read().head
    assert current is not None
    assert snapshots.load_root_snapshot(current.root_snapshot).generation == 2


def test_stale_commit_cannot_adopt_and_overwrite_a_newer_head() -> None:
    coordinator, heads, snapshots = _coordinator()
    stale_base = heads.read()
    winner = coordinator.commit(
        CommitRequest(
            daily_rows={date(2026, 7, 28): _rows(8432)},
            commit_kind=CommitKind.SCHEDULED,
            created_at=NOW,
            time_zone="UTC",
        ),
        expected_base=stale_base,
    )

    with pytest.raises(PreconditionFailed):
        coordinator.commit(
            CommitRequest(
                daily_rows={date(2026, 7, 28): _rows(1)},
                commit_kind=CommitKind.SCHEDULED,
                created_at=NOW,
                time_zone="UTC",
            ),
            expected_base=stale_base,
        )

    current = heads.read().head
    assert current == winner.head_version.head
    assert current is not None
    root = snapshots.load_root_snapshot(current.root_snapshot)
    assert root.generation == 1
