"""One-shot construction and CAS publication of a logical data commit."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime

from pydantic import JsonValue

from health_data_pipeline.models import (
    ChangeSet,
    CommitKind,
    ContentReference,
    DailyRevisionReference,
    DataHead,
    RootSnapshot,
    SnapshotReferences,
    YearShard,
    YearShardReference,
)
from health_data_pipeline.snapshot import (
    SnapshotRepository,
    build_root_snapshot,
    build_year_shard,
    evolve_snapshot_state,
)
from health_data_pipeline.storage import HeadRepository, HeadVersion


@dataclass(frozen=True, slots=True)
class CommitRequest:
    """All validated inputs for one logical commit attempt."""

    daily_rows: Mapping[date, Sequence[Mapping[str, object]] | None]
    commit_kind: CommitKind
    created_at: datetime
    data_type_cursors: Mapping[str, JsonValue] | None = None
    webhook_cursor: Mapping[str, JsonValue] | None = None
    backfill_cursors: Mapping[str, JsonValue] | None = None
    fitbit_last_sync_time: datetime | None = None
    time_zone: str | None = None
    raw_replacements: tuple[JsonValue, ...] = ()
    ingestion_manifests: tuple[ContentReference, ...] = ()
    references: SnapshotReferences | None = None
    daily_schema_version: int = 1


@dataclass(frozen=True, slots=True)
class CommitResult:
    """A successfully committed HEAD and its immutable Root Snapshot."""

    head_version: HeadVersion
    root_snapshot: RootSnapshot
    changed_metric_dates: tuple[date, ...]


class CommitCoordinator:
    """Build a candidate once and perform exactly one HEAD CAS."""

    def __init__(
        self,
        *,
        heads: HeadRepository,
        snapshots: SnapshotRepository,
    ) -> None:
        self._heads = heads
        self._snapshots = snapshots

    def commit(
        self,
        request: CommitRequest,
        *,
        expected_base: HeadVersion,
    ) -> CommitResult:
        """Build and CAS only against the HEAD used to calculate this commit."""

        base = expected_base
        parent = self._load_parent(base)
        next_generation = 1 if parent is None else parent.generation + 1

        replacements_by_year: dict[
            int,
            dict[date, DailyRevisionReference | None],
        ] = defaultdict(dict)
        for metric_date, rows in request.daily_rows.items():
            if rows is None:
                replacements_by_year[metric_date.year][metric_date] = None
                continue
            reference = self._snapshots.publish_daily_revision(
                metric_date,
                rows,
                source_generation=next_generation,
                schema_version=request.daily_schema_version,
            )
            replacements_by_year[metric_date.year][metric_date] = reference

        year_updates: dict[int, YearShardReference | None] = {}
        actually_changed_dates: set[date] = set()
        for year, replacements in replacements_by_year.items():
            previous_shard = self._load_previous_shard(parent, year)
            candidate_shard = build_year_shard(
                year,
                previous=previous_shard,
                replacements=replacements,
            )
            actually_changed_dates.update(
                _changed_dates(previous_shard, candidate_shard, replacements)
            )
            if previous_shard == candidate_shard:
                continue
            if candidate_shard.daily_revisions:
                year_updates[year] = self._snapshots.publish_year_shard(candidate_shard)
            else:
                year_updates[year] = None

        previous_state = parent.state if parent is not None else None
        next_state = evolve_snapshot_state(
            previous_state,
            year_shard_updates=year_updates,
            data_type_cursors=request.data_type_cursors,
            webhook_cursor=request.webhook_cursor,
            backfill_cursors=request.backfill_cursors,
            fitbit_last_sync_time=(
                request.fitbit_last_sync_time
                if request.fitbit_last_sync_time is not None
                else (previous_state.fitbit_last_sync_time if previous_state is not None else None)
            ),
            time_zone=request.time_zone,
        )
        changed_years = tuple(
            sorted(
                int(year)
                for year in set(previous_state.daily_year_shards if previous_state else {}).union(
                    next_state.daily_year_shards
                )
                if (previous_state.daily_year_shards if previous_state else {}).get(year)
                != next_state.daily_year_shards.get(year)
            )
        )
        kind = CommitKind.INITIAL if parent is None else request.commit_kind
        change_set = ChangeSet(
            commit_kind=kind,
            changed_metric_dates=tuple(sorted(actually_changed_dates)),
            changed_years=changed_years,
            raw_replacements=request.raw_replacements,
            ingestion_manifests=request.ingestion_manifests,
            created_at=request.created_at,
        )
        root = build_root_snapshot(
            state=next_state,
            change_set=change_set,
            parent=parent,
            parent_reference=base.head.root_snapshot if base.head is not None else None,
            references=request.references,
        )
        root_reference = self._snapshots.publish_root_snapshot(root)
        candidate_head = DataHead(
            generation=root.generation,
            root_snapshot=root_reference,
            parent_generation=root.generation - 1,
            updated_at=request.created_at,
        )
        committed = self._heads.compare_and_swap(candidate_head, base=base)
        return CommitResult(
            head_version=committed,
            root_snapshot=root,
            changed_metric_dates=tuple(sorted(actually_changed_dates)),
        )

    def _load_parent(self, base: HeadVersion) -> RootSnapshot | None:
        if base.head is None:
            return None
        parent = self._snapshots.load_root_snapshot(base.head.root_snapshot)
        if parent.generation != base.head.generation:
            raise ValueError("HEAD generation does not match Root Snapshot generation")
        return parent

    def _load_previous_shard(
        self,
        parent: RootSnapshot | None,
        year: int,
    ) -> YearShard | None:
        if parent is None:
            return None
        reference = parent.state.daily_year_shards.get(f"{year:04d}")
        return self._snapshots.load_year_shard(reference) if reference is not None else None


def _changed_dates(
    previous: YearShard | None,
    candidate: YearShard,
    replacements: Mapping[date, DailyRevisionReference | None],
) -> set[date]:
    old = previous.daily_revisions if previous is not None else {}
    changed: set[date] = set()
    for metric_date in replacements:
        key = metric_date.isoformat()
        if old.get(key) != candidate.daily_revisions.get(key):
            changed.add(metric_date)
    return changed
