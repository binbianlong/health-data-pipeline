"""Year-sharded snapshot construction and publication."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from typing import cast

from pydantic import JsonValue, ValidationError

from health_data_pipeline.canonical import (
    canonical_json_bytes,
    canonical_ndjson_bytes,
    daily_revision_object_name,
    sha256_hex,
    snapshot_object_name,
    year_shard_object_name,
)
from health_data_pipeline.models import (
    ChangeSet,
    ContentReference,
    DailyRevisionReference,
    RootSnapshot,
    SnapshotReferences,
    SnapshotState,
    YearShard,
    YearShardReference,
)
from health_data_pipeline.storage import (
    ContentAddressedStore,
    StorageIntegrityError,
)

DAILY_UNIQUE_KEY = (
    "metric_name",
    "statistic",
    "period_kind",
    "dimension_key",
)
_FORBIDDEN_DAILY_COMMIT_FIELDS = frozenset(
    {
        "commit_generation",
        "commit_id",
        "commit_created_at",
        "created_at",
        "generation",
        "head_generation",
        "snapshot_uri",
        "source_generation",
    }
)
_MAX_YEAR = 9999


class SnapshotValidationError(ValueError):
    """A Snapshot or shard transition violates a commit invariant."""


class _Unset:
    pass


_UNSET = _Unset()


def build_year_shard(
    year: int,
    *,
    previous: YearShard | None = None,
    replacements: (
        Mapping[date, DailyRevisionReference | None] | Mapping[str, DailyRevisionReference | None]
    ),
) -> YearShard:
    """Copy a complete year map and replace only explicitly supplied dates."""

    if previous is not None and previous.year != year:
        msg = f"previous shard year {previous.year} does not match {year}"
        raise SnapshotValidationError(msg)

    revisions = dict(previous.daily_revisions) if previous is not None else {}
    for raw_date, replacement in replacements.items():
        metric_date = _parse_metric_date(raw_date)
        if metric_date.year != year:
            msg = f"replacement date {metric_date} is outside shard year {year}"
            raise SnapshotValidationError(msg)
        key = metric_date.isoformat()
        if replacement is None:
            revisions.pop(key, None)
            continue

        existing = revisions.get(key)
        if existing is not None and existing.canonical_sha256 == replacement.canonical_sha256:
            if (
                existing.uri != replacement.uri
                or existing.object_sha256 != replacement.object_sha256
                or existing.schema_version != replacement.schema_version
                or existing.row_count != replacement.row_count
            ):
                msg = f"equal canonical hash has conflicting metadata for {key}"
                raise SnapshotValidationError(msg)
            # The same daily content keeps the generation at which that content
            # first became current, as required by the Year Shard contract.
            revisions[key] = existing
        else:
            revisions[key] = replacement

    return YearShard(
        year=year,
        daily_revisions=dict(sorted(revisions.items())),
    )


def evolve_snapshot_state(
    previous: SnapshotState | None,
    *,
    year_shard_updates: (
        Mapping[int, YearShardReference | None] | Mapping[str, YearShardReference | None] | None
    ) = None,
    data_type_cursors: Mapping[str, JsonValue] | None = None,
    webhook_cursor: Mapping[str, JsonValue] | None = None,
    backfill_cursors: Mapping[str, JsonValue] | None = None,
    fitbit_last_sync_time: datetime | _Unset | None = _UNSET,
    time_zone: str | None = None,
) -> SnapshotState:
    """Create a new complete state without mutating the committed parent."""

    shard_references = dict(previous.daily_year_shards) if previous is not None else {}
    for raw_year, reference in (year_shard_updates or {}).items():
        year = _parse_year(raw_year)
        key = f"{year:04d}"
        if reference is None:
            shard_references.pop(key, None)
        else:
            shard_references[key] = reference

    if isinstance(fitbit_last_sync_time, _Unset):
        next_last_sync = previous.fitbit_last_sync_time if previous is not None else None
    else:
        next_last_sync = fitbit_last_sync_time

    return SnapshotState(
        daily_year_shards=dict(sorted(shard_references.items())),
        data_type_cursors=dict(
            data_type_cursors
            if data_type_cursors is not None
            else (previous.data_type_cursors if previous is not None else {})
        ),
        webhook_cursor=dict(
            webhook_cursor
            if webhook_cursor is not None
            else (previous.webhook_cursor if previous is not None else {})
        ),
        backfill_cursors=dict(
            backfill_cursors
            if backfill_cursors is not None
            else (previous.backfill_cursors if previous is not None else {})
        ),
        fitbit_last_sync_time=next_last_sync,
        time_zone=(
            time_zone
            if time_zone is not None
            else (previous.time_zone if previous is not None else "Asia/Tokyo")
        ),
    )


def build_root_snapshot(
    *,
    state: SnapshotState,
    change_set: ChangeSet,
    parent: RootSnapshot | None = None,
    parent_reference: ContentReference | None = None,
    references: SnapshotReferences | None = None,
) -> RootSnapshot:
    """Build and validate the next Root Snapshot in a logical chain."""

    if parent is None:
        if parent_reference is not None:
            msg = "a genesis snapshot cannot have parent_reference"
            raise SnapshotValidationError(msg)
        generation = 1
    else:
        if parent_reference is None:
            msg = "a non-genesis snapshot requires parent_reference"
            raise SnapshotValidationError(msg)
        generation = parent.generation + 1

    candidate = RootSnapshot(
        generation=generation,
        parent_snapshot=parent_reference,
        state=state,
        change_set=change_set,
        references=references or SnapshotReferences(),
    )
    validate_root_transition(parent, candidate)
    return candidate


def validate_root_transition(
    parent: RootSnapshot | None,
    candidate: RootSnapshot,
) -> None:
    """Validate complete-state and change-set separation across one commit."""

    expected_generation = 1 if parent is None else parent.generation + 1
    if candidate.generation != expected_generation:
        msg = (
            f"candidate generation {candidate.generation} does not follow {expected_generation - 1}"
        )
        raise SnapshotValidationError(msg)

    previous_shards = parent.state.daily_year_shards if parent is not None else {}
    current_shards = candidate.state.daily_year_shards
    all_years = set(previous_shards).union(current_shards)
    actually_changed_years = {
        int(year) for year in all_years if previous_shards.get(year) != current_shards.get(year)
    }
    declared_changed_years = set(candidate.change_set.changed_years)
    if actually_changed_years != declared_changed_years:
        msg = (
            "change_set.changed_years does not match the complete state delta: "
            f"declared={sorted(declared_changed_years)}, "
            f"actual={sorted(actually_changed_years)}"
        )
        raise SnapshotValidationError(msg)

    if parent is None and candidate.change_set.commit_kind.value != "INITIAL":
        msg = "the genesis snapshot must use commit_kind INITIAL"
        raise SnapshotValidationError(msg)
    if parent is not None and candidate.change_set.commit_kind.value == "INITIAL":
        msg = "only the genesis snapshot may use commit_kind INITIAL"
        raise SnapshotValidationError(msg)


class SnapshotRepository:
    """Publish and verify daily revisions, Year Shards, and Root Snapshots."""

    def __init__(self, content_store: ContentAddressedStore) -> None:
        self._content_store = content_store

    def publish_daily_revision(
        self,
        metric_date: date | str,
        rows: Iterable[object],
        *,
        source_generation: int,
        schema_version: int = 1,
        unique_key_fields: Sequence[str] = DAILY_UNIQUE_KEY,
    ) -> DailyRevisionReference:
        materialized_rows = [_mapping_row(row) for row in rows]
        _validate_daily_rows(materialized_rows, unique_key_fields)
        canonical_content = canonical_ndjson_bytes(
            materialized_rows,
            sort_fields=unique_key_fields,
        )
        canonical_hash = sha256_hex(canonical_content)
        object_name = daily_revision_object_name(
            metric_date,
            canonical_hash,
            schema_version=schema_version,
        )
        published = self._content_store.publish(
            object_name,
            canonical_content,
            logical_content_type="application/x-ndjson",
        )
        return DailyRevisionReference(
            **published.reference.model_dump(),
            schema_version=schema_version,
            row_count=len(materialized_rows),
            source_generation=source_generation,
        )

    def publish_year_shard(self, shard: YearShard) -> YearShardReference:
        canonical_content = canonical_json_bytes(shard)
        canonical_hash = sha256_hex(canonical_content)
        object_name = year_shard_object_name(shard.year, canonical_hash)
        published = self._content_store.publish(
            object_name,
            canonical_content,
            logical_content_type="application/json",
        )
        return YearShardReference(
            **published.reference.model_dump(),
            date_count=len(shard.daily_revisions),
        )

    def publish_root_snapshot(self, snapshot: RootSnapshot) -> ContentReference:
        canonical_content = canonical_json_bytes(snapshot)
        canonical_hash = sha256_hex(canonical_content)
        object_name = snapshot_object_name(canonical_hash)
        published = self._content_store.publish(
            object_name,
            canonical_content,
            logical_content_type="application/json",
        )
        return published.reference

    def load_year_shard(self, reference: YearShardReference) -> YearShard:
        expected_prefix = "manifests/snapshot-shards/"
        if not reference.uri.startswith(expected_prefix):
            msg = f"not a Year Shard URI: {reference.uri}"
            raise StorageIntegrityError(msg)
        payload = self._content_store.read(reference)
        try:
            shard = YearShard.model_validate_json(payload)
        except ValidationError as exc:
            msg = f"invalid Year Shard schema: {reference.uri}"
            raise StorageIntegrityError(msg) from exc
        expected_name = year_shard_object_name(
            shard.year,
            reference.canonical_sha256,
        )
        if reference.uri != expected_name:
            msg = f"Year Shard URI does not match its year/hash: {reference.uri}"
            raise StorageIntegrityError(msg)
        if reference.date_count != len(shard.daily_revisions):
            msg = f"Year Shard date_count mismatch: {reference.uri}"
            raise StorageIntegrityError(msg)
        return shard

    def load_root_snapshot(self, reference: ContentReference) -> RootSnapshot:
        expected_name = snapshot_object_name(reference.canonical_sha256)
        if reference.uri != expected_name:
            msg = f"Root Snapshot URI does not match its hash: {reference.uri}"
            raise StorageIntegrityError(msg)
        payload = self._content_store.read(reference)
        try:
            return RootSnapshot.model_validate_json(payload)
        except ValidationError as exc:
            msg = f"invalid Root Snapshot schema: {reference.uri}"
            raise StorageIntegrityError(msg) from exc

    def read_daily_revision(self, reference: DailyRevisionReference) -> bytes:
        payload = self._content_store.read(reference)
        row_count = 0 if not payload else payload.count(b"\n")
        if row_count != reference.row_count or (payload and not payload.endswith(b"\n")):
            msg = f"daily revision row_count or trailing LF mismatch: {reference.uri}"
            raise StorageIntegrityError(msg)
        return payload


def _validate_daily_rows(
    rows: Sequence[Mapping[str, object]],
    unique_key_fields: Sequence[str],
) -> None:
    seen_keys: set[bytes] = set()
    for row in rows:
        forbidden = _FORBIDDEN_DAILY_COMMIT_FIELDS.intersection(row)
        if forbidden:
            msg = f"daily row contains commit metadata: {sorted(forbidden)}"
            raise SnapshotValidationError(msg)
        try:
            key = canonical_json_bytes([row[field] for field in unique_key_fields])
        except KeyError as exc:
            msg = f"daily row is missing unique key field: {exc.args[0]}"
            raise SnapshotValidationError(msg) from exc
        if key in seen_keys:
            msg = "daily rows contain a duplicate unique key"
            raise SnapshotValidationError(msg)
        seen_keys.add(key)


def _mapping_row(row: object) -> Mapping[str, object]:
    dump = getattr(row, "model_dump", None)
    if callable(dump):
        row = dump(mode="python")
    if not isinstance(row, Mapping):
        msg = f"daily row must be a mapping or Pydantic model, got {type(row).__name__}"
        raise SnapshotValidationError(msg)
    if any(not isinstance(key, str) for key in row):
        msg = "daily row keys must be strings"
        raise SnapshotValidationError(msg)
    return cast(Mapping[str, object], row)


def _parse_metric_date(value: date | str) -> date:
    if isinstance(value, datetime):
        msg = "metric date must not be a datetime"
        raise SnapshotValidationError(msg)
    if isinstance(value, date):
        return value
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        msg = f"invalid metric date: {value!r}"
        raise SnapshotValidationError(msg) from exc
    if parsed.isoformat() != value:
        msg = f"metric date is not canonical ISO format: {value!r}"
        raise SnapshotValidationError(msg)
    return parsed


def _parse_year(value: int | str) -> int:
    if isinstance(value, bool):
        msg = f"invalid year: {value!r}"
        raise SnapshotValidationError(msg)
    try:
        year = int(value)
    except (TypeError, ValueError) as exc:
        msg = f"invalid year: {value!r}"
        raise SnapshotValidationError(msg) from exc
    if year < 1 or year > _MAX_YEAR:
        msg = f"invalid year: {year}"
        raise SnapshotValidationError(msg)
    if isinstance(value, str) and value != f"{year:04d}":
        msg = f"year string must contain four digits: {value!r}"
        raise SnapshotValidationError(msg)
    return year
