from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from health_data_pipeline.models import (
    ChangeSet,
    CommitKind,
    ContentReference,
    DailyRevisionReference,
    DataHead,
    RootSnapshot,
    SnapshotReferences,
    SnapshotState,
    YearShard,
    YearShardReference,
)
from health_data_pipeline.snapshot import (
    SnapshotRepository,
    SnapshotValidationError,
    build_root_snapshot,
    build_year_shard,
    evolve_snapshot_state,
    validate_root_transition,
)
from health_data_pipeline.storage import (
    ContentAddressedStore,
    InMemoryObjectStore,
    StorageIntegrityError,
)

NOW = datetime(2026, 7, 28, tzinfo=UTC)


def _daily_row(
    *,
    statistic: str = "observed",
    period_kind: str = "DAY",
    value: int = 8432,
) -> dict[str, object]:
    return {
        "metric_name": "steps",
        "statistic": statistic,
        "period_kind": period_kind,
        "dimension_key": None,
        "value_num": Decimal(value),
        "value_text": None,
        "unit": "count",
        "aggregation": "daily_sum",
    }


def _repository() -> tuple[SnapshotRepository, InMemoryObjectStore]:
    store = InMemoryObjectStore(clock=lambda: NOW)
    return SnapshotRepository(ContentAddressedStore(store)), store


def test_daily_revision_is_sorted_content_addressed_and_commit_free() -> None:
    repository, _ = _repository()
    rows = [
        _daily_row(statistic="mean", period_kind="CALENDAR_MONTH", value=7000),
        _daily_row(),
    ]

    reference = repository.publish_daily_revision(
        date(2026, 7, 28),
        rows,
        source_generation=12,
    )
    payload = repository.read_daily_revision(reference)
    decoded = [json.loads(line) for line in payload.splitlines()]

    assert reference.canonical_sha256 in reference.uri
    assert "metric_date=2026-07-28" in reference.uri
    assert decoded[0]["period_kind"] == "CALENDAR_MONTH"
    assert decoded[1]["period_kind"] == "DAY"
    assert all("generation" not in row for row in decoded)
    assert all("created_at" not in row for row in decoded)


def test_daily_revision_rejects_commit_metadata_and_duplicate_keys() -> None:
    repository, _ = _repository()
    with pytest.raises(SnapshotValidationError, match="commit metadata"):
        repository.publish_daily_revision(
            "2026-07-28",
            [{**_daily_row(), "created_at": NOW}],
            source_generation=2,
        )

    with pytest.raises(SnapshotValidationError, match="duplicate unique key"):
        repository.publish_daily_revision(
            "2026-07-28",
            [_daily_row(value=1), _daily_row(value=2)],
            source_generation=2,
        )


def test_daily_v2_uses_record_kind_in_its_unique_key() -> None:
    repository, _ = _repository()
    metric = {**_daily_row(), "record_kind": "METRIC"}
    session = {
        **_daily_row(),
        "record_kind": "SLEEP_SESSION",
    }

    reference = repository.publish_daily_revision(
        "2026-07-28",
        [metric, session],
        source_generation=2,
        schema_version=2,
    )

    assert reference.schema_version == 2
    assert "schema_version=2" in reference.uri


def test_equal_daily_content_keeps_original_source_generation() -> None:
    repository, _ = _repository()
    original = repository.publish_daily_revision(
        "2026-07-28",
        [_daily_row()],
        source_generation=4,
    )
    same_content_later = DailyRevisionReference(
        **original.model_dump(exclude={"source_generation"}),
        source_generation=9,
    )
    first = build_year_shard(
        2026,
        replacements={"2026-07-28": original},
    )
    second = build_year_shard(
        2026,
        previous=first,
        replacements={"2026-07-28": same_content_later},
    )

    assert second.daily_revisions["2026-07-28"].source_generation == 4
    assert second == first


def test_year_shard_rejects_a_date_from_another_year() -> None:
    repository, _ = _repository()
    reference = repository.publish_daily_revision(
        "2025-12-31",
        [_daily_row()],
        source_generation=1,
    )
    with pytest.raises(SnapshotValidationError, match="outside shard year"):
        build_year_shard(
            2026,
            replacements={"2025-12-31": reference},
        )


def test_root_snapshot_and_year_shard_round_trip_as_complete_state() -> None:
    repository, store = _repository()
    daily = repository.publish_daily_revision(
        "2026-07-28",
        [_daily_row()],
        source_generation=1,
    )
    shard = build_year_shard(2026, replacements={"2026-07-28": daily})
    shard_reference = repository.publish_year_shard(shard)
    state = evolve_snapshot_state(
        None,
        year_shard_updates={2026: shard_reference},
        webhook_cursor={"event": "abc"},
    )
    change_set = ChangeSet(
        commit_kind=CommitKind.INITIAL,
        changed_metric_dates=(date(2026, 7, 28),),
        changed_years=(2026,),
        created_at=NOW,
    )
    snapshot = build_root_snapshot(state=state, change_set=change_set)
    root_reference = repository.publish_root_snapshot(snapshot)

    assert repository.load_year_shard(shard_reference) == shard
    assert repository.load_root_snapshot(root_reference) == snapshot
    assert len(tuple(store.list("manifests/snapshot-shards/"))) == 1
    assert len(tuple(store.list("manifests/snapshots/"))) == 1


def test_root_transition_requires_change_set_to_match_changed_shards() -> None:
    repository, _ = _repository()
    daily = repository.publish_daily_revision(
        "2026-07-28",
        [_daily_row()],
        source_generation=1,
    )
    shard = build_year_shard(2026, replacements={"2026-07-28": daily})
    shard_reference = repository.publish_year_shard(shard)
    state = evolve_snapshot_state(None, year_shard_updates={2026: shard_reference})
    missing_delta = ChangeSet(
        commit_kind=CommitKind.INITIAL,
        created_at=NOW,
    )

    with pytest.raises(SnapshotValidationError, match="changed_years"):
        build_root_snapshot(state=state, change_set=missing_delta)


def test_retention_and_audit_references_remain_separate() -> None:
    hash_value = "a" * 64
    retention = ContentReference(
        uri=f"blobs/normalized/sha256/{hash_value}.json.gz",
        canonical_sha256=hash_value,
        object_sha256="b" * 64,
    )
    references = SnapshotReferences.model_validate(
        {
            "retention": [
                {
                    **retention.model_dump(),
                    "retain_until": None,
                }
            ],
            "audit": [
                {
                    "uri": retention.uri,
                    "canonical_sha256": retention.canonical_sha256,
                    "payload_retain_until": NOW,
                }
            ],
        }
    )

    assert references.retention[0].uri == retention.uri
    assert references.audit[0].uri == retention.uri
    assert not hasattr(references.audit[0], "retain_until")


@pytest.mark.parametrize(
    "uri",
    [
        "/absolute.json.gz",
        "gs://bucket/object.json.gz",
        "prefix/../object.json.gz",
        "object.json.gz?generation=1",
    ],
)
def test_content_reference_requires_bucket_relative_object_name(uri: str) -> None:
    with pytest.raises(ValidationError, match=r"bucket-relative|invalid path"):
        ContentReference(
            uri=uri,
            canonical_sha256="a" * 64,
            object_sha256="b" * 64,
        )


def test_year_shard_model_validates_canonical_dates_and_year() -> None:
    reference = DailyRevisionReference(
        uri=f"daily-canonical/sha256={'a' * 64}.ndjson.gz",
        canonical_sha256="a" * 64,
        object_sha256="b" * 64,
        row_count=1,
        source_generation=1,
    )
    with pytest.raises(ValidationError, match="outside shard year"):
        YearShard(year=2026, daily_revisions={"2025-12-31": reference})
    with pytest.raises(ValidationError, match="not an ISO date"):
        YearShard(year=2026, daily_revisions={"yesterday": reference})


def test_snapshot_state_validates_year_keys_timezone_and_timestamps() -> None:
    shard_reference = YearShardReference(
        uri=f"manifests/shard/sha256={'a' * 64}.json.gz",
        canonical_sha256="a" * 64,
        object_sha256="b" * 64,
        date_count=0,
    )
    with pytest.raises(ValidationError, match="four-digit year"):
        SnapshotState(daily_year_shards={"26": shard_reference})
    with pytest.raises(ValidationError, match="unknown IANA"):
        SnapshotState(time_zone="Mars/Olympus_Mons")
    with pytest.raises(ValidationError, match="timezone"):
        SnapshotState(
            fitbit_last_sync_time=datetime(2026, 7, 28),
        )


def test_change_set_normalizes_order_and_rejects_missing_changed_year() -> None:
    normalized = ChangeSet(
        commit_kind=CommitKind.WEBHOOK,
        changed_metric_dates=(date(2026, 7, 2), date(2026, 7, 1), date(2026, 7, 1)),
        changed_years=(2026, 2026),
        created_at=NOW,
    )
    assert normalized.changed_metric_dates == (date(2026, 7, 1), date(2026, 7, 2))
    assert normalized.changed_years == (2026,)

    with pytest.raises(ValidationError, match="omits years"):
        ChangeSet(
            commit_kind=CommitKind.WEBHOOK,
            changed_metric_dates=(date(2026, 7, 1),),
            created_at=NOW,
        )


def test_snapshot_and_head_generation_models_reject_invalid_chains() -> None:
    state = SnapshotState()
    initial_change = ChangeSet(commit_kind=CommitKind.INITIAL, created_at=NOW)
    with pytest.raises(ValidationError, match="generation 1"):
        RootSnapshot(
            generation=1,
            parent_snapshot=ContentReference(
                uri=f"manifests/snapshot/sha256={'a' * 64}.json.gz",
                canonical_sha256="a" * 64,
                object_sha256="b" * 64,
            ),
            state=state,
            change_set=initial_change,
        )

    root_reference = ContentReference(
        uri=f"manifests/snapshot/sha256={'c' * 64}.json.gz",
        canonical_sha256="c" * 64,
        object_sha256="d" * 64,
    )
    with pytest.raises(ValidationError, match="parent_generation"):
        DataHead(
            generation=4,
            parent_generation=1,
            root_snapshot=root_reference,
            updated_at=NOW,
        )


def test_snapshot_references_reject_duplicate_uri_within_each_role() -> None:
    payload = {
        "uri": f"blobs/sha256={'a' * 64}.json.gz",
        "canonical_sha256": "a" * 64,
        "object_sha256": "b" * 64,
        "retain_until": None,
    }
    with pytest.raises(ValidationError, match="duplicate URIs"):
        SnapshotReferences.model_validate({"retention": [payload, payload]})


def test_year_shard_replacement_supports_delete_and_rejects_hash_conflict() -> None:
    repository, _ = _repository()
    original = repository.publish_daily_revision(
        "2026-07-28",
        [_daily_row()],
        source_generation=1,
    )
    shard = build_year_shard(2026, replacements={"2026-07-28": original})
    assert (
        build_year_shard(
            2026,
            previous=shard,
            replacements={"2026-07-28": None},
        ).daily_revisions
        == {}
    )
    conflict = original.model_copy(
        update={
            "object_sha256": "f" * 64,
            "source_generation": 2,
        }
    )
    with pytest.raises(SnapshotValidationError, match="conflicting metadata"):
        build_year_shard(
            2026,
            previous=shard,
            replacements={"2026-07-28": conflict},
        )
    with pytest.raises(SnapshotValidationError, match="does not match"):
        build_year_shard(
            2025,
            previous=shard,
            replacements={},
        )


def test_evolve_state_inherits_cursors_and_can_remove_a_year() -> None:
    shard = YearShardReference(
        uri=f"manifests/shard/sha256={'a' * 64}.json.gz",
        canonical_sha256="a" * 64,
        object_sha256="b" * 64,
        date_count=1,
    )
    previous = SnapshotState(
        daily_year_shards={"2026": shard},
        data_type_cursors={"steps": "cursor"},
        webhook_cursor={"event": 3},
        backfill_cursors={"sleep": "2020-01-01"},
        fitbit_last_sync_time=NOW,
        time_zone="UTC",
    )
    evolved = evolve_snapshot_state(
        previous,
        year_shard_updates={"2026": None},
    )
    assert evolved.daily_year_shards == {}
    assert evolved.data_type_cursors == previous.data_type_cursors
    assert evolved.webhook_cursor == previous.webhook_cursor
    assert evolved.backfill_cursors == previous.backfill_cursors
    assert evolved.fitbit_last_sync_time == NOW
    assert evolved.time_zone == "UTC"


def test_root_builder_requires_correct_parent_inputs_and_commit_kind() -> None:
    state = SnapshotState(time_zone="UTC")
    initial_change = ChangeSet(commit_kind=CommitKind.INITIAL, created_at=NOW)
    parent = build_root_snapshot(state=state, change_set=initial_change)
    repository, _ = _repository()
    parent_reference = repository.publish_root_snapshot(parent)

    with pytest.raises(SnapshotValidationError, match="cannot have parent"):
        build_root_snapshot(
            state=state,
            change_set=initial_change,
            parent_reference=parent_reference,
        )
    with pytest.raises(SnapshotValidationError, match="requires parent_reference"):
        build_root_snapshot(
            state=state,
            change_set=ChangeSet(commit_kind=CommitKind.WEBHOOK, created_at=NOW),
            parent=parent,
        )
    with pytest.raises(SnapshotValidationError, match="only the genesis"):
        build_root_snapshot(
            state=state,
            change_set=initial_change,
            parent=parent,
            parent_reference=parent_reference,
        )
    with pytest.raises(SnapshotValidationError, match="genesis snapshot"):
        build_root_snapshot(
            state=state,
            change_set=ChangeSet(commit_kind=CommitKind.WEBHOOK, created_at=NOW),
        )


def test_validate_transition_rejects_generation_gap() -> None:
    state = SnapshotState(time_zone="UTC")
    parent = build_root_snapshot(
        state=state,
        change_set=ChangeSet(commit_kind=CommitKind.INITIAL, created_at=NOW),
    )
    parent_reference = ContentReference(
        uri=f"manifests/snapshot/sha256={'a' * 64}.json.gz",
        canonical_sha256="a" * 64,
        object_sha256="b" * 64,
    )
    candidate = RootSnapshot(
        generation=3,
        parent_snapshot=parent_reference,
        state=state,
        change_set=ChangeSet(commit_kind=CommitKind.WEBHOOK, created_at=NOW),
    )
    with pytest.raises(SnapshotValidationError, match="does not follow"):
        validate_root_transition(parent, candidate)


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (1, "daily row must be a mapping"),
        ({1: "value"}, "keys must be strings"),
        ({"metric_name": "steps"}, "missing unique key"),
    ],
)
def test_daily_revision_rejects_invalid_row_shapes(row: object, message: str) -> None:
    repository, _ = _repository()
    with pytest.raises(SnapshotValidationError, match=message):
        repository.publish_daily_revision(
            "2026-07-28",
            [row],
            source_generation=1,
        )


def test_snapshot_repository_rejects_wrong_reference_shape_and_row_count() -> None:
    repository, _ = _repository()
    daily = repository.publish_daily_revision(
        "2026-07-28",
        [_daily_row()],
        source_generation=1,
    )
    with pytest.raises(StorageIntegrityError, match="row_count"):
        repository.read_daily_revision(daily.model_copy(update={"row_count": 2}))

    shard = build_year_shard(2026, replacements={"2026-07-28": daily})
    shard_reference = repository.publish_year_shard(shard)
    with pytest.raises(StorageIntegrityError, match="not a Year Shard URI"):
        repository.load_year_shard(
            shard_reference.model_copy(update={"uri": "other/shard.json.gz"})
        )
    with pytest.raises(StorageIntegrityError, match="date_count"):
        repository.load_year_shard(shard_reference.model_copy(update={"date_count": 2}))

    root = build_root_snapshot(
        state=evolve_snapshot_state(
            None,
            year_shard_updates={2026: shard_reference},
        ),
        change_set=ChangeSet(
            commit_kind=CommitKind.INITIAL,
            changed_metric_dates=(date(2026, 7, 28),),
            changed_years=(2026,),
            created_at=NOW,
        ),
    )
    root_reference = repository.publish_root_snapshot(root)
    with pytest.raises(StorageIntegrityError, match="does not match"):
        repository.load_root_snapshot(
            root_reference.model_copy(update={"uri": "other/root.json.gz"})
        )
