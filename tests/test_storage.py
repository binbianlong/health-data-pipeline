from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from health_data_pipeline.canonical import (
    CANONICALIZATION_VERSION,
    ENCODING_VERSION,
    canonical_json_bytes,
    deterministic_gzip,
    encode_content,
    sha256_hex,
    snapshot_object_name,
)
from health_data_pipeline.models import ContentReference, DataHead
from health_data_pipeline.storage import (
    ContentAddressedStore,
    GoogleCloudStorageObjectStore,
    HeadRepository,
    HeadVersion,
    InMemoryObjectStore,
    ObjectNotFound,
    PreconditionFailed,
    StorageIntegrityError,
)

NOW = datetime(2026, 7, 28, tzinfo=UTC)


class _GoogleApiError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"Google API error {code}")
        self.code = code


def _publish_snapshot_reference(
    content_store: ContentAddressedStore,
    payload: bytes,
) -> ContentReference:
    encoded = encode_content(payload)
    return content_store.publish(
        snapshot_object_name(encoded.canonical_sha256),
        payload,
        logical_content_type="application/json",
    ).reference


def test_in_memory_store_enforces_create_and_update_preconditions() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    created = store.put(
        "state/example.json",
        b"one",
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )

    with pytest.raises(PreconditionFailed):
        store.put(
            "state/example.json",
            b"two",
            if_generation_match=0,
            metadata={},
            content_type="application/json",
            cache_control="no-store",
        )

    updated = store.put(
        "state/example.json",
        b"two",
        if_generation_match=created.generation,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )
    assert updated.generation != created.generation
    assert store.get("state/example.json").data == b"two"


def test_content_publish_is_idempotent_without_overwrite() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    repository = ContentAddressedStore(store)
    payload = b'{"schema_version":3}'
    encoded = encode_content(payload)
    object_name = snapshot_object_name(encoded.canonical_sha256)

    first = repository.publish(
        object_name,
        payload,
        logical_content_type="application/json",
    )
    second = repository.publish(
        object_name,
        payload,
        logical_content_type="application/json",
    )

    assert first.created is True
    assert second.created is False
    assert second.object_generation == first.object_generation
    assert len(tuple(store.list("manifests/snapshots/"))) == 1
    assert repository.read(first.reference) == payload


def test_content_publish_uses_persisted_gcs_headers_before_media_download() -> None:
    payload = b"{}"
    encoded = encode_content(payload)
    object_name = snapshot_object_name(encoded.canonical_sha256)
    blob = Mock()
    blob.name = object_name
    blob.generation = 19
    blob.metadata = {
        "canonicalization": CANONICALIZATION_VERSION,
        "encoding": ENCODING_VERSION,
        "canonical-sha256": encoded.canonical_sha256,
        "object-sha256": encoded.object_sha256,
        "logical-content-type": "application/json",
    }
    blob.content_type = "application/gzip"
    blob.cache_control = "no-transform"
    blob.time_created = NOW
    blob.upload_from_string.side_effect = _GoogleApiError(412)

    def download_response(**_: object) -> bytes:
        blob.cache_control = "no-cache, no-store, max-age=0, must-revalidate, no-transform"
        return encoded.object_bytes

    blob.download_as_bytes.side_effect = download_response
    bucket = Mock()
    bucket.blob.return_value = blob
    repository = ContentAddressedStore(GoogleCloudStorageObjectStore(bucket))

    published = repository.publish(
        object_name,
        payload,
        logical_content_type="application/json",
    )

    assert published.created is False
    assert published.object_generation == 19


def test_existing_content_addressed_object_must_match_exactly() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    repository = ContentAddressedStore(store)
    expected = encode_content(b'{"expected":true}')
    object_name = snapshot_object_name(expected.canonical_sha256)
    store.put(
        object_name,
        b"not-gzip",
        if_generation_match=0,
        metadata={},
        content_type="application/gzip",
        cache_control="no-transform",
    )

    with pytest.raises(StorageIntegrityError, match="collision"):
        repository.publish(
            object_name,
            b'{"expected":true}',
            logical_content_type="application/json",
        )


def test_head_compare_and_swap_allows_only_one_stale_writer() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    content_store = ContentAddressedStore(store)
    head_repository = HeadRepository(store)
    first_root = _publish_snapshot_reference(content_store, b'{"root":1}')
    base = head_repository.read()
    first = DataHead(
        generation=1,
        root_snapshot=first_root,
        parent_generation=0,
        updated_at=NOW,
    )
    committed = head_repository.compare_and_swap(first, base=base)

    stale_base = committed
    winner_root = _publish_snapshot_reference(content_store, b'{"root":2}')
    loser_root = _publish_snapshot_reference(content_store, b'{"root":3}')
    winner = DataHead(
        generation=2,
        root_snapshot=winner_root,
        parent_generation=1,
        updated_at=NOW + timedelta(seconds=1),
    )
    loser = DataHead(
        generation=2,
        root_snapshot=loser_root,
        parent_generation=1,
        updated_at=NOW + timedelta(seconds=2),
    )

    head_repository.compare_and_swap(winner, base=stale_base)
    with pytest.raises(PreconditionFailed):
        head_repository.compare_and_swap(loser, base=stale_base)

    assert head_repository.read().head == winner
    # A CAS loser never deletes its already-created immutable object.
    assert store.get(loser_root.uri).data


def test_head_repository_rejects_logical_generation_skip_before_writing() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    content_store = ContentAddressedStore(store)
    head_repository = HeadRepository(store)
    root = _publish_snapshot_reference(content_store, b'{"root":1}')
    base = head_repository.read()

    invalid = DataHead(
        generation=2,
        root_snapshot=root,
        parent_generation=1,
        updated_at=NOW,
    )
    with pytest.raises(ValueError, match="first HEAD"):
        head_repository.compare_and_swap(invalid, base=base)
    assert head_repository.read().head is None


def test_gcs_adapter_reads_one_generation_with_crc32c() -> None:
    blob = Mock()
    blob.name = "state/data-head.json"
    blob.generation = "17"
    blob.metadata = {"canonicalization": "fhcj-1"}
    blob.content_type = "application/json"
    blob.cache_control = "no-store"
    blob.time_created = NOW

    def download_response(**_: object) -> bytes:
        blob.generation = "18"
        blob.metadata = {}
        blob.content_type = "application/octet-stream"
        blob.cache_control = "no-cache, no-store, max-age=0, must-revalidate"
        blob.time_created = NOW + timedelta(seconds=1)
        return b'{"generation":1}'

    blob.download_as_bytes.side_effect = download_response
    bucket = Mock()
    bucket.blob.return_value = blob
    adapter = GoogleCloudStorageObjectStore(bucket)

    stored = adapter.get("state/data-head.json")

    blob.reload.assert_called_once_with()
    blob.download_as_bytes.assert_called_once_with(
        if_generation_match=17,
        checksum="crc32c",
    )
    assert stored.generation == 17
    assert stored.data == b'{"generation":1}'
    assert stored.metadata["canonicalization"] == "fhcj-1"
    assert stored.content_type == "application/json"
    assert stored.cache_control == "no-store"
    assert stored.created_at == NOW


def test_gcs_adapter_put_forwards_generation_precondition_and_metadata() -> None:
    blob = Mock()
    blob.name = "state/data-head.json"
    blob.generation = 18
    blob.time_created = NOW
    bucket = Mock()
    bucket.blob.return_value = blob
    adapter = GoogleCloudStorageObjectStore(bucket)

    stored = adapter.put(
        "state/data-head.json",
        b'{"generation":2}',
        if_generation_match=17,
        metadata={"canonical-sha256": "a" * 64},
        content_type="application/json",
        cache_control="no-store",
    )

    blob.upload_from_string.assert_called_once_with(
        b'{"generation":2}',
        content_type="application/json",
        if_generation_match=17,
        checksum="crc32c",
    )
    assert blob.metadata == {"canonical-sha256": "a" * 64}
    assert blob.cache_control == "no-store"
    assert stored.generation == 18


def test_gcs_adapter_translates_not_found_and_precondition_failure() -> None:
    missing_blob = Mock()
    missing_blob.reload.side_effect = _GoogleApiError(404)
    conflicting_blob = Mock()
    conflicting_blob.upload_from_string.side_effect = _GoogleApiError(412)
    bucket = Mock()
    bucket.blob.side_effect = [missing_blob, conflicting_blob]
    adapter = GoogleCloudStorageObjectStore(bucket)

    with pytest.raises(ObjectNotFound):
        adapter.get("missing.json")

    with pytest.raises(PreconditionFailed) as exc_info:
        adapter.put(
            "state/data-head.json",
            b"candidate",
            if_generation_match=21,
            metadata={},
            content_type="application/json",
            cache_control="no-store",
        )
    assert exc_info.value.expected_generation == 21
    assert exc_info.value.actual_generation is None


def test_gcs_adapter_list_and_delete_keep_generation_guards() -> None:
    listed_blob = Mock()
    listed_blob.name = "manifests/snapshots/one.json.gz"
    listed_blob.generation = 31
    listed_blob.metadata = {}
    listed_blob.content_type = "application/gzip"
    listed_blob.cache_control = "no-transform"
    listed_blob.time_created = NOW

    def download_response(**_: object) -> bytes:
        listed_blob.cache_control = "no-cache, no-store, max-age=0, must-revalidate"
        return b"gzip"

    listed_blob.download_as_bytes.side_effect = download_response
    delete_blob = Mock()
    bucket = Mock()
    bucket.list_blobs.return_value = [listed_blob]
    bucket.blob.return_value = delete_blob
    adapter = GoogleCloudStorageObjectStore(bucket)

    results = adapter.list("manifests/snapshots/")
    adapter.delete("manifests/snapshots/one.json.gz", if_generation_match=31)

    bucket.list_blobs.assert_called_once_with(prefix="manifests/snapshots/")
    listed_blob.download_as_bytes.assert_called_once_with(
        if_generation_match=31,
        checksum="crc32c",
    )
    assert results[0].generation == 31
    assert results[0].cache_control == "no-transform"
    delete_blob.delete.assert_called_once_with(if_generation_match=31)


def test_gcs_adapter_factory_is_lazy_and_uses_requested_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bucket = Mock()
    client = Mock()
    client.bucket.return_value = bucket
    factory = Mock(return_value=client)
    module = SimpleNamespace(Client=factory)
    monkeypatch.setattr(
        "health_data_pipeline.storage.importlib.import_module",
        lambda name: module,
    )

    adapter = GoogleCloudStorageObjectStore.from_bucket_name(
        "health-bucket",
        project="health-project",
    )

    assert isinstance(adapter, GoogleCloudStorageObjectStore)
    factory.assert_called_once_with(project="health-project")
    client.bucket.assert_called_once_with("health-bucket")


def test_gcs_adapter_rejects_invalid_mutation_preconditions() -> None:
    adapter = GoogleCloudStorageObjectStore(Mock())
    with pytest.raises(ValueError, match="non-negative"):
        adapter.put(
            "object",
            b"value",
            if_generation_match=-1,
            metadata={},
            content_type="application/octet-stream",
            cache_control="no-store",
        )
    with pytest.raises(ValueError, match="positive"):
        adapter.delete("object", if_generation_match=0)


def test_gcs_adapter_rejects_missing_generation_and_naive_creation_time() -> None:
    no_generation = Mock()
    no_generation.name = "object"
    no_generation.generation = None
    no_generation.metadata = {}
    no_generation.content_type = None
    no_generation.cache_control = None
    no_generation.time_created = NOW
    bucket = Mock()
    bucket.blob.return_value = no_generation
    adapter = GoogleCloudStorageObjectStore(bucket)
    with pytest.raises(StorageIntegrityError, match="did not return a generation"):
        adapter.get("object")

    naive_time = Mock()
    naive_time.name = "object"
    naive_time.generation = 1
    naive_time.metadata = {}
    naive_time.content_type = "text/plain"
    naive_time.cache_control = "no-store"
    naive_time.time_created = datetime(2026, 7, 28)
    naive_time.download_as_bytes.return_value = b"value"
    bucket.blob.return_value = naive_time
    with pytest.raises(StorageIntegrityError, match="naive creation"):
        adapter.get("object")


def test_in_memory_store_rejects_invalid_delete_and_naive_clock() -> None:
    store = InMemoryObjectStore(clock=lambda: datetime(2026, 7, 28))
    with pytest.raises(ObjectNotFound):
        store.get("missing")
    with pytest.raises(ValueError, match="non-negative"):
        store.put(
            "object",
            b"value",
            if_generation_match=-1,
            metadata={},
            content_type="text/plain",
            cache_control="no-store",
        )
    with pytest.raises(ValueError, match="positive"):
        store.delete("object", if_generation_match=0)
    with pytest.raises(ValueError, match="timezone-aware"):
        store.put(
            "object",
            b"value",
            if_generation_match=0,
            metadata={},
            content_type="text/plain",
            cache_control="no-store",
        )


def test_content_read_detects_object_gzip_canonical_and_metadata_corruption() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    repository = ContentAddressedStore(store)
    published = repository.publish(
        snapshot_object_name(encode_content(b"good").canonical_sha256),
        b"good",
        logical_content_type="application/json",
    )
    current = store.get(published.reference.uri)
    store.put(
        published.reference.uri,
        b"corrupt",
        if_generation_match=current.generation,
        metadata=current.metadata,
        content_type=current.content_type,
        cache_control=current.cache_control,
    )
    with pytest.raises(StorageIntegrityError, match="object hash"):
        repository.read(published.reference)

    invalid_gzip = b"not-gzip"
    invalid_reference = ContentReference(
        uri=f"manifests/snapshots/sha256/{'a' * 64}.json.gz",
        canonical_sha256="a" * 64,
        object_sha256=sha256_hex(invalid_gzip),
    )
    store.put(
        invalid_reference.uri,
        invalid_gzip,
        if_generation_match=0,
        metadata={
            "canonicalization": CANONICALIZATION_VERSION,
            "encoding": ENCODING_VERSION,
            "canonical-sha256": invalid_reference.canonical_sha256,
            "object-sha256": invalid_reference.object_sha256,
        },
        content_type="application/gzip",
        cache_control="no-transform",
    )
    with pytest.raises(StorageIntegrityError, match="invalid gzip"):
        repository.read(invalid_reference)

    canonical = b"different"
    compressed = deterministic_gzip(canonical)
    wrong_canonical_reference = ContentReference(
        uri=f"manifests/snapshots/sha256/{'b' * 64}.json.gz",
        canonical_sha256="b" * 64,
        object_sha256=sha256_hex(compressed),
    )
    store.put(
        wrong_canonical_reference.uri,
        compressed,
        if_generation_match=0,
        metadata={
            "canonicalization": CANONICALIZATION_VERSION,
            "encoding": ENCODING_VERSION,
            "canonical-sha256": wrong_canonical_reference.canonical_sha256,
            "object-sha256": wrong_canonical_reference.object_sha256,
        },
        content_type="application/gzip",
        cache_control="no-transform",
    )
    with pytest.raises(StorageIntegrityError, match="canonical content hash"):
        repository.read(wrong_canonical_reference)


def test_head_read_rejects_metadata_schema_and_noncanonical_bytes() -> None:
    content_store = ContentAddressedStore(InMemoryObjectStore(clock=lambda: NOW))
    root = _publish_snapshot_reference(content_store, b'{"root":1}')
    candidate = DataHead(
        generation=1,
        parent_generation=0,
        root_snapshot=root,
        updated_at=NOW,
    )

    metadata_store = InMemoryObjectStore(clock=lambda: NOW)
    canonical = canonical_json_bytes(candidate)
    metadata_store.put(
        "state/data-head.json",
        canonical,
        if_generation_match=0,
        metadata={"canonical-sha256": "f" * 64},
        content_type="application/json",
        cache_control="no-store",
    )
    with pytest.raises(StorageIntegrityError, match="metadata mismatch"):
        HeadRepository(metadata_store).read()

    invalid_store = InMemoryObjectStore(clock=lambda: NOW)
    invalid_store.put(
        "state/data-head.json",
        b"{}",
        if_generation_match=0,
        metadata={},
        content_type="application/json",
        cache_control="no-store",
    )
    with pytest.raises(StorageIntegrityError, match="invalid HEAD"):
        HeadRepository(invalid_store).read()

    noncanonical_store = InMemoryObjectStore(clock=lambda: NOW)
    noncanonical = json.dumps(
        candidate.model_dump(mode="json"),
        ensure_ascii=False,
        indent=2,
    ).encode()
    noncanonical_store.put(
        "state/data-head.json",
        noncanonical,
        if_generation_match=0,
        metadata={"canonical-sha256": sha256_hex(noncanonical)},
        content_type="application/json",
        cache_control="no-store",
    )
    with pytest.raises(StorageIntegrityError, match="not canonical"):
        HeadRepository(noncanonical_store).read()


def test_head_version_and_next_generation_are_consistent() -> None:
    with pytest.raises(ValueError, match="both exist"):
        HeadVersion(head=None, object_generation=1)
