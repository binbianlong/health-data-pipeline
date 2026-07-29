"""Storage boundaries and GCS-style consistency primitives.

The production Google Cloud Storage adapter implements ``ObjectStore``.  Core
logic only depends on generation preconditions, which keeps unit tests fully
offline and makes the single HEAD compare-and-swap invariant explicit.
"""

from __future__ import annotations

import gzip
import importlib
import json
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import NoReturn, Protocol, cast

from health_data_pipeline.canonical import (
    CANONICALIZATION_VERSION,
    ENCODING_VERSION,
    canonical_json_bytes,
    encode_content,
    sha256_hex,
)
from health_data_pipeline.models import ContentReference, DataHead

HEAD_OBJECT_NAME = "state/data-head.json"
HTTP_NOT_FOUND = 404
HTTP_PRECONDITION_FAILED = 412


class StorageError(RuntimeError):
    """Base class for storage failures."""


class ObjectNotFound(StorageError):  # noqa: N818
    """The requested object does not exist."""

    def __init__(self, object_name: str) -> None:
        super().__init__(f"object not found: {object_name}")
        self.object_name = object_name


class PreconditionFailed(StorageError):  # noqa: N818
    """A GCS generation precondition did not match."""

    def __init__(
        self,
        object_name: str,
        *,
        expected_generation: int,
        actual_generation: int | None,
    ) -> None:
        super().__init__(
            "generation precondition failed for "
            f"{object_name}: expected {expected_generation}, actual {actual_generation}"
        )
        self.object_name = object_name
        self.expected_generation = expected_generation
        self.actual_generation = actual_generation


class StorageIntegrityError(StorageError):
    """Stored bytes or integrity metadata disagree with their reference."""


@dataclass(frozen=True, slots=True)
class StoredObject:
    """A value returned by an ObjectStore."""

    name: str
    data: bytes
    generation: int
    metadata: Mapping[str, str]
    content_type: str
    cache_control: str
    created_at: datetime


class ObjectStore(Protocol):
    """Minimal synchronous interface required from a GCS adapter."""

    def get(self, object_name: str) -> StoredObject:
        """Read one object or raise ObjectNotFound."""

    def put(
        self,
        object_name: str,
        data: bytes,
        *,
        if_generation_match: int,
        metadata: Mapping[str, str],
        content_type: str,
        cache_control: str,
    ) -> StoredObject:
        """Create or replace one object using a required precondition."""

    def list(self, prefix: str) -> Iterable[StoredObject]:
        """List objects for maintenance workflows such as reachability GC."""

    def delete(self, object_name: str, *, if_generation_match: int) -> None:
        """Delete one explicitly resolved object with a generation guard."""


class _GcsBlob(Protocol):
    name: str
    generation: int | str | None
    metadata: Mapping[str, str] | None
    content_type: str | None
    cache_control: str | None
    time_created: datetime | None

    def reload(self) -> None: ...

    def download_as_bytes(
        self,
        *,
        if_generation_match: int,
        checksum: str,
    ) -> bytes: ...

    def upload_from_string(
        self,
        data: bytes,
        *,
        content_type: str,
        if_generation_match: int,
        checksum: str,
    ) -> None: ...

    def delete(self, *, if_generation_match: int) -> None: ...


class _GcsBucket(Protocol):
    def blob(self, blob_name: str) -> _GcsBlob: ...

    def list_blobs(self, *, prefix: str) -> Iterable[_GcsBlob]: ...


class _GcsClient(Protocol):
    def bucket(self, bucket_name: str) -> _GcsBucket: ...


@dataclass(frozen=True, slots=True)
class _BlobProperties:
    """GCS object properties captured before a media download mutates Blob."""

    name: str
    generation: int
    metadata: Mapping[str, str]
    content_type: str
    cache_control: str
    created_at: datetime

    def stored_object(self, data: bytes) -> StoredObject:
        return StoredObject(
            name=self.name,
            data=bytes(data),
            generation=self.generation,
            metadata=self.metadata,
            content_type=self.content_type,
            cache_control=self.cache_control,
            created_at=self.created_at,
        )


class GoogleCloudStorageObjectStore:
    """Production ObjectStore backed by ``google-cloud-storage``.

    The adapter accepts a structural bucket dependency so tests can use mocks
    and importing this module never creates credentials or network clients.
    """

    def __init__(
        self,
        bucket: _GcsBucket,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._bucket = bucket
        self._clock = clock or (lambda: datetime.now(UTC))

    @classmethod
    def from_bucket_name(
        cls,
        bucket_name: str,
        *,
        project: str | None = None,
    ) -> GoogleCloudStorageObjectStore:
        """Create the production adapter without importing GCS during tests."""

        storage_module = importlib.import_module("google.cloud.storage")
        raw_factory: object = getattr(storage_module, "Client", None)
        if not callable(raw_factory):
            msg = "google.cloud.storage.Client is unavailable"
            raise TypeError(msg)
        factory = cast(Callable[..., object], raw_factory)
        client = cast(_GcsClient, factory(project=project))
        return cls(client.bucket(bucket_name))

    def get(self, object_name: str) -> StoredObject:
        blob = self._bucket.blob(object_name)
        expected_generation: int | None = None
        try:
            blob.reload()
            properties = self._snapshot_blob_properties(blob)
            expected_generation = properties.generation
            data = blob.download_as_bytes(
                if_generation_match=expected_generation,
                checksum="crc32c",
            )
        except Exception as exc:
            _raise_translated_gcs_error(
                exc,
                object_name,
                expected_generation=expected_generation,
            )
        return properties.stored_object(data)

    def put(
        self,
        object_name: str,
        data: bytes,
        *,
        if_generation_match: int,
        metadata: Mapping[str, str],
        content_type: str,
        cache_control: str,
    ) -> StoredObject:
        if if_generation_match < 0:
            msg = "if_generation_match must be non-negative"
            raise ValueError(msg)
        blob = self._bucket.blob(object_name)
        blob.metadata = dict(metadata)
        blob.cache_control = cache_control
        try:
            blob.upload_from_string(
                data,
                content_type=content_type,
                if_generation_match=if_generation_match,
                checksum="crc32c",
            )
        except Exception as exc:
            _raise_translated_gcs_error(
                exc,
                object_name,
                expected_generation=if_generation_match,
            )
        return StoredObject(
            name=object_name,
            data=bytes(data),
            generation=_required_generation(blob, object_name),
            metadata=MappingProxyType(dict(metadata)),
            content_type=content_type,
            cache_control=cache_control,
            created_at=self._blob_created_at(blob),
        )

    def list(self, prefix: str) -> tuple[StoredObject, ...]:
        """List consistent object versions.

        This downloads each payload and is intended only for bounded
        maintenance prefixes.  Normal reads always resolve exact URIs from
        HEAD/Snapshot and never use listing to choose a current revision.
        """

        stored_objects: list[StoredObject] = []
        try:
            blobs = self._bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                properties = self._snapshot_blob_properties(blob)
                data = blob.download_as_bytes(
                    if_generation_match=properties.generation,
                    checksum="crc32c",
                )
                stored_objects.append(properties.stored_object(data))
        except Exception as exc:
            _raise_translated_gcs_error(exc, prefix, expected_generation=None)
        return tuple(sorted(stored_objects, key=lambda stored: stored.name))

    def delete(self, object_name: str, *, if_generation_match: int) -> None:
        if if_generation_match <= 0:
            msg = "delete requires a positive if_generation_match"
            raise ValueError(msg)
        blob = self._bucket.blob(object_name)
        try:
            blob.delete(if_generation_match=if_generation_match)
        except Exception as exc:
            _raise_translated_gcs_error(
                exc,
                object_name,
                expected_generation=if_generation_match,
            )

    def _snapshot_blob_properties(self, blob: _GcsBlob) -> _BlobProperties:
        return _BlobProperties(
            name=blob.name,
            generation=_required_generation(blob, blob.name),
            metadata=MappingProxyType(dict(blob.metadata or {})),
            content_type=blob.content_type or "application/octet-stream",
            cache_control=blob.cache_control or "",
            created_at=self._blob_created_at(blob),
        )

    def _blob_created_at(self, blob: _GcsBlob) -> datetime:
        created_at = blob.time_created or self._clock()
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            msg = f"GCS returned a naive creation timestamp for {blob.name}"
            raise StorageIntegrityError(msg)
        return created_at.astimezone(UTC)


class InMemoryObjectStore:
    """Thread-safe GCS semantics used by offline tests."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._objects: dict[str, StoredObject] = {}
        self._next_generation = 1
        self._lock = threading.RLock()

    def get(self, object_name: str) -> StoredObject:
        with self._lock:
            try:
                stored = self._objects[object_name]
            except KeyError as exc:
                raise ObjectNotFound(object_name) from exc
            return _copy_stored_object(stored)

    def put(
        self,
        object_name: str,
        data: bytes,
        *,
        if_generation_match: int,
        metadata: Mapping[str, str],
        content_type: str,
        cache_control: str,
    ) -> StoredObject:
        if if_generation_match < 0:
            msg = "if_generation_match must be non-negative"
            raise ValueError(msg)
        with self._lock:
            current = self._objects.get(object_name)
            actual_generation = current.generation if current is not None else None
            matches = (
                current is None
                if if_generation_match == 0
                else actual_generation == if_generation_match
            )
            if not matches:
                raise PreconditionFailed(
                    object_name,
                    expected_generation=if_generation_match,
                    actual_generation=actual_generation,
                )

            stored = StoredObject(
                name=object_name,
                data=bytes(data),
                generation=self._next_generation,
                metadata=MappingProxyType(dict(metadata)),
                content_type=content_type,
                cache_control=cache_control,
                created_at=self._validated_now(),
            )
            self._next_generation += 1
            self._objects[object_name] = stored
            return _copy_stored_object(stored)

    def list(self, prefix: str) -> tuple[StoredObject, ...]:
        with self._lock:
            return tuple(
                _copy_stored_object(self._objects[name])
                for name in sorted(self._objects)
                if name.startswith(prefix)
            )

    def delete(self, object_name: str, *, if_generation_match: int) -> None:
        if if_generation_match <= 0:
            msg = "delete requires a positive if_generation_match"
            raise ValueError(msg)
        with self._lock:
            current = self._objects.get(object_name)
            actual_generation = current.generation if current is not None else None
            if actual_generation != if_generation_match:
                raise PreconditionFailed(
                    object_name,
                    expected_generation=if_generation_match,
                    actual_generation=actual_generation,
                )
            del self._objects[object_name]

    def _validated_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            msg = "ObjectStore clock must return a timezone-aware datetime"
            raise ValueError(msg)
        return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class PublishedContent:
    """Result of idempotently publishing immutable canonical content."""

    reference: ContentReference
    object_generation: int
    canonical_size: int
    object_size: int
    created: bool


class ContentAddressedStore:
    """Idempotent immutable object publication and verified reads."""

    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    def publish(
        self,
        object_name: str,
        canonical_content: bytes,
        *,
        logical_content_type: str,
    ) -> PublishedContent:
        encoded = encode_content(canonical_content)
        if encoded.canonical_sha256 not in object_name:
            msg = "content-addressed object name does not contain canonical SHA-256"
            raise ValueError(msg)
        metadata = {
            "canonicalization": CANONICALIZATION_VERSION,
            "encoding": ENCODING_VERSION,
            "canonical-sha256": encoded.canonical_sha256,
            "object-sha256": encoded.object_sha256,
            "logical-content-type": logical_content_type,
        }
        created = True
        try:
            stored = self._store.put(
                object_name,
                encoded.object_bytes,
                if_generation_match=0,
                metadata=metadata,
                content_type="application/gzip",
                cache_control="no-transform",
            )
        except PreconditionFailed:
            created = False
            stored = self._store.get(object_name)
            _verify_existing_immutable_object(
                stored,
                expected_bytes=encoded.object_bytes,
                expected_metadata=metadata,
            )

        reference = ContentReference(
            uri=object_name,
            canonical_sha256=encoded.canonical_sha256,
            object_sha256=encoded.object_sha256,
        )
        return PublishedContent(
            reference=reference,
            object_generation=stored.generation,
            canonical_size=len(canonical_content),
            object_size=len(encoded.object_bytes),
            created=created,
        )

    def read(self, reference: ContentReference) -> bytes:
        """Read, decompress, and validate immutable canonical content."""

        stored = self._store.get(reference.uri)
        actual_object_hash = sha256_hex(stored.data)
        if actual_object_hash != reference.object_sha256:
            msg = f"object hash mismatch for {reference.uri}"
            raise StorageIntegrityError(msg)
        try:
            canonical_content = gzip.decompress(stored.data)
        except (gzip.BadGzipFile, EOFError, OSError) as exc:
            msg = f"invalid gzip object: {reference.uri}"
            raise StorageIntegrityError(msg) from exc
        actual_canonical_hash = sha256_hex(canonical_content)
        if actual_canonical_hash != reference.canonical_sha256:
            msg = f"canonical content hash mismatch for {reference.uri}"
            raise StorageIntegrityError(msg)
        _verify_integrity_metadata(stored, reference)
        return canonical_content


@dataclass(frozen=True, slots=True)
class HeadVersion:
    """A parsed HEAD and the GCS generation needed for its next CAS."""

    head: DataHead | None
    object_generation: int | None

    def __post_init__(self) -> None:
        if (self.head is None) != (self.object_generation is None):
            msg = "head and object_generation must either both exist or both be absent"
            raise ValueError(msg)


class HeadRepository:
    """Read and compare-and-swap the single mutable data HEAD."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        object_name: str = HEAD_OBJECT_NAME,
    ) -> None:
        self._store = store
        self._object_name = object_name

    def read(self) -> HeadVersion:
        try:
            stored = self._store.get(self._object_name)
        except ObjectNotFound:
            return HeadVersion(head=None, object_generation=None)
        actual_hash = sha256_hex(stored.data)
        metadata_hash = stored.metadata.get("canonical-sha256")
        if metadata_hash is not None and metadata_hash != actual_hash:
            msg = f"HEAD integrity metadata mismatch for {self._object_name}"
            raise StorageIntegrityError(msg)
        try:
            payload = json.loads(stored.data)
            head = DataHead.model_validate(payload)
        except (json.JSONDecodeError, ValueError) as exc:
            msg = f"invalid HEAD object: {self._object_name}"
            raise StorageIntegrityError(msg) from exc
        if canonical_json_bytes(head) != stored.data:
            msg = f"HEAD is not canonical FHCJ-1: {self._object_name}"
            raise StorageIntegrityError(msg)
        return HeadVersion(head=head, object_generation=stored.generation)

    def compare_and_swap(
        self,
        candidate: DataHead,
        *,
        base: HeadVersion,
    ) -> HeadVersion:
        """Commit a next HEAD exactly once, with no retry or deletion."""

        _validate_next_head(candidate, base)
        canonical_content = canonical_json_bytes(candidate)
        canonical_hash = sha256_hex(canonical_content)
        expected_generation = base.object_generation or 0
        stored = self._store.put(
            self._object_name,
            canonical_content,
            if_generation_match=expected_generation,
            metadata={
                "canonicalization": CANONICALIZATION_VERSION,
                "canonical-sha256": canonical_hash,
                "logical-content-type": "application/json",
            },
            content_type="application/json",
            cache_control="no-store",
        )
        return HeadVersion(head=candidate, object_generation=stored.generation)


def _validate_next_head(candidate: DataHead, base: HeadVersion) -> None:
    if base.head is None:
        if candidate.generation != 1 or candidate.parent_generation != 0:
            msg = "the first HEAD must be generation 1 with parent_generation 0"
            raise ValueError(msg)
        return
    if candidate.generation != base.head.generation + 1:
        msg = "candidate HEAD generation must increment the base by one"
        raise ValueError(msg)
    if candidate.parent_generation != base.head.generation:
        msg = "candidate HEAD parent_generation must equal the base generation"
        raise ValueError(msg)


def _verify_existing_immutable_object(
    stored: StoredObject,
    *,
    expected_bytes: bytes,
    expected_metadata: Mapping[str, str],
) -> None:
    if stored.data != expected_bytes:
        msg = f"immutable object collision or corruption: {stored.name}"
        raise StorageIntegrityError(msg)
    if stored.content_type != "application/gzip" or stored.cache_control != "no-transform":
        msg = f"immutable object transport metadata mismatch: {stored.name}"
        raise StorageIntegrityError(msg)
    for key, value in expected_metadata.items():
        if stored.metadata.get(key) != value:
            msg = f"immutable object metadata mismatch for {stored.name}: {key}"
            raise StorageIntegrityError(msg)


def _verify_integrity_metadata(
    stored: StoredObject,
    reference: ContentReference,
) -> None:
    expected = {
        "canonicalization": CANONICALIZATION_VERSION,
        "encoding": ENCODING_VERSION,
        "canonical-sha256": reference.canonical_sha256,
        "object-sha256": reference.object_sha256,
    }
    for key, value in expected.items():
        if stored.metadata.get(key) != value:
            msg = f"integrity metadata mismatch for {stored.name}: {key}"
            raise StorageIntegrityError(msg)


def _required_generation(blob: _GcsBlob, object_name: str) -> int:
    raw_generation = blob.generation
    try:
        generation = int(raw_generation) if raw_generation is not None else 0
    except (TypeError, ValueError) as exc:
        msg = f"GCS returned an invalid generation for {object_name}"
        raise StorageIntegrityError(msg) from exc
    if generation <= 0:
        msg = f"GCS did not return a generation for {object_name}"
        raise StorageIntegrityError(msg)
    return generation


def _raise_translated_gcs_error(
    exc: Exception,
    object_name: str,
    *,
    expected_generation: int | None,
) -> NoReturn:
    status = _gcs_status_code(exc)
    if status == HTTP_NOT_FOUND:
        raise ObjectNotFound(object_name) from exc
    if status == HTTP_PRECONDITION_FAILED:
        raise PreconditionFailed(
            object_name,
            expected_generation=expected_generation or 0,
            actual_generation=None,
        ) from exc
    raise exc


def _gcs_status_code(exc: Exception) -> int | None:
    raw_code: object = getattr(exc, "code", None)
    if callable(raw_code):
        raw_code = cast(Callable[[], object], raw_code)()
    if isinstance(raw_code, int):
        return raw_code

    response: object = getattr(exc, "response", None)
    response_status: object = getattr(response, "status_code", None)
    return response_status if isinstance(response_status, int) else None


def _copy_stored_object(stored: StoredObject) -> StoredObject:
    return StoredObject(
        name=stored.name,
        data=bytes(stored.data),
        generation=stored.generation,
        metadata=MappingProxyType(dict(stored.metadata)),
        content_type=stored.content_type,
        cache_control=stored.cache_control,
        created_at=stored.created_at,
    )
