"""Object-store boundary and content-addressed publication primitives."""

from __future__ import annotations

import gzip
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Protocol

from health_data_pipeline.state.canonical import (
    CANONICALIZATION_VERSION,
    ENCODING_VERSION,
    encode_content,
    sha256_hex,
)
from health_data_pipeline.state.models import ContentReference


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

    def list_names(self, prefix: str) -> Iterable[str]:
        """List object names without downloading their payloads."""

    def delete(self, object_name: str, *, if_generation_match: int) -> None:
        """Delete one explicitly resolved object with a generation guard."""


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

    def list_names(self, prefix: str) -> tuple[str, ...]:
        with self._lock:
            return tuple(name for name in sorted(self._objects) if name.startswith(prefix))

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
