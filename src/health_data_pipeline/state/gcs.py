"""Google Cloud Storage adapter for the object-store boundary."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import NoReturn, Protocol, cast

from health_data_pipeline.state.objects import (
    ObjectNotFound,
    PreconditionFailed,
    StorageIntegrityError,
    StoredObject,
)

GCS_LIST_PAGE_SIZE = 1_000
HTTP_NOT_FOUND = 404
HTTP_PRECONDITION_FAILED = 412


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

    def list_blobs(
        self,
        *,
        prefix: str,
        page_size: int,
    ) -> Iterable[_GcsBlob]: ...


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
    """Production object store backed by ``google-cloud-storage``."""

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
        """List consistent object versions for bounded maintenance prefixes."""

        stored_objects: list[StoredObject] = []
        try:
            blobs = self._bucket.list_blobs(
                prefix=prefix,
                page_size=GCS_LIST_PAGE_SIZE,
            )
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

    def list_names(self, prefix: str) -> tuple[str, ...]:
        """List metadata only, without a payload download per object."""

        try:
            names = tuple(
                blob.name
                for blob in self._bucket.list_blobs(
                    prefix=prefix,
                    page_size=GCS_LIST_PAGE_SIZE,
                )
            )
        except Exception as exc:
            _raise_translated_gcs_error(exc, prefix, expected_generation=None)
        return tuple(sorted(names))

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
