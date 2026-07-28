"""Advisory GCS lease used only to reduce duplicate synchronization work."""

from __future__ import annotations

import json
import secrets
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from health_data_pipeline.canonical import canonical_json_bytes
from health_data_pipeline.storage import (
    ObjectNotFound,
    ObjectStore,
    PreconditionFailed,
)


@dataclass(frozen=True, slots=True)
class LeaseRecord:
    token: str
    acquired_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class LeaseHandle:
    object_name: str
    object_generation: int
    record: LeaseRecord


class LeaseRepository:
    """Acquire, refresh and release an advisory lease with generation guards."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        owner_hmac: str,
        ttl: timedelta = timedelta(minutes=15),
    ) -> None:
        if ttl <= timedelta(0):
            raise ValueError("lease TTL must be positive")
        self._store = store
        self._object_name = f"leases/{owner_hmac}.json"
        self._ttl = ttl

    def acquire(self, *, now: datetime | None = None) -> LeaseHandle | None:
        current_time = _utc(now)
        expected_generation = 0
        try:
            existing = self._store.get(self._object_name)
        except ObjectNotFound:
            pass
        else:
            record = _parse_record(existing.data)
            if record.expires_at > current_time:
                return None
            expected_generation = existing.generation

        record = LeaseRecord(
            token=secrets.token_urlsafe(32),
            acquired_at=current_time,
            expires_at=current_time + self._ttl,
        )
        try:
            stored = self._store.put(
                self._object_name,
                canonical_json_bytes(_record_payload(record)),
                if_generation_match=expected_generation,
                metadata={"schema": "advisory-lease-v1"},
                content_type="application/json",
                cache_control="no-store",
            )
        except PreconditionFailed:
            return None
        return LeaseHandle(
            object_name=self._object_name,
            object_generation=stored.generation,
            record=record,
        )

    def refresh(
        self,
        handle: LeaseHandle,
        *,
        now: datetime | None = None,
    ) -> LeaseHandle | None:
        current_time = _utc(now)
        try:
            current = self._store.get(handle.object_name)
        except ObjectNotFound:
            return None
        current_record = _parse_record(current.data)
        if current.generation != handle.object_generation or not secrets.compare_digest(
            current_record.token, handle.record.token
        ):
            return None
        refreshed = LeaseRecord(
            token=handle.record.token,
            acquired_at=handle.record.acquired_at,
            expires_at=current_time + self._ttl,
        )
        try:
            stored = self._store.put(
                handle.object_name,
                canonical_json_bytes(_record_payload(refreshed)),
                if_generation_match=handle.object_generation,
                metadata={"schema": "advisory-lease-v1"},
                content_type="application/json",
                cache_control="no-store",
            )
        except PreconditionFailed:
            return None
        return LeaseHandle(
            object_name=handle.object_name,
            object_generation=stored.generation,
            record=refreshed,
        )

    def release(self, handle: LeaseHandle) -> bool:
        try:
            current = self._store.get(handle.object_name)
        except ObjectNotFound:
            return False
        record = _parse_record(current.data)
        if current.generation != handle.object_generation or not secrets.compare_digest(
            record.token, handle.record.token
        ):
            return False
        try:
            self._store.delete(
                handle.object_name,
                if_generation_match=handle.object_generation,
            )
        except (ObjectNotFound, PreconditionFailed):
            return False
        return True


def _record_payload(record: LeaseRecord) -> dict[str, Any]:
    value = asdict(record)
    value["acquired_at"] = record.acquired_at
    value["expires_at"] = record.expires_at
    return value


def _parse_record(data: bytes) -> LeaseRecord:
    try:
        value = json.loads(data)
        token = value["token"]
        acquired_at = datetime.fromisoformat(value["acquired_at"])
        expires_at = datetime.fromisoformat(value["expires_at"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid advisory lease object") from exc
    if not isinstance(token, str) or not token:
        raise ValueError("invalid advisory lease token")
    return LeaseRecord(
        token=token,
        acquired_at=_utc(acquired_at),
        expires_at=_utc(expires_at),
    )


def _utc(value: datetime | None) -> datetime:
    result = datetime.now(UTC) if value is None else value
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("lease time must be timezone-aware")
    return result.astimezone(UTC)
