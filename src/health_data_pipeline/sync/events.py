"""Durable, idempotent persistence for authenticated webhook events."""

from __future__ import annotations

import hashlib
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from health_data_pipeline.state.objects import ObjectNotFound, ObjectStore, PreconditionFailed


@dataclass(frozen=True, slots=True)
class VerifiedWebhook:
    """Sanitized event that is safe to persist for sync planning."""

    event_id: str
    owner_hmac: str
    payload: Mapping[str, Any]
    raw_body: bytes


class GcsWebhookEventSink:
    """Persist authenticated webhooks as immutable content-addressed objects."""

    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    def persist(self, event: VerifiedWebhook, *, received_at: datetime) -> str:
        utc = received_at.astimezone(UTC)
        path = (
            f"webhook-events/received_date={utc.date().isoformat()}/"
            f"sha256/{event.event_id[:2]}/{event.event_id}.json"
        )
        try:
            self._store.put(
                path,
                event.raw_body,
                if_generation_match=0,
                metadata={
                    "body-sha256": event.event_id,
                    "owner-hmac": event.owner_hmac,
                    "received-at": utc.isoformat().replace("+00:00", "Z"),
                },
                content_type="application/json",
                cache_control="no-store",
            )
        except PreconditionFailed:
            try:
                existing = self._store.get(path)
            except ObjectNotFound as exc:
                raise RuntimeError("webhook object disappeared after create conflict") from exc
            if not secrets.compare_digest(
                hashlib.sha256(existing.data).hexdigest(), event.event_id
            ):
                raise RuntimeError("webhook content-address collision")  # noqa: B904
        return path
