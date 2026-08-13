"""Google Health webhook authentication and identifier sanitization."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

from health_data_pipeline.setup.identity import identifier_hmac
from health_data_pipeline.sync.events import VerifiedWebhook


class WebhookAuthenticationError(ValueError):
    """Webhook authorization or signature validation failed."""


class WebhookPayloadError(ValueError):
    """Webhook JSON is malformed or cannot be processed yet."""


class WebhookSignatureVerifier(Protocol):
    def verify(self, signature: bytes, payload: bytes) -> bool: ...


def _decode_signature(value: str) -> bytes:
    compact = value.strip()
    if not compact:
        raise WebhookAuthenticationError("missing webhook signature")
    padding = "=" * (-len(compact) % 4)
    try:
        return base64.b64decode(compact + padding, altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as exc:
        raise WebhookAuthenticationError("invalid webhook signature encoding") from exc


@dataclass(frozen=True, slots=True)
class VerifiedWebhookBatch:
    """Validated owner events from one signed webhook request."""

    owner_hmac: str
    events: tuple[VerifiedWebhook, ...]


@dataclass(frozen=True, slots=True)
class WebhookVerification:
    """Authorized subscriber verification handshake."""

    payload: Mapping[str, Any]


def _notification_items(payload: Any) -> tuple[Mapping[str, Any], ...]:
    if isinstance(payload, Mapping):
        return (payload,)
    if not isinstance(payload, list):
        raise WebhookPayloadError("webhook payload must be an object or a non-empty array")
    if not payload:
        raise WebhookPayloadError("webhook notification batch must not be empty")
    if not all(isinstance(item, Mapping) for item in payload):
        raise WebhookPayloadError("webhook notification batch items must be objects")
    return tuple(cast(Mapping[str, Any], item) for item in payload)


def _verified_owner_event(
    notification: Mapping[str, Any],
    *,
    application_hmac_key: bytes,
    expected_owner_hmac: str,
) -> VerifiedWebhook | None:
    data = notification.get("data")
    if not isinstance(data, Mapping):
        raise WebhookPayloadError("webhook payload has no data object")
    raw_owner = data.get("healthUserId")
    if not isinstance(raw_owner, str) or not raw_owner:
        raise WebhookPayloadError("webhook payload has no healthUserId")
    data_type = data.get("dataType")
    if not isinstance(data_type, str) or not data_type:
        raise WebhookPayloadError("webhook payload has no dataType")
    owner = identifier_hmac(application_hmac_key, raw_owner)
    if not hmac.compare_digest(owner, expected_owner_hmac):
        return None
    sanitized_data = dict(data)
    sanitized_data.pop("healthUserId", None)
    sanitized_data["ownerHmac"] = owner
    sanitized_payload = dict(notification)
    sanitized_payload["data"] = sanitized_data
    sanitized_body = json.dumps(
        sanitized_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return VerifiedWebhook(
        event_id=hashlib.sha256(sanitized_body).hexdigest(),
        owner_hmac=owner,
        payload=cast(Mapping[str, Any], sanitized_payload),
        raw_body=sanitized_body,
    )


class GoogleHealthWebhookAuthenticator:
    """Authenticate a webhook before any payload is persisted."""

    def __init__(
        self,
        *,
        expected_authorization: str,
        signature_verifier: WebhookSignatureVerifier,
        application_hmac_key: bytes,
        expected_owner_hmac: str,
    ) -> None:
        self._expected_authorization = expected_authorization.encode("utf-8")
        self._signature_verifier = signature_verifier
        self._application_hmac_key = application_hmac_key
        self._expected_owner_hmac = expected_owner_hmac

    def authenticate(
        self,
        *,
        authorization: str | None,
        signature_header: str | None,
        content_type: str | None,
        raw_body: bytes,
    ) -> VerifiedWebhookBatch | WebhookVerification:
        if not content_type or content_type.split(";", 1)[0].strip().lower() != (
            "application/json"
        ):
            raise WebhookPayloadError("webhook content type must be application/json")
        supplied = (authorization or "").encode("utf-8")
        if not hmac.compare_digest(supplied, self._expected_authorization):
            raise WebhookAuthenticationError("webhook authorization failed")
        try:
            payload = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WebhookPayloadError("webhook body is not valid JSON") from exc
        if isinstance(payload, Mapping) and payload.get("type") == "verification":
            return WebhookVerification(payload={"type": "verification"})
        signature = _decode_signature(signature_header or "")
        if not self._signature_verifier.verify(signature, raw_body):
            raise WebhookAuthenticationError("webhook signature verification failed")

        notifications = _notification_items(payload)
        if not self._expected_owner_hmac:
            raise WebhookPayloadError("webhook owner is not configured")

        events: list[VerifiedWebhook] = []
        event_ids: set[str] = set()
        for notification in notifications:
            event = _verified_owner_event(
                notification,
                application_hmac_key=self._application_hmac_key,
                expected_owner_hmac=self._expected_owner_hmac,
            )
            if event is None or event.event_id in event_ids:
                continue
            event_ids.add(event.event_id)
            events.append(event)
        return VerifiedWebhookBatch(
            owner_hmac=self._expected_owner_hmac,
            events=tuple(events),
        )
