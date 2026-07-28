"""Rotating Google Health webhook public-key verification."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
from tink import json_proto_keyset_format, signature

GOOGLE_HEALTH_WEBHOOK_KEYSET_URL = (
    "https://www.gstatic.com/googlehealthapi/webhooks/webhooks_public_keyset.json"
)
HTTP_OK = 200


class GoogleHealthKeysetError(RuntimeError):
    """The official Google Health verification keyset could not be loaded."""


class GoogleHealthKeysetVerifier:
    """Cache the official keyset and refresh immediately after a verify failure."""

    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        refresh_seconds: float = 6 * 60 * 60,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if refresh_seconds <= 0:
            raise ValueError("keyset refresh interval must be positive")
        self._client = client or httpx.Client(timeout=20.0, follow_redirects=False)
        self._refresh_seconds = refresh_seconds
        self._monotonic = monotonic
        self._primitive: Any | None = None
        self._refresh_at = 0.0
        self._lock = threading.Lock()
        signature.register()

    def verify(self, signature_bytes: bytes, payload: bytes) -> bool:
        primitive = self._get_primitive(force=False)
        if _verify(primitive, signature_bytes, payload):
            return True
        refreshed = self._get_primitive(force=True)
        return _verify(refreshed, signature_bytes, payload)

    def _get_primitive(self, *, force: bool) -> Any:
        now = self._monotonic()
        with self._lock:
            if not force and self._primitive is not None and now < self._refresh_at:
                return self._primitive
            response = self._client.get(GOOGLE_HEALTH_WEBHOOK_KEYSET_URL)
            if response.status_code != HTTP_OK:
                raise GoogleHealthKeysetError(
                    f"webhook keyset request failed ({response.status_code})"
                )
            try:
                handle = json_proto_keyset_format.parse_without_secret(response.text)
                primitive = handle.primitive(signature.PublicKeyVerify)
            except Exception as exc:
                raise GoogleHealthKeysetError("webhook keyset is invalid") from exc
            self._primitive = primitive
            self._refresh_at = now + self._refresh_seconds
            return primitive


def _verify(primitive: Any, signature_bytes: bytes, payload: bytes) -> bool:
    try:
        primitive.verify(signature_bytes, payload)
    except Exception:
        return False
    return True
