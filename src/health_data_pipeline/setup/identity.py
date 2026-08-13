"""Stable pseudonymous identifiers shared by setup and webhook intake."""

from __future__ import annotations

import hashlib
import hmac


def identifier_hmac(key: bytes, identifier: str) -> str:
    """Return a deterministic HMAC without persisting the source identifier."""

    if not key:
        raise ValueError("HMAC key must not be empty")
    if not identifier:
        raise ValueError("identifier must not be empty")
    return hmac.new(key, identifier.encode("utf-8"), hashlib.sha256).hexdigest()
