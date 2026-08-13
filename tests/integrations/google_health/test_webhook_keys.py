from __future__ import annotations

from typing import Any

import httpx
import pytest

from health_data_pipeline.integrations.google_health.webhook_keys import (
    GoogleHealthKeysetError,
    GoogleHealthKeysetVerifier,
)

_PARSE_KEYSET_TARGET = (
    "health_data_pipeline.integrations.google_health.webhook_keys."
    "json_proto_keyset_format.parse_without_secret"
)
_REGISTER_SIGNATURE_TARGET = (
    "health_data_pipeline.integrations.google_health.webhook_keys.signature.register"
)


class Primitive:
    def __init__(self, accepted: bytes) -> None:
        self.accepted = accepted
        self.calls = 0

    def verify(self, signature_bytes: bytes, payload: bytes) -> None:
        self.calls += 1
        if signature_bytes != self.accepted or payload != b"payload":
            raise ValueError("invalid signature")


class Handle:
    def __init__(self, primitive: Primitive) -> None:
        self.primitive_value = primitive

    def primitive(self, primitive_class: Any) -> Primitive:
        return self.primitive_value


def test_keyset_is_cached_until_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primitive = Primitive(b"signature")
    fetches = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal fetches
        fetches += 1
        return httpx.Response(200, text='{"primaryKeyId":1}')

    monkeypatch.setattr(
        _PARSE_KEYSET_TARGET,
        lambda _: Handle(primitive),
    )
    monkeypatch.setattr(_REGISTER_SIGNATURE_TARGET, lambda: None)
    verifier = GoogleHealthKeysetVerifier(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        monotonic=lambda: 10.0,
    )

    assert verifier.verify(b"signature", b"payload")
    assert verifier.verify(b"signature", b"payload")
    assert fetches == 1


def test_verify_failure_forces_key_rotation_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handles = iter((Handle(Primitive(b"old")), Handle(Primitive(b"new"))))
    fetches = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal fetches
        fetches += 1
        return httpx.Response(200, text="{}")

    monkeypatch.setattr(
        _PARSE_KEYSET_TARGET,
        lambda _: next(handles),
    )
    monkeypatch.setattr(_REGISTER_SIGNATURE_TARGET, lambda: None)
    verifier = GoogleHealthKeysetVerifier(
        client=httpx.Client(transport=httpx.MockTransport(handler))
    )

    assert verifier.verify(b"new", b"payload")
    assert fetches == 2


def test_keyset_http_and_parse_errors_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_REGISTER_SIGNATURE_TARGET, lambda: None)
    failed = GoogleHealthKeysetVerifier(
        client=httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(503, text="secret response"))
        )
    )
    with pytest.raises(GoogleHealthKeysetError) as captured:
        failed.verify(b"signature", b"payload")
    assert "503" in str(captured.value)
    assert "secret response" not in str(captured.value)

    monkeypatch.setattr(
        _PARSE_KEYSET_TARGET,
        lambda _: (_ for _ in ()).throw(ValueError("sensitive parse detail")),
    )
    invalid = GoogleHealthKeysetVerifier(
        client=httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, text="invalid"))
        )
    )
    with pytest.raises(GoogleHealthKeysetError, match="keyset is invalid"):
        invalid.verify(b"signature", b"payload")
