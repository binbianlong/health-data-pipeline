from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pytest

from health_data_pipeline.oauth import (
    InvalidOAuthSession,
    InvalidSetupCode,
    OAuthGrant,
    OAuthSessionService,
    OwnerMismatch,
    OwnerRegistry,
    SessionRecord,
    SessionStatus,
    SetupCodeService,
    read_state,
)
from health_data_pipeline.storage import InMemoryObjectStore

NOW = datetime(2026, 7, 28, tzinfo=UTC)


class FakeCipher:
    def encrypt(self, plaintext: bytes, *, associated_data: bytes) -> str:
        return (associated_data + b":" + plaintext).hex()

    def decrypt(self, ciphertext: str, *, associated_data: bytes) -> bytes:
        decoded = bytes.fromhex(ciphertext)
        prefix = associated_data + b":"
        assert decoded.startswith(prefix)
        return decoded[len(prefix) :]


class FakeProvider:
    def __init__(self, grant: OAuthGrant) -> None:
        self.grant = grant
        self.exchanges = 0

    def authorization_url(self, *, state: str, nonce: str, code_challenge: str) -> str:
        return (
            "https://accounts.example/authorize?"
            f"state={state}&nonce={nonce}&code_challenge={code_challenge}"
        )

    def exchange_code(self, *, code: str, code_verifier: str, expected_nonce: str) -> OAuthGrant:
        assert code == "authorization-code"
        assert code_verifier
        assert expected_nonce
        self.exchanges += 1
        return self.grant


class TokenSink:
    def __init__(self) -> None:
        self.tokens: list[str] = []

    def store(self, refresh_token: str) -> None:
        self.tokens.append(refresh_token)


def _service(
    store: InMemoryObjectStore,
    grant: OAuthGrant | None = None,
) -> tuple[OAuthSessionService, SetupCodeService, FakeProvider, TokenSink]:
    setup = SetupCodeService(store)
    provider = FakeProvider(grant or OAuthGrant("google-sub", "health-user", "refresh-token"))
    sink = TokenSink()
    service = OAuthSessionService(
        store=store,
        setup_codes=setup,
        cipher=FakeCipher(),
        provider=provider,
        owner_registry=OwnerRegistry(store, b"application-key"),
        refresh_tokens=sink,
        application_hmac_key=b"application-key",
    )
    return service, setup, provider, sink


def test_setup_code_and_callback_follow_state_machine() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    service, setup, provider, sink = _service(store)
    code = setup.issue(now=NOW).code
    session = service.create_session(setup_code=code, now=NOW)
    start = service.start_authorization(session_id=session.session_id, now=NOW)
    state = parse_qs(urlparse(start.authorization_url).query)["state"][0]

    grant = service.complete_callback(
        session_id=session.session_id,
        state=state,
        code="authorization-code",
        now=NOW,
    )

    assert grant.sub == "google-sub"
    assert provider.exchanges == 1
    assert sink.tokens == ["refresh-token"]
    session_objects = store.list("state/oauth/sessions/")
    record = SessionRecord.from_bytes(session_objects[0].data)
    assert record.status is SessionStatus.COMPLETED
    assert read_state(store, "state/owner.json") is not None

    with pytest.raises(InvalidOAuthSession):
        service.complete_callback(
            session_id=session.session_id,
            state=state,
            code="authorization-code",
            now=NOW,
        )
    assert provider.exchanges == 1


def test_setup_code_is_one_time_and_expires() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    service, setup, _, _ = _service(store)
    issued = setup.issue(now=NOW)
    service.create_session(setup_code=issued.code, now=NOW)

    with pytest.raises(InvalidSetupCode):
        service.create_session(setup_code=issued.code, now=NOW)

    expired = setup.issue(now=NOW).code
    with pytest.raises(InvalidSetupCode):
        service.create_session(
            setup_code=expired,
            now=NOW + timedelta(minutes=11),
        )


def test_reauthorization_rejects_different_owner() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    first, setup, _, _ = _service(store)
    code = setup.issue(now=NOW).code
    session = first.create_session(setup_code=code, now=NOW)
    start = first.start_authorization(session_id=session.session_id, now=NOW)
    state = parse_qs(urlparse(start.authorization_url).query)["state"][0]
    first.complete_callback(
        session_id=session.session_id,
        state=state,
        code="authorization-code",
        now=NOW,
    )

    second, second_setup, _, sink = _service(
        store,
        OAuthGrant("another-sub", "another-health-user", "new-token"),
    )
    second_code = second_setup.issue(now=NOW).code
    second_session = second.create_session(setup_code=second_code, now=NOW)
    second_start = second.start_authorization(session_id=second_session.session_id, now=NOW)
    second_state = parse_qs(urlparse(second_start.authorization_url).query)["state"][0]

    with pytest.raises(OwnerMismatch):
        second.complete_callback(
            session_id=second_session.session_id,
            state=second_state,
            code="authorization-code",
            now=NOW,
        )
    assert sink.tokens == []
