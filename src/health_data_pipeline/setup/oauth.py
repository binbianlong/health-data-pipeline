"""One-time setup code and OAuth session state machine.

All persistent mutations use generation preconditions through ``ObjectStore``.
The concrete GCS adapter is intentionally injected so the state machine can be
tested without credentials or network access.
"""

# ruff: noqa: FURB162, N818, PLC0415, PLR2004, TRY300, TRY301

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Final, Protocol, cast

from health_data_pipeline.setup.identity import identifier_hmac
from health_data_pipeline.state import versioned as _versioned_state
from health_data_pipeline.state.objects import (
    ObjectStore,
    PreconditionFailed,
)

VersionedObject = _versioned_state.VersionedObject
read_state: Final = _versioned_state.read_state
write_state: Final = _versioned_state.write_state

SETUP_CODE_PATH = "state/oauth/setup-code.json"
OWNER_PATH = "state/owner.json"
SESSION_PREFIX = "state/oauth/sessions"


class OAuthStateError(RuntimeError):
    """Base class for setup and OAuth state errors."""


CasMismatch = PreconditionFailed


class InvalidSetupCode(OAuthStateError):
    """The setup code is wrong, expired, or has already been consumed."""


class InvalidOAuthSession(OAuthStateError):
    """The OAuth session is missing, expired, replayed, or in the wrong state."""


class OwnerMismatch(OAuthStateError):
    """The OAuth account differs from the pipeline owner."""


class SecretCipher(Protocol):
    def encrypt(self, plaintext: bytes, *, associated_data: bytes) -> str: ...

    def decrypt(self, ciphertext: str, *, associated_data: bytes) -> bytes: ...


class RefreshTokenSink(Protocol):
    def store(self, refresh_token: str) -> None: ...


class OAuthProvider(Protocol):
    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
    ) -> str: ...

    def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
        expected_nonce: str,
    ) -> OAuthGrant: ...


class SessionStatus(StrEnum):
    ISSUED = "ISSUED"
    AUTHORIZING = "AUTHORIZING"
    CALLBACK_CLAIMED = "CALLBACK_CLAIMED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


_TERMINAL = frozenset({SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.EXPIRED})
_ALLOWED_TRANSITIONS: Mapping[SessionStatus, frozenset[SessionStatus]] = {
    SessionStatus.ISSUED: frozenset(
        {SessionStatus.AUTHORIZING, SessionStatus.FAILED, SessionStatus.EXPIRED}
    ),
    SessionStatus.AUTHORIZING: frozenset(
        {
            SessionStatus.CALLBACK_CLAIMED,
            SessionStatus.FAILED,
            SessionStatus.EXPIRED,
        }
    ),
    SessionStatus.CALLBACK_CLAIMED: frozenset(
        {SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.EXPIRED}
    ),
    SessionStatus.COMPLETED: frozenset(),
    SessionStatus.FAILED: frozenset(),
    SessionStatus.EXPIRED: frozenset(),
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise OAuthStateError("stored timestamp is not timezone-aware")
    return parsed.astimezone(UTC)


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _token(byte_count: int = 32) -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(byte_count)).rstrip(b"=").decode()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _session_path(session_id_hmac: str) -> str:
    return f"{SESSION_PREFIX}/{session_id_hmac}.json"


@dataclass(frozen=True, slots=True)
class SetupCodeRecord:
    schema_version: int
    status: str
    code_sha256: str
    issued_at: str
    expires_at: str
    consumed_at: str | None = None


@dataclass(frozen=True, slots=True)
class SessionRecord:
    schema_version: int
    session_id_hmac: str
    status: SessionStatus
    issued_at: str
    expires_at: str
    csrf_token_hash: str
    oauth_state_hash: str | None = None
    oidc_nonce_hash: str | None = None
    oidc_nonce_ciphertext: str | None = None
    pkce_challenge: str | None = None
    pkce_verifier_ciphertext: str | None = None
    callback_claimed_at: str | None = None
    completed_at: str | None = None
    failed_at: str | None = None

    @classmethod
    def from_bytes(cls, data: bytes) -> SessionRecord:
        try:
            raw = json.loads(data)
            raw["status"] = SessionStatus(raw["status"])
            return cls(**raw)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise OAuthStateError("invalid stored OAuth session") from exc

    def to_bytes(self) -> bytes:
        value = asdict(self)
        value["status"] = self.status.value
        return _canonical_json(value)

    def expires(self) -> datetime:
        return _parse_timestamp(self.expires_at)


@dataclass(frozen=True, slots=True)
class OAuthGrant:
    """A verified OAuth result.

    ``sub`` must come from a cryptographically verified ID token with matching
    issuer, audience and nonce. ``health_user_id`` must come from Google Health.
    """

    sub: str
    health_user_id: str
    refresh_token: str


@dataclass(frozen=True, slots=True)
class IssuedSetupCode:
    code: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class IssuedSession:
    session_id: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class AuthorizationStart:
    authorization_url: str
    expires_at: datetime


class SetupCodeService:
    """Issue and atomically consume a single active setup code."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        ttl: timedelta = timedelta(minutes=10),
    ) -> None:
        self._store = store
        self._ttl = ttl

    def issue(self, *, now: datetime | None = None) -> IssuedSetupCode:
        issued_at = _utc_now() if now is None else now.astimezone(UTC)
        code = _token()
        record = SetupCodeRecord(
            schema_version=1,
            status="ISSUED",
            code_sha256=_sha256_text(code),
            issued_at=_timestamp(issued_at),
            expires_at=_timestamp(issued_at + self._ttl),
        )
        current = read_state(self._store, SETUP_CODE_PATH)
        expected = 0 if current is None else current.generation
        write_state(
            self._store,
            SETUP_CODE_PATH,
            _canonical_json(asdict(record)),
            if_generation_match=expected,
        )
        return IssuedSetupCode(code=code, expires_at=issued_at + self._ttl)

    def consume(self, code: str, *, now: datetime | None = None) -> SetupCodeRecord:
        current_time = _utc_now() if now is None else now.astimezone(UTC)
        current = read_state(self._store, SETUP_CODE_PATH)
        if current is None:
            raise InvalidSetupCode("no setup code has been issued")
        try:
            raw = json.loads(current.data)
            record = SetupCodeRecord(**raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise OAuthStateError("invalid stored setup code") from exc
        valid_hash = hmac.compare_digest(record.code_sha256, _sha256_text(code))
        if (
            record.status != "ISSUED"
            or current_time >= _parse_timestamp(record.expires_at)
            or not valid_hash
        ):
            raise InvalidSetupCode("setup code is invalid or expired")
        consumed = replace(record, status="CONSUMED", consumed_at=_timestamp(current_time))
        try:
            write_state(
                self._store,
                SETUP_CODE_PATH,
                _canonical_json(asdict(consumed)),
                if_generation_match=current.generation,
            )
        except CasMismatch as exc:
            raise InvalidSetupCode("setup code has already been consumed") from exc
        return consumed


class OwnerRegistry:
    """Persist only HMACs of the immutable owner identifiers."""

    def __init__(self, store: ObjectStore, application_hmac_key: bytes) -> None:
        self._store = store
        self._key = application_hmac_key

    def verify_or_create(self, *, sub: str, health_user_id: str) -> None:
        sub_hmac = identifier_hmac(self._key, sub)
        health_user_id_hmac = identifier_hmac(self._key, health_user_id)
        candidate = {
            "schema_version": 1,
            "sub_hmac": sub_hmac,
            "health_user_id_hmac": health_user_id_hmac,
        }
        current = read_state(self._store, OWNER_PATH)
        if current is None:
            try:
                write_state(
                    self._store,
                    OWNER_PATH,
                    _canonical_json(candidate),
                    if_generation_match=0,
                )
                return
            except CasMismatch:
                current = read_state(self._store, OWNER_PATH)
        if current is None:
            raise OAuthStateError("owner creation raced but no owner exists")
        try:
            stored = json.loads(current.data)
        except json.JSONDecodeError as exc:
            raise OAuthStateError("invalid stored owner") from exc
        matches = hmac.compare_digest(
            str(stored.get("sub_hmac", "")), sub_hmac
        ) and hmac.compare_digest(
            str(stored.get("health_user_id_hmac", "")),
            health_user_id_hmac,
        )
        if not matches:
            raise OwnerMismatch("OAuth account does not match the configured owner")


class OAuthSessionService:
    """Drive setup and OAuth using an explicit, CAS-protected state machine."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        setup_codes: SetupCodeService,
        cipher: SecretCipher,
        provider: OAuthProvider,
        owner_registry: OwnerRegistry,
        refresh_tokens: RefreshTokenSink,
        application_hmac_key: bytes,
        session_ttl: timedelta = timedelta(minutes=10),
    ) -> None:
        self._store = store
        self._setup_codes = setup_codes
        self._cipher = cipher
        self._provider = provider
        self._owner_registry = owner_registry
        self._refresh_tokens = refresh_tokens
        self._application_hmac_key = application_hmac_key
        self._session_ttl = session_ttl

    def _read_session(self, session_id: str) -> tuple[SessionRecord, int, str]:
        session_hmac = identifier_hmac(self._application_hmac_key, session_id)
        path = _session_path(session_hmac)
        current = read_state(self._store, path)
        if current is None:
            raise InvalidOAuthSession("OAuth session does not exist")
        record = SessionRecord.from_bytes(current.data)
        if not hmac.compare_digest(record.session_id_hmac, session_hmac):
            raise InvalidOAuthSession("OAuth session identifier mismatch")
        return record, current.generation, path

    def _transition(
        self,
        record: SessionRecord,
        generation: int,
        path: str,
        target: SessionStatus,
        **changes: str | None,
    ) -> SessionRecord:
        if target not in _ALLOWED_TRANSITIONS[record.status]:
            raise InvalidOAuthSession(
                f"OAuth session cannot transition from {record.status} to {target}"
            )
        record_data: dict[str, Any] = asdict(record)
        record_data.update(changes)
        record_data["status"] = target
        updated = SessionRecord(**record_data)
        try:
            write_state(
                self._store,
                path,
                updated.to_bytes(),
                if_generation_match=generation,
            )
        except CasMismatch as exc:
            raise InvalidOAuthSession("OAuth session was concurrently updated") from exc
        return updated

    def _expire_if_needed(
        self,
        record: SessionRecord,
        generation: int,
        path: str,
        now: datetime,
    ) -> None:
        if now < record.expires() or record.status in _TERMINAL:
            return
        self._transition(
            record,
            generation,
            path,
            SessionStatus.EXPIRED,
            failed_at=_timestamp(now),
        )
        raise InvalidOAuthSession("OAuth session has expired")

    def create_session(
        self,
        *,
        setup_code: str,
        now: datetime | None = None,
    ) -> IssuedSession:
        current_time = _utc_now() if now is None else now.astimezone(UTC)
        self._setup_codes.consume(setup_code, now=current_time)
        session_id = _token()
        session_hmac = identifier_hmac(self._application_hmac_key, session_id)
        expires_at = current_time + self._session_ttl
        record = SessionRecord(
            schema_version=1,
            session_id_hmac=session_hmac,
            status=SessionStatus.ISSUED,
            issued_at=_timestamp(current_time),
            expires_at=_timestamp(expires_at),
            csrf_token_hash=_sha256_text(_token()),
        )
        write_state(
            self._store,
            _session_path(session_hmac),
            record.to_bytes(),
            if_generation_match=0,
        )
        return IssuedSession(session_id=session_id, expires_at=expires_at)

    def start_authorization(
        self,
        *,
        session_id: str,
        now: datetime | None = None,
    ) -> AuthorizationStart:
        current_time = _utc_now() if now is None else now.astimezone(UTC)
        record, generation, path = self._read_session(session_id)
        self._expire_if_needed(record, generation, path, current_time)
        if record.status is not SessionStatus.ISSUED:
            raise InvalidOAuthSession("OAuth session has already been started")

        oauth_state = _token()
        nonce = _token()
        verifier = _token(64)
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        aad = record.session_id_hmac.encode("ascii")
        self._transition(
            record,
            generation,
            path,
            SessionStatus.AUTHORIZING,
            oauth_state_hash=_sha256_text(oauth_state),
            oidc_nonce_hash=_sha256_text(nonce),
            oidc_nonce_ciphertext=self._cipher.encrypt(nonce.encode("utf-8"), associated_data=aad),
            pkce_challenge=challenge,
            pkce_verifier_ciphertext=self._cipher.encrypt(
                verifier.encode("utf-8"), associated_data=aad
            ),
        )
        return AuthorizationStart(
            authorization_url=self._provider.authorization_url(
                state=oauth_state,
                nonce=nonce,
                code_challenge=challenge,
            ),
            expires_at=record.expires(),
        )

    def complete_callback(
        self,
        *,
        session_id: str,
        state: str,
        code: str,
        now: datetime | None = None,
    ) -> OAuthGrant:
        current_time = _utc_now() if now is None else now.astimezone(UTC)
        record, generation, path = self._read_session(session_id)
        self._expire_if_needed(record, generation, path, current_time)
        if record.status is not SessionStatus.AUTHORIZING:
            raise InvalidOAuthSession("OAuth callback was already claimed")
        if record.oauth_state_hash is None or not hmac.compare_digest(
            record.oauth_state_hash, _sha256_text(state)
        ):
            self._transition(
                record,
                generation,
                path,
                SessionStatus.FAILED,
                failed_at=_timestamp(current_time),
            )
            raise InvalidOAuthSession("OAuth state validation failed")
        if record.pkce_verifier_ciphertext is None or record.oidc_nonce_ciphertext is None:
            raise InvalidOAuthSession("OAuth session has incomplete PKCE state")

        claimed = self._transition(
            record,
            generation,
            path,
            SessionStatus.CALLBACK_CLAIMED,
            callback_claimed_at=_timestamp(current_time),
        )
        aad = claimed.session_id_hmac.encode("ascii")
        try:
            verifier = self._cipher.decrypt(
                cast(str, claimed.pkce_verifier_ciphertext), associated_data=aad
            ).decode("utf-8")
            nonce = self._cipher.decrypt(
                cast(str, claimed.oidc_nonce_ciphertext), associated_data=aad
            ).decode("utf-8")
            grant = self._provider.exchange_code(
                code=code,
                code_verifier=verifier,
                expected_nonce=nonce,
            )
            if not grant.refresh_token:
                raise OAuthStateError("OAuth response did not include a refresh token")
            self._owner_registry.verify_or_create(
                sub=grant.sub, health_user_id=grant.health_user_id
            )
            self._refresh_tokens.store(grant.refresh_token)
        except Exception:
            latest, latest_generation, latest_path = self._read_session(session_id)
            if latest.status is SessionStatus.CALLBACK_CLAIMED:
                self._transition(
                    latest,
                    latest_generation,
                    latest_path,
                    SessionStatus.FAILED,
                    failed_at=_timestamp(current_time),
                )
            raise

        latest, latest_generation, latest_path = self._read_session(session_id)
        if latest.status is not SessionStatus.CALLBACK_CLAIMED:
            raise InvalidOAuthSession("OAuth callback state changed unexpectedly")
        self._transition(
            latest,
            latest_generation,
            latest_path,
            SessionStatus.COMPLETED,
            completed_at=_timestamp(current_time),
        )
        return grant


class AesGcmCipher:
    """Small SecretCipher adapter using an injected 256-bit AES key."""

    def __init__(self, key: bytes) -> None:
        if len(key) != 32:
            raise ValueError("AES-GCM key must be 32 bytes")
        self._key = key

    def encrypt(self, plaintext: bytes, *, associated_data: bytes) -> str:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        nonce = secrets.token_bytes(12)
        ciphertext = AESGCM(self._key).encrypt(nonce, plaintext, associated_data)
        return base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")

    def decrypt(self, ciphertext: str, *, associated_data: bytes) -> bytes:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        encoded = base64.urlsafe_b64decode(ciphertext.encode("ascii"))
        if len(encoded) < 13:
            raise OAuthStateError("encrypted OAuth field is truncated")
        return AESGCM(self._key).decrypt(encoded[:12], encoded[12:], associated_data)
