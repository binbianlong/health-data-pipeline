from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from health_data_pipeline.config import AppConfig
from health_data_pipeline.google_health import (
    GoogleHealthWebhookAuthenticator,
    WebhookAuthenticationError,
    WebhookPayloadError,
    WebhookVerification,
    identifier_hmac,
)
from health_data_pipeline.oauth import AuthorizationStart, IssuedSession, OAuthGrant
from health_data_pipeline.runtime import DynamicOwnerWebhookAuthenticator
from health_data_pipeline.storage import InMemoryObjectStore
from health_data_pipeline.web import (
    SESSION_COOKIE,
    WebDependencies,
    create_app,
)


class SignatureVerifier:
    def verify(self, signature: bytes, payload: bytes) -> bool:
        return signature == b"valid"


class EventSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def persist(self, event: Any, *, received_at: datetime) -> str:
        self.events.append((event, received_at))
        return "webhook-events/event.json"


class Tasks:
    def __init__(self) -> None:
        self.syncs: list[tuple[str, datetime]] = []

    def enqueue_sync_job(self, *, owner_hmac: str, ready_at: datetime) -> None:
        self.syncs.append((owner_hmac, ready_at))


class OAuth:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.completed: list[str] = []

    def create_session(self, *, setup_code: str) -> IssuedSession:
        self.created.append(setup_code)
        return IssuedSession(
            session_id="session-id",
            expires_at=datetime(2026, 7, 28, tzinfo=UTC),
        )

    def start_authorization(self, *, session_id: str) -> AuthorizationStart:
        assert session_id == "session-id"
        return AuthorizationStart(
            authorization_url="https://accounts.example/authorize?state=oauth-state",
            expires_at=datetime(2026, 7, 28, tzinfo=UTC),
        )

    def complete_callback(self, *, session_id: str, state: str, code: str) -> OAuthGrant:
        assert session_id == "session-id"
        assert state == "oauth-state"
        self.completed.append(code)
        return OAuthGrant("sub", "health-user", "refresh")


class Oidc:
    def __init__(self, email: str = "tasks@example.iam.gserviceaccount.com") -> None:
        self.email = email

    def verify(self, token: str, *, audience: str) -> dict[str, Any]:
        assert token == "id-token"
        return {"email": self.email, "email_verified": True, "aud": audience}


class Views:
    def __init__(self) -> None:
        self.generations: list[int] = []

    def apply_latest(self, requested_generation: int) -> int:
        self.generations.append(requested_generation)
        return requested_generation


class Steps:
    def read_current(self) -> tuple[bytes, int]:
        return b"<svg></svg>", 12


def _config() -> AppConfig:
    return AppConfig(
        project_id="project",
        region="us-central1",
        health_bucket="bucket",
        cloud_run_job="sync",
        cloud_tasks_queue="queue",
        cloud_tasks_service_account="tasks@example.iam.gserviceaccount.com",
        public_base_url="https://public.example",
        cloud_run_service_url="https://service.example",
        oauth_client_id="client",
        oauth_redirect_uri="https://service.example/setup/oauth/callback",
        notion_data_source_id="data-source",
    )


def _client(
    *,
    clock: Any | None = None,
    oidc: Any | None = None,
) -> tuple[TestClient, EventSink, Tasks, OAuth, Views]:
    key = b"owner-key"
    sink = EventSink()
    tasks = Tasks()
    oauth = OAuth()
    views = Views()
    dependencies = WebDependencies(
        config=_config(),
        webhook_authenticator=GoogleHealthWebhookAuthenticator(
            expected_authorization="Bearer secret",
            signature_verifier=SignatureVerifier(),
            application_hmac_key=key,
            expected_owner_hmac=identifier_hmac(key, "health-user"),
        ),
        webhook_events=sink,
        tasks=tasks,  # type: ignore[arg-type]
        oauth_sessions=oauth,  # type: ignore[arg-type]
        oidc_verifier=oidc or Oidc(),
        expected_task_service_account="tasks@example.iam.gserviceaccount.com",
        views=views,  # type: ignore[arg-type]
        steps=Steps(),  # type: ignore[arg-type]
        clock=clock or (lambda: datetime(2026, 7, 28, 12, 7, tzinfo=UTC)),
    )
    return (
        TestClient(create_app(dependencies), base_url="https://public.example"),
        sink,
        tasks,
        oauth,
        views,
    )


def test_health_endpoint_avoids_cloud_run_reserved_paths() -> None:
    client, _, _, _, _ = _client()

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_webhook_persists_before_enqueuing_job() -> None:
    times = iter(
        (
            datetime(2026, 7, 28, 12, 9, 59, tzinfo=UTC),
            datetime(2026, 7, 28, 12, 10, tzinfo=UTC),
        )
    )
    client, sink, tasks, _, _ = _client(clock=lambda: next(times))
    body = json.dumps(
        {
            "data": {
                "healthUserId": "health-user",
                "dataType": "steps",
                "operation": "UPSERT",
                "intervals": [],
            }
        }
    ).encode()

    response = client.post(
        "/webhooks/google-health",
        content=body,
        headers={
            "Authorization": "Bearer secret",
            "Content-Type": "application/json",
            "GOOGLE-HEALTH-API-SIGNATURE": base64.b64encode(b"valid").decode(),
        },
    )

    assert response.status_code == 204
    assert len(sink.events) == 1
    assert sink.events[0][1] == datetime(2026, 7, 28, 12, 9, 59, tzinfo=UTC)
    assert tasks.syncs == [
        (
            identifier_hmac(b"owner-key", "health-user"),
            datetime(2026, 7, 28, 12, 10, tzinfo=UTC),
        )
    ]


def test_subscriber_verification_succeeds_before_owner_oauth_setup() -> None:
    authenticator = DynamicOwnerWebhookAuthenticator(
        store=InMemoryObjectStore(clock=lambda: datetime(2026, 7, 28, tzinfo=UTC)),
        expected_authorization="Bearer secret",
        signature_verifier=SignatureVerifier(),
        application_hmac_key=b"owner-key",
    )
    verification_body = b'{"type":"verification"}'

    verified = authenticator.authenticate(
        authorization="Bearer secret",
        signature_header=None,
        content_type="application/json",
        raw_body=verification_body,
    )

    assert isinstance(verified, WebhookVerification)
    with pytest.raises(WebhookAuthenticationError):
        authenticator.authenticate(
            authorization=None,
            signature_header=None,
            content_type="application/json",
            raw_body=verification_body,
        )

    notification_body = json.dumps(
        {
            "data": {
                "healthUserId": "health-user",
                "dataType": "steps",
                "intervals": [],
            }
        }
    ).encode()
    with pytest.raises(WebhookPayloadError):
        authenticator.authenticate(
            authorization="Bearer secret",
            signature_header=base64.b64encode(b"valid").decode(),
            content_type="application/json",
            raw_body=notification_body,
        )


def test_subscriber_verification_http_status_contract() -> None:
    client, _, _, _, _ = _client()
    verification_body = b'{"type":"verification"}'

    authorized = client.post(
        "/webhooks/google-health",
        content=verification_body,
        headers={
            "Authorization": "Bearer secret",
            "Content-Type": "application/json",
        },
    )
    unauthorized = client.post(
        "/webhooks/google-health",
        content=verification_body,
        headers={"Content-Type": "application/json"},
    )

    assert authorized.status_code == 201
    assert unauthorized.status_code == 401


def test_apply_views_accepts_expected_service_account() -> None:
    client, _, _, _, views = _client()

    response = client.post(
        "/internal/tasks/apply-views",
        json={"requested_generation": 12},
        headers={"Authorization": "Bearer id-token"},
    )

    assert response.status_code == 204
    assert views.generations == [12]


def test_apply_views_rejects_anonymous_and_wrong_service_account() -> None:
    anonymous_client, _, _, _, anonymous_views = _client()
    wrong_client, _, _, _, wrong_views = _client(oidc=Oidc("other@example.com"))

    anonymous = anonymous_client.post(
        "/internal/tasks/apply-views",
        json={"requested_generation": 12},
    )
    wrong_identity = wrong_client.post(
        "/internal/tasks/apply-views",
        json={"requested_generation": 12},
        headers={"Authorization": "Bearer id-token"},
    )

    assert anonymous.status_code == 401
    assert wrong_identity.status_code == 403
    assert anonymous_views.generations == []
    assert wrong_views.generations == []


def test_setup_code_is_posted_then_oauth_redirects() -> None:
    assert SESSION_COOKIE == "__session"
    client, _, _, oauth, _ = _client()
    form = client.get("/setup")
    assert form.status_code == 200
    assert form.headers["referrer-policy"] == "same-origin"
    assert 'name="csrf_token"' not in form.text
    assert form.headers.get_list("set-cookie") == []

    created = client.post(
        "/setup/session",
        data={"setup_code": "one-time-code"},
        headers={"Origin": "https://public.example"},
        follow_redirects=False,
    )
    started = client.get("/setup/oauth/start", follow_redirects=False)
    callback = client.get("/setup/oauth/callback?code=authorization-code&state=oauth-state")

    assert created.status_code == 303
    assert created.headers["location"] == "/setup/oauth/start"
    assert started.status_code == 303
    assert started.headers["location"].startswith("https://accounts.example/")
    assert callback.status_code == 200
    assert oauth.created == ["one-time-code"]
    assert oauth.completed == ["authorization-code"]
    for response in (created, callback):
        cookie_headers = response.headers.get_list("set-cookie")
        assert len(cookie_headers) == 1
        cookie_header = cookie_headers[0]
        assert cookie_header.startswith(f"{SESSION_COOKIE}=")
        assert "Path=/" in cookie_header
        assert "Secure" in cookie_header
        assert "HttpOnly" in cookie_header
        assert "Domain=" not in cookie_header


@pytest.mark.parametrize(
    "origin",
    [
        None,
        "null",
        "https://attacker.example",
        "https://public.example.attacker.example",
        "http://public.example",
        "https://public.example:443",
        "https://public.example/",
    ],
)
def test_setup_session_rejects_untrusted_origin(origin: str | None) -> None:
    client, _, _, oauth, _ = _client()
    headers = {} if origin is None else {"Origin": origin}

    response = client.post(
        "/setup/session",
        data={"setup_code": "one-time-code"},
        headers=headers,
        follow_redirects=False,
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "origin rejected"}
    assert oauth.created == []


@pytest.mark.parametrize(
    "body",
    [
        b"unexpected=value",
        b"setup_code=one&setup_code=two",
        b"setup_code=",
    ],
)
def test_setup_session_rejects_ambiguous_form(body: bytes) -> None:
    client, _, _, oauth, _ = _client()

    response = client.post(
        "/setup/session",
        content=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://public.example",
        },
        follow_redirects=False,
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "invalid setup form"}
    assert oauth.created == []


def test_public_svg_has_short_cache_policy() -> None:
    client, _, _, _, _ = _client()

    response = client.get("/public/steps.svg")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300"
    assert response.headers["etag"] == '"head-12"'
