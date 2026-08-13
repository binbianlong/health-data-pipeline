from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from health_data_pipeline.integrations.google_health.webhook import (
    GoogleHealthWebhookAuthenticator,
    WebhookAuthenticationError,
    WebhookPayloadError,
    WebhookVerification,
)
from health_data_pipeline.runtime.composition import DynamicOwnerWebhookAuthenticator
from health_data_pipeline.runtime.config import AppConfig
from health_data_pipeline.runtime.service import (
    SESSION_COOKIE,
    WebDependencies,
    create_app,
)
from health_data_pipeline.setup.identity import identifier_hmac
from health_data_pipeline.setup.oauth import AuthorizationStart, IssuedSession, OAuthGrant
from health_data_pipeline.state.objects import InMemoryObjectStore


class SignatureVerifier:
    def verify(self, signature: bytes, payload: bytes) -> bool:
        return signature == b"valid"


class EventSink:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.events: list[Any] = []
        self.calls = 0
        self.fail_on_call = fail_on_call

    def persist(self, event: Any, *, received_at: datetime) -> str:
        self.calls += 1
        if self.calls == self.fail_on_call:
            raise RuntimeError("event persistence failed")
        self.events.append((event, received_at))
        return "webhook-events/event.json"


class Tasks:
    def __init__(self, *, fail: bool = False) -> None:
        self.syncs: list[tuple[str, datetime]] = []
        self.fail = fail

    def enqueue_sync_job(self, *, owner_hmac: str, ready_at: datetime) -> None:
        if self.fail:
            raise RuntimeError("task enqueue failed")
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
            authorization_url="https://accounts.google.com/authorize?state=oauth-state",
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


def _config(*, maintenance_mode: bool = False) -> AppConfig:
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
        maintenance_mode=maintenance_mode,
    )


def _client(
    *,
    clock: Any | None = None,
    oidc: Any | None = None,
    event_sink: EventSink | None = None,
    task_dispatcher: Tasks | None = None,
    on_complete: Any | None = None,
    maintenance_mode: bool = False,
    raise_server_exceptions: bool = True,
) -> tuple[TestClient, EventSink, Tasks, OAuth, Views]:
    key = b"owner-key"
    sink = event_sink or EventSink()
    tasks = task_dispatcher or Tasks()
    oauth = OAuth()
    views = Views()
    dependencies = WebDependencies(
        config=_config(maintenance_mode=maintenance_mode),
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
        on_complete=on_complete,
    )
    return (
        TestClient(
            create_app(dependencies),
            base_url="https://public.example",
            raise_server_exceptions=raise_server_exceptions,
        ),
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


def test_request_completion_hook_runs_for_success_and_not_found() -> None:
    completed: list[str] = []
    client, _, _, _, _ = _client(on_complete=lambda: completed.append("done"))

    assert client.get("/health").status_code == 200
    assert client.get("/does-not-exist").status_code == 404
    assert completed == ["done", "done"]


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


def test_webhook_maintenance_mode_persists_without_enqueuing_job() -> None:
    client, sink, tasks, _, _ = _client(maintenance_mode=True)
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
    assert tasks.syncs == []


def test_webhook_batch_persists_each_event_and_enqueues_one_job() -> None:
    times = iter(
        (
            datetime(2026, 7, 28, 12, 9, 59, tzinfo=UTC),
            datetime(2026, 7, 28, 12, 10, tzinfo=UTC),
        )
    )
    client, sink, tasks, _, _ = _client(clock=lambda: next(times))
    body = json.dumps(
        [
            {
                "data": {
                    "healthUserId": "health-user",
                    "dataType": "steps",
                    "intervals": [],
                }
            },
            {
                "data": {
                    "healthUserId": "health-user",
                    "dataType": "sleep",
                    "intervals": [],
                }
            },
        ]
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
    assert len(sink.events) == 2
    assert {event.payload["data"]["dataType"] for event, _ in sink.events} == {
        "steps",
        "sleep",
    }
    assert {received_at for _, received_at in sink.events} == {
        datetime(2026, 7, 28, 12, 9, 59, tzinfo=UTC)
    }
    assert tasks.syncs == [
        (
            identifier_hmac(b"owner-key", "health-user"),
            datetime(2026, 7, 28, 12, 10, tzinfo=UTC),
        )
    ]


def test_webhook_batch_rejects_invalid_item_without_writes() -> None:
    client, sink, tasks, _, _ = _client()
    body = json.dumps(
        [
            {"data": {"healthUserId": "health-user", "dataType": "steps"}},
            {"data": []},
        ]
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

    assert response.status_code == 400
    assert sink.events == []
    assert tasks.syncs == []


def test_webhook_batch_does_not_enqueue_when_a_later_write_fails() -> None:
    sink = EventSink(fail_on_call=2)
    client, _, tasks, _, _ = _client(
        event_sink=sink,
        raise_server_exceptions=False,
    )
    body = json.dumps(
        [
            {"data": {"healthUserId": "health-user", "dataType": "steps"}},
            {"data": {"healthUserId": "health-user", "dataType": "sleep"}},
        ]
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

    assert response.status_code == 500
    assert len(sink.events) == 1
    assert tasks.syncs == []


def test_webhook_batch_returns_failure_after_writes_when_enqueue_fails() -> None:
    task_dispatcher = Tasks(fail=True)
    client, sink, _, _, _ = _client(
        task_dispatcher=task_dispatcher,
        raise_server_exceptions=False,
    )
    body = json.dumps(
        [
            {"data": {"healthUserId": "health-user", "dataType": "steps"}},
            {"data": {"healthUserId": "health-user", "dataType": "sleep"}},
        ]
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

    assert response.status_code == 500
    assert len(sink.events) == 2
    assert task_dispatcher.syncs == []


def test_webhook_batch_without_owned_events_acknowledges_without_work() -> None:
    client, sink, tasks, _, _ = _client()
    body = json.dumps(
        [
            {
                "data": {
                    "healthUserId": "someone-else",
                    "dataType": "steps",
                    "intervals": [],
                }
            }
        ]
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
    assert sink.events == []
    assert tasks.syncs == []


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
    assert form.headers["content-security-policy"] == (
        "default-src 'none'; "
        "form-action 'self' https://accounts.google.com; "
        "base-uri 'none'; frame-ancestors 'none'"
    )
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
    assert started.headers["location"].startswith("https://accounts.google.com/")
    assert callback.status_code == 200
    assert oauth.created == ["one-time-code"]
    assert oauth.completed == ["authorization-code"]
    for response in (created, started):
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["referrer-policy"] == "same-origin"
        assert response.headers["x-content-type-options"] == "nosniff"
        assert "content-security-policy" not in response.headers
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
