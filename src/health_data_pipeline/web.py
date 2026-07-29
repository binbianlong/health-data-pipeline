"""FastAPI service for webhook, OAuth setup, views and public steps SVG."""

# ruff: noqa: B904, PLC0415, PLR0915, PLR2004

from __future__ import annotations

import hashlib
import html
import importlib
import os
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from urllib.parse import parse_qs

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

from health_data_pipeline.config import AppConfig
from health_data_pipeline.google_health import (
    VerifiedWebhook,
    VerifiedWebhookBatch,
    WebhookAuthenticationError,
    WebhookPayloadError,
    WebhookVerification,
)
from health_data_pipeline.job import CloudTaskDispatcher
from health_data_pipeline.oauth import (
    InvalidOAuthSession,
    InvalidSetupCode,
    OAuthSessionService,
)
from health_data_pipeline.storage import ObjectNotFound, ObjectStore, PreconditionFailed
from health_data_pipeline.views import StepsSvgService, ViewApplicationService

# Firebase Hosting forwards only this specially named cookie to Cloud Run.
# It is issued only after the one-time setup code is accepted.
SESSION_COOKIE = "__session"


class OidcVerifier(Protocol):
    def verify(self, token: str, *, audience: str) -> Mapping[str, Any]: ...


class WebhookEventSink(Protocol):
    def persist(self, event: VerifiedWebhook, *, received_at: datetime) -> str: ...


class WebhookAuthenticator(Protocol):
    def authenticate(
        self,
        *,
        authorization: str | None,
        signature_header: str | None,
        content_type: str | None,
        raw_body: bytes,
    ) -> VerifiedWebhookBatch | WebhookVerification: ...


@dataclass(frozen=True, slots=True)
class WebDependencies:
    config: AppConfig
    webhook_authenticator: WebhookAuthenticator
    webhook_events: WebhookEventSink
    tasks: CloudTaskDispatcher
    oauth_sessions: OAuthSessionService
    oidc_verifier: OidcVerifier
    expected_task_service_account: str
    views: ViewApplicationService
    steps: StepsSvgService
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)


class GcsWebhookEventSink:
    """Persist authenticated raw webhooks as immutable, idempotent objects."""

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
                raise RuntimeError("webhook content-address collision")
        return path


class GoogleOidcVerifier:
    """Verify Google-signed Cloud Tasks ID tokens."""

    def verify(self, token: str, *, audience: str) -> Mapping[str, Any]:
        from google.auth.transport import requests
        from google.oauth2 import id_token

        verifier = cast(Callable[..., Mapping[str, Any]], id_token.verify_oauth2_token)
        claims = verifier(token, requests.Request(), audience=audience)
        if claims.get("iss") not in {
            "accounts.google.com",
            "https://accounts.google.com",
        }:
            raise ValueError("unexpected OIDC issuer")
        return claims


def _bearer_token(value: str | None) -> str:
    if value is None:
        raise HTTPException(status_code=401, detail="missing bearer token")
    scheme, separator, token = value.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="invalid bearer token")
    return token


def _dependencies(request: Request) -> WebDependencies:
    dependencies = getattr(request.app.state, "dependencies", None)
    if not isinstance(dependencies, WebDependencies):
        raise HTTPException(status_code=503, detail="service is not configured")
    return dependencies


def _secure_response_headers(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["X-Content-Type-Options"] = "nosniff"


def _secure_html_headers(
    response: Response,
    *,
    form_action: str = "'self'",
) -> None:
    _secure_response_headers(response)
    response.headers["Content-Security-Policy"] = (
        f"default-src 'none'; form-action {form_action}; base-uri 'none'; frame-ancestors 'none'"
    )


def _setup_form(message: str | None = None) -> str:
    notice = "" if message is None else f"<p>{html.escape(message)}</p>"
    return f"""<!doctype html>
<html lang="ja">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Health data setup</title></head>
<body><main><h1>Google Health 接続</h1>{notice}
<form method="post" action="/setup/session">
<label>セットアップコード
<input name="setup_code" type="password" required autocomplete="one-time-code">
</label><button type="submit">Googleアカウントへ接続</button>
</form></main></body></html>"""


def create_app(dependencies: WebDependencies | None = None) -> FastAPI:
    app = FastAPI(
        title="health-data-pipeline",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.dependencies = dependencies

    @app.get("/health")
    def health() -> Mapping[str, str]:
        return {"status": "ok"}

    @app.post("/webhooks/google-health", status_code=204)
    async def google_health_webhook(request: Request) -> Response:
        deps = _dependencies(request)
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                too_large = int(content_length) > deps.config.webhook_max_body_bytes
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="invalid content length") from exc
            if too_large:
                raise HTTPException(status_code=413, detail="request is too large")
        body = await request.body()
        if len(body) > deps.config.webhook_max_body_bytes:
            raise HTTPException(status_code=413, detail="request is too large")
        try:
            result = deps.webhook_authenticator.authenticate(
                authorization=request.headers.get("authorization"),
                signature_header=request.headers.get("google-health-api-signature"),
                content_type=request.headers.get("content-type"),
                raw_body=body,
            )
        except WebhookAuthenticationError as exc:
            raise HTTPException(status_code=401, detail="webhook rejected") from exc
        except WebhookPayloadError as exc:
            raise HTTPException(status_code=400, detail="invalid webhook") from exc
        if isinstance(result, WebhookVerification):
            return Response(status_code=201)

        if not result.events:
            return Response(status_code=204)
        received_at = deps.clock()
        for event in result.events:
            deps.webhook_events.persist(event, received_at=received_at)
        # Bucket from the time at which the durable event is ready. If a write
        # crosses a bucket boundary, using the earlier receipt time could target
        # a task that has already dispatched and is still name-deduplicated.
        deps.tasks.enqueue_sync_job(
            owner_hmac=result.owner_hmac,
            ready_at=deps.clock(),
        )
        return Response(status_code=204)

    @app.post("/internal/tasks/apply-views", status_code=204)
    async def apply_views(request: Request) -> Response:
        deps = _dependencies(request)
        token = _bearer_token(request.headers.get("authorization"))
        try:
            claims = deps.oidc_verifier.verify(token, audience=deps.config.cloud_run_service_url)
        except Exception as exc:
            raise HTTPException(status_code=401, detail="invalid task identity") from exc
        email = claims.get("email")
        email_verified = claims.get("email_verified", True)
        if (
            not isinstance(email, str)
            or not secrets.compare_digest(email, deps.expected_task_service_account)
            or email_verified is not True
        ):
            raise HTTPException(status_code=403, detail="unexpected task identity")
        try:
            payload = await request.json()
            requested = payload["requested_generation"]
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="invalid task payload") from exc
        if not isinstance(requested, int) or isinstance(requested, bool) or requested < 1:
            raise HTTPException(status_code=400, detail="invalid task generation")
        deps.views.apply_latest(requested)
        return Response(status_code=204)

    @app.get("/public/steps.svg")
    def public_steps(request: Request) -> Response:
        deps = _dependencies(request)
        try:
            content, generation = deps.steps.read_current()
        except Exception as exc:
            raise HTTPException(status_code=404, detail="steps view is unavailable") from exc
        return Response(
            content=content,
            media_type="image/svg+xml",
            headers={
                "Cache-Control": "public, max-age=300",
                "ETag": f'"head-{generation}"',
                "X-Content-Type-Options": "nosniff",
            },
        )

    @app.get("/setup", response_class=HTMLResponse)
    def setup_form() -> HTMLResponse:
        response = HTMLResponse(_setup_form())
        # Chromium applies form-action to every redirect in a form submission.
        # Permit only Google's exact OAuth origin in addition to this service.
        _secure_html_headers(
            response,
            form_action="'self' https://accounts.google.com",
        )
        return response

    @app.post("/setup/session")
    async def setup_session(request: Request) -> Response:
        deps = _dependencies(request)
        expected_origin = deps.config.public_base_url
        origin = request.headers.get("origin")
        if origin is None or not secrets.compare_digest(origin, expected_origin):
            raise HTTPException(status_code=403, detail="origin rejected")
        media_type = request.headers.get("content-type", "").split(";", 1)[0]
        if media_type != "application/x-www-form-urlencoded":
            raise HTTPException(status_code=415, detail="unsupported content type")
        body = await request.body()
        if len(body) > 4096:
            raise HTTPException(status_code=413, detail="form is too large")
        try:
            fields = parse_qs(
                body.decode("utf-8"),
                strict_parsing=True,
                keep_blank_values=True,
            )
        except (UnicodeDecodeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="invalid setup form") from exc
        setup_codes = fields.get("setup_code")
        if (
            set(fields) != {"setup_code"}
            or setup_codes is None
            or len(setup_codes) != 1
            or not setup_codes[0]
        ):
            raise HTTPException(status_code=400, detail="invalid setup form")
        setup_code = setup_codes[0]
        try:
            session = deps.oauth_sessions.create_session(setup_code=setup_code)
        except InvalidSetupCode as exc:
            raise HTTPException(status_code=403, detail="setup code rejected") from exc
        response = RedirectResponse("/setup/oauth/start", status_code=303)
        response.set_cookie(
            SESSION_COOKIE,
            session.session_id,
            max_age=deps.config.setup_session_ttl_seconds,
            secure=True,
            httponly=True,
            samesite="lax",
            path="/",
        )
        _secure_response_headers(response)
        return response

    @app.get("/setup/oauth/start")
    def oauth_start(request: Request) -> Response:
        deps = _dependencies(request)
        session_id = request.cookies.get(SESSION_COOKIE)
        if not session_id:
            raise HTTPException(status_code=401, detail="setup session is missing")
        try:
            authorization = deps.oauth_sessions.start_authorization(session_id=session_id)
        except InvalidOAuthSession as exc:
            raise HTTPException(status_code=401, detail="setup session rejected") from exc
        response = RedirectResponse(authorization.authorization_url, status_code=303)
        _secure_response_headers(response)
        return response

    @app.get("/setup/oauth/callback")
    def oauth_callback(
        request: Request,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
    ) -> Response:
        deps = _dependencies(request)
        session_id = request.cookies.get(SESSION_COOKIE)
        if error is not None or code is None or state is None or session_id is None:
            raise HTTPException(status_code=400, detail="OAuth callback rejected")
        try:
            deps.oauth_sessions.complete_callback(
                session_id=session_id,
                state=state,
                code=code,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail="OAuth callback rejected") from exc
        response = HTMLResponse(
            '<!doctype html><html lang="ja"><meta charset="utf-8">'
            "<title>接続完了</title><p>Google Healthとの接続が完了しました。</p>"
            "</html>"
        )
        response.delete_cookie(
            SESSION_COOKIE,
            path="/",
            secure=True,
            httponly=True,
            samesite="lax",
        )
        _secure_html_headers(response)
        return response

    return app


WebFactory = Callable[[], WebDependencies]


def _load_web_dependencies() -> WebDependencies:
    dotted = os.environ.get(
        "HEALTH_PIPELINE_WEB_FACTORY",
        "health_data_pipeline.runtime:build_web_dependencies",
    )
    module_name, separator, attribute = dotted.partition(":")
    if not separator or not module_name or not attribute:
        raise RuntimeError("HEALTH_PIPELINE_WEB_FACTORY must be module:callable")
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute, None)
    if not callable(factory):
        raise TypeError(f"web factory is not callable: {dotted}")
    return cast(WebFactory, factory)()


app = create_app()


def run_service() -> None:
    """Configure and run the service; never called during module import."""

    import uvicorn

    app.state.dependencies = _load_web_dependencies()
    uvicorn.run(
        app,
        host="0.0.0.0",  # noqa: S104 - required inside the Cloud Run container.
        port=int(os.environ.get("PORT", "8080")),
        proxy_headers=True,
    )
