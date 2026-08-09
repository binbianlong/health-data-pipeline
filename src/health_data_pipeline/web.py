"""Compatibility entrypoint and composition root for the FastAPI service."""

from __future__ import annotations

import hashlib
import importlib
import os
import secrets
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, cast

from fastapi import FastAPI, Request, Response

from health_data_pipeline.google_health import VerifiedWebhook
from health_data_pipeline.presentation.web import (
    SESSION_COOKIE,
    OidcVerifier,
    TaskDispatcher,
    WebDependencies,
    WebhookAuthenticator,
    WebhookEventSink,
    register_routes,
)
from health_data_pipeline.storage import ObjectNotFound, ObjectStore, PreconditionFailed

__all__ = [
    "SESSION_COOKIE",
    "GcsWebhookEventSink",
    "GoogleOidcVerifier",
    "OidcVerifier",
    "TaskDispatcher",
    "WebDependencies",
    "WebhookAuthenticator",
    "WebhookEventSink",
    "app",
    "create_app",
    "run_service",
]


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
                raise RuntimeError("webhook content-address collision")  # noqa: B904
        return path


class GoogleOidcVerifier:
    """Verify Google-signed Cloud Tasks ID tokens."""

    def verify(self, token: str, *, audience: str) -> Mapping[str, Any]:
        from google.auth.transport import requests  # noqa: PLC0415
        from google.oauth2 import id_token  # noqa: PLC0415

        verifier = cast(Callable[..., Mapping[str, Any]], id_token.verify_oauth2_token)
        claims = verifier(token, requests.Request(), audience=audience)
        if claims.get("iss") not in {
            "accounts.google.com",
            "https://accounts.google.com",
        }:
            raise ValueError("unexpected OIDC issuer")
        return claims


def create_app(dependencies: WebDependencies | None = None) -> FastAPI:
    app = FastAPI(
        title="health-data-pipeline",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.dependencies = dependencies

    @app.middleware("http")
    async def complete_request(request: Request, call_next: Callable[[Request], Any]) -> Response:
        try:
            return cast(Response, await call_next(request))
        finally:
            deps = getattr(request.app.state, "dependencies", None)
            if isinstance(deps, WebDependencies) and deps.on_complete is not None:
                deps.on_complete()

    register_routes(app)
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

    import uvicorn  # noqa: PLC0415

    app.state.dependencies = _load_web_dependencies()
    uvicorn.run(
        app,
        host="0.0.0.0",  # noqa: S104 - required inside the Cloud Run container.
        port=int(os.environ.get("PORT", "8080")),
        proxy_headers=True,
    )
