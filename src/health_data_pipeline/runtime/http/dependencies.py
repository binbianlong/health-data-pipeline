"""Typed dependencies shared by the HTTP presentation routers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from fastapi import HTTPException, Request

from health_data_pipeline.integrations.google_health.webhook import (
    VerifiedWebhookBatch,
    WebhookVerification,
)
from health_data_pipeline.runtime.config import AppConfig
from health_data_pipeline.setup.oauth import OAuthSessionService
from health_data_pipeline.sync.contracts import (
    SyncTaskDispatcher,
    ViewTaskDispatcher,
)
from health_data_pipeline.sync.events import VerifiedWebhook
from health_data_pipeline.views.service import ViewApplicationService
from health_data_pipeline.views.steps import StepsSvgService


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


class TaskDispatcher(SyncTaskDispatcher, ViewTaskDispatcher, Protocol):
    """Task operations required across the service and job entrypoints."""


@dataclass(frozen=True, slots=True)
class WebDependencies:
    config: AppConfig
    webhook_authenticator: WebhookAuthenticator
    webhook_events: WebhookEventSink
    tasks: TaskDispatcher
    oauth_sessions: OAuthSessionService
    oidc_verifier: OidcVerifier
    expected_task_service_account: str
    views: ViewApplicationService
    steps: StepsSvgService
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    on_complete: Callable[[], object] | None = None


def request_dependencies(request: Request) -> WebDependencies:
    """Return the configured dependency bundle for a request."""

    dependencies = getattr(request.app.state, "dependencies", None)
    if not isinstance(dependencies, WebDependencies):
        raise HTTPException(status_code=503, detail="service is not configured")
    return dependencies
