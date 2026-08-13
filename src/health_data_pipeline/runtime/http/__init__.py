"""HTTP presentation routers and their dependency contract."""

from health_data_pipeline.runtime.http.dependencies import (
    OidcVerifier,
    TaskDispatcher,
    WebDependencies,
    WebhookAuthenticator,
    WebhookEventSink,
)
from health_data_pipeline.runtime.http.routing import register_routes
from health_data_pipeline.runtime.http.setup_oauth import SESSION_COOKIE

__all__ = [
    "SESSION_COOKIE",
    "OidcVerifier",
    "TaskDispatcher",
    "WebDependencies",
    "WebhookAuthenticator",
    "WebhookEventSink",
    "register_routes",
]
