"""Cloud Run Service HTTP entrypoint."""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from typing import Any, cast

from fastapi import FastAPI, Request, Response

from health_data_pipeline.runtime.http import (
    SESSION_COOKIE,
    OidcVerifier,
    TaskDispatcher,
    WebDependencies,
    WebhookAuthenticator,
    WebhookEventSink,
    register_routes,
)

__all__ = [
    "SESSION_COOKIE",
    "OidcVerifier",
    "TaskDispatcher",
    "WebDependencies",
    "WebhookAuthenticator",
    "WebhookEventSink",
    "app",
    "create_app",
    "run_service",
]


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
        "health_data_pipeline.runtime.composition:build_web_dependencies",
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
