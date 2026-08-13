"""Install all HTTP presentation routers on an application."""

from __future__ import annotations

from typing import Final

from fastapi import APIRouter, FastAPI

from health_data_pipeline.runtime.http.health import router as health_router
from health_data_pipeline.runtime.http.internal_views import router as internal_views_router
from health_data_pipeline.runtime.http.public_views import router as public_views_router
from health_data_pipeline.runtime.http.setup_oauth import router as setup_oauth_router
from health_data_pipeline.runtime.http.webhook import router as webhook_router

ROUTERS: Final[tuple[APIRouter, ...]] = (
    health_router,
    webhook_router,
    internal_views_router,
    public_views_router,
    setup_oauth_router,
)


def register_routes(app: FastAPI) -> None:
    """Register each responsibility-specific router exactly once."""

    for router in ROUTERS:
        app.include_router(router)
