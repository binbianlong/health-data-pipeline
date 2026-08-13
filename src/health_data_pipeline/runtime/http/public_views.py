"""Public materialized-view route registration."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response

from health_data_pipeline.runtime.http.dependencies import request_dependencies

router = APIRouter()


@router.get("/public/steps.svg")
def public_steps(request: Request) -> Response:
    deps = request_dependencies(request)
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
