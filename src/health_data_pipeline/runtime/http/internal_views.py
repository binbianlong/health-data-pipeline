"""Authenticated internal materialized-view route registration."""

from __future__ import annotations

import secrets

from fastapi import APIRouter, HTTPException, Request, Response

from health_data_pipeline.runtime.http.dependencies import request_dependencies
from health_data_pipeline.runtime.http.security import bearer_token

router = APIRouter()


@router.post("/internal/tasks/apply-views", status_code=204)
async def apply_views(request: Request) -> Response:
    deps = request_dependencies(request)
    token = bearer_token(request.headers.get("authorization"))
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
