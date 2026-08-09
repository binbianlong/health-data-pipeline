"""Google Health webhook route registration."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response

from health_data_pipeline.google_health import (
    WebhookAuthenticationError,
    WebhookPayloadError,
    WebhookVerification,
)
from health_data_pipeline.presentation.web.dependencies import request_dependencies

router = APIRouter()


@router.post("/webhooks/google-health", status_code=204)
async def google_health_webhook(request: Request) -> Response:
    deps = request_dependencies(request)
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
    if deps.config.maintenance_mode:
        return Response(status_code=204)
    # Bucket from the time at which the durable event is ready. If a write
    # crosses a bucket boundary, using the earlier receipt time could target
    # a task that has already dispatched and is still name-deduplicated.
    deps.tasks.enqueue_sync_job(
        owner_hmac=result.owner_hmac,
        ready_at=deps.clock(),
    )
    return Response(status_code=204)
