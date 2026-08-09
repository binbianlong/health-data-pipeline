"""Liveness route registration."""

from __future__ import annotations

from collections.abc import Mapping

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health() -> Mapping[str, str]:
    return {"status": "ok"}
