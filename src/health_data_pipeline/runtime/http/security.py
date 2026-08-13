"""Shared authentication parsing and browser-security response headers."""

from __future__ import annotations

from fastapi import HTTPException, Response


def bearer_token(value: str | None) -> str:
    if value is None:
        raise HTTPException(status_code=401, detail="missing bearer token")
    scheme, separator, token = value.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="invalid bearer token")
    return token


def secure_response_headers(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["X-Content-Type-Options"] = "nosniff"


def secure_html_headers(
    response: Response,
    *,
    form_action: str = "'self'",
) -> None:
    secure_response_headers(response)
    response.headers["Content-Security-Policy"] = (
        f"default-src 'none'; form-action {form_action}; base-uri 'none'; frame-ancestors 'none'"
    )
