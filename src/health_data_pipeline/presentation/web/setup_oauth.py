"""Owner setup and Google OAuth route registration."""

from __future__ import annotations

import html
import secrets
from urllib.parse import parse_qs

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

from health_data_pipeline.oauth import InvalidOAuthSession, InvalidSetupCode
from health_data_pipeline.presentation.web.dependencies import request_dependencies
from health_data_pipeline.presentation.web.security import (
    secure_html_headers,
    secure_response_headers,
)

# Firebase Hosting forwards only this specially named cookie to Cloud Run.
# It is issued only after the one-time setup code is accepted.
SESSION_COOKIE = "__session"
SETUP_FORM_MAX_BODY_BYTES = 4096

router = APIRouter()


def setup_form_html(message: str | None = None) -> str:
    notice = "" if message is None else f"<p>{html.escape(message)}</p>"
    return f"""<!doctype html>
<html lang="ja">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Health data setup</title></head>
<body><main><h1>Google Health 接続</h1>{notice}
<form method="post" action="/setup/session">
<label>セットアップコード
<input name="setup_code" type="password" required autocomplete="one-time-code">
</label><button type="submit">Googleアカウントへ接続</button>
</form></main></body></html>"""


@router.get("/setup", response_class=HTMLResponse)
def setup_form() -> HTMLResponse:
    response = HTMLResponse(setup_form_html())
    # Chromium applies form-action to every redirect in a form submission.
    # Permit only Google's exact OAuth origin in addition to this service.
    secure_html_headers(
        response,
        form_action="'self' https://accounts.google.com",
    )
    return response


@router.post("/setup/session")
async def setup_session(request: Request) -> Response:
    deps = request_dependencies(request)
    expected_origin = deps.config.public_base_url
    origin = request.headers.get("origin")
    if origin is None or not secrets.compare_digest(origin, expected_origin):
        raise HTTPException(status_code=403, detail="origin rejected")
    media_type = request.headers.get("content-type", "").split(";", 1)[0]
    if media_type != "application/x-www-form-urlencoded":
        raise HTTPException(status_code=415, detail="unsupported content type")
    body = await request.body()
    if len(body) > SETUP_FORM_MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="form is too large")
    try:
        fields = parse_qs(
            body.decode("utf-8"),
            strict_parsing=True,
            keep_blank_values=True,
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="invalid setup form") from exc
    setup_codes = fields.get("setup_code")
    if (
        set(fields) != {"setup_code"}
        or setup_codes is None
        or len(setup_codes) != 1
        or not setup_codes[0]
    ):
        raise HTTPException(status_code=400, detail="invalid setup form")
    setup_code = setup_codes[0]
    try:
        session = deps.oauth_sessions.create_session(setup_code=setup_code)
    except InvalidSetupCode as exc:
        raise HTTPException(status_code=403, detail="setup code rejected") from exc
    response = RedirectResponse("/setup/oauth/start", status_code=303)
    response.set_cookie(
        SESSION_COOKIE,
        session.session_id,
        max_age=deps.config.setup_session_ttl_seconds,
        secure=True,
        httponly=True,
        samesite="lax",
        path="/",
    )
    secure_response_headers(response)
    return response


@router.get("/setup/oauth/start")
def oauth_start(request: Request) -> Response:
    deps = request_dependencies(request)
    session_id = request.cookies.get(SESSION_COOKIE)
    if not session_id:
        raise HTTPException(status_code=401, detail="setup session is missing")
    try:
        authorization = deps.oauth_sessions.start_authorization(session_id=session_id)
    except InvalidOAuthSession as exc:
        raise HTTPException(status_code=401, detail="setup session rejected") from exc
    response = RedirectResponse(authorization.authorization_url, status_code=303)
    secure_response_headers(response)
    return response


@router.get("/setup/oauth/callback")
def oauth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
) -> Response:
    deps = request_dependencies(request)
    session_id = request.cookies.get(SESSION_COOKIE)
    if error is not None or code is None or state is None or session_id is None:
        raise HTTPException(status_code=400, detail="OAuth callback rejected")
    try:
        deps.oauth_sessions.complete_callback(
            session_id=session_id,
            state=state,
            code=code,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail="OAuth callback rejected") from exc
    response = HTMLResponse(
        '<!doctype html><html lang="ja"><meta charset="utf-8">'
        "<title>接続完了</title><p>Google Healthとの接続が完了しました。</p>"
        "</html>"
    )
    response.delete_cookie(
        SESSION_COOKIE,
        path="/",
        secure=True,
        httponly=True,
        samesite="lax",
    )
    secure_html_headers(response)
    return response
