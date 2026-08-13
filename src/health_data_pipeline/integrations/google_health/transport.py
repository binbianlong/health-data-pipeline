"""HTTP and refresh-token adapters used by Google Health clients."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any, cast

import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials

from health_data_pipeline.integrations.google_health.client import HttpResponse
from health_data_pipeline.integrations.google_health.oauth import OAuthClientCredentials


class AccessTokenConfigurationError(RuntimeError):
    """The refresh-token configuration cannot produce an access token."""


class RefreshingAccessToken:
    """Thread-safe OAuth access-token provider backed by a refresh token."""

    def __init__(
        self,
        *,
        oauth_client: OAuthClientCredentials,
        refresh_token: str,
        token_uri: str,
        scopes: tuple[str, ...],
    ) -> None:
        if not refresh_token:
            raise AccessTokenConfigurationError("refresh token is empty")
        self._credentials = Credentials(  # type: ignore[no-untyped-call]
            token=None,
            refresh_token=refresh_token,
            token_uri=token_uri,
            client_id=oauth_client.client_id,
            client_secret=oauth_client.client_secret,
            scopes=scopes,
        )
        self._request = GoogleAuthRequest()
        self._lock = threading.Lock()

    def __call__(self) -> str:
        with self._lock:
            if not self._credentials.valid:
                self._credentials.refresh(  # type: ignore[no-untyped-call]
                    self._request
                )
            token = self._credentials.token
        if not isinstance(token, str) or not token:
            raise RuntimeError("OAuth refresh returned no access token")
        return token


class HttpxTransport:
    """Small HTTP transport implementation shared by production adapters."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(follow_redirects=False)

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        params: Mapping[str, str | int] | None = None,
        json: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        return cast(
            HttpResponse,
            self._client.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json,
                timeout=timeout,
            ),
        )
