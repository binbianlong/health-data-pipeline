"""Production adapters for Google Cloud secrets and HTTP clients."""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import secretmanager
from google.oauth2.credentials import Credentials

from health_data_pipeline.google_health import HttpResponse


class SecretValueError(RuntimeError):
    """A required secret is missing or malformed."""


class SecretManager:
    """Read and append Secret Manager versions without exposing values in logs."""

    def __init__(
        self,
        *,
        project_id: str,
        client: secretmanager.SecretManagerServiceClient | None = None,
    ) -> None:
        self._project_id = project_id
        self._client = client or secretmanager.SecretManagerServiceClient()

    def read_bytes(self, secret_id: str) -> bytes:
        name = f"projects/{self._project_id}/secrets/{secret_id}/versions/latest"
        response = self._client.access_secret_version(request={"name": name})
        value = bytes(response.payload.data)
        if not value:
            raise SecretValueError(f"secret has no active value: {secret_id}")
        return value

    def read_text(self, secret_id: str) -> str:
        try:
            return self.read_bytes(secret_id).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SecretValueError(f"secret is not UTF-8 text: {secret_id}") from exc

    def add_text_version(self, secret_id: str, value: str) -> None:
        if not value:
            raise SecretValueError("refusing to store an empty secret version")
        parent = f"projects/{self._project_id}/secrets/{secret_id}"
        self._client.add_secret_version(
            request={
                "parent": parent,
                "payload": {"data": value.encode("utf-8")},
            }
        )


@dataclass(frozen=True, slots=True)
class OAuthClientCredentials:
    """Normalized fields from a Google OAuth client JSON secret."""

    client_id: str
    client_secret: str

    @classmethod
    def from_json(cls, value: str) -> OAuthClientCredentials:
        try:
            document = json.loads(value)
        except json.JSONDecodeError as exc:
            raise SecretValueError("OAuth client secret is not valid JSON") from exc
        if not isinstance(document, Mapping):
            raise SecretValueError("OAuth client secret must be a JSON object")
        nested = document.get("web", document)
        if not isinstance(nested, Mapping):
            raise SecretValueError("OAuth client secret has no web client object")
        client_id = nested.get("client_id")
        client_secret = nested.get("client_secret")
        if not isinstance(client_id, str) or not client_id:
            raise SecretValueError("OAuth client secret has no client_id")
        if not isinstance(client_secret, str) or not client_secret:
            raise SecretValueError("OAuth client secret has no client_secret")
        return cls(client_id=client_id, client_secret=client_secret)


class SecretRefreshTokenSink:
    """Append a newly authorized refresh token as a Secret Manager version."""

    def __init__(self, secrets: SecretManager, secret_id: str) -> None:
        self._secrets = secrets
        self._secret_id = secret_id

    def store(self, refresh_token: str) -> None:
        self._secrets.add_text_version(self._secret_id, refresh_token)


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
            raise SecretValueError("refresh token is empty")
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
    """Small ``HttpTransport`` implementation shared by production adapters."""

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
