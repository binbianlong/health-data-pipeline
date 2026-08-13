"""Google OAuth authorization-code provider used by the setup session."""

from __future__ import annotations

import json
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import id_token

from health_data_pipeline.setup.oauth import OAuthGrant

GOOGLE_HEALTH_SCOPES = (
    "openid",
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",
    "https://www.googleapis.com/auth/googlehealth.settings.readonly",
)
HTTP_SUCCESS_MIN = 200
HTTP_SUCCESS_MAX_EXCLUSIVE = 300


class OAuthClientConfigurationError(RuntimeError):
    """The OAuth client configuration is missing or malformed."""


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
            raise OAuthClientConfigurationError(
                "OAuth client configuration is not valid JSON"
            ) from exc
        if not isinstance(document, Mapping):
            raise OAuthClientConfigurationError("OAuth client configuration must be a JSON object")
        nested = document.get("web", document)
        if not isinstance(nested, Mapping):
            raise OAuthClientConfigurationError(
                "OAuth client configuration has no web client object"
            )
        client_id = nested.get("client_id")
        client_secret = nested.get("client_secret")
        if not isinstance(client_id, str) or not client_id:
            raise OAuthClientConfigurationError("OAuth client configuration has no client_id")
        if not isinstance(client_secret, str) or not client_secret:
            raise OAuthClientConfigurationError("OAuth client configuration has no client_secret")
        return cls(client_id=client_id, client_secret=client_secret)


class GoogleOAuthExchangeError(RuntimeError):
    """The OAuth result could not be verified or linked to Google Health."""


class GoogleOAuthProvider:
    """Authorization Code + PKCE provider with OIDC nonce verification."""

    def __init__(
        self,
        *,
        oauth_client: OAuthClientCredentials,
        redirect_uri: str,
        authorization_url: str,
        token_url: str,
        health_api_base_url: str,
        client: httpx.Client | None = None,
    ) -> None:
        self._oauth_client = oauth_client
        self._redirect_uri = redirect_uri
        self._authorization_url = authorization_url
        self._token_url = token_url
        self._health_api_base_url = health_api_base_url.rstrip("/")
        self._client = client or httpx.Client(follow_redirects=False, timeout=30.0)

    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
    ) -> str:
        query = urlencode(
            {
                "client_id": self._oauth_client.client_id,
                "redirect_uri": self._redirect_uri,
                "response_type": "code",
                "scope": " ".join(GOOGLE_HEALTH_SCOPES),
                "access_type": "offline",
                "prompt": "consent",
                "include_granted_scopes": "true",
                "state": state,
                "nonce": nonce,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
        )
        return f"{self._authorization_url}?{query}"

    def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
        expected_nonce: str,
    ) -> OAuthGrant:
        response = self._client.post(
            self._token_url,
            data={
                "client_id": self._oauth_client.client_id,
                "client_secret": self._oauth_client.client_secret,
                "code": code,
                "code_verifier": code_verifier,
                "grant_type": "authorization_code",
                "redirect_uri": self._redirect_uri,
            },
        )
        if not HTTP_SUCCESS_MIN <= response.status_code < HTTP_SUCCESS_MAX_EXCLUSIVE:
            raise GoogleOAuthExchangeError(f"OAuth token exchange failed ({response.status_code})")
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise GoogleOAuthExchangeError("OAuth token response is not an object")
        access_token = _required_string(payload, "access_token")
        refresh_token = _required_string(payload, "refresh_token")
        encoded_id_token = _required_string(payload, "id_token")

        try:
            claims = id_token.verify_oauth2_token(  # type: ignore[no-untyped-call]
                encoded_id_token,
                GoogleAuthRequest(),
                audience=self._oauth_client.client_id,
            )
        except Exception as exc:
            raise GoogleOAuthExchangeError("OIDC ID token verification failed") from exc
        issuer = claims.get("iss")
        nonce = claims.get("nonce")
        subject = claims.get("sub")
        if issuer not in {"accounts.google.com", "https://accounts.google.com"}:
            raise GoogleOAuthExchangeError("unexpected OIDC issuer")
        if not isinstance(nonce, str) or not secrets.compare_digest(nonce, expected_nonce):
            raise GoogleOAuthExchangeError("OIDC nonce does not match")
        if not isinstance(subject, str) or not subject:
            raise GoogleOAuthExchangeError("OIDC token has no subject")

        identity_response = self._client.get(
            f"{self._health_api_base_url}/v4/users/me/identity",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
        )
        if not (HTTP_SUCCESS_MIN <= identity_response.status_code < HTTP_SUCCESS_MAX_EXCLUSIVE):
            raise GoogleOAuthExchangeError(
                f"Google Health identity lookup failed ({identity_response.status_code})"
            )
        identity = identity_response.json()
        if not isinstance(identity, Mapping):
            raise GoogleOAuthExchangeError("Google Health identity is not an object")
        health_user_id = _required_string(identity, "healthUserId")
        return OAuthGrant(
            sub=subject,
            health_user_id=health_user_id,
            refresh_token=refresh_token,
        )


def _required_string(value: Mapping[str, Any], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise GoogleOAuthExchangeError(f"OAuth response has no {field}")
    return result
