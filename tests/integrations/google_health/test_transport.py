from __future__ import annotations

from typing import Any

import httpx
import pytest

from health_data_pipeline.integrations.google_health.oauth import (
    OAuthClientConfigurationError,
    OAuthClientCredentials,
)
from health_data_pipeline.integrations.google_health.transport import (
    AccessTokenConfigurationError,
    HttpxTransport,
    RefreshingAccessToken,
)


@pytest.mark.parametrize(
    "document",
    [
        '{"web":{"client_id":"id","client_secret":"secret"}}',
        '{"client_id":"id","client_secret":"secret"}',
    ],
)
def test_oauth_client_credentials_supports_downloaded_and_flat_json(
    document: str,
) -> None:
    credentials = OAuthClientCredentials.from_json(document)

    assert credentials.client_id == "id"
    assert credentials.client_secret == "secret"


@pytest.mark.parametrize("document", ["not-json", "[]", '{"web":{}}'])
def test_oauth_client_credentials_rejects_malformed_json(document: str) -> None:
    with pytest.raises(OAuthClientConfigurationError):
        OAuthClientCredentials.from_json(document)


def test_refreshing_access_token_requires_refresh_token() -> None:
    with pytest.raises(AccessTokenConfigurationError, match="refresh token"):
        RefreshingAccessToken(
            oauth_client=OAuthClientCredentials("id", "secret"),
            refresh_token="",
            token_uri="https://accounts.example/token",
            scopes=("scope",),
        )


def test_refreshing_access_token_refreshes_only_when_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[Any] = []

    class Credentials:
        valid = False
        token: str | None = None

        def __init__(self, **kwargs: Any) -> None:
            created.append(self)

        def refresh(self, request: Any) -> None:
            self.token = "access-token"
            self.valid = True

    monkeypatch.setattr(
        "health_data_pipeline.integrations.google_health.transport.Credentials",
        Credentials,
    )
    monkeypatch.setattr(
        "health_data_pipeline.integrations.google_health.transport.GoogleAuthRequest",
        object,
    )
    provider = RefreshingAccessToken(
        oauth_client=OAuthClientCredentials("id", "secret"),
        refresh_token="refresh",
        token_uri="https://accounts.example/token",
        scopes=("scope",),
    )

    assert provider() == "access-token"
    assert provider() == "access-token"
    assert len(created) == 1


def test_httpx_transport_forwards_request_without_network() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"ok": True})

    transport = HttpxTransport(httpx.Client(transport=httpx.MockTransport(handler)))

    response = transport.request(
        "GET",
        "https://example.test/resource",
        headers={"Authorization": "Bearer token"},
        params={"pageSize": 10},
        timeout=1.0,
    )

    assert response.status_code == 200
    assert captured[0].url.params["pageSize"] == "10"
