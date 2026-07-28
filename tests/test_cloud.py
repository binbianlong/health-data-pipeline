from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from google.cloud import secretmanager

from health_data_pipeline.cloud import (
    HttpxTransport,
    OAuthClientCredentials,
    RefreshingAccessToken,
    SecretManager,
    SecretRefreshTokenSink,
    SecretValueError,
)


class SecretClient:
    def __init__(self, value: bytes = b"value") -> None:
        self.value = value
        self.access_requests: list[dict[str, str]] = []
        self.add_requests: list[dict[str, Any]] = []

    def access_secret_version(self, *, request: dict[str, str]) -> Any:
        self.access_requests.append(request)
        return SimpleNamespace(payload=SimpleNamespace(data=self.value))

    def add_secret_version(self, *, request: dict[str, Any]) -> None:
        self.add_requests.append(request)


def test_secret_manager_reads_and_appends_exact_resource_names() -> None:
    client = SecretClient(b"secret-text")
    manager = SecretManager(
        project_id="project",
        client=cast(secretmanager.SecretManagerServiceClient, client),
    )

    assert manager.read_text("oauth-client") == "secret-text"
    SecretRefreshTokenSink(manager, "refresh-token").store("new-refresh-token")

    assert client.access_requests[0]["name"] == (
        "projects/project/secrets/oauth-client/versions/latest"
    )
    assert client.add_requests[0]["parent"] == ("projects/project/secrets/refresh-token")
    assert client.add_requests[0]["payload"]["data"] == b"new-refresh-token"


@pytest.mark.parametrize("value", [b"", b"\xff"])
def test_secret_manager_rejects_empty_or_non_utf8_values(value: bytes) -> None:
    manager = SecretManager(
        project_id="project",
        client=cast(secretmanager.SecretManagerServiceClient, SecretClient(value)),
    )

    with pytest.raises(SecretValueError):
        manager.read_text("secret")


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
    with pytest.raises(SecretValueError):
        OAuthClientCredentials.from_json(document)


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

    monkeypatch.setattr("health_data_pipeline.cloud.Credentials", Credentials)
    monkeypatch.setattr("health_data_pipeline.cloud.GoogleAuthRequest", object)
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
