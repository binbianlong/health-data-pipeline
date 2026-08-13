from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from google.cloud import secretmanager

from health_data_pipeline.integrations.gcp.secrets import (
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
