from __future__ import annotations

import json

import httpx
import pytest

from health_data_pipeline.subscriber import (
    WEBHOOK_SUPPORTED_DATA_TYPES,
    GoogleHealthSubscriberProvisioner,
    SubscriberProvisioningError,
)


def _provisioner(client: httpx.Client) -> GoogleHealthSubscriberProvisioner:
    return GoogleHealthSubscriberProvisioner(
        project_id="project-id",
        endpoint_uri="https://service.example/webhooks/google-health",
        endpoint_authorization="endpoint-secret",
        client=client,
    )


@pytest.mark.parametrize("exists", [False, True])
def test_upsert_creates_or_updates_subscriber_offline(
    monkeypatch: pytest.MonkeyPatch,
    exists: bool,
) -> None:
    calls: list[httpx.Request] = []
    name = "projects/123456/subscribers/health-data-pipeline"

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123456"})
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"subscribers": [{"name": name}] if exists else []},
            )
        return httpx.Response(200, json={"name": name})

    monkeypatch.setattr("health_data_pipeline.subscriber._access_token", lambda: "token")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    result = _provisioner(client).upsert()

    assert result == name
    mutation = calls[-1]
    assert mutation.method == ("PATCH" if exists else "POST")
    body = json.loads(mutation.content)
    assert body["endpointAuthorization"]["secret"] == "endpoint-secret"
    assert body["subscriberConfigs"][0]["subscriptionCreatePolicy"] == "AUTOMATIC"
    assert set(body["subscriberConfigs"][0]["dataTypes"]) == set(WEBHOOK_SUPPORTED_DATA_TYPES)
    assert "oxygen-saturation" not in WEBHOOK_SUPPORTED_DATA_TYPES
    assert "daily-oxygen-saturation" in WEBHOOK_SUPPORTED_DATA_TYPES
    assert mutation.headers["authorization"] == "Bearer token"


def test_upsert_rejects_invalid_project_and_subscriber_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("health_data_pipeline.subscriber._access_token", lambda: "token")
    invalid_project = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"name": "invalid"}))
    )
    with pytest.raises(SubscriberProvisioningError, match="project number"):
        _provisioner(invalid_project).upsert()

    def invalid_list(request: httpx.Request) -> httpx.Response:
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123"})
        return httpx.Response(200, json={"subscribers": "not-an-array"})

    client = httpx.Client(transport=httpx.MockTransport(invalid_list))
    with pytest.raises(SubscriberProvisioningError, match="invalid subscribers"):
        _provisioner(client).upsert()


def test_upsert_reports_remote_failure_without_response_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("health_data_pipeline.subscriber._access_token", lambda: "token")
    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(503, text="secret-body"))
    )

    with pytest.raises(SubscriberProvisioningError) as captured:
        _provisioner(client).upsert()

    assert "503" in str(captured.value)
    assert "secret-body" not in str(captured.value)
