from __future__ import annotations

import json

import httpx
import pytest

from health_data_pipeline.integrations.google_health.subscriber import (
    WEBHOOK_SUPPORTED_DATA_TYPES,
    GoogleHealthSubscriberProvisioner,
    SubscriberProvisioningError,
)

_ACCESS_TOKEN_TARGET = "health_data_pipeline.integrations.google_health.subscriber._access_token"


def _provisioner(client: httpx.Client) -> GoogleHealthSubscriberProvisioner:
    return GoogleHealthSubscriberProvisioner(
        project_id="project-id",
        endpoint_uri="https://service.example/webhooks/google-health",
        endpoint_authorization="endpoint-secret",
        client=client,
        sleeper=lambda _: None,
    )


@pytest.mark.parametrize("exists", [False, True])
def test_upsert_creates_or_updates_subscriber_offline(
    monkeypatch: pytest.MonkeyPatch,
    exists: bool,
) -> None:
    calls: list[httpx.Request] = []
    name = "projects/123456/subscribers/health-data-pipeline"
    health_list_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal health_list_count
        calls.append(request)
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123456"})
        if request.method == "GET":
            health_list_count += 1
            if health_list_count > 1:
                return httpx.Response(
                    200,
                    json={"subscribers": [{"name": name, "state": "ACTIVE"}]},
                )
            return httpx.Response(
                200,
                json={"subscribers": [{"name": name, "state": "ACTIVE"}] if exists else []},
            )
        return httpx.Response(200, json={"name": "operations/subscriber", "done": True})

    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    result = _provisioner(client).upsert()

    assert result == name
    mutation = next(request for request in calls if request.method in {"POST", "PATCH"})
    assert mutation.method == ("PATCH" if exists else "POST")
    body = json.loads(mutation.content)
    assert body["endpointAuthorization"]["secret"] == "endpoint-secret"
    assert body["subscriberConfigs"][0]["subscriptionCreatePolicy"] == "AUTOMATIC"
    assert set(body["subscriberConfigs"][0]["dataTypes"]) == set(WEBHOOK_SUPPORTED_DATA_TYPES)
    assert "oxygen-saturation" not in WEBHOOK_SUPPORTED_DATA_TYPES
    assert "daily-oxygen-saturation" in WEBHOOK_SUPPORTED_DATA_TYPES
    assert mutation.headers["authorization"] == "Bearer token"
    assert mutation.headers["x-goog-user-project"] == "project-id"
    assert ("name" in body) is exists
    for request in calls:
        if request.url.host == "health.googleapis.com":
            assert request.headers["x-goog-user-project"] == "project-id"


def test_upsert_rejects_operation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123456"})
        if request.method == "GET":
            return httpx.Response(200, json={"subscribers": []})
        return httpx.Response(
            200,
            json={"name": "operations/subscriber", "done": True, "error": {"code": 9}},
        )

    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    with pytest.raises(SubscriberProvisioningError, match="operation failed"):
        _provisioner(client).upsert()


def test_upsert_accepts_verified_subscriber_when_list_omits_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "projects/123456/subscribers/health-data-pipeline"
    mutated = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal mutated
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123456"})
        if request.method == "POST":
            mutated = True
            return httpx.Response(200, json={"name": "operations/subscriber", "done": True})
        return httpx.Response(
            200,
            json={"subscribers": [{"name": name}]} if mutated else {"subscribers": []},
        )

    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    assert _provisioner(client).upsert() == name


def test_upsert_rejects_explicitly_unverified_subscriber(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "projects/123456/subscribers/health-data-pipeline"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123456"})
        if request.method == "PATCH":
            return httpx.Response(200, json={"name": "operations/subscriber", "done": True})
        return httpx.Response(
            200,
            json={"subscribers": [{"name": name, "state": "UNVERIFIED"}]},
        )

    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    with pytest.raises(SubscriberProvisioningError, match="did not become ACTIVE"):
        _provisioner(client).upsert()


def test_upsert_rejects_nonterminal_operation_for_existing_active_subscriber(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123456"})
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "subscribers": [
                        {
                            "name": "projects/123456/subscribers/health-data-pipeline",
                            "state": "ACTIVE",
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"name": "operations/subscriber", "done": False})

    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    with pytest.raises(SubscriberProvisioningError, match="still in progress"):
        _provisioner(client).upsert()


def test_upsert_finds_existing_subscriber_on_second_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[httpx.Request] = []
    name = "projects/123456/subscribers/health-data-pipeline"
    mutated = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal mutated
        calls.append(request)
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/123456"})
        if request.method == "PATCH":
            mutated = True
            return httpx.Response(200, json={"name": "operations/subscriber", "done": True})
        if mutated:
            return httpx.Response(
                200,
                json={"subscribers": [{"name": name, "state": "ACTIVE"}]},
            )
        if request.url.params.get("pageToken") == "next-page":
            return httpx.Response(
                200,
                json={"subscribers": [{"name": name, "state": "ACTIVE"}]},
            )
        return httpx.Response(
            200,
            json={"subscribers": [], "nextPageToken": "next-page"},
        )

    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    assert _provisioner(client).upsert() == name

    health_gets = [
        request
        for request in calls
        if request.url.host == "health.googleapis.com" and request.method == "GET"
    ]
    mutation = next(request for request in calls if request.method == "PATCH")
    assert health_gets[0].url.params["pageSize"] == "1000"
    assert health_gets[1].url.params["pageToken"] == "next-page"
    assert mutation.url.path.endswith(f"/v4/{name}")


def test_upsert_rejects_invalid_project_and_subscriber_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
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
    monkeypatch.setattr(_ACCESS_TOKEN_TARGET, lambda: "token")
    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(503, text="secret-body"))
    )

    with pytest.raises(SubscriberProvisioningError) as captured:
        _provisioner(client).upsert()

    assert "503" in str(captured.value)
    assert "secret-body" not in str(captured.value)
