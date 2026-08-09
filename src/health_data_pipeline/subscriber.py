"""Idempotent Google Health webhook Subscriber provisioning."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

import google.auth
import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest

from health_data_pipeline.data_types import (
    WEBHOOK_SUPPORTED_DATA_TYPES as _CATALOG_WEBHOOK_SUPPORTED_DATA_TYPES,
)

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
HTTP_SUCCESS_MIN = 200
HTTP_SUCCESS_MAX_EXCLUSIVE = 300
ACTIVATION_ATTEMPTS = 6
ACTIVATION_DELAY_SECONDS = 1.0
WEBHOOK_SUPPORTED_DATA_TYPES = _CATALOG_WEBHOOK_SUPPORTED_DATA_TYPES


class SubscriberProvisioningError(RuntimeError):
    """Google Health Subscriber provisioning failed."""


class GoogleHealthSubscriberProvisioner:
    """Create or replace one AUTOMATIC subscriber configuration."""

    def __init__(
        self,
        *,
        project_id: str,
        endpoint_uri: str,
        endpoint_authorization: str,
        subscriber_id: str = "health-data-pipeline",
        client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._project_id = project_id
        self._endpoint_uri = endpoint_uri
        self._endpoint_authorization = endpoint_authorization
        self._subscriber_id = subscriber_id
        self._client = client or httpx.Client(timeout=30.0, follow_redirects=False)
        self._sleeper = sleeper

    def upsert(self) -> str:
        token = _access_token()
        project_number = self._project_number(token)
        parent = f"projects/{project_number}"
        name = f"{parent}/subscribers/{self._subscriber_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "x-goog-user-project": self._project_id,
        }
        body = {
            "endpointUri": self._endpoint_uri,
            "subscriberConfigs": [
                {
                    "dataTypes": list(WEBHOOK_SUPPORTED_DATA_TYPES),
                    "subscriptionCreatePolicy": "AUTOMATIC",
                }
            ],
            "endpointAuthorization": {
                "secret": self._endpoint_authorization,
            },
        }
        subscribers = self._list_subscribers(parent, headers)
        exists = _find_subscriber(subscribers, name) is not None
        if exists:
            response = self._client.patch(
                f"https://health.googleapis.com/v4/{name}",
                params={"updateMask": ("endpointUri,subscriberConfigs,endpointAuthorization")},
                headers=headers,
                json={"name": name, **body},
            )
            _require_mutation_accepted(response, "update subscriber")
        else:
            response = self._client.post(
                f"https://health.googleapis.com/v4/{parent}/subscribers",
                params={"subscriberId": self._subscriber_id},
                headers=headers,
                json=body,
            )
            _require_mutation_accepted(response, "create subscriber")
        self._wait_until_active(parent, name, headers)
        return name

    def _list_subscribers(
        self,
        parent: str,
        headers: Mapping[str, str],
    ) -> tuple[Mapping[str, Any], ...]:
        subscribers: list[Mapping[str, Any]] = []
        page_token: str | None = None
        seen_tokens: set[str] = set()
        while True:
            params: dict[str, str | int] = {"pageSize": 1000}
            if page_token is not None:
                params["pageToken"] = page_token
            response = self._client.get(
                f"https://health.googleapis.com/v4/{parent}/subscribers",
                headers=headers,
                params=params,
            )
            _require_success(response, "list subscribers")
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise SubscriberProvisioningError("subscriber list is not a JSON object")
            subscribers.extend(_validated_subscribers(payload.get("subscribers", ())))

            next_page_token = payload.get("nextPageToken")
            if next_page_token in (None, ""):
                return tuple(subscribers)
            if not isinstance(next_page_token, str) or next_page_token in seen_tokens:
                raise SubscriberProvisioningError("subscriber list has invalid pagination")
            seen_tokens.add(next_page_token)
            page_token = next_page_token

    def _wait_until_active(
        self,
        parent: str,
        name: str,
        headers: Mapping[str, str],
    ) -> None:
        for attempt in range(ACTIVATION_ATTEMPTS):
            subscriber = _find_subscriber(self._list_subscribers(parent, headers), name)
            if subscriber is not None:
                state = subscriber.get("state")
                if state in (None, "ACTIVE"):
                    # A completed create/patch operation already means endpoint
                    # verification succeeded. The current v4 list response can
                    # omit the output-only state field, so absence is compatible
                    # with success while explicit UNVERIFIED/INACTIVE is not.
                    return
            if attempt + 1 < ACTIVATION_ATTEMPTS:
                self._sleeper(ACTIVATION_DELAY_SECONDS)
        raise SubscriberProvisioningError("Google Health subscriber did not become ACTIVE")

    def _project_number(self, token: str) -> str:
        response = self._client.get(
            f"https://cloudresourcemanager.googleapis.com/v3/projects/{self._project_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "x-goog-user-project": self._project_id,
            },
        )
        _require_success(response, "resolve project number")
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise SubscriberProvisioningError("project response is not a JSON object")
        name = payload.get("name")
        if not isinstance(name, str) or not name.startswith("projects/"):
            raise SubscriberProvisioningError("project response has no project number")
        return name.removeprefix("projects/")


def _access_token() -> str:
    credentials, _ = google.auth.default(scopes=(CLOUD_PLATFORM_SCOPE,))
    credentials.refresh(GoogleAuthRequest())  # type: ignore[no-untyped-call]
    token = credentials.token
    if not isinstance(token, str) or not token:
        raise SubscriberProvisioningError("application default credentials returned no token")
    return token


def _validated_subscribers(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise SubscriberProvisioningError("subscriber list has invalid subscribers")
    subscribers: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise SubscriberProvisioningError("subscriber list has invalid subscribers")
        subscribers.append(cast(Mapping[str, Any], item))
    return tuple(subscribers)


def _find_subscriber(
    value: Sequence[Mapping[str, Any]],
    expected_name: str,
) -> Mapping[str, Any] | None:
    for subscriber in value:
        if subscriber.get("name") == expected_name:
            return subscriber
    return None


def _require_success(response: httpx.Response, operation: str) -> None:
    if not HTTP_SUCCESS_MIN <= response.status_code < HTTP_SUCCESS_MAX_EXCLUSIVE:
        raise SubscriberProvisioningError(
            f"Google Health {operation} failed ({response.status_code})"
        )


def _require_mutation_accepted(response: httpx.Response, operation: str) -> None:
    _require_success(response, operation)
    try:
        payload = response.json()
    except ValueError as exc:
        raise SubscriberProvisioningError(
            f"Google Health {operation} returned invalid JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise SubscriberProvisioningError(f"Google Health {operation} returned invalid JSON")
    if payload.get("error") is not None:
        raise SubscriberProvisioningError(f"Google Health {operation} operation failed")
    if payload.get("done") is not True:
        raise SubscriberProvisioningError(
            f"Google Health {operation} operation is still in progress"
        )
