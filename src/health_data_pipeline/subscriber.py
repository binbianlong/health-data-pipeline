"""Idempotent Google Health webhook Subscriber provisioning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import google.auth
import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
HTTP_SUCCESS_MIN = 200
HTTP_SUCCESS_MAX_EXCLUSIVE = 300
WEBHOOK_SUPPORTED_DATA_TYPES = (
    "steps",
    "distance",
    "active-zone-minutes",
    "heart-rate",
    "daily-resting-heart-rate",
    "heart-rate-variability",
    "daily-heart-rate-variability",
    "daily-oxygen-saturation",
    "sleep",
    "respiratory-rate-sleep-summary",
    "exercise",
)


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
    ) -> None:
        self._project_id = project_id
        self._endpoint_uri = endpoint_uri
        self._endpoint_authorization = endpoint_authorization
        self._subscriber_id = subscriber_id
        self._client = client or httpx.Client(timeout=30.0, follow_redirects=False)

    def upsert(self) -> str:
        token = _access_token()
        project_number = self._project_number(token)
        parent = f"projects/{project_number}"
        name = f"{parent}/subscribers/{self._subscriber_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        body = {
            "name": name,
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
        list_response = self._client.get(
            f"https://health.googleapis.com/v4/{parent}/subscribers",
            headers=headers,
        )
        _require_success(list_response, "list subscribers")
        list_payload = list_response.json()
        if not isinstance(list_payload, Mapping):
            raise SubscriberProvisioningError("subscriber list is not a JSON object")
        subscribers = list_payload.get("subscribers", ())
        exists = _contains_subscriber(subscribers, name)
        if exists:
            response = self._client.patch(
                f"https://health.googleapis.com/v4/{name}",
                params={"updateMask": ("endpointUri,subscriberConfigs,endpointAuthorization")},
                headers=headers,
                json=body,
            )
            _require_success(response, "update subscriber")
        else:
            response = self._client.post(
                f"https://health.googleapis.com/v4/{parent}/subscribers",
                params={"subscriberId": self._subscriber_id},
                headers=headers,
                json=body,
            )
            _require_success(response, "create subscriber")
        return name

    def _project_number(self, token: str) -> str:
        response = self._client.get(
            f"https://cloudresourcemanager.googleapis.com/v3/projects/{self._project_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
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


def _contains_subscriber(value: Any, expected_name: str) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise SubscriberProvisioningError("subscriber list has invalid subscribers")
    return any(
        isinstance(item, Mapping) and cast(Mapping[str, Any], item).get("name") == expected_name
        for item in value
    )


def _require_success(response: httpx.Response, operation: str) -> None:
    if not HTTP_SUCCESS_MIN <= response.status_code < HTTP_SUCCESS_MAX_EXCLUSIVE:
        raise SubscriberProvisioningError(
            f"Google Health {operation} failed ({response.status_code})"
        )
