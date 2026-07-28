from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from health_data_pipeline.google_health import (
    FetchWindow,
    GoogleHealthClient,
    GoogleHealthWebhookAuthenticator,
    VerifiedWebhook,
    WebhookAuthenticationError,
    data_type_spec,
    identifier_hmac,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class FakeTransport:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.requests: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


class AcceptingVerifier:
    def __init__(self, expected: bytes) -> None:
        self.expected = expected

    def verify(self, signature: bytes, payload: bytes) -> bool:
        return signature == b"signed" and payload == self.expected


def _window(days: int) -> FetchWindow:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return FetchWindow(start, start + timedelta(days=days))


def test_list_data_points_pages_and_splits_short_window() -> None:
    transport = FakeTransport(
        [
            FakeResponse(200, {"dataPoints": [{"heartRate": {"beatsPerMinute": "60"}}]}),
            FakeResponse(200, {"dataPoints": [{"heartRate": {"beatsPerMinute": "61"}}]}),
        ]
    )
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
        sleeper=lambda _: None,
    )

    points = client.list_data_points("heart-rate", _window(20))

    assert len(points) == 2
    assert len(transport.requests) == 2
    assert "heart_rate.sample_time.physical_time" in transport.requests[0]["params"]["filter"]
    assert transport.requests[0]["params"]["pageSize"] == 10_000
    assert transport.requests[0]["url"].endswith(
        "/v4/users/me/dataTypes/heart-rate/dataPoints:reconcile"
    )
    assert transport.requests[0]["params"]["dataSourceFamily"].endswith(
        "/dataSourceFamilies/google-wearables"
    )


def test_sleep_uses_end_time_filter_and_small_page_size() -> None:
    transport = FakeTransport(
        [
            FakeResponse(
                200,
                {"dataPoints": [], "nextPageToken": "next"},
            ),
            FakeResponse(200, {"dataPoints": []}),
        ]
    )
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
        sleeper=lambda _: None,
    )

    tuple(client.iter_data_point_pages("sleep", _window(2)))

    assert transport.requests[0]["params"]["pageSize"] == 25
    assert "sleep.interval.civil_end_time" in transport.requests[0]["params"]["filter"]
    assert transport.requests[1]["params"]["pageToken"] == "next"


def test_daily_filter_uses_camel_case_date_field() -> None:
    transport = FakeTransport([FakeResponse(200, {"dataPoints": []})])
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
        sleeper=lambda _: None,
    )

    client.list_data_points("daily-resting-heart-rate", _window(2))

    expression = transport.requests[0]["params"]["filter"]
    assert 'dailyRestingHeartRate.date >= "2026-01-01"' in expression
    assert data_type_spec("active-minutes").max_window == timedelta(days=14)
    assert data_type_spec("active-zone-minutes").max_window == timedelta(days=90)


def test_transient_status_is_retried_without_exposing_body() -> None:
    delays: list[float] = []
    transport = FakeTransport(
        [
            FakeResponse(429, {"secret": "do not expose"}),
            FakeResponse(200, {"dataPoints": []}),
        ]
    )
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
        sleeper=delays.append,
    )

    client.list_data_points("steps", _window(1))

    assert delays == [1]


def test_latest_sync_prefers_sense_2_over_newer_tracker() -> None:
    transport = FakeTransport(
        [
            FakeResponse(
                200,
                {
                    "pairedDevices": [
                        {
                            "deviceType": "TRACKER",
                            "deviceVersion": "Charge 6",
                            "lastSyncTime": "2026-07-28T12:00:00Z",
                        },
                        {
                            "deviceType": "TRACKER",
                            "deviceVersion": "Fitbit Sense 2",
                            "lastSyncTime": "2026-07-28T10:00:00Z",
                        },
                        {
                            "deviceType": "SCALE",
                            "deviceVersion": "Sense 2",
                            "lastSyncTime": "2026-07-28T13:00:00Z",
                        },
                    ]
                },
            )
        ]
    )
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
    )

    result = client.latest_fitbit_sync_time()

    assert result == datetime(2026, 7, 28, 10, tzinfo=UTC)


def test_latest_sync_falls_back_to_newest_tracker_when_sense_2_is_absent() -> None:
    transport = FakeTransport(
        [
            FakeResponse(
                200,
                {
                    "pairedDevices": [
                        {
                            "deviceType": "TRACKER",
                            "deviceVersion": "Charge 5",
                            "lastSyncTime": "2026-07-28T09:00:00Z",
                        },
                        {
                            "deviceType": "TRACKER",
                            "deviceVersion": "Versa 4",
                            "lastSyncTime": "2026-07-28T11:00:00+00:00",
                        },
                    ]
                },
            )
        ]
    )
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
    )

    result = client.latest_fitbit_sync_time()

    assert result == datetime(2026, 7, 28, 11, tzinfo=UTC)


def test_latest_sync_does_not_fallback_when_sense_2_has_no_valid_sync() -> None:
    transport = FakeTransport(
        [
            FakeResponse(
                200,
                {
                    "pairedDevices": [
                        {
                            "deviceType": "TRACKER",
                            "deviceVersion": "Sense-2",
                            "lastSyncTime": "invalid",
                        },
                        {
                            "deviceType": "TRACKER",
                            "deviceVersion": "Charge 6",
                            "lastSyncTime": "2026-07-28T12:00:00Z",
                        },
                    ]
                },
            )
        ]
    )
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
    )

    assert client.latest_fitbit_sync_time() is None


def test_webhook_authenticates_signature_and_owner() -> None:
    body = json.dumps(
        {"data": {"healthUserId": "owner", "dataType": "steps", "intervals": []}}
    ).encode()
    key = b"hmac-key"
    authenticator = GoogleHealthWebhookAuthenticator(
        expected_authorization="Bearer webhook-secret",
        signature_verifier=AcceptingVerifier(body),
        application_hmac_key=key,
        expected_owner_hmac=identifier_hmac(key, "owner"),
    )

    event = authenticator.authenticate(
        authorization="Bearer webhook-secret",
        signature_header=base64.b64encode(b"signed").decode(),
        content_type="application/json; charset=utf-8",
        raw_body=body,
    )

    assert isinstance(event, VerifiedWebhook)
    assert event.owner_hmac == identifier_hmac(key, "owner")
    assert b"healthUserId" not in event.raw_body
    assert b"ownerHmac" in event.raw_body


def test_webhook_rejects_bad_authorization_before_persistence() -> None:
    body = b'{"data":{"healthUserId":"owner"}}'
    authenticator = GoogleHealthWebhookAuthenticator(
        expected_authorization="expected",
        signature_verifier=AcceptingVerifier(body),
        application_hmac_key=b"key",
        expected_owner_hmac=identifier_hmac(b"key", "owner"),
    )

    with pytest.raises(WebhookAuthenticationError):
        authenticator.authenticate(
            authorization="wrong",
            signature_header=base64.b64encode(b"signed").decode(),
            content_type="application/json",
            raw_body=body,
        )
