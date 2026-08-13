from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from health_data_pipeline.health.sources import data_type_spec
from health_data_pipeline.integrations.google_health.client import (
    GoogleHealthClient,
    GoogleHealthError,
)
from health_data_pipeline.integrations.google_health.webhook import (
    GoogleHealthWebhookAuthenticator,
    VerifiedWebhookBatch,
    WebhookAuthenticationError,
    WebhookPayloadError,
)
from health_data_pipeline.setup.identity import identifier_hmac
from health_data_pipeline.sync.contracts import FetchWindow


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
        self.calls: list[tuple[bytes, bytes]] = []

    def verify(self, signature: bytes, payload: bytes) -> bool:
        self.calls.append((signature, payload))
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


def test_repeated_data_point_page_token_is_rejected() -> None:
    transport = FakeTransport(
        [
            FakeResponse(200, {"dataPoints": [], "nextPageToken": "repeated"}),
            FakeResponse(200, {"dataPoints": [], "nextPageToken": "repeated"}),
        ]
    )
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
        sleeper=lambda _: None,
    )

    with pytest.raises(
        GoogleHealthError,
        match="nextPageToken repeated within one query window",
    ):
        tuple(client.iter_data_point_pages("sleep", _window(2)))

    assert len(transport.requests) == 2
    assert transport.requests[1]["params"]["pageToken"] == "repeated"


@pytest.mark.parametrize(
    ("data_type", "filter_field"),
    [
        ("daily-resting-heart-rate", "daily_resting_heart_rate.date"),
        ("daily-heart-rate-variability", "daily_heart_rate_variability.date"),
        ("daily-oxygen-saturation", "daily_oxygen_saturation.date"),
    ],
)
def test_daily_filter_uses_snake_case_date_field(
    data_type: str,
    filter_field: str,
) -> None:
    transport = FakeTransport([FakeResponse(200, {"dataPoints": []})])
    client = GoogleHealthClient(
        transport=transport,
        access_token=lambda: "token",
        sleeper=lambda _: None,
    )

    client.list_data_points(data_type, _window(2))

    expression = transport.requests[0]["params"]["filter"]
    assert f'{filter_field} >= "2026-01-01"' in expression
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
    verifier = AcceptingVerifier(body)
    authenticator = GoogleHealthWebhookAuthenticator(
        expected_authorization="Bearer webhook-secret",
        signature_verifier=verifier,
        application_hmac_key=key,
        expected_owner_hmac=identifier_hmac(key, "owner"),
    )

    batch = authenticator.authenticate(
        authorization="Bearer webhook-secret",
        signature_header=base64.b64encode(b"signed").decode(),
        content_type="application/json; charset=utf-8",
        raw_body=body,
    )

    assert isinstance(batch, VerifiedWebhookBatch)
    assert len(batch.events) == 1
    event = batch.events[0]
    assert event.owner_hmac == identifier_hmac(key, "owner")
    assert b"healthUserId" not in event.raw_body
    assert b"ownerHmac" in event.raw_body
    assert verifier.calls == [(b"signed", body)]


def test_webhook_authenticates_array_with_one_raw_signature_check() -> None:
    payload = [
        {
            "data": {
                "healthUserId": "owner",
                "dataType": "steps",
                "intervals": [{"startTime": "2026-07-28T00:00:00Z"}],
            }
        },
        {
            "data": {
                "healthUserId": "owner",
                "dataType": "sleep",
                "intervals": [{"startTime": "2026-07-27T22:00:00Z"}],
            }
        },
    ]
    body = json.dumps(payload).encode()
    key = b"hmac-key"
    verifier = AcceptingVerifier(body)
    authenticator = GoogleHealthWebhookAuthenticator(
        expected_authorization="Bearer webhook-secret",
        signature_verifier=verifier,
        application_hmac_key=key,
        expected_owner_hmac=identifier_hmac(key, "owner"),
    )

    batch = authenticator.authenticate(
        authorization="Bearer webhook-secret",
        signature_header=base64.b64encode(b"signed").decode(),
        content_type="application/json",
        raw_body=body,
    )

    assert isinstance(batch, VerifiedWebhookBatch)
    assert len(batch.events) == 2
    assert verifier.calls == [(b"signed", body)]
    assert all(b"healthUserId" not in event.raw_body for event in batch.events)
    assert all(b"ownerHmac" in event.raw_body for event in batch.events)
    assert batch.events[0].event_id != batch.events[1].event_id


@pytest.mark.parametrize(
    "payload",
    [
        [],
        [
            {"data": {"healthUserId": "owner", "dataType": "steps"}},
            "not-an-object",
        ],
        [
            {"data": {"healthUserId": "owner", "dataType": "steps"}},
            {"data": []},
        ],
        [
            {"data": {"healthUserId": "owner", "dataType": "steps"}},
            {"data": {"dataType": "sleep"}},
        ],
        [
            {"data": {"healthUserId": "owner", "dataType": "steps"}},
            {"data": {"healthUserId": "owner"}},
        ],
        "not-an-object-or-array",
    ],
)
def test_webhook_batch_rejects_invalid_items_atomically(payload: Any) -> None:
    body = json.dumps(payload).encode()
    key = b"hmac-key"
    authenticator = GoogleHealthWebhookAuthenticator(
        expected_authorization="Bearer webhook-secret",
        signature_verifier=AcceptingVerifier(body),
        application_hmac_key=key,
        expected_owner_hmac=identifier_hmac(key, "owner"),
    )

    with pytest.raises(WebhookPayloadError):
        authenticator.authenticate(
            authorization="Bearer webhook-secret",
            signature_header=base64.b64encode(b"signed").decode(),
            content_type="application/json",
            raw_body=body,
        )


def test_webhook_batch_keeps_only_expected_owner_and_deduplicates() -> None:
    owner_notification = {"data": {"healthUserId": "owner", "dataType": "steps", "intervals": []}}
    payload = [
        owner_notification,
        {"data": {"healthUserId": "other", "dataType": "sleep", "intervals": []}},
        owner_notification,
    ]
    body = json.dumps(payload).encode()
    key = b"hmac-key"
    authenticator = GoogleHealthWebhookAuthenticator(
        expected_authorization="Bearer webhook-secret",
        signature_verifier=AcceptingVerifier(body),
        application_hmac_key=key,
        expected_owner_hmac=identifier_hmac(key, "owner"),
    )

    batch = authenticator.authenticate(
        authorization="Bearer webhook-secret",
        signature_header=base64.b64encode(b"signed").decode(),
        content_type="application/json",
        raw_body=body,
    )

    assert isinstance(batch, VerifiedWebhookBatch)
    assert len(batch.events) == 1
    assert batch.events[0].payload["data"]["dataType"] == "steps"

    foreign_body = json.dumps(payload[1:2]).encode()
    foreign_authenticator = GoogleHealthWebhookAuthenticator(
        expected_authorization="Bearer webhook-secret",
        signature_verifier=AcceptingVerifier(foreign_body),
        application_hmac_key=key,
        expected_owner_hmac=identifier_hmac(key, "owner"),
    )
    foreign_batch = foreign_authenticator.authenticate(
        authorization="Bearer webhook-secret",
        signature_header=base64.b64encode(b"signed").decode(),
        content_type="application/json",
        raw_body=foreign_body,
    )

    assert isinstance(foreign_batch, VerifiedWebhookBatch)
    assert foreign_batch.events == ()


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
