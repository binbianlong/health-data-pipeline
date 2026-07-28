"""Google Health API and webhook adapters.

The module deliberately uses small protocols instead of binding business logic
to ``httpx`` or Tink.  Production can provide those implementations while unit
tests remain completely offline.
"""

# ruff: noqa: FURB162, PLR2004, SIM102, TRY301

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol, cast

HEALTH_DATA_SOURCE_FAMILY = "google-wearables"


class GoogleHealthError(RuntimeError):
    """Base error for Google Health access."""


class GoogleHealthAuthenticationError(GoogleHealthError):
    """The access token is invalid or lacks required scopes."""


class GoogleHealthRequestError(GoogleHealthError):
    """A permanent or exhausted transient API request failed."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class WebhookAuthenticationError(ValueError):
    """Webhook authorization or signature validation failed."""


class WebhookPayloadError(ValueError):
    """Webhook JSON is malformed or belongs to another owner."""


class HttpResponse(Protocol):
    status_code: int

    def json(self) -> Any: ...


class HttpTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        params: Mapping[str, str | int] | None = None,
        json: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse: ...


class AccessTokenProvider(Protocol):
    def __call__(self) -> str: ...


@dataclass(frozen=True, slots=True)
class DataTypeSpec:
    name: str
    max_window: timedelta
    page_size: int
    record_kind: str
    filter_field: str


_SHORT_WINDOW_TYPES = frozenset(
    {
        "active-minutes",
        "calories-in-heart-rate-zone",
        "heart-rate",
        "total-calories",
    }
)
_SMALL_PAGE_TYPES = frozenset({"sleep", "exercise"})
_DAILY_TYPES = frozenset(
    {
        "daily-resting-heart-rate",
        "daily-heart-rate-variability",
        "daily-oxygen-saturation",
    }
)
_SAMPLE_TYPES = frozenset(
    {
        "heart-rate",
        "heart-rate-variability",
        "oxygen-saturation",
        "respiratory-rate-sleep-summary",
    }
)


def _lower_camel(kebab: str) -> str:
    head, *tail = kebab.split("-")
    return head + "".join(word[:1].upper() + word[1:] for word in tail)


def data_type_spec(data_type: str) -> DataTypeSpec:
    """Return conservative API limits for a Google Health data type."""

    if data_type in _DAILY_TYPES:
        record_kind = "daily"
        filter_field = f"{_lower_camel(data_type)}.date"
    elif data_type in _SAMPLE_TYPES:
        record_kind = "sample"
        filter_field = f"{data_type.replace('-', '_')}.sample_time.physical_time"
    elif data_type == "sleep":
        record_kind = "session"
        filter_field = "sleep.interval.civil_end_time"
    elif data_type == "exercise":
        record_kind = "session"
        filter_field = "exercise.interval.civil_start_time"
    else:
        record_kind = "interval"
        filter_field = f"{data_type.replace('-', '_')}.interval.start_time"
    return DataTypeSpec(
        name=data_type,
        max_window=timedelta(days=14 if data_type in _SHORT_WINDOW_TYPES else 90),
        page_size=25 if data_type in _SMALL_PAGE_TYPES else 10_000,
        record_kind=record_kind,
        filter_field=filter_field,
    )


@dataclass(frozen=True, slots=True)
class FetchWindow:
    start: datetime
    end: datetime
    civil_start: date | None = None
    civil_end: date | None = None

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("fetch window datetimes must be timezone-aware")
        if self.start >= self.end:
            raise ValueError("fetch window must satisfy start < end")
        if (self.civil_start is None) != (self.civil_end is None):
            raise ValueError("civil window boundaries must be provided together")
        if (
            self.civil_start is not None
            and self.civil_end is not None
            and self.civil_start >= self.civil_end
        ):
            raise ValueError("civil window must satisfy start < end")


@dataclass(frozen=True, slots=True)
class DataPointPage:
    data_type: str
    window: FetchWindow
    page_number: int
    data_points: tuple[Mapping[str, Any], ...]
    next_page_token: str | None
    raw_payload: Mapping[str, Any]


def _rfc3339(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def split_fetch_window(window: FetchWindow, maximum: timedelta) -> Iterator[FetchWindow]:
    if window.civil_start is not None and window.end - window.start > maximum:
        raise ValueError("civil fetch windows must be pre-split at data-type limits")
    cursor = window.start
    while cursor < window.end:
        chunk_end = min(cursor + maximum, window.end)
        yield FetchWindow(
            cursor,
            chunk_end,
            civil_start=window.civil_start,
            civil_end=window.civil_end,
        )
        cursor = chunk_end


class GoogleHealthClient:
    """Minimal synchronous v4 client with paging and bounded query windows."""

    def __init__(
        self,
        *,
        transport: HttpTransport,
        access_token: AccessTokenProvider,
        base_url: str = "https://health.googleapis.com",
        user: str = "me",
        data_source_family: str = HEALTH_DATA_SOURCE_FAMILY,
        max_attempts: int = 5,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._transport = transport
        self._access_token = access_token
        self._base_url = base_url.rstrip("/")
        self._user = user
        self._data_source_family = data_source_family
        self._max_attempts = max_attempts
        self._sleeper = sleeper

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
    ) -> Mapping[str, Any]:
        for attempt in range(self._max_attempts):
            response = self._transport.request(
                method,
                f"{self._base_url}{path}",
                headers={
                    "Authorization": f"Bearer {self._access_token()}",
                    "Accept": "application/json",
                },
                params=params,
                timeout=30.0,
            )
            if response.status_code in (401, 403):
                raise GoogleHealthAuthenticationError(
                    f"Google Health authentication failed ({response.status_code})"
                )
            if response.status_code == 429 or response.status_code >= 500:
                if attempt + 1 < self._max_attempts:
                    self._sleeper(min(2**attempt, 16))
                    continue
            if response.status_code < 200 or response.status_code >= 300:
                raise GoogleHealthRequestError(
                    response.status_code,
                    f"Google Health request failed ({response.status_code})",
                )
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise GoogleHealthError("Google Health response must be a JSON object")
            return cast(Mapping[str, Any], payload)
        raise AssertionError("retry loop exhausted unexpectedly")

    def iter_data_point_pages(
        self,
        data_type: str,
        window: FetchWindow,
    ) -> Iterator[DataPointPage]:
        """Yield every page, splitting requests at data-type API limits."""

        spec = data_type_spec(data_type)
        page_number = 0
        for chunk in split_fetch_window(window, spec.max_window):
            page_token: str | None = None
            while True:
                if spec.record_kind in {"daily", "session"}:
                    start_value = (chunk.civil_start or chunk.start.date()).isoformat()
                    end_value = (chunk.civil_end or chunk.end.date()).isoformat()
                else:
                    start_value = _rfc3339(chunk.start)
                    end_value = _rfc3339(chunk.end)
                params: dict[str, str | int] = {
                    "filter": (
                        f'{spec.filter_field} >= "{start_value}" AND '
                        f'{spec.filter_field} < "{end_value}"'
                    ),
                    "pageSize": spec.page_size,
                    "dataSourceFamily": (
                        f"users/{self._user}/dataSourceFamilies/{self._data_source_family}"
                    ),
                }
                if page_token:
                    params["pageToken"] = page_token
                payload = self._request_json(
                    "GET",
                    (f"/v4/users/{self._user}/dataTypes/{data_type}/dataPoints:reconcile"),
                    params=params,
                )
                raw_points = payload.get("dataPoints", ())
                if not isinstance(raw_points, Sequence) or isinstance(
                    raw_points, (str, bytes, bytearray)
                ):
                    raise GoogleHealthError("dataPoints must be an array")
                points: list[Mapping[str, Any]] = []
                for point in raw_points:
                    if not isinstance(point, Mapping):
                        raise GoogleHealthError("each data point must be an object")
                    points.append(cast(Mapping[str, Any], point))

                raw_next = payload.get("nextPageToken")
                if raw_next is not None and not isinstance(raw_next, str):
                    raise GoogleHealthError("nextPageToken must be a string")
                page_token = raw_next or None
                yield DataPointPage(
                    data_type=data_type,
                    window=chunk,
                    page_number=page_number,
                    data_points=tuple(points),
                    next_page_token=page_token,
                    raw_payload=payload,
                )
                page_number += 1
                if page_token is None:
                    break

    def list_data_points(
        self, data_type: str, window: FetchWindow
    ) -> tuple[Mapping[str, Any], ...]:
        return tuple(
            point
            for page in self.iter_data_point_pages(data_type, window)
            for point in page.data_points
        )

    def get_paired_devices(self) -> tuple[Mapping[str, Any], ...]:
        devices: list[Mapping[str, Any]] = []
        page_token: str | None = None
        while True:
            params: dict[str, str | int] = {"pageSize": 100}
            if page_token:
                params["pageToken"] = page_token
            payload = self._request_json(
                "GET", f"/v4/users/{self._user}/pairedDevices", params=params
            )
            raw_devices = payload.get("pairedDevices", payload.get("devices", ()))
            if not isinstance(raw_devices, Sequence) or isinstance(
                raw_devices, (str, bytes, bytearray)
            ):
                raise GoogleHealthError("paired devices must be an array")
            for device in raw_devices:
                if not isinstance(device, Mapping):
                    raise GoogleHealthError("each paired device must be an object")
                device_type = device.get("deviceType", device.get("type"))
                if device_type == "TRACKER":
                    devices.append(cast(Mapping[str, Any], device))
            raw_next = payload.get("nextPageToken")
            if raw_next is not None and not isinstance(raw_next, str):
                raise GoogleHealthError("nextPageToken must be a string")
            page_token = raw_next or None
            if page_token is None:
                break
        return tuple(devices)

    def latest_fitbit_sync_time(self) -> datetime | None:
        """Return Sense 2 sync time, or another tracker only when Sense 2 is absent."""

        trackers = self.get_paired_devices()
        sense_2_trackers = tuple(device for device in trackers if _is_fitbit_sense_2(device))
        if sense_2_trackers:
            return _latest_device_sync_time(sense_2_trackers)
        return _latest_device_sync_time(trackers)

    def get_settings(self) -> Mapping[str, Any]:
        return self._request_json("GET", f"/v4/users/{self._user}/settings")

    def get_identity(self) -> Mapping[str, Any]:
        payload = self._request_json("GET", f"/v4/users/{self._user}/identity")
        if not isinstance(payload.get("healthUserId"), str):
            raise GoogleHealthError("identity response has no healthUserId")
        return payload


def _is_fitbit_sense_2(device: Mapping[str, Any]) -> bool:
    version = device.get("deviceVersion")
    if not isinstance(version, str):
        return False
    normalized = "".join(character for character in version.casefold() if character.isalnum())
    return normalized in {"sense2", "fitbitsense2"}


def _latest_device_sync_time(
    devices: Sequence[Mapping[str, Any]],
) -> datetime | None:
    latest: datetime | None = None
    for device in devices:
        raw = device.get("lastSyncTime")
        if not isinstance(raw, str):
            continue
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            continue
        parsed = parsed.astimezone(UTC)
        if latest is None or parsed > latest:
            latest = parsed
    return latest


class SignaturePrimitive(Protocol):
    """Shape implemented by Tink MAC and public-key verify primitives."""

    def verify_mac(self, signature: bytes, data: bytes) -> None: ...


class WebhookSignatureVerifier(Protocol):
    def verify(self, signature: bytes, payload: bytes) -> bool: ...


class TinkCompatibleVerifier:
    """Adapter for Tink primitives without importing Tink at module import time."""

    def __init__(self, primitive: Any) -> None:
        self._primitive = primitive

    def verify(self, signature: bytes, payload: bytes) -> bool:
        try:
            verify_mac = getattr(self._primitive, "verify_mac", None)
            if callable(verify_mac):
                verify_mac(signature, payload)
            else:
                verify = getattr(self._primitive, "verify", None)
                if not callable(verify):
                    raise TypeError("primitive exposes neither verify_mac nor verify")
                verify(signature, payload)
        except Exception:  # Tink deliberately exposes verification as exceptions.
            return False
        return True


def _decode_signature(value: str) -> bytes:
    compact = value.strip()
    if not compact:
        raise WebhookAuthenticationError("missing webhook signature")
    padding = "=" * (-len(compact) % 4)
    try:
        return base64.b64decode(compact + padding, altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as exc:
        raise WebhookAuthenticationError("invalid webhook signature encoding") from exc


def identifier_hmac(key: bytes, identifier: str) -> str:
    if not key:
        raise ValueError("HMAC key must not be empty")
    if not identifier:
        raise ValueError("identifier must not be empty")
    return hmac.new(key, identifier.encode("utf-8"), hashlib.sha256).hexdigest()


@dataclass(frozen=True, slots=True)
class VerifiedWebhook:
    event_id: str
    owner_hmac: str
    payload: Mapping[str, Any]
    raw_body: bytes


@dataclass(frozen=True, slots=True)
class WebhookVerification:
    """Authorized subscriber verification handshake."""

    payload: Mapping[str, Any]


class GoogleHealthWebhookAuthenticator:
    """Authenticate a webhook before any payload is persisted."""

    def __init__(
        self,
        *,
        expected_authorization: str,
        signature_verifier: WebhookSignatureVerifier,
        application_hmac_key: bytes,
        expected_owner_hmac: str,
    ) -> None:
        self._expected_authorization = expected_authorization.encode("utf-8")
        self._signature_verifier = signature_verifier
        self._application_hmac_key = application_hmac_key
        self._expected_owner_hmac = expected_owner_hmac

    def authenticate(
        self,
        *,
        authorization: str | None,
        signature_header: str | None,
        content_type: str | None,
        raw_body: bytes,
    ) -> VerifiedWebhook | WebhookVerification:
        if not content_type or content_type.split(";", 1)[0].strip().lower() != (
            "application/json"
        ):
            raise WebhookPayloadError("webhook content type must be application/json")
        supplied = (authorization or "").encode("utf-8")
        if not hmac.compare_digest(supplied, self._expected_authorization):
            raise WebhookAuthenticationError("webhook authorization failed")
        try:
            payload = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WebhookPayloadError("webhook body is not valid JSON") from exc
        if not isinstance(payload, Mapping):
            raise WebhookPayloadError("webhook payload must be an object")
        if payload.get("type") == "verification":
            return WebhookVerification(payload={"type": "verification"})
        signature = _decode_signature(signature_header or "")
        if not self._signature_verifier.verify(signature, raw_body):
            raise WebhookAuthenticationError("webhook signature verification failed")
        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise WebhookPayloadError("webhook payload has no data object")
        raw_owner = data.get("healthUserId")
        if not isinstance(raw_owner, str) or not raw_owner:
            raise WebhookPayloadError("webhook payload has no healthUserId")
        owner = identifier_hmac(self._application_hmac_key, raw_owner)
        if not hmac.compare_digest(owner, self._expected_owner_hmac):
            raise WebhookPayloadError("webhook owner does not match")
        sanitized_data = dict(data)
        sanitized_data.pop("healthUserId", None)
        sanitized_data["ownerHmac"] = owner
        sanitized_payload = dict(payload)
        sanitized_payload["data"] = sanitized_data
        sanitized_body = json.dumps(
            sanitized_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return VerifiedWebhook(
            event_id=hashlib.sha256(sanitized_body).hexdigest(),
            owner_hmac=owner,
            payload=cast(Mapping[str, Any], sanitized_payload),
            raw_body=sanitized_body,
        )
