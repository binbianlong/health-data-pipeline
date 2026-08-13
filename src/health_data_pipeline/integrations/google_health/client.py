"""Google Health API and webhook adapters.

The module deliberately uses small protocols instead of binding business logic
to ``httpx`` or Tink.  Production can provide those implementations while unit
tests remain completely offline.
"""

# ruff: noqa: FURB162, PLR2004, SIM102

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from health_data_pipeline.health.sources import data_type_spec
from health_data_pipeline.sync.contracts import DataPointPage, FetchWindow

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
            seen_page_tokens: set[str] = set()
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
                if page_token is not None:
                    if page_token in seen_page_tokens:
                        raise GoogleHealthError("nextPageToken repeated within one query window")
                    seen_page_tokens.add(page_token)
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
