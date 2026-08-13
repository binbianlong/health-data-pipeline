from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from health_data_pipeline.integrations.notion import (
    MAX_RICH_TEXT_CHARS,
    NotionApiError,
    NotionDataSourceGateway,
    _body_blocks,
)
from health_data_pipeline.views import DailyView


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        return self._payload


class RecordingNotionClient:
    def __init__(
        self,
        *,
        fail_on_path: str | None = None,
        responses: list[FakeResponse] | None = None,
    ) -> None:
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.fail_on_path = fail_on_path
        self.responses = responses or []

    def request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, str | int] | None = None,
    ) -> FakeResponse:
        del params
        self.calls.append((method, path, json))
        if path == self.fail_on_path:
            raise RuntimeError("injected Notion failure")
        if self.responses:
            return self.responses.pop(0)
        if method == "POST" and path == "/v1/pages":
            return FakeResponse(200, {"id": "new-page"})
        if method == "GET" and path == "/v1/data_sources/source":
            return FakeResponse(
                200,
                {
                    "properties": {
                        "Name": {"type": "title", "title": {}},
                    }
                },
            )
        if method == "GET" and path.endswith("/children"):
            return FakeResponse(
                200,
                {
                    "results": [{"id": "old-block"}],
                    "has_more": False,
                    "next_cursor": None,
                },
            )
        return FakeResponse(200, {})


def _view() -> DailyView:
    return DailyView(
        metric_date=date(2026, 7, 28),
        source_generation=7,
        data_status="CANONICAL_SYNCED",
        fitbit_last_sync_time=datetime(2026, 7, 28, tzinfo=UTC),
        metrics=(),
    )


def test_existing_page_commits_properties_after_replacing_body() -> None:
    client = RecordingNotionClient()
    gateway = NotionDataSourceGateway(
        token="token",
        data_source_id="source",
        client=client,  # type: ignore[arg-type]
    )

    gateway.upsert_daily_page(
        existing_page_id="page",
        view=_view(),
        body="body",
        content_sha256="a" * 64,
    )

    assert [call[:2] for call in client.calls] == [
        ("GET", "/v1/blocks/page/children"),
        ("DELETE", "/v1/blocks/old-block"),
        ("PATCH", "/v1/blocks/page/children"),
        ("PATCH", "/v1/pages/page"),
    ]


def test_body_failure_does_not_advance_page_generation_marker() -> None:
    client = RecordingNotionClient(fail_on_path="/v1/blocks/page/children")
    gateway = NotionDataSourceGateway(
        token="token",
        data_source_id="source",
        client=client,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="injected"):
        gateway.upsert_daily_page(
            existing_page_id="page",
            view=_view(),
            body="body",
            content_sha256="a" * 64,
        )

    assert ("PATCH", "/v1/pages/page") not in [call[:2] for call in client.calls]


def test_new_page_uses_date_as_recovery_key_before_body_commit() -> None:
    client = RecordingNotionClient()
    gateway = NotionDataSourceGateway(
        token="token",
        data_source_id="source",
        client=client,  # type: ignore[arg-type]
    )

    page_id = gateway.upsert_daily_page(
        existing_page_id=None,
        view=_view(),
        body="body",
        content_sha256="a" * 64,
    )

    assert page_id == "new-page"
    create_payload = client.calls[0][2]
    assert create_payload is not None
    assert create_payload["properties"] == {
        "Date": {"title": [{"type": "text", "text": {"content": "2026-07-28"}}]},
        "Metric date": {"date": {"start": "2026-07-28"}},
    }
    assert [call[:2] for call in client.calls][-1] == (
        "PATCH",
        "/v1/pages/new-page",
    )


def test_configure_schema_renames_title_and_adds_required_properties() -> None:
    client = RecordingNotionClient()
    gateway = NotionDataSourceGateway(
        token="token",
        data_source_id="source",
        client=client,  # type: ignore[arg-type]
    )

    assert gateway.ensure_schema()

    method, path, payload = client.calls[-1]
    assert (method, path) == ("PATCH", "/v1/data_sources/source")
    assert payload is not None
    properties = payload["properties"]
    assert properties["Name"] == {"name": "Date"}
    assert properties["Metric date"] == {"date": {}}
    assert properties["Source HEAD generation"] == {"number": {}}
    assert properties["Content SHA-256"] == {"rich_text": {}}
    assert properties["Fitbit last sync time"] == {"date": {}}


def test_rate_limit_retries_after_server_requested_delay() -> None:
    client = RecordingNotionClient(
        responses=[
            FakeResponse(429, {"sensitive": "not exposed"}, headers={"Retry-After": "0.25"}),
            FakeResponse(200, {"properties": {"Name": {"type": "title", "title": {}}}}),
        ]
    )
    delays: list[float] = []
    gateway = NotionDataSourceGateway(
        token="token",
        data_source_id="source",
        client=client,  # type: ignore[arg-type]
        sleeper=delays.append,
    )

    assert gateway.ensure_schema()
    assert delays == [0.25]
    assert [call[:2] for call in client.calls[:2]] == [
        ("GET", "/v1/data_sources/source"),
        ("GET", "/v1/data_sources/source"),
    ]


def test_rate_limit_retry_is_bounded_and_error_omits_response_data() -> None:
    client = RecordingNotionClient(
        responses=[
            FakeResponse(429, {"secret": "private"}, headers={"Retry-After": "2"}) for _ in range(4)
        ]
    )
    delays: list[float] = []
    gateway = NotionDataSourceGateway(
        token="token",
        data_source_id="source",
        client=client,  # type: ignore[arg-type]
        sleeper=delays.append,
    )

    with pytest.raises(NotionApiError, match=r"Notion request failed \(429\)") as exc_info:
        gateway.ensure_schema()

    assert len(client.calls) == 4
    assert delays == [2.0, 2.0, 2.0]
    assert "private" not in str(exc_info.value)
    assert "token" not in str(exc_info.value)


def test_body_blocks_preserve_complete_line_boundaries_when_they_fit() -> None:
    first_line = "a" * 1_000 + "\n"
    second_line = "b" * 1_000 + "\n"
    final_line = "tail\n"
    body = first_line + second_line + final_line

    blocks = _body_blocks(body)
    chunks = [block["code"]["rich_text"][0]["text"]["content"] for block in blocks]

    assert chunks == [first_line, second_line + final_line]
    assert "".join(chunks) == body
    assert all(len(chunk) <= MAX_RICH_TEXT_CHARS for chunk in chunks)
    assert all(chunk.endswith("\n") for chunk in chunks)
