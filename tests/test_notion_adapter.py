from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from health_data_pipeline.notion_adapter import NotionDataSourceGateway
from health_data_pipeline.views import DailyView


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


class RecordingNotionClient:
    def __init__(self, *, fail_on_path: str | None = None) -> None:
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.fail_on_path = fail_on_path

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
    assert set(create_payload["properties"]) == {"Date"}
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
    assert properties["Source HEAD generation"] == {"number": {}}
    assert properties["Content SHA-256"] == {"rich_text": {}}
    assert properties["Fitbit last sync time"] == {"date": {}}
