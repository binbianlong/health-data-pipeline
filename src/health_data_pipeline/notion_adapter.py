"""Current Notion API adapter and GCS-backed view source."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from math import isfinite
from typing import Any, cast
from zoneinfo import ZoneInfo

import httpx

from health_data_pipeline.models import RootSnapshot
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import HeadRepository, ObjectNotFound
from health_data_pipeline.views import (
    DailyMetric,
    DailyView,
    NotionGateway,
    NotionPage,
    StepSample,
)

NOTION_VERSION = "2026-03-11"
DATE_PROPERTY = "Date"
GENERATION_PROPERTY = "Source HEAD generation"
HASH_PROPERTY = "Content SHA-256"
STATUS_PROPERTY = "Data status"
SYNC_TIME_PROPERTY = "Fitbit last sync time"
MAX_RICH_TEXT_CHARS = 1_900
HTTP_SUCCESS_MIN = 200
HTTP_SUCCESS_MAX_EXCLUSIVE = 300
HTTP_NO_CONTENT = 204
HTTP_TOO_MANY_REQUESTS = 429
NOTION_REQUEST_ATTEMPTS = 4
DEFAULT_RETRY_AFTER_SECONDS = 1.0
MAX_RETRY_AFTER_SECONDS = 60.0
DATA_STATUS_OPTIONS = (
    "PROVISIONAL",
    "CANONICAL_SYNCED",
    "CANONICAL_STALE",
    "MISSING",
    "MIXED",
    "INSUFFICIENT_DATA",
)


class NotionApiError(RuntimeError):
    """A Notion request failed without leaking its response body."""


class NotionDataSourceGateway(NotionGateway):
    """Upsert one daily page in a Notion data source."""

    def __init__(
        self,
        *,
        token: str,
        data_source_id: str,
        client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not token:
            raise ValueError("Notion token must not be empty")
        if not data_source_id:
            raise ValueError("Notion data source ID must not be empty")
        self._data_source_id = data_source_id
        self._sleeper = sleeper
        self._client = client or httpx.Client(
            base_url="https://api.notion.com",
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": NOTION_VERSION,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    def ensure_schema(self) -> bool:
        """Create or repair the dedicated data source's required properties."""

        source = self._request(
            "GET",
            f"/v1/data_sources/{self._data_source_id}",
        )
        raw_properties = source.get("properties")
        if not isinstance(raw_properties, Mapping):
            raise NotionApiError("Notion data source has no properties")
        properties = cast(Mapping[str, Any], raw_properties)
        title_names = [
            name
            for name, value in properties.items()
            if isinstance(name, str) and isinstance(value, Mapping) and value.get("type") == "title"
        ]
        if len(title_names) != 1:
            raise NotionApiError("Notion data source must have exactly one title property")
        title_name = title_names[0]
        if DATE_PROPERTY in properties and title_name != DATE_PROPERTY:
            raise NotionApiError(f"{DATE_PROPERTY} exists but is not the title property")

        updates: dict[str, Any] = {}
        if title_name != DATE_PROPERTY:
            updates[title_name] = {"name": DATE_PROPERTY}
        desired: dict[str, dict[str, Any]] = {
            GENERATION_PROPERTY: {"number": {}},
            HASH_PROPERTY: {"rich_text": {}},
            STATUS_PROPERTY: {
                "select": {"options": [{"name": name} for name in DATA_STATUS_OPTIONS]}
            },
            SYNC_TIME_PROPERTY: {"date": {}},
        }
        for name, schema in desired.items():
            expected_type = next(iter(schema))
            current = properties.get(name)
            if not isinstance(current, Mapping) or current.get("type") != expected_type:
                updates[name] = schema
        if not updates:
            return False
        self._request(
            "PATCH",
            f"/v1/data_sources/{self._data_source_id}",
            json={"properties": updates},
        )
        return True

    def find_daily_page(self, metric_date: date) -> NotionPage | None:
        payload = self._request(
            "POST",
            f"/v1/data_sources/{self._data_source_id}/query",
            json={
                "filter": {
                    "property": DATE_PROPERTY,
                    "title": {"equals": metric_date.isoformat()},
                },
                "page_size": 2,
            },
        )
        raw_results = payload.get("results", ())
        if not isinstance(raw_results, Sequence) or isinstance(
            raw_results, (str, bytes, bytearray)
        ):
            raise NotionApiError("Notion query returned invalid results")
        if not raw_results:
            return None
        if len(raw_results) > 1:
            raise NotionApiError(f"multiple Notion pages exist for {metric_date}")
        page = raw_results[0]
        if not isinstance(page, Mapping):
            raise NotionApiError("Notion query returned an invalid page")
        page_id = _required_string(page, "id")
        properties = page.get("properties")
        if not isinstance(properties, Mapping):
            raise NotionApiError("Notion page has no properties")
        generation = _number_property(properties, GENERATION_PROPERTY)
        content_hash = _rich_text_property(properties, HASH_PROPERTY)
        return NotionPage(
            page_id=page_id,
            source_generation=generation,
            content_sha256=content_hash or None,
        )

    def upsert_daily_page(
        self,
        *,
        existing_page_id: str | None,
        view: DailyView,
        body: str,
        content_sha256: str,
    ) -> str:
        properties = _page_properties(view, content_sha256)
        children = _body_blocks(body)
        if existing_page_id is None:
            page = self._request(
                "POST",
                "/v1/pages",
                json={
                    "parent": {
                        "type": "data_source_id",
                        "data_source_id": self._data_source_id,
                    },
                    # Date is the recovery key if the response is lost. The
                    # generation/hash marker is committed only after the body.
                    "properties": _date_properties(view.metric_date),
                },
            )
            existing_page_id = _required_string(page, "id")

        self._replace_page_children(existing_page_id, children)
        # This property update is the Notion-side commit point. If body
        # replacement fails, the old marker remains and the next task retries.
        self._request(
            "PATCH",
            f"/v1/pages/{existing_page_id}",
            json={"properties": properties},
        )
        return existing_page_id

    def _replace_page_children(
        self,
        page_id: str,
        children: Sequence[Mapping[str, Any]],
    ) -> None:
        cursor: str | None = None
        existing_ids: list[str] = []
        while True:
            params: dict[str, str | int] = {"page_size": 100}
            if cursor is not None:
                params["start_cursor"] = cursor
            payload = self._request(
                "GET",
                f"/v1/blocks/{page_id}/children",
                params=params,
            )
            raw_results = payload.get("results", ())
            if not isinstance(raw_results, Sequence) or isinstance(
                raw_results, (str, bytes, bytearray)
            ):
                raise NotionApiError("Notion block query returned invalid results")
            for block in raw_results:
                if not isinstance(block, Mapping):
                    raise NotionApiError("Notion block query returned an invalid block")
                existing_ids.append(_required_string(block, "id"))
            has_more = payload.get("has_more")
            raw_cursor = payload.get("next_cursor")
            if has_more is not True:
                break
            if not isinstance(raw_cursor, str) or not raw_cursor:
                raise NotionApiError("Notion pagination has no next cursor")
            cursor = raw_cursor

        for block_id in existing_ids:
            self._request("DELETE", f"/v1/blocks/{block_id}")
        if children:
            self._request(
                "PATCH",
                f"/v1/blocks/{page_id}/children",
                json={"children": list(children)},
            )

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
        params: Mapping[str, str | int] | None = None,
    ) -> Mapping[str, Any]:
        for attempt in range(NOTION_REQUEST_ATTEMPTS):
            response = self._client.request(method, path, json=json, params=params)
            if response.status_code != HTTP_TOO_MANY_REQUESTS:
                break
            if attempt == NOTION_REQUEST_ATTEMPTS - 1:
                break
            self._sleeper(_retry_after_seconds(response.headers.get("Retry-After")))
        if not HTTP_SUCCESS_MIN <= response.status_code < HTTP_SUCCESS_MAX_EXCLUSIVE:
            raise NotionApiError(f"Notion request failed ({response.status_code})")
        if response.status_code == HTTP_NO_CONTENT:
            return {}
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise NotionApiError("Notion response must be a JSON object")
        return cast(Mapping[str, Any], payload)


def _retry_after_seconds(raw_value: str | None) -> float:
    if raw_value is None:
        return DEFAULT_RETRY_AFTER_SECONDS
    try:
        value = float(raw_value)
    except ValueError:
        return DEFAULT_RETRY_AFTER_SECONDS
    if not isfinite(value) or value < 0:
        return DEFAULT_RETRY_AFTER_SECONDS
    return min(value, MAX_RETRY_AFTER_SECONDS)


class SnapshotViewSource:
    """Materialize Notion rows and step samples from a committed Snapshot."""

    def __init__(
        self,
        *,
        heads: HeadRepository,
        snapshots: SnapshotRepository,
    ) -> None:
        self._heads = heads
        self._snapshots = snapshots

    def latest_generation(self) -> int:
        current = self._heads.read().head
        return 0 if current is None else current.generation

    def views_between(
        self,
        *,
        after_generation: int,
        through_generation: int,
    ) -> Sequence[DailyView]:
        """Materialize every date changed in the unapplied generation range.

        Each date is rendered from the target Snapshot, so repeated changes in
        intermediate generations collapse to one final, monotonic write.
        """

        root = self._root_for_generation(through_generation)
        dates: set[date]
        if after_generation < 0 or after_generation >= through_generation:
            if after_generation == through_generation:
                return ()
            raise ValueError("invalid generation range")
        if after_generation == 0:
            dates = set(self._available_dates(root))
        else:
            dates = set()
            cursor = root
            while cursor.generation > after_generation:
                dates.update(cursor.change_set.changed_metric_dates)
                if cursor.generation == after_generation + 1:
                    break
                if cursor.parent_snapshot is None:
                    dates.update(self._available_dates(root))
                    break
                try:
                    cursor = self._snapshots.load_root_snapshot(cursor.parent_snapshot)
                except ObjectNotFound:
                    # A pruned audit chain must degrade to a full current-state
                    # reconciliation instead of silently dropping dates.
                    dates.update(self._available_dates(root))
                    break
        return tuple(self._daily_view(root, metric_date) for metric_date in sorted(dates))

    def for_generation(self, generation: int) -> Sequence[StepSample]:
        root = self._root_for_generation(generation)
        available = self._available_dates(root)
        commit_date = root.change_set.created_at.astimezone(ZoneInfo(root.state.time_zone)).date()
        last = max(available[-1], commit_date) if available else commit_date
        target_dates = tuple(
            date.fromordinal(last.toordinal() - offset) for offset in range(6, -1, -1)
        )
        return tuple(
            StepSample(
                metric_date=metric_date,
                steps=self._step_count(root, metric_date),
            )
            for metric_date in target_dates
        )

    def observations_for_date(
        self,
        root: RootSnapshot,
        metric_date: date,
    ) -> tuple[Mapping[str, Any], ...]:
        return tuple(
            row
            for row in self._daily_rows(root, metric_date)
            if row.get("period_kind") == "DAY" and row.get("statistic") == "observed"
        )

    def _root_for_generation(self, generation: int) -> RootSnapshot:
        current = self._heads.read().head
        if current is None:
            raise ValueError("no committed HEAD exists")
        if generation < 1 or generation > current.generation:
            raise ValueError("requested generation is outside the HEAD chain")
        root = self._snapshots.load_root_snapshot(current.root_snapshot)
        while root.generation > generation:
            if root.parent_snapshot is None:
                raise ValueError("Snapshot chain ended before requested generation")
            root = self._snapshots.load_root_snapshot(root.parent_snapshot)
        if root.generation != generation:
            raise ValueError("Snapshot chain does not contain requested generation")
        return root

    def _available_dates(self, root: RootSnapshot) -> tuple[date, ...]:
        result: list[date] = []
        for reference in root.state.daily_year_shards.values():
            shard = self._snapshots.load_year_shard(reference)
            result.extend(date.fromisoformat(value) for value in shard.daily_revisions)
        return tuple(sorted(result))

    def _daily_rows(
        self,
        root: RootSnapshot,
        metric_date: date,
    ) -> tuple[Mapping[str, Any], ...]:
        shard_reference = root.state.daily_year_shards.get(f"{metric_date.year:04d}")
        if shard_reference is None:
            return ()
        shard = self._snapshots.load_year_shard(shard_reference)
        revision = shard.daily_revisions.get(metric_date.isoformat())
        if revision is None:
            return ()
        payload = self._snapshots.read_daily_revision(revision)
        rows: list[Mapping[str, Any]] = []
        for line in payload.splitlines():
            value = json.loads(line)
            if not isinstance(value, Mapping):
                raise TypeError("daily revision row is not an object")
            rows.append(cast(Mapping[str, Any], value))
        return tuple(rows)

    def _daily_view(self, root: RootSnapshot, metric_date: date) -> DailyView:
        rows = self._daily_rows(root, metric_date)
        observed = {
            str(row["metric_name"]): row
            for row in rows
            if row.get("statistic") == "observed" and row.get("period_kind") == "DAY"
        }
        weekly = {
            str(row["metric_name"]): row
            for row in rows
            if row.get("statistic") == "mean" and row.get("period_kind") == "CALENDAR_WEEK"
        }
        monthly = {
            str(row["metric_name"]): row
            for row in rows
            if row.get("statistic") == "mean" and row.get("period_kind") == "CALENDAR_MONTH"
        }
        metrics = tuple(
            DailyMetric(
                metric_name=name,
                statistic="value",
                value=_optional_number(row.get("value_num")),
                unit=_optional_string(row.get("unit")),
                status=str(row.get("data_status", "UNKNOWN")),
                weekly_average=_optional_number(weekly.get(name, {}).get("value_num")),
                monthly_average=_optional_number(monthly.get(name, {}).get("value_num")),
                coverage=_optional_number(row.get("coverage_ratio")),
                weekly_coverage=_optional_number(weekly.get(name, {}).get("coverage_ratio")),
                monthly_coverage=_optional_number(monthly.get(name, {}).get("coverage_ratio")),
                quality_flags=tuple(str(value) for value in row.get("quality_flags", ())),
            )
            for name, row in sorted(observed.items())
        )
        statuses = {metric.status for metric in metrics}
        data_status = (
            next(iter(statuses)) if len(statuses) == 1 else ("MISSING" if not statuses else "MIXED")
        )
        return DailyView(
            metric_date=metric_date,
            source_generation=root.generation,
            data_status=data_status,
            fitbit_last_sync_time=root.state.fitbit_last_sync_time,
            metrics=metrics,
        )

    def _step_count(self, root: RootSnapshot, metric_date: date) -> int | None:
        for row in self._daily_rows(root, metric_date):
            if (
                row.get("metric_name") == "steps"
                and row.get("statistic") == "observed"
                and row.get("period_kind") == "DAY"
            ):
                value = _optional_number(row.get("value_num"))
                return None if value is None else round(value)
        return None


def _date_properties(metric_date: date) -> dict[str, Any]:
    return {
        DATE_PROPERTY: {"title": [{"type": "text", "text": {"content": metric_date.isoformat()}}]}
    }


def _page_properties(view: DailyView, content_sha256: str) -> dict[str, Any]:
    properties: dict[str, Any] = {
        **_date_properties(view.metric_date),
        GENERATION_PROPERTY: {"number": view.source_generation},
        HASH_PROPERTY: {"rich_text": [{"type": "text", "text": {"content": content_sha256}}]},
        STATUS_PROPERTY: {"select": {"name": view.data_status}},
    }
    properties[SYNC_TIME_PROPERTY] = {
        "date": (
            None
            if view.fitbit_last_sync_time is None
            else {"start": view.fitbit_last_sync_time.isoformat()}
        )
    }
    return properties


def _body_blocks(body: str) -> list[Mapping[str, Any]]:
    chunks = [
        body[index : index + MAX_RICH_TEXT_CHARS]
        for index in range(0, len(body), MAX_RICH_TEXT_CHARS)
    ]
    return [
        {
            "object": "block",
            "type": "code",
            "code": {
                "language": "markdown",
                "rich_text": [{"type": "text", "text": {"content": chunk}}],
            },
        }
        for chunk in chunks
    ]


def _required_string(value: Mapping[str, Any], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise NotionApiError(f"Notion response has no {field}")
    return result


def _number_property(properties: Mapping[str, Any], name: str) -> int:
    raw = properties.get(name)
    if not isinstance(raw, Mapping):
        return 0
    value = raw.get("number")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _rich_text_property(properties: Mapping[str, Any], name: str) -> str:
    raw = properties.get(name)
    if not isinstance(raw, Mapping):
        return ""
    values = raw.get("rich_text")
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        return ""
    return "".join(
        str(value.get("plain_text", "")) for value in values if isinstance(value, Mapping)
    )


def _optional_number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return cast(int | float, value)


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) else None
