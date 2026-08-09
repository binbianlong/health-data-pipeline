"""Compatibility facade for Notion and snapshot-view adapters."""

from collections.abc import Mapping
from typing import Any

from health_data_pipeline.adapters.notion.gateway import (
    DATA_STATUS_OPTIONS,
    DATE_PROPERTY,
    DEFAULT_RETRY_AFTER_SECONDS,
    GENERATION_PROPERTY,
    HASH_PROPERTY,
    HTTP_NO_CONTENT,
    HTTP_SUCCESS_MAX_EXCLUSIVE,
    HTTP_SUCCESS_MIN,
    HTTP_TOO_MANY_REQUESTS,
    MAX_RETRY_AFTER_SECONDS,
    MAX_RICH_TEXT_CHARS,
    METRIC_DATE_PROPERTY,
    NOTION_REQUEST_ATTEMPTS,
    NOTION_VERSION,
    STATUS_PROPERTY,
    SYNC_TIME_PROPERTY,
    NotionApiError,
    NotionDataSourceGateway,
)
from health_data_pipeline.adapters.notion.gateway import _body_blocks as _gateway_body_blocks
from health_data_pipeline.presentation.snapshot_views import SnapshotViewSource


def _body_blocks(body: str) -> list[Mapping[str, Any]]:
    """Compatibility wrapper for the former private helper."""

    return _gateway_body_blocks(body)


__all__ = [
    "DATA_STATUS_OPTIONS",
    "DATE_PROPERTY",
    "DEFAULT_RETRY_AFTER_SECONDS",
    "GENERATION_PROPERTY",
    "HASH_PROPERTY",
    "HTTP_NO_CONTENT",
    "HTTP_SUCCESS_MAX_EXCLUSIVE",
    "HTTP_SUCCESS_MIN",
    "HTTP_TOO_MANY_REQUESTS",
    "MAX_RETRY_AFTER_SECONDS",
    "MAX_RICH_TEXT_CHARS",
    "METRIC_DATE_PROPERTY",
    "NOTION_REQUEST_ATTEMPTS",
    "NOTION_VERSION",
    "STATUS_PROPERTY",
    "SYNC_TIME_PROPERTY",
    "NotionApiError",
    "NotionDataSourceGateway",
    "SnapshotViewSource",
]
