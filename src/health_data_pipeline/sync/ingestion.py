"""Google Health fetch, immutable raw publication, and ingestion manifests."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

from pydantic import JsonValue

from health_data_pipeline.health.daily import Observation, SourceQueryStatus
from health_data_pipeline.health.normalization import (
    data_point_metric_date,
    normalize_daily_observations,
)
from health_data_pipeline.health.sleep import (
    SleepSession,
    normalize_sleep_sessions,
    reconcile_sleep_points,
    resolve_sleep_data,
)
from health_data_pipeline.health.sources import data_type_spec
from health_data_pipeline.state.canonical import (
    canonical_json_bytes,
    canonical_ndjson_bytes,
    content_addressed_object_name,
    sha256_hex,
)
from health_data_pipeline.state.models import ContentReference
from health_data_pipeline.state.objects import ContentAddressedStore
from health_data_pipeline.sync.contracts import DataPointPage, FetchWindow


class HealthPageSource(Protocol):
    def iter_data_point_pages(
        self,
        data_type: str,
        window: FetchWindow,
    ) -> Iterable[DataPointPage]: ...


@dataclass(frozen=True, slots=True)
class IngestedQuery:
    data_type: str
    metric_date: date
    data_points: tuple[Mapping[str, Any], ...]
    manifest: ContentReference
    replacement_scope: Mapping[str, Any]
    source_status: SourceQueryStatus


class SourceIngestor:
    """Persist one exact replacement interval before returning normalized source rows."""

    def __init__(
        self,
        *,
        health: HealthPageSource,
        content: ContentAddressedStore,
        daily_schema_version: int,
        normalizer_version: str,
    ) -> None:
        self._health = health
        self._content = content
        self._daily_schema_version = daily_schema_version
        self._normalizer_version = normalizer_version

    def ingest_dates(
        self,
        data_type: str,
        dates: Sequence[date],
        *,
        zone: ZoneInfo,
        expected_generation: int,
        heartbeat: Callable[[], None] | None = None,
    ) -> tuple[IngestedQuery, ...]:
        ordered_dates = tuple(sorted(set(dates)))
        if not ordered_dates:
            raise ValueError("ingestion dates must not be empty")
        if ordered_dates != tuple(_inclusive_dates(ordered_dates[0], ordered_dates[-1])):
            raise ValueError("ingestion dates must form one contiguous civil interval")

        spec = data_type_spec(data_type)
        first_date = ordered_dates[0]
        civil_end = ordered_dates[-1] + timedelta(days=1)
        start = datetime.combine(first_date, time.min, tzinfo=zone).astimezone(UTC)
        end = datetime.combine(civil_end, time.min, tzinfo=zone).astimezone(UTC)
        if end - start > spec.max_window:
            raise ValueError("ingestion interval exceeds the data type API window")
        pages: list[DataPointPage] = []
        raw_references: list[ContentReference] = []
        for page in self._health.iter_data_point_pages(
            data_type,
            FetchWindow(
                start=start,
                end=end,
                civil_start=first_date,
                civil_end=civil_end,
            ),
        ):
            pages.append(page)
            raw_references.append(self._publish_api_page(page))
            if heartbeat is not None:
                heartbeat()
        source_points = tuple(point for page in pages for point in page.data_points)
        points, duplicate_source_count = deduplicate_data_points(data_type, source_points)
        sleep_resolution = resolve_sleep_data(points) if data_type == "sleep" else None
        normalized = normalize_daily_observations(points, sleep_resolution=sleep_resolution)
        normalized_sleep_sessions = (
            normalize_sleep_sessions(points, resolution=sleep_resolution)
            if sleep_resolution is not None
            else {}
        )
        sleep_session_count = sum(len(sessions) for sessions in normalized_sleep_sessions.values())
        sleep_source_count = sum(1 for point in points if isinstance(point.get("sleep"), Mapping))
        daily_observation_count = sum(len(values) for values in normalized.values())
        points_by_date, source_statuses = partition_ingested_points(
            data_type,
            ordered_dates,
            points,
            normalized,
            normalized_sleep_sessions,
        )
        flattened_reference = self._publish_flattened_source(
            data_type,
            spec.record_kind,
            points,
        )
        replacement_kind = _replacement_kind(spec.record_kind)
        replacement_scope, raw_replacement_scope = _replacement_scopes(
            data_type=data_type,
            replacement_kind=replacement_kind,
            ordered_dates=ordered_dates,
            physical_start=start,
            physical_end=end,
        )
        manifest_payload = {
            "schema_version": 3,
            "data_type": data_type,
            "query_window": {
                "physical_start": start,
                "physical_end": end,
                "civil_start": first_date,
                "civil_end": civil_end,
                "time_zone": zone.key,
            },
            "replacement_scope": replacement_scope,
            "page_count": len(pages),
            "api_pages": raw_references,
            "flattened_source": flattened_reference,
            "source_stream_row_count": len(source_points),
            "flattened_source_row_count": len(points),
            "duplicate_source_row_count": duplicate_source_count,
            "daily_observation_count": daily_observation_count,
            "schema_version_daily": self._daily_schema_version,
            "normalizer_version": self._normalizer_version,
            "sleep_session_source_count": sleep_source_count,
            "sleep_session_materialized_count": sleep_session_count,
            "sleep_session_unmaterialized_count": max(0, sleep_source_count - sleep_session_count),
            "source_query_statuses": {
                metric_date.isoformat(): {
                    "status": status.status,
                    "source_count": status.source_count,
                    "normalized_count": status.normalized_count,
                }
                for metric_date, status in sorted(source_statuses.items())
            },
            "unassigned_source_row_count": len(points)
            - sum(len(day_points) for day_points in points_by_date.values()),
            "expected_head_generation": expected_generation,
            "normalization_validation_result": "VALID",
        }
        canonical = canonical_json_bytes(manifest_payload)
        digest = sha256_hex(canonical)
        object_name = content_addressed_object_name(
            f"manifests/ingestion/date={first_date.isoformat()}",
            digest,
            suffix=".json.gz",
        )
        published = self._content.publish(
            object_name,
            canonical,
            logical_content_type="application/json",
        )
        return tuple(
            IngestedQuery(
                data_type=data_type,
                metric_date=metric_date,
                data_points=tuple(points_by_date.get(metric_date, ())),
                manifest=published.reference,
                replacement_scope=raw_replacement_scope,
                source_status=source_statuses[metric_date],
            )
            for metric_date in ordered_dates
        )

    def _publish_api_page(self, page: DataPointPage) -> ContentReference:
        canonical = canonical_json_bytes(_canonical_safe(page.raw_payload))
        digest = sha256_hex(canonical)
        object_name = content_addressed_object_name(
            "blobs/api-pages",
            digest,
            suffix=".json.gz",
        )
        return self._content.publish(
            object_name,
            canonical,
            logical_content_type="application/json",
        ).reference

    def _publish_flattened_source(
        self,
        data_type: str,
        record_kind: str,
        points: Sequence[Mapping[str, Any]],
    ) -> ContentReference:
        canonical = canonical_ndjson_bytes(_canonical_safe(point) for point in points)
        digest = sha256_hex(canonical)
        object_name = content_addressed_object_name(
            "blobs/flattened",
            digest,
            suffix=".ndjson.gz",
            partitions=(
                f"record_kind={record_kind.upper()}",
                f"data_type={data_type}",
            ),
        )
        return self._content.publish(
            object_name,
            canonical,
            logical_content_type=(
                f"application/x-ndjson; layer=flattened-source; data-type={data_type}"
            ),
        ).reference


def deduplicate_data_points(
    data_type: str,
    points: Sequence[Mapping[str, Any]],
) -> tuple[tuple[Mapping[str, Any], ...], int]:
    """Deduplicate source rows without inventing a sample revision order.

    Reconciled sample data can legitimately reuse ``dataPointName`` for
    distinct observations.  Rows at different sample times are therefore
    retained, while a named sample at the same time with conflicting payloads
    remains fail-closed.  Other record kinds retain their strict identity
    checks so ambiguous anonymous rows cannot be silently dropped or
    double-counted, while sleep uses its explicit update timestamp to resolve
    revisions.
    """

    if data_type == "sleep":
        reconciled = reconcile_sleep_points(points)
        return reconciled, len(points) - len(reconciled)
    sample_kind = data_type_spec(data_type).record_kind == "sample"
    sample_signatures: set[bytes] = set()
    sample_identity_signatures: dict[tuple[str, bytes], bytes] = {}
    named_signatures: dict[str, bytes] = {}
    anonymous_signatures: set[bytes] = set()
    selected: list[Mapping[str, Any]] = []
    duplicate_count = 0
    for point in points:
        signature = canonical_json_bytes(_canonical_safe(point))
        raw_name = point.get("dataPointName")
        if sample_kind:
            if signature in sample_signatures:
                duplicate_count += 1
                continue
            sample_signatures.add(signature)
            sample_name = raw_name if isinstance(raw_name, str) and raw_name else ""
            identity = (sample_name, _sample_time_signature(point))
            previous = sample_identity_signatures.get(identity)
            if previous is not None:
                raise ValueError("reconciled sample identity has conflicting payloads")
            sample_identity_signatures[identity] = signature
        elif isinstance(raw_name, str) and raw_name:
            previous = named_signatures.get(raw_name)
            if previous is not None:
                if previous == signature:
                    duplicate_count += 1
                    continue
                raise ValueError("reconciled data point identity has conflicting payloads")
            named_signatures[raw_name] = signature
        elif signature in anonymous_signatures:
            raise ValueError("anonymous reconciled data point has an ambiguous duplicate")
        else:
            anonymous_signatures.add(signature)
        selected.append(point)
    return tuple(selected), duplicate_count


def _sample_time_signature(point: Mapping[str, Any]) -> bytes:
    sample_times = [
        payload["sampleTime"]
        for key, payload in point.items()
        if key != "dataPointName"
        and isinstance(payload, Mapping)
        and isinstance(payload.get("sampleTime"), Mapping)
    ]
    if len(sample_times) != 1:
        raise ValueError("reconciled sample must contain exactly one sampleTime")
    sample_time = sample_times[0]
    physical_time = sample_time.get("physicalTime")
    if isinstance(physical_time, str) and physical_time:
        return canonical_json_bytes(physical_time)
    return canonical_json_bytes(_canonical_safe(sample_time))


def partition_ingested_points(
    data_type: str,
    ordered_dates: Sequence[date],
    points: Sequence[Mapping[str, Any]],
    normalized: Mapping[date, Sequence[Observation]],
    sleep_sessions: Mapping[date, Sequence[SleepSession]],
) -> tuple[dict[date, list[Mapping[str, Any]]], dict[date, SourceQueryStatus]]:
    """Bind source rows and query outcomes to the exact replacement days."""

    requested_dates = set(ordered_dates)
    if (set(normalized) | set(sleep_sessions)).difference(requested_dates):
        raise ValueError("normalized observation falls outside replacement scope")
    dated_points = tuple((point, data_point_metric_date(point)) for point in points)
    if {point_date for _, point_date in dated_points if point_date is not None}.difference(
        requested_dates
    ):
        raise ValueError("source data point falls outside replacement scope")

    points_by_date: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
    if len(ordered_dates) == 1:
        points_by_date[ordered_dates[0]].extend(points)
    else:
        for point, point_date in dated_points:
            if point_date is not None:
                points_by_date[point_date].append(point)

    statuses: dict[date, SourceQueryStatus] = {}
    for metric_date in ordered_dates:
        day_points = tuple(points_by_date.get(metric_date, ()))
        normalized_count = len(normalized.get(metric_date, ())) + len(
            sleep_sessions.get(metric_date, ())
        )
        statuses[metric_date] = SourceQueryStatus(
            data_type=data_type,
            status=_source_query_result(
                data_type,
                day_points,
                normalized_count=normalized_count,
            ),
            source_count=len(day_points),
            normalized_count=normalized_count,
        )
    return points_by_date, statuses


def unique_manifest_references(
    references: Iterable[ContentReference],
) -> tuple[ContentReference, ...]:
    by_uri: dict[str, ContentReference] = {}
    for reference in references:
        existing = by_uri.get(reference.uri)
        if existing is not None and existing != reference:
            raise ValueError(f"manifest URI has conflicting integrity metadata: {reference.uri}")
        by_uri[reference.uri] = reference
    return tuple(by_uri[uri] for uri in sorted(by_uri))


def unique_raw_replacements(
    replacements: Iterable[Mapping[str, Any]],
) -> tuple[JsonValue, ...]:
    by_canonical: dict[bytes, JsonValue] = {}
    for replacement in replacements:
        canonical = canonical_json_bytes(replacement)
        by_canonical[canonical] = cast(JsonValue, dict(replacement))
    return tuple(by_canonical[key] for key in sorted(by_canonical))


def _replacement_scopes(
    *,
    data_type: str,
    replacement_kind: str,
    ordered_dates: Sequence[date],
    physical_start: datetime,
    physical_end: datetime,
) -> tuple[dict[str, object], Mapping[str, Any]]:
    first_date = ordered_dates[0]
    civil_end = ordered_dates[-1] + timedelta(days=1)
    replacement_scope: dict[str, object] = {"kind": replacement_kind}
    if replacement_kind == "PHYSICAL_WINDOW_REPLACE":
        replacement_scope.update({"physical_start": physical_start, "physical_end": physical_end})
        raw: Mapping[str, Any] = {
            "data_type": data_type,
            "kind": replacement_kind,
            "physical_start": physical_start.isoformat().replace("+00:00", "Z"),
            "physical_end": physical_end.isoformat().replace("+00:00", "Z"),
        }
    elif len(ordered_dates) == 1:
        replacement_scope["metric_date"] = first_date
        raw = {
            "data_type": data_type,
            "kind": replacement_kind,
            "metric_date": first_date.isoformat(),
        }
    else:
        replacement_scope.update({"civil_start": first_date, "civil_end": civil_end})
        raw = {
            "data_type": data_type,
            "kind": replacement_kind,
            "civil_start": first_date.isoformat(),
            "civil_end": civil_end.isoformat(),
        }
    return replacement_scope, raw


def _replacement_kind(record_kind: str) -> str:
    if record_kind == "daily":
        return "CIVIL_DAY_REPLACE"
    if record_kind == "session":
        return "SESSION_DAY_REPLACE"
    return "PHYSICAL_WINDOW_REPLACE"


def _canonical_safe(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return cast(
        Mapping[str, Any],
        json.loads(
            json.dumps(value, ensure_ascii=False, allow_nan=False),
            parse_float=Decimal,
        ),
    )


def _source_query_result(
    data_type: str,
    points: Sequence[Mapping[str, Any]],
    *,
    normalized_count: int,
) -> str:
    if not points:
        return "SUCCESS_EMPTY"
    if data_type == "sleep":
        processed_values = []
        for point in points:
            sleep = point.get("sleep")
            metadata = sleep.get("metadata") if isinstance(sleep, Mapping) else None
            if isinstance(metadata, Mapping) and isinstance(metadata.get("processed"), bool):
                processed_values.append(metadata["processed"])
        if processed_values and not all(processed_values):
            return "SOURCE_PROCESSING"
    if normalized_count == 0:
        return "INVALID_SOURCE"
    return "SUCCESS_WITH_DATA"


def _inclusive_dates(start: date, end: date) -> Iterable[date]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)
