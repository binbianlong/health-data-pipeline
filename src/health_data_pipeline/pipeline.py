"""Incremental Google Health ingestion and one-shot HEAD commit pipeline."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import JsonValue

from health_data_pipeline.budget import (
    class_a_backfill_allowed,
    class_a_normalizer_upgrade_allowed,
)
from health_data_pipeline.canonical import (
    canonical_json_bytes,
    canonical_ndjson_bytes,
    content_addressed_object_name,
    sha256_hex,
)
from health_data_pipeline.commit import CommitCoordinator, CommitRequest
from health_data_pipeline.daily import (
    DayContext,
    Observation,
    affected_metric_dates,
    build_daily_rows,
)
from health_data_pipeline.google_health import (
    DataPointPage,
    FetchWindow,
    data_type_spec,
)
from health_data_pipeline.job import SyncOutcome
from health_data_pipeline.lease import LeaseRepository
from health_data_pipeline.models import CommitKind, ContentReference, RootSnapshot
from health_data_pipeline.normalization import normalize_daily_observations
from health_data_pipeline.snapshot import SnapshotRepository
from health_data_pipeline.storage import (
    ContentAddressedStore,
    HeadRepository,
    ObjectStore,
    PreconditionFailed,
)

SUPPORTED_DATA_TYPES = (
    "steps",
    "distance",
    "active-zone-minutes",
    "heart-rate",
    "daily-resting-heart-rate",
    "heart-rate-variability",
    "daily-heart-rate-variability",
    "oxygen-saturation",
    "daily-oxygen-saturation",
    "sleep",
    "respiratory-rate-sleep-summary",
    "exercise",
)

_METRICS_BY_DATA_TYPE: Mapping[str, frozenset[str]] = {
    "steps": frozenset({"steps"}),
    "distance": frozenset({"distance"}),
    "active-zone-minutes": frozenset({"active_zone_minutes"}),
    "heart-rate": frozenset({"heart_rate"}),
    "daily-resting-heart-rate": frozenset({"resting_heart_rate"}),
    "heart-rate-variability": frozenset({"hrv_rmssd"}),
    "daily-heart-rate-variability": frozenset({"hrv_rmssd"}),
    "oxygen-saturation": frozenset({"spo2"}),
    "daily-oxygen-saturation": frozenset({"spo2"}),
    "sleep": frozenset(
        {
            "sleep_minutes",
            "sleep_rem_minutes",
            "sleep_deep_minutes",
            "sleep_light_minutes",
            "sleep_awake_minutes",
        }
    ),
    "respiratory-rate-sleep-summary": frozenset({"sleep_respiratory_rate"}),
    "exercise": frozenset({"exercise_minutes"}),
}
_BACKFILL_BATCH_DAYS = 31
_LEGACY_BACKFILL_BATCH_DAYS = 14
_BACKFILL_EARLIEST_DATE = date(2009, 1, 1)
_BACKFILL_HIGH_FREQUENCY_DAYS = 90
_RECONCILIATION_MAX_DAYS = 90
_BACKFILL_TIME_ZONE = ZoneInfo("Asia/Tokyo")
_BACKFILL_META_KEY = "_schedule"
_BACKFILL_POLICY_VERSION = 2
_NORMALIZER_VERSION = "daily-v3"
_PRIORITY_BACKFILL_DATA_TYPES = ("sleep", "exercise")
_HIGH_FREQUENCY_DATA_TYPES = frozenset(
    {
        "heart-rate",
        "heart-rate-variability",
        "oxygen-saturation",
    }
)


@dataclass(frozen=True, slots=True)
class PendingWebhook:
    object_name: str
    data_type: str
    dates: tuple[date, ...]


@dataclass(frozen=True, slots=True)
class IngestedQuery:
    data_type: str
    metric_date: date
    data_points: tuple[Mapping[str, Any], ...]
    manifest: ContentReference
    replacement_scope: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class BackfillChunk:
    data_type: str
    dates: tuple[date, ...]
    floor: date
    next_data_type_index: int | None = None


class SyncAlert(Protocol):
    def evaluate(
        self,
        *,
        last_sync_time: datetime | None,
        now: datetime,
    ) -> bool: ...


class HealthApi(Protocol):
    def get_settings(self) -> Mapping[str, Any]: ...

    def latest_fitbit_sync_time(self) -> datetime | None: ...

    def iter_data_point_pages(
        self,
        data_type: str,
        window: FetchWindow,
    ) -> Iterable[DataPointPage]: ...


class IncrementalSyncPipeline:
    """Prefer pending webhook intervals and reconcile after Fitbit sync advances."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        content: ContentAddressedStore,
        heads: HeadRepository,
        snapshots: SnapshotRepository,
        commits: CommitCoordinator,
        health: HealthApi,
        owner_hmac: str,
        default_time_zone: str = "Asia/Tokyo",
        sync_alert: SyncAlert | None = None,
        backfill_ready: Callable[[int], bool] | None = None,
        now: Any = None,
    ) -> None:
        self._store = store
        self._content = content
        self._heads = heads
        self._snapshots = snapshots
        self._commits = commits
        self._health = health
        self._owner_hmac = owner_hmac
        self._default_time_zone = default_time_zone
        self._sync_alert = sync_alert
        self._backfill_ready = backfill_ready or (lambda _generation: True)
        self._now = now or (lambda: datetime.now(UTC))
        self._leases = LeaseRepository(store, owner_hmac=owner_hmac)

    def run(self) -> SyncOutcome:
        now = _aware_utc(self._now())
        lease = self._leases.acquire(now=now)
        if lease is None:
            return SyncOutcome(
                committed=False,
                generation=self._current_generation(),
                owner_hmac=self._owner_hmac,
            )
        try:
            return self._run_with_lease(now)
        finally:
            self._leases.release(lease)

    def has_pending_webhooks(self) -> bool:
        """Return whether a durable webhook is not represented by current HEAD."""

        base = self._heads.read()
        root = (
            None
            if base.head is None
            else self._snapshots.load_root_snapshot(base.head.root_snapshot)
        )
        processed: set[str] = set()
        if root is not None:
            raw_processed = root.state.webhook_cursor.get("processed_objects", [])
            if isinstance(raw_processed, list):
                processed = {value for value in raw_processed if isinstance(value, str)}
        return any(name not in processed for name in self._store.list_names("webhook-events/"))

    def _run_with_lease(self, now: datetime) -> SyncOutcome:
        base = self._heads.read()
        root = (
            None
            if base.head is None
            else self._snapshots.load_root_snapshot(base.head.root_snapshot)
        )
        settings = self._health.get_settings()
        time_zone = _time_zone(settings, self._default_time_zone)
        zone = ZoneInfo(time_zone)
        today = now.astimezone(zone).date()
        fitbit_last_sync = self._health.latest_fitbit_sync_time()
        if self._sync_alert is not None:
            self._sync_alert.evaluate(last_sync_time=fitbit_last_sync, now=now)
        pending, event_names = self._pending_webhooks(root, zone=zone)
        sync_advanced = fitbit_last_sync is not None and (
            root is None
            or root.state.fitbit_last_sync_time is None
            or fitbit_last_sync > root.state.fitbit_last_sync_time
        )
        backfill_day = now.astimezone(_BACKFILL_TIME_ZONE).date()
        idle_root = root if not pending and not sync_advanced else None
        backfill_chunks = (
            _select_backfill_chunks(
                idle_root.state.backfill_cursors,
                today=backfill_day,
            )
            if (
                idle_root is not None
                and self._backfill_ready(idle_root.generation)
                and _backfill_budget_allows(
                    self._store,
                    idle_root.state.backfill_cursors,
                    now=now,
                )
            )
            else ()
        )
        if idle_root is not None and not backfill_chunks:
            return SyncOutcome(
                committed=False,
                generation=idle_root.generation,
                owner_hmac=self._owner_hmac,
            )

        fetch_dates, reconciled_data_types, truncated_reconciliation = _plan_fetch_dates(
            root,
            pending,
            sync_advanced=sync_advanced,
            today=today,
            zone=zone,
            backfill_chunks=backfill_chunks,
        )

        expected_generation = 1 if root is None else root.generation + 1
        ingested: list[IngestedQuery] = []
        if backfill_chunks:
            for chunk in backfill_chunks:
                ingested.extend(
                    self._ingest_backfill_chunk(
                        chunk,
                        zone=zone,
                        expected_generation=expected_generation,
                    )
                )
        else:
            for data_type in SUPPORTED_DATA_TYPES:
                for metric_date in sorted(fetch_dates.get(data_type, ())):
                    ingested.extend(
                        self._ingest_dates(
                            data_type,
                            (metric_date,),
                            zone=zone,
                            expected_generation=expected_generation,
                        )
                    )

        changed_dates = {query.metric_date for query in ingested}
        changed_dates.update(
            point_date
            for query in ingested
            for point_date in normalize_daily_observations(query.data_points)
        )
        if not changed_dates:
            changed_dates.add(today)
        current_observations = self._merge_changed_observations(root, ingested)
        targets = affected_metric_dates(changed_dates, today=today)
        daily_rows = self._recalculate_rows(
            root,
            targets,
            current_observations,
            zone=zone,
            now=now,
            fitbit_last_sync=fitbit_last_sync,
        )
        cursors = dict(root.state.data_type_cursors) if root is not None else {}
        cursor_time = now.isoformat().replace("+00:00", "Z")
        if not backfill_chunks:
            for data_type in reconciled_data_types:
                cursors[data_type] = cast(
                    JsonValue,
                    {
                        "reconciled_through_date": today.isoformat(),
                        "reconciled_at": cursor_time,
                    },
                )
        webhook_cursor = (
            dict(root.state.webhook_cursor)
            if backfill_chunks and root is not None
            else {"processed_objects": cast(JsonValue, event_names)}
        )
        backfill_cursors = _next_backfill_cursors(
            root,
            backfill_chunks,
            truncated_reconciliation,
            completed_on=backfill_day,
        )
        commit_kind = (
            CommitKind.BACKFILL
            if backfill_chunks
            else (CommitKind.WEBHOOK if pending else CommitKind.SCHEDULED)
        )
        committed_sync_time = (
            root.state.fitbit_last_sync_time
            if backfill_chunks and root is not None
            else fitbit_last_sync
        )
        try:
            committed = self._commits.commit(
                CommitRequest(
                    daily_rows=daily_rows,
                    commit_kind=commit_kind,
                    created_at=now,
                    data_type_cursors=cursors,
                    webhook_cursor=webhook_cursor,
                    backfill_cursors=backfill_cursors,
                    fitbit_last_sync_time=committed_sync_time,
                    time_zone=time_zone,
                    raw_replacements=_unique_raw_replacements(
                        item.replacement_scope for item in ingested
                    ),
                    ingestion_manifests=_unique_manifest_references(
                        item.manifest for item in ingested
                    ),
                ),
                expected_base=base,
            )
        except PreconditionFailed:
            return SyncOutcome(
                committed=False,
                generation=self._current_generation(),
                owner_hmac=self._owner_hmac,
            )
        return SyncOutcome(
            committed=True,
            generation=committed.root_snapshot.generation,
            owner_hmac=self._owner_hmac,
        )

    def _ingest_backfill_chunk(
        self,
        chunk: BackfillChunk,
        *,
        zone: ZoneInfo,
        expected_generation: int,
    ) -> tuple[IngestedQuery, ...]:
        """Range-fetch priority sessions; keep legacy high-volume work daily."""

        if chunk.next_data_type_index is None:
            return self._ingest_dates(
                chunk.data_type,
                chunk.dates,
                zone=zone,
                expected_generation=expected_generation,
            )

        ingested: list[IngestedQuery] = []
        for metric_date in chunk.dates:
            ingested.extend(
                self._ingest_dates(
                    chunk.data_type,
                    (metric_date,),
                    zone=zone,
                    expected_generation=expected_generation,
                )
            )
        return tuple(ingested)

    def _ingest_dates(
        self,
        data_type: str,
        dates: Sequence[date],
        *,
        zone: ZoneInfo,
        expected_generation: int,
    ) -> tuple[IngestedQuery, ...]:
        ordered_dates = tuple(sorted(set(dates)))
        if not ordered_dates:
            raise ValueError("ingestion dates must not be empty")
        if ordered_dates != tuple(_inclusive_dates(ordered_dates[0], ordered_dates[-1])):
            raise ValueError("ingestion dates must form one contiguous civil interval")

        spec = data_type_spec(data_type)
        first_date = ordered_dates[0]
        civil_end = ordered_dates[-1] + timedelta(days=1)
        start = datetime.combine(
            first_date,
            time.min,
            tzinfo=zone,
        ).astimezone(UTC)
        end = datetime.combine(
            civil_end,
            time.min,
            tzinfo=zone,
        ).astimezone(UTC)
        if end - start > spec.max_window:
            raise ValueError("ingestion interval exceeds the data type API window")
        pages = tuple(
            self._health.iter_data_point_pages(
                data_type,
                FetchWindow(
                    start=start,
                    end=end,
                    civil_start=first_date,
                    civil_end=civil_end,
                ),
            )
        )
        raw_references = tuple(self._publish_api_page(page) for page in pages)
        points = tuple(point for page in pages for point in page.data_points)
        normalized = normalize_daily_observations(points)
        normalized_reference = self._publish_normalized_source(
            data_type,
            spec.record_kind,
            points,
        )
        replacement_kind = _replacement_kind(spec.record_kind)
        replacement_scope: dict[str, object] = {"kind": replacement_kind}
        if replacement_kind == "PHYSICAL_WINDOW_REPLACE":
            replacement_scope.update(
                {
                    "physical_start": start,
                    "physical_end": end,
                }
            )
            raw_replacement_scope: Mapping[str, Any] = {
                "data_type": data_type,
                "kind": replacement_kind,
                "physical_start": start.isoformat().replace("+00:00", "Z"),
                "physical_end": end.isoformat().replace("+00:00", "Z"),
            }
        elif len(ordered_dates) == 1:
            replacement_scope["metric_date"] = first_date
            raw_replacement_scope = {
                "data_type": data_type,
                "kind": replacement_kind,
                "metric_date": first_date.isoformat(),
            }
        else:
            replacement_scope.update(
                {
                    "civil_start": first_date,
                    "civil_end": civil_end,
                }
            )
            raw_replacement_scope = {
                "data_type": data_type,
                "kind": replacement_kind,
                "civil_start": first_date.isoformat(),
                "civil_end": civil_end.isoformat(),
            }
        manifest_payload = {
            "schema_version": 1,
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
            "api_pages": list(raw_references),
            "normalized_source": normalized_reference,
            "source_count": len(points),
            "normalized_count": sum(len(values) for values in normalized.values()),
            "deduplicate_count": 0,
            "schema_version_daily": 1,
            "normalizer_version": _NORMALIZER_VERSION,
            "expected_head_generation": expected_generation,
            "validation_result": "VALID",
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
        points_by_date: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
        if len(ordered_dates) == 1:
            points_by_date[first_date].extend(points)
        else:
            requested_dates = set(ordered_dates)
            for point in points:
                for point_date in normalize_daily_observations((point,)):
                    if point_date not in requested_dates:
                        raise ValueError("normalized observation falls outside replacement scope")
                    points_by_date[point_date].append(point)
        return tuple(
            IngestedQuery(
                data_type=data_type,
                metric_date=metric_date,
                data_points=tuple(points_by_date.get(metric_date, ())),
                manifest=published.reference,
                replacement_scope=raw_replacement_scope,
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

    def _publish_normalized_source(
        self,
        data_type: str,
        record_kind: str,
        points: Sequence[Mapping[str, Any]],
    ) -> ContentReference:
        canonical = canonical_ndjson_bytes(
            (_canonical_safe(point) for point in points),
        )
        digest = sha256_hex(canonical)
        object_name = content_addressed_object_name(
            "blobs/normalized",
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
            logical_content_type=f"application/x-ndjson; data-type={data_type}",
        ).reference

    def _pending_webhooks(
        self,
        root: RootSnapshot | None,
        *,
        zone: ZoneInfo,
    ) -> tuple[tuple[PendingWebhook, ...], list[str]]:
        processed = set()
        if root is not None:
            raw_processed = root.state.webhook_cursor.get("processed_objects", [])
            if isinstance(raw_processed, list):
                processed = {value for value in raw_processed if isinstance(value, str)}
        current_names = list(self._store.list_names("webhook-events/"))
        pending: list[PendingWebhook] = []
        for object_name in current_names:
            if object_name in processed:
                continue
            stored = self._store.get(object_name)
            event = _parse_webhook_event(
                object_name,
                stored.data,
                zone=zone,
            )
            if event is not None:
                pending.append(event)
        return tuple(pending), current_names

    def _merge_changed_observations(
        self,
        root: RootSnapshot | None,
        ingested: Sequence[IngestedQuery],
    ) -> dict[date, tuple[Observation, ...]]:
        by_date_and_type: dict[tuple[date, str], list[Observation]] = defaultdict(list)
        for item in ingested:
            normalized = normalize_daily_observations(item.data_points)
            for metric_date, observations in normalized.items():
                by_date_and_type[(metric_date, item.data_type)].extend(observations)

        result: dict[date, tuple[Observation, ...]] = {}
        queried_dates = {item.metric_date for item in ingested}
        queried_dates.update(metric_date for metric_date, _ in by_date_and_type)
        for metric_date in queried_dates:
            existing = {
                observation.metric_name: observation
                for observation in self._observations_for_date(root, metric_date)
            }
            for item in (query for query in ingested if query.metric_date == metric_date):
                for metric_name in _METRICS_BY_DATA_TYPE[item.data_type]:
                    current = existing.get(metric_name)
                    if current is not None and item.data_type in current.source_data_types:
                        existing.pop(metric_name)
            candidates = [
                observation
                for (day, _), observations in by_date_and_type.items()
                if day == metric_date
                for observation in observations
            ]
            for observation in sorted(candidates, key=_observation_priority):
                current = existing.get(observation.metric_name)
                if current is None or _observation_priority(observation) >= _observation_priority(
                    current
                ):
                    existing[observation.metric_name] = observation
            result[metric_date] = tuple(existing[name] for name in sorted(existing))
        return result

    def _recalculate_rows(
        self,
        root: RootSnapshot | None,
        targets: Sequence[date],
        changed: Mapping[date, Sequence[Observation]],
        *,
        zone: ZoneInfo,
        now: datetime,
        fitbit_last_sync: datetime | None,
    ) -> dict[date, Sequence[Mapping[str, object]] | None]:
        if not targets:
            return {}
        earliest = min(
            min(target - timedelta(days=target.weekday()), target.replace(day=1))
            for target in targets
        )
        latest = max(targets)
        history: dict[date, Sequence[Observation]] = {}
        cursor = earliest
        while cursor <= latest:
            if cursor in changed:
                history[cursor] = changed[cursor]
            else:
                history[cursor] = self._observations_for_date(root, cursor)
            cursor += timedelta(days=1)

        rows: dict[date, Sequence[Mapping[str, object]] | None] = {}
        for metric_date in targets:
            observations = tuple(
                replace(
                    observation,
                    data_status=_data_status(
                        metric_date,
                        zone=zone,
                        now=now,
                        fitbit_last_sync=fitbit_last_sync,
                    ),
                )
                for observation in history.get(metric_date, ())
            )
            history[metric_date] = observations
            if not observations:
                if metric_date in changed:
                    rows[metric_date] = None
                continue
            start = datetime.combine(metric_date, time.min, tzinfo=zone).astimezone(UTC)
            end = datetime.combine(
                metric_date + timedelta(days=1),
                time.min,
                tzinfo=zone,
            ).astimezone(UTC)
            context = DayContext(
                metric_date=metric_date,
                fitbit_last_sync_time=fitbit_last_sync or datetime(1970, 1, 1, tzinfo=UTC),
                time_zone=zone.key,
                physical_day_start=start,
                physical_day_end=end,
                logic_version=_NORMALIZER_VERSION,
            )
            rows[metric_date] = build_daily_rows(context, observations, history)
        return rows

    def _observations_for_date(
        self,
        root: RootSnapshot | None,
        metric_date: date,
    ) -> tuple[Observation, ...]:
        if root is None:
            return ()
        shard_reference = root.state.daily_year_shards.get(f"{metric_date.year:04d}")
        if shard_reference is None:
            return ()
        shard = self._snapshots.load_year_shard(shard_reference)
        revision = shard.daily_revisions.get(metric_date.isoformat())
        if revision is None:
            return ()
        payload = self._snapshots.read_daily_revision(revision)
        result: list[Observation] = []
        for line in payload.splitlines():
            row = json.loads(line)
            if (
                not isinstance(row, Mapping)
                or row.get("period_kind") != "DAY"
                or row.get("statistic") != "observed"
            ):
                continue
            value = row.get("value_num")
            result.append(
                Observation(
                    metric_date=metric_date,
                    metric_name=str(row["metric_name"]),
                    value=(None if value is None else Decimal(str(value))),
                    unit=str(row.get("unit", "")),
                    aggregation=str(row.get("aggregation", "daily_value")),
                    data_status=str(row.get("data_status", "CANONICAL_SYNCED")),
                    sample_count=_positive_int(row.get("sample_count"), 1),
                    expected_count=_positive_int(row.get("expected_count"), 1),
                    quality_flags=tuple(str(item) for item in row.get("quality_flags", ())),
                    source_data_types=tuple(str(item) for item in row.get("source_data_types", ())),
                )
            )
        return tuple(result)

    def _current_generation(self) -> int | None:
        current = self._heads.read().head
        return None if current is None else current.generation


def _parse_webhook_event(
    object_name: str,
    data: bytes,
    *,
    zone: ZoneInfo,
) -> PendingWebhook | None:
    try:
        value = json.loads(data)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, Mapping):
        return None
    raw_event = value.get("data", value.get("event", value))
    if not isinstance(raw_event, Mapping):
        return None
    data_type = raw_event.get("dataType")
    if not isinstance(data_type, str):
        return None
    dates: set[date] = set()
    raw_intervals = raw_event.get("intervals", ())
    if isinstance(raw_intervals, Sequence) and not isinstance(
        raw_intervals, (str, bytes, bytearray)
    ):
        for interval in raw_intervals:
            if not isinstance(interval, Mapping):
                continue
            dates.update(_interval_dates(interval, zone=zone))
    return PendingWebhook(
        object_name=object_name,
        data_type=data_type,
        dates=tuple(sorted(dates)),
    )


def _select_backfill_chunk(
    cursors: Mapping[str, JsonValue],
    *,
    today: date,
) -> BackfillChunk | None:
    """Return the first chunk selected by the current multi-type policy."""

    chunks = _select_backfill_chunks(cursors, today=today)
    return chunks[0] if chunks else None


def _backfill_budget_allows(
    store: ObjectStore,
    cursors: Mapping[str, JsonValue],
    *,
    now: datetime,
) -> bool:
    if class_a_backfill_allowed(store, now=now):
        return True
    schedule = cursors.get(_BACKFILL_META_KEY)
    schedule_mapping = schedule if isinstance(schedule, Mapping) else {}
    previous_version = schedule_mapping.get("normalizer_version")
    upgrade_pending = isinstance(previous_version, str) and previous_version != _NORMALIZER_VERSION
    return upgrade_pending and class_a_normalizer_upgrade_allowed(store, now=now)


def _select_backfill_chunks(
    cursors: Mapping[str, JsonValue],
    *,
    today: date,
) -> tuple[BackfillChunk, ...]:
    """Select the same recent-first 31-day interval for sleep and exercise.

    The two session types share one HEAD commit, so one historical day cannot
    become current for sleep without the matching exercise replacement. The
    Class A monthly gate is evaluated separately before this selector runs.
    """

    schedule = cursors.get(_BACKFILL_META_KEY)
    schedule_mapping = schedule if isinstance(schedule, Mapping) else {}
    migration_pending = (
        schedule_mapping.get("policy_version") != _BACKFILL_POLICY_VERSION
        or schedule_mapping.get("normalizer_version") != _NORMALIZER_VERSION
    )
    newest_default = today
    floor = _BACKFILL_EARLIEST_DATE
    next_ends: list[date] = []
    all_complete = True
    for data_type in _PRIORITY_BACKFILL_DATA_TYPES:
        cursor = cursors.get(data_type)
        cursor_mapping = cursor if isinstance(cursor, Mapping) else {}
        complete = cursor_mapping.get("complete") is True
        all_complete = all_complete and complete
        if migration_pending:
            next_end = newest_default
        else:
            next_end = _iso_date(cursor_mapping.get("next_end_date")) or newest_default
        next_end = min(next_end, newest_default)
        if not complete or migration_pending:
            next_ends.append(next_end)
    if all_complete and not migration_pending:
        if schedule_mapping.get("last_completed_civil_date") == today.isoformat():
            return ()
        legacy = _select_legacy_backfill_chunk(
            cursors,
            schedule=schedule_mapping,
            today=today,
        )
        return () if legacy is None else (legacy,)

    # Re-fetching an already-covered partner interval is safe and ensures a
    # migrated or previously divergent cursor never skips either session type.
    common_end = max(next_ends, default=newest_default)
    if common_end < floor:
        return ()
    common_start = max(
        floor,
        common_end - timedelta(days=_BACKFILL_BATCH_DAYS - 1),
    )
    dates = tuple(_inclusive_dates(common_start, common_end))
    return tuple(
        BackfillChunk(
            data_type=data_type,
            dates=dates,
            floor=floor,
        )
        for data_type in _PRIORITY_BACKFILL_DATA_TYPES
    )


def _select_legacy_backfill_chunk(
    cursors: Mapping[str, JsonValue],
    *,
    schedule: Mapping[str, object],
    today: date,
) -> BackfillChunk | None:
    """Continue the former round-robin after priority session history completes."""

    raw_index = schedule.get("next_data_type_index", 0)
    start_index = (
        raw_index
        if isinstance(raw_index, int)
        and not isinstance(raw_index, bool)
        and 0 <= raw_index < len(SUPPORTED_DATA_TYPES)
        else 0
    )
    newest_default = today
    for offset in range(len(SUPPORTED_DATA_TYPES)):
        index = (start_index + offset) % len(SUPPORTED_DATA_TYPES)
        data_type = SUPPORTED_DATA_TYPES[index]
        floor = _backfill_floor(data_type, today=today)
        cursor = cursors.get(data_type)
        cursor_mapping = cursor if isinstance(cursor, Mapping) else {}
        if cursor_mapping.get("complete") is True:
            continue
        next_end = _iso_date(cursor_mapping.get("next_end_date")) or newest_default
        next_end = min(next_end, newest_default)
        if next_end < floor:
            continue
        chunk_start = max(
            floor,
            next_end - timedelta(days=_LEGACY_BACKFILL_BATCH_DAYS - 1),
        )
        return BackfillChunk(
            data_type=data_type,
            dates=tuple(_inclusive_dates(chunk_start, next_end)),
            floor=floor,
            next_data_type_index=(index + 1) % len(SUPPORTED_DATA_TYPES),
        )
    return None


def _advance_backfill_cursors(
    cursors: Mapping[str, JsonValue],
    chunk: BackfillChunk,
    *,
    completed_on: date,
) -> dict[str, JsonValue]:
    return _advance_backfill_cursors_many(
        cursors,
        (chunk,),
        completed_on=completed_on,
    )


def _advance_backfill_cursors_many(
    cursors: Mapping[str, JsonValue],
    chunks: Sequence[BackfillChunk],
    *,
    completed_on: date,
) -> dict[str, JsonValue]:
    if not chunks:
        raise ValueError("backfill cursor advancement requires at least one chunk")
    result = dict(cursors)
    for chunk in chunks:
        oldest = min(chunk.dates)
        complete = oldest <= chunk.floor
        result[chunk.data_type] = cast(
            JsonValue,
            {
                "next_end_date": (oldest - timedelta(days=1)).isoformat(),
                "complete": complete,
            },
        )
    result[_BACKFILL_META_KEY] = cast(
        JsonValue,
        {
            "last_completed_civil_date": completed_on.isoformat(),
            "normalizer_version": _NORMALIZER_VERSION,
            "next_data_type_index": next(
                (
                    chunk.next_data_type_index
                    for chunk in chunks
                    if chunk.next_data_type_index is not None
                ),
                _backfill_schedule_index(cursors),
            ),
            "policy_version": _BACKFILL_POLICY_VERSION,
        },
    )
    return result


def _backfill_schedule_index(cursors: Mapping[str, JsonValue]) -> int:
    raw_schedule = cursors.get(_BACKFILL_META_KEY)
    schedule = raw_schedule if isinstance(raw_schedule, Mapping) else {}
    raw_index = schedule.get("next_data_type_index", 0)
    if (
        isinstance(raw_index, int)
        and not isinstance(raw_index, bool)
        and 0 <= raw_index < len(SUPPORTED_DATA_TYPES)
    ):
        return raw_index
    return 0


def _backfill_floor(data_type: str, *, today: date) -> date:
    if data_type in _HIGH_FREQUENCY_DATA_TYPES:
        return today - timedelta(days=_BACKFILL_HIGH_FREQUENCY_DAYS - 1)
    return _BACKFILL_EARLIEST_DATE


def _plan_fetch_dates(
    root: RootSnapshot | None,
    pending: Sequence[PendingWebhook],
    *,
    sync_advanced: bool,
    today: date,
    zone: ZoneInfo,
    backfill_chunks: Sequence[BackfillChunk],
) -> tuple[dict[str, set[date]], set[str], dict[str, date]]:
    fetch_dates: dict[str, set[date]] = defaultdict(set)
    for event in pending:
        if event.data_type in _METRICS_BY_DATA_TYPE:
            event_dates = event.dates or (today,)
            for data_type in _webhook_replacement_data_types(event.data_type):
                fetch_dates[data_type].update(event_dates)
    reconciled_data_types: set[str] = set()
    truncated: dict[str, date] = {}
    if sync_advanced or root is None:
        for data_type in SUPPORTED_DATA_TYPES:
            requested_start = _reconciliation_start_date(
                root,
                data_type,
                today=today,
                zone=zone,
            )
            fetch_start = _reconciliation_fetch_start(
                data_type,
                requested_start=requested_start,
                today=today,
            )
            fetch_dates[data_type].update(_inclusive_dates(fetch_start, today))
            reconciled_data_types.add(data_type)
            if data_type not in _HIGH_FREQUENCY_DATA_TYPES and requested_start < fetch_start:
                truncated[data_type] = fetch_start - timedelta(days=1)
    elif backfill_chunks:
        for chunk in backfill_chunks:
            fetch_dates[chunk.data_type].update(chunk.dates)
    return fetch_dates, reconciled_data_types, truncated


def _webhook_replacement_data_types(data_type: str) -> tuple[str, ...]:
    """Return every API representation needed to replace webhook metrics safely.

    HRV and SpO2 each have detail and daily-summary data types which normalize to
    one metric. Re-fetching only the notified representation could discard the
    still-current sibling value, so both representations share one replacement
    set for the affected civil dates.
    """

    metrics = _METRICS_BY_DATA_TYPE.get(data_type, frozenset())
    return tuple(
        candidate
        for candidate in SUPPORTED_DATA_TYPES
        if metrics & _METRICS_BY_DATA_TYPE[candidate]
    )


def _reconciliation_start_date(
    root: RootSnapshot | None,
    data_type: str,
    *,
    today: date,
    zone: ZoneInfo,
) -> date:
    """Use the last successful civil cursor, with Fitbit sync as migration fallback."""

    if root is None:
        return today - timedelta(days=1)
    cursor = root.state.data_type_cursors.get(data_type)
    cursor_mapping = cursor if isinstance(cursor, Mapping) else {}
    cursor_date = _iso_date(cursor_mapping.get("reconciled_through_date"))
    if cursor_date is None:
        cursor_date = _timestamp_civil_date(
            cursor_mapping.get("reconciled_at", cursor_mapping.get("reconciled_through")),
            zone=zone,
        )
    if cursor_date is None and root.state.fitbit_last_sync_time is not None:
        cursor_date = root.state.fitbit_last_sync_time.astimezone(zone).date()
    return min(cursor_date or (today - timedelta(days=1)), today)


def _reconciliation_fetch_start(
    data_type: str,
    *,
    requested_start: date,
    today: date,
) -> date:
    """Bound foreground reconciliation while preserving the 90-day detail guarantee."""

    return max(
        min(requested_start, today),
        today - timedelta(days=_RECONCILIATION_MAX_DAYS - 1),
        _backfill_floor(data_type, today=today),
    )


def _reopen_backfill_gaps(
    cursors: Mapping[str, JsonValue],
    gap_ends: Mapping[str, date],
) -> dict[str, JsonValue]:
    """Return truncated non-high-frequency ranges to the normal backfill rotation."""

    result = dict(cursors)
    for data_type, gap_end in gap_ends.items():
        result[data_type] = cast(
            JsonValue,
            {
                "next_end_date": gap_end.isoformat(),
                "complete": False,
            },
        )
    return result


def _next_backfill_cursors(
    root: RootSnapshot | None,
    backfill_chunks: Sequence[BackfillChunk],
    truncated_reconciliation: Mapping[str, date],
    *,
    completed_on: date,
) -> Mapping[str, JsonValue] | None:
    if backfill_chunks and root is not None:
        return _advance_backfill_cursors_many(
            root.state.backfill_cursors,
            backfill_chunks,
            completed_on=completed_on,
        )
    if truncated_reconciliation:
        previous = root.state.backfill_cursors if root is not None else {}
        return _reopen_backfill_gaps(previous, truncated_reconciliation)
    return None


def _inclusive_dates(start: date, end: date) -> Iterable[date]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def _iso_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _timestamp_civil_date(value: Any, *, zone: ZoneInfo) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(zone).date()


def _unique_manifest_references(
    references: Iterable[ContentReference],
) -> tuple[ContentReference, ...]:
    by_uri: dict[str, ContentReference] = {}
    for reference in references:
        existing = by_uri.get(reference.uri)
        if existing is not None and existing != reference:
            raise ValueError(f"manifest URI has conflicting integrity metadata: {reference.uri}")
        by_uri[reference.uri] = reference
    return tuple(by_uri[uri] for uri in sorted(by_uri))


def _unique_raw_replacements(
    replacements: Iterable[Mapping[str, Any]],
) -> tuple[JsonValue, ...]:
    by_canonical: dict[bytes, JsonValue] = {}
    for replacement in replacements:
        canonical = canonical_json_bytes(replacement)
        by_canonical[canonical] = cast(JsonValue, dict(replacement))
    return tuple(by_canonical[key] for key in sorted(by_canonical))


def _interval_dates(
    value: Mapping[str, Any],
    *,
    zone: ZoneInfo,
) -> set[date]:
    result: set[date] = set()
    civil = value.get("civilDateTimeInterval")
    if isinstance(civil, Mapping):
        civil_start = _civil_edge_date(civil.get("startDateTime"))
        civil_end = _civil_edge_date(civil.get("endDateTime"))
        result.update(_date_range(civil_start, civil_end))
    physical = value.get("physicalTimeInterval")
    if isinstance(physical, Mapping):
        physical_start = _physical_edge_time(physical.get("startTime"))
        physical_end = _physical_edge_time(physical.get("endTime"))
        if physical_start is not None:
            local_start = physical_start.astimezone(zone).date()
            local_end = (
                (physical_end - timedelta(microseconds=1)).astimezone(zone).date()
                if physical_end is not None and physical_end > physical_start
                else local_start
            )
            result.update(_date_range(local_start, local_end))
    return result


def _civil_edge_date(value: Any) -> date | None:
    if not isinstance(value, Mapping):
        return None
    raw_date = value.get("date")
    if not isinstance(raw_date, Mapping):
        return None
    try:
        return date(
            int(raw_date["year"]),
            int(raw_date["month"]),
            int(raw_date["day"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _physical_edge_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _date_range(start: date | None, end: date | None) -> set[date]:
    if start is None:
        return set()
    final = start if end is None or end < start else end
    values: set[date] = set()
    cursor = start
    while cursor <= final:
        values.add(cursor)
        cursor += timedelta(days=1)
    return values


def _time_zone(settings: Mapping[str, Any], default: str) -> str:
    value = settings.get("timeZone", default)
    if not isinstance(value, str):
        value = default
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError:
        return default
    return value


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


def _observation_priority(value: Observation) -> tuple[int, str]:
    daily = any(source.startswith("daily-") for source in value.source_data_types)
    return (1 if daily else 0, value.metric_name)


def _data_status(
    metric_date: date,
    *,
    zone: ZoneInfo,
    now: datetime,
    fitbit_last_sync: datetime | None,
) -> str:
    day_end = datetime.combine(
        metric_date + timedelta(days=1),
        time.min,
        tzinfo=zone,
    ).astimezone(UTC)
    finalization = datetime.combine(
        metric_date + timedelta(days=1),
        time(4, 5),
        tzinfo=zone,
    ).astimezone(UTC)
    if now < finalization:
        return "PROVISIONAL"
    if fitbit_last_sync is not None and fitbit_last_sync >= day_end:
        return "CANONICAL_SYNCED"
    return "CANONICAL_STALE"


def _positive_int(value: Any, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return default


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("pipeline clock must return a timezone-aware datetime")
    return value.astimezone(UTC)
