"""Incremental Google Health ingestion and one-shot HEAD commit pipeline."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

from pydantic import JsonValue

from health_data_pipeline.health.daily import (
    Observation,
    SourceQueryStatus,
    affected_metric_dates,
)
from health_data_pipeline.health.normalization import normalize_daily_observations
from health_data_pipeline.ops.class_a import (
    class_a_backfill_allowed,
    class_a_normalizer_upgrade_allowed,
)
from health_data_pipeline.state.commit import CommitCoordinator, CommitRequest
from health_data_pipeline.state.head import HeadRepository
from health_data_pipeline.state.lease import LeaseOwnershipLostError, LeaseRepository
from health_data_pipeline.state.models import CommitKind, RootSnapshot
from health_data_pipeline.state.objects import (
    ContentAddressedStore,
    ObjectStore,
    PreconditionFailed,
)
from health_data_pipeline.state.snapshots import SnapshotRepository
from health_data_pipeline.sync import planning as sync_planning
from health_data_pipeline.sync.contracts import DataPointPage, FetchWindow, SyncOutcome
from health_data_pipeline.sync.ingestion import (
    IngestedQuery,
    SourceIngestor,
    unique_manifest_references,
    unique_raw_replacements,
)
from health_data_pipeline.sync.planning import (
    SUPPORTED_DATA_TYPES,
    BackfillChunk,
    PendingWebhook,
)
from health_data_pipeline.sync.projection import DailyProjector

_DAILY_SCHEMA_VERSION = 3
_LEASE_REFRESH_MARGIN = timedelta(minutes=5)

_BACKFILL_META_KEY = sync_planning._BACKFILL_META_KEY
_BACKFILL_TIME_ZONE = sync_planning._BACKFILL_TIME_ZONE
_NORMALIZER_VERSION = sync_planning._NORMALIZER_VERSION
_advance_backfill_cursors_many = sync_planning._advance_backfill_cursors_many
_next_backfill_cursors = sync_planning._next_backfill_cursors
_parse_webhook_event = sync_planning._parse_webhook_event
_plan_fetch_dates = sync_planning._plan_fetch_dates
_reopen_backfill_gaps = sync_planning._reopen_backfill_gaps
_select_backfill_chunks = sync_planning._select_backfill_chunks
_time_zone = sync_planning._time_zone


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
        self._ingestor = SourceIngestor(
            health=health,
            content=content,
            daily_schema_version=_DAILY_SCHEMA_VERSION,
            normalizer_version=_NORMALIZER_VERSION,
        )
        self._projector = DailyProjector(
            snapshots=snapshots,
            normalizer_version=_NORMALIZER_VERSION,
        )

    def run(self) -> SyncOutcome:
        now = _aware_utc(self._now())
        lease = self._leases.acquire(now=now)
        if lease is None:
            return SyncOutcome(
                committed=False,
                generation=self._current_generation(),
                owner_hmac=self._owner_hmac,
            )

        current_lease = lease

        def heartbeat() -> None:
            nonlocal current_lease
            current_time = _aware_utc(self._now())
            if current_lease.record.expires_at - current_time > _LEASE_REFRESH_MARGIN:
                return
            refreshed = self._leases.refresh(
                current_lease,
                now=current_time,
            )
            if refreshed is None:
                raise LeaseOwnershipLostError("sync lease ownership was lost")
            current_lease = refreshed

        try:
            return self._run_with_lease(now, heartbeat=heartbeat)
        finally:
            self._leases.release(current_lease)

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

    def _run_with_lease(
        self,
        now: datetime,
        *,
        heartbeat: Callable[[], None],
    ) -> SyncOutcome:
        base = self._heads.read()
        root = (
            None
            if base.head is None
            else self._snapshots.load_root_snapshot(base.head.root_snapshot)
        )
        settings = self._health.get_settings()
        time_zone, time_zone_source = _time_zone(settings, self._default_time_zone)
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
        ingested = self._execute_ingestion_plan(
            fetch_dates,
            backfill_chunks,
            zone=zone,
            expected_generation=expected_generation,
            heartbeat=heartbeat,
        )

        changed_dates = {query.metric_date for query in ingested}
        changed_dates.update(
            point_date
            for query in ingested
            for point_date in normalize_daily_observations(query.data_points)
        )
        changed_sleep_sessions = self._projector.merge_changed_sleep_sessions(ingested)
        changed_dates.update(changed_sleep_sessions)
        changed_source_statuses = self._projector.merge_changed_source_statuses(root, ingested)
        changed_dates.update(changed_source_statuses)
        if not changed_dates:
            changed_dates.add(today)
        current_observations = self._projector.merge_changed_observations(
            root,
            ingested,
            source_statuses=changed_source_statuses,
        )
        targets = affected_metric_dates(changed_dates, today=today)
        daily_rows = self._projector.recalculate_rows(
            root,
            targets,
            current_observations,
            changed_sleep_sessions,
            changed_source_statuses,
            zone=zone,
            time_zone_source=time_zone_source,
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
        heartbeat()
        try:
            committed = self._commits.commit(
                CommitRequest(
                    daily_rows=daily_rows,
                    commit_kind=commit_kind,
                    created_at=now,
                    data_type_cursors=cursors,
                    webhook_cursor=webhook_cursor,
                    backfill_cursors=backfill_cursors,
                    fitbit_last_sync_time=fitbit_last_sync,
                    preserve_fitbit_last_sync_time=bool(backfill_chunks),
                    time_zone=time_zone,
                    raw_replacements=unique_raw_replacements(
                        item.replacement_scope for item in ingested
                    ),
                    ingestion_manifests=unique_manifest_references(
                        item.manifest for item in ingested
                    ),
                    daily_schema_version=_DAILY_SCHEMA_VERSION,
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

    def _execute_ingestion_plan(
        self,
        fetch_dates: Mapping[str, Iterable[date]],
        backfill_chunks: Sequence[BackfillChunk],
        *,
        zone: ZoneInfo,
        expected_generation: int,
        heartbeat: Callable[[], None],
    ) -> tuple[IngestedQuery, ...]:
        """Execute either foreground dates or isolated backfill chunks."""

        ingested: list[IngestedQuery] = []
        if backfill_chunks:
            for chunk in backfill_chunks:
                ingested.extend(
                    self._ingest_backfill_chunk(
                        chunk,
                        zone=zone,
                        expected_generation=expected_generation,
                        heartbeat=heartbeat,
                    )
                )
            return tuple(ingested)

        for data_type in SUPPORTED_DATA_TYPES:
            for metric_date in sorted(fetch_dates.get(data_type, ())):
                unit = self._ingestor.ingest_dates(
                    data_type,
                    (metric_date,),
                    zone=zone,
                    expected_generation=expected_generation,
                    heartbeat=heartbeat,
                )
                ingested.extend(unit)
                heartbeat()
        return tuple(ingested)

    def _ingest_backfill_chunk(
        self,
        chunk: BackfillChunk,
        *,
        zone: ZoneInfo,
        expected_generation: int,
        heartbeat: Callable[[], None] | None = None,
    ) -> tuple[IngestedQuery, ...]:
        """Range-fetch priority sessions; keep legacy high-volume work daily."""

        if chunk.next_data_type_index is None:
            unit = self._ingestor.ingest_dates(
                chunk.data_type,
                chunk.dates,
                zone=zone,
                expected_generation=expected_generation,
                heartbeat=heartbeat,
            )
            if heartbeat is not None:
                heartbeat()
            return unit

        ingested: list[IngestedQuery] = []
        for metric_date in chunk.dates:
            unit = self._ingestor.ingest_dates(
                chunk.data_type,
                (metric_date,),
                zone=zone,
                expected_generation=expected_generation,
                heartbeat=heartbeat,
            )
            ingested.extend(unit)
            if heartbeat is not None:
                heartbeat()
        return tuple(ingested)

    def _ingest_dates(
        self,
        data_type: str,
        dates: Sequence[date],
        *,
        zone: ZoneInfo,
        expected_generation: int,
        heartbeat: Callable[[], None] | None = None,
    ) -> tuple[IngestedQuery, ...]:
        """Compatibility wrapper for callers migrating to ``SourceIngestor``."""

        return self._ingestor.ingest_dates(
            data_type,
            dates,
            zone=zone,
            expected_generation=expected_generation,
            heartbeat=heartbeat,
        )

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
        *,
        source_statuses: Mapping[date, Sequence[SourceQueryStatus]],
    ) -> dict[date, tuple[Observation, ...]]:
        """Compatibility wrapper for callers migrating to ``DailyProjector``."""

        return self._projector.merge_changed_observations(
            root,
            ingested,
            source_statuses=source_statuses,
        )

    def _current_generation(self) -> int | None:
        current = self._heads.read().head
        return None if current is None else current.generation


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


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("pipeline clock must return a timezone-aware datetime")
    return value.astimezone(UTC)
