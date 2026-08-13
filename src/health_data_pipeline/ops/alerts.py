"""Small stateful operational alerts that never log health measurements."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from health_data_pipeline.state.canonical import canonical_json_bytes
from health_data_pipeline.state.objects import (
    ObjectNotFound,
    ObjectStore,
    PreconditionFailed,
)

FITBIT_SYNC_ALERT_PATH = "state/alerts/fitbit-sync.json"


@dataclass(frozen=True, slots=True)
class FitbitSyncAlertState:
    stale: bool
    alerted_for_sync_time: str | None
    updated_at: str


class FitbitSyncAlertService:
    """Emit at most one event for each five-day stale-sync episode."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        emit: Callable[[str, dict[str, object]], None],
        threshold: timedelta = timedelta(days=5),
    ) -> None:
        self._store = store
        self._emit = emit
        self._threshold = threshold

    def evaluate(
        self,
        *,
        last_sync_time: datetime | None,
        now: datetime,
    ) -> bool:
        current_time = _utc(now)
        sync_time = None if last_sync_time is None else _utc(last_sync_time)
        stale = sync_time is None or current_time - sync_time >= self._threshold
        marker = "unknown" if sync_time is None else _timestamp(sync_time)
        previous, generation = self._read()
        if stale and previous.stale and previous.alerted_for_sync_time == marker:
            return False
        if not stale and not previous.stale:
            return False

        next_state = FitbitSyncAlertState(
            stale=stale,
            alerted_for_sync_time=marker if stale else None,
            updated_at=_timestamp(current_time),
        )
        try:
            self._store.put(
                FITBIT_SYNC_ALERT_PATH,
                canonical_json_bytes(asdict(next_state)),
                if_generation_match=generation,
                metadata={"schema": "fitbit-sync-alert-v1"},
                content_type="application/json",
                cache_control="no-store",
            )
        except PreconditionFailed:
            return False
        if stale:
            self._emit(
                "fitbit_sync_stale",
                {"threshold_days": 5},
            )
            return True
        return False

    def _read(self) -> tuple[FitbitSyncAlertState, int]:
        try:
            stored = self._store.get(FITBIT_SYNC_ALERT_PATH)
        except ObjectNotFound:
            return (
                FitbitSyncAlertState(
                    stale=False,
                    alerted_for_sync_time=None,
                    updated_at="1970-01-01T00:00:00Z",
                ),
                0,
            )
        try:
            value = json.loads(stored.data)
            return FitbitSyncAlertState(**value), stored.generation
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid Fitbit sync alert state") from exc


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("alert time must be timezone-aware")
    return value.astimezone(UTC)
