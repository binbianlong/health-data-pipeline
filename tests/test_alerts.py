from datetime import UTC, datetime, timedelta

from health_data_pipeline.alerts import FitbitSyncAlertService
from health_data_pipeline.storage import InMemoryObjectStore

NOW = datetime(2026, 7, 28, tzinfo=UTC)


def test_stale_fitbit_sync_emits_once_until_sync_recovers() -> None:
    events: list[tuple[str, dict[str, object]]] = []
    service = FitbitSyncAlertService(
        InMemoryObjectStore(clock=lambda: NOW),
        emit=lambda event, payload: events.append((event, payload)),
    )
    old = NOW - timedelta(days=6)
    assert service.evaluate(last_sync_time=old, now=NOW) is True
    assert service.evaluate(last_sync_time=old, now=NOW + timedelta(hours=1)) is False
    assert events == [("fitbit_sync_stale", {"threshold_days": 5})]
    assert service.evaluate(last_sync_time=NOW, now=NOW + timedelta(hours=2)) is False
    assert (
        service.evaluate(
            last_sync_time=NOW,
            now=NOW + timedelta(days=6),
        )
        is True
    )
    assert events == [
        ("fitbit_sync_stale", {"threshold_days": 5}),
        ("fitbit_sync_stale", {"threshold_days": 5}),
    ]
