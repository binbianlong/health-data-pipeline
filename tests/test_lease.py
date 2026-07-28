from datetime import UTC, datetime, timedelta

from health_data_pipeline.lease import LeaseRepository
from health_data_pipeline.storage import InMemoryObjectStore

NOW = datetime(2026, 7, 28, tzinfo=UTC)


def test_lease_is_advisory_and_can_be_reacquired_after_expiry() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    leases = LeaseRepository(store, owner_hmac="a" * 64)
    first = leases.acquire(now=NOW)
    assert first is not None
    assert leases.acquire(now=NOW + timedelta(minutes=1)) is None
    second = leases.acquire(now=NOW + timedelta(minutes=16))
    assert second is not None
    assert second.record.token != first.record.token


def test_stale_holder_cannot_refresh_or_release_new_lease() -> None:
    store = InMemoryObjectStore(clock=lambda: NOW)
    leases = LeaseRepository(store, owner_hmac="b" * 64)
    stale = leases.acquire(now=NOW)
    assert stale is not None
    current = leases.acquire(now=NOW + timedelta(minutes=16))
    assert current is not None
    assert leases.refresh(stale, now=NOW + timedelta(minutes=17)) is None
    assert leases.release(stale) is False
    assert leases.release(current) is True
