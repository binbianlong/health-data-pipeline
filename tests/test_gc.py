from datetime import UTC, datetime, timedelta

from health_data_pipeline.gc import StoredObject, plan_reachability_gc

NOW = datetime(2026, 7, 28, tzinfo=UTC)


def test_retention_reference_pins_old_object() -> None:
    plan = plan_reachability_gc(
        [StoredObject("gs://bucket/current", NOW - timedelta(days=400))],
        retention_references=["gs://bucket/current"],
        audit_references=[],
        now=NOW,
    )
    assert plan.retained == ("gs://bucket/current",)
    assert plan.delete == ()


def test_audit_reference_does_not_pin_payload() -> None:
    plan = plan_reachability_gc(
        [StoredObject("gs://bucket/raw", NOW - timedelta(days=31))],
        retention_references=[],
        audit_references=["gs://bucket/raw"],
        now=NOW,
    )
    assert plan.delete == ("gs://bucket/raw",)


def test_cas_orphan_gets_grace_period() -> None:
    plan = plan_reachability_gc(
        [StoredObject("gs://bucket/orphan", NOW - timedelta(days=1))],
        retention_references=[],
        audit_references=[],
        now=NOW,
    )
    assert plan.too_young == ("gs://bucket/orphan",)
