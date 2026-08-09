from __future__ import annotations

from typing import Any

import pytest
from scripts.check_scheduler_resume import validate_scheduler_resume


def _plan(*, prior_paused: bool | None, planned_paused: bool = False) -> dict[str, Any]:
    resource = {
        "address": "google_cloud_scheduler_job.hourly_sync[0]",
        "values": {"paused": planned_paused},
    }
    prior_resources = []
    if prior_paused is not None:
        prior_resources.append(
            {
                "address": resource["address"],
                "values": {"paused": prior_paused},
            }
        )
    return {
        "planned_values": {"root_module": {"resources": [resource]}},
        "prior_state": {"values": {"root_module": {"resources": prior_resources}}},
    }


def test_scheduler_resume_guard_requires_explicit_override() -> None:
    with pytest.raises(ValueError, match="requires explicit approval"):
        validate_scheduler_resume(
            _plan(prior_paused=True),
            scheduler_paused=False,
            allow_scheduler_resume=False,
        )

    validate_scheduler_resume(
        _plan(prior_paused=True),
        scheduler_paused=False,
        allow_scheduler_resume=True,
    )


def test_scheduler_resume_guard_allows_an_already_enabled_scheduler() -> None:
    validate_scheduler_resume(
        _plan(prior_paused=False),
        scheduler_paused=False,
        allow_scheduler_resume=False,
    )


def test_scheduler_resume_guard_fails_closed_for_new_or_unproved_state() -> None:
    with pytest.raises(ValueError, match="creating an enabled"):
        validate_scheduler_resume(
            _plan(prior_paused=None),
            scheduler_paused=False,
            allow_scheduler_resume=False,
        )
    with pytest.raises(ValueError, match="does not prove"):
        validate_scheduler_resume(
            _plan(prior_paused=False, planned_paused=True),
            scheduler_paused=False,
            allow_scheduler_resume=True,
        )


def test_scheduler_resume_guard_does_not_block_a_paused_plan() -> None:
    validate_scheduler_resume(
        {},
        scheduler_paused=True,
        allow_scheduler_resume=False,
    )
