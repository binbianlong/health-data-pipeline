"""Fail closed when a Terraform plan would resume the recovery scheduler."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

SCHEDULER_ADDRESS = "google_cloud_scheduler_job.hourly_sync[0]"
_ARGUMENT_COUNT = 3


def _boolean(value: str, *, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{name} must be true or false")


def _resource_values(plan_section: object, address: str) -> Mapping[str, Any] | None:
    if not isinstance(plan_section, Mapping):
        return None
    root = plan_section.get("root_module")
    if not isinstance(root, Mapping):
        return None
    pending: list[Mapping[str, Any]] = [root]
    while pending:
        module = pending.pop()
        resources = module.get("resources", ())
        if isinstance(resources, list):
            for resource in resources:
                if not isinstance(resource, Mapping) or resource.get("address") != address:
                    continue
                values = resource.get("values")
                return values if isinstance(values, Mapping) else None
        children = module.get("child_modules", ())
        if isinstance(children, list):
            pending.extend(child for child in children if isinstance(child, Mapping))
    return None


def validate_scheduler_resume(
    plan: Mapping[str, Any],
    *,
    scheduler_paused: bool,
    allow_scheduler_resume: bool,
) -> None:
    """Validate the refreshed prior state and the saved plan's desired state."""

    if scheduler_paused:
        return
    planned = _resource_values(plan.get("planned_values"), SCHEDULER_ADDRESS)
    if planned is None or planned.get("paused") is not False:
        raise ValueError("Terraform plan does not prove an enabled recovery scheduler")
    prior = _resource_values(plan.get("prior_state", {}).get("values"), SCHEDULER_ADDRESS)
    if prior is None:
        if allow_scheduler_resume:
            return
        raise ValueError("creating an enabled recovery scheduler requires explicit approval")
    prior_paused = prior.get("paused")
    if prior_paused is False:
        return
    if prior_paused is True and allow_scheduler_resume:
        return
    if prior_paused is True:
        raise ValueError("resuming a paused recovery scheduler requires explicit approval")
    raise ValueError("Terraform prior state does not prove the scheduler state")


def _read_plan(path: str) -> Mapping[str, Any]:
    plan = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(plan, Mapping):
        raise TypeError("Terraform plan JSON must be an object")
    return plan


def main(arguments: list[str] | None = None) -> None:
    args = sys.argv[1:] if arguments is None else arguments
    if len(args) != _ARGUMENT_COUNT:
        raise SystemExit("usage: check_scheduler_resume.py PLAN_JSON SCHEDULER_PAUSED ALLOW_RESUME")
    try:
        plan = _read_plan(args[0])
        validate_scheduler_resume(
            plan,
            scheduler_paused=_boolean(args[1], name="SCHEDULER_PAUSED"),
            allow_scheduler_resume=_boolean(args[2], name="ALLOW_SCHEDULER_RESUME"),
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"scheduler resume guard failed: {exc}") from exc


if __name__ == "__main__":
    main()
