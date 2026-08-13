"""Cloud Run Job console entrypoint."""

# ruff: noqa: PLW0603

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from typing import cast

from health_data_pipeline.sync.runner import JobRunner

JobFactory = Callable[[], JobRunner]
_configured_factory: JobFactory | None = None


def configure_job_factory(factory: JobFactory) -> None:
    """Install the production composition root without importing it in tests."""

    global _configured_factory
    _configured_factory = factory


def _load_factory() -> JobFactory:
    if _configured_factory is not None:
        return _configured_factory
    dotted = os.environ.get(
        "HEALTH_PIPELINE_JOB_FACTORY",
        "health_data_pipeline.runtime.composition:build_job_runner",
    )
    module_name, separator, attribute = dotted.partition(":")
    if not separator or not module_name or not attribute:
        raise RuntimeError("HEALTH_PIPELINE_JOB_FACTORY must be module:callable")
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute, None)
    if not callable(factory):
        raise TypeError(f"job factory is not callable: {dotted}")
    return cast(JobFactory, factory)


def _maintenance_mode_enabled() -> bool:
    value = os.environ.get("MAINTENANCE_MODE", "false").strip().lower()
    if value not in {"true", "false"}:
        raise RuntimeError("MAINTENANCE_MODE must be true or false")
    return value == "true"


def main() -> None:
    """Run one composed job unless maintenance mode disables it."""

    if _maintenance_mode_enabled():
        return
    runner = _load_factory()()
    runner.run()


if __name__ == "__main__":
    main()
