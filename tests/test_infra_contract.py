from __future__ import annotations

import re
from pathlib import Path

from health_data_pipeline.google_health import data_type_spec
from health_data_pipeline.pipeline import SUPPORTED_DATA_TYPES

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_STORAGE_TERRAFORM = _PROJECT_ROOT / "infra" / "main" / "storage.tf"
_NORMALIZED_PREFIX = "blobs/normalized/record_kind="


def _lifecycle_prefixes(terraform: str, *, age: int) -> set[str]:
    condition = re.search(
        rf"condition\s*\{{\s*age\s*=\s*{age}\s*"
        r"matches_prefix\s*=\s*\[(?P<prefixes>.*?)\]\s*\}",
        terraform,
        flags=re.DOTALL,
    )
    assert condition is not None
    return set(re.findall(r'"([^"]+)"', condition.group("prefixes")))


def test_normalized_lifecycle_covers_every_generated_record_kind() -> None:
    terraform = _STORAGE_TERRAFORM.read_text(encoding="utf-8")
    generated_prefixes = {
        f"{_NORMALIZED_PREFIX}{data_type_spec(data_type).record_kind.upper()}/"
        for data_type in SUPPORTED_DATA_TYPES
    }
    ninety_day_prefixes = {
        prefix
        for prefix in _lifecycle_prefixes(terraform, age=90)
        if prefix.startswith(_NORMALIZED_PREFIX)
    }
    year_prefixes = {
        prefix
        for prefix in _lifecycle_prefixes(terraform, age=365)
        if prefix.startswith(_NORMALIZED_PREFIX)
    }

    assert generated_prefixes == {
        f"{_NORMALIZED_PREFIX}DAILY/",
        f"{_NORMALIZED_PREFIX}INTERVAL/",
        f"{_NORMALIZED_PREFIX}SAMPLE/",
        f"{_NORMALIZED_PREFIX}SESSION/",
    }
    assert ninety_day_prefixes == {
        f"{_NORMALIZED_PREFIX}INTERVAL/",
        f"{_NORMALIZED_PREFIX}SAMPLE/",
    }
    assert year_prefixes == {
        f"{_NORMALIZED_PREFIX}DAILY/",
        f"{_NORMALIZED_PREFIX}SESSION/",
    }
    assert generated_prefixes == ninety_day_prefixes | year_prefixes
