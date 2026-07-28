from __future__ import annotations

import gzip
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import cast

import pytest

from health_data_pipeline.canonical import (
    CANONICALIZATION_VERSION,
    ENCODING_VERSION,
    CanonicalizationError,
    canonical_json_bytes,
    canonical_ndjson_bytes,
    content_addressed_object_name,
    daily_revision_object_name,
    deterministic_gzip,
    encode_content,
    snapshot_object_name,
    year_shard_object_name,
)


def test_fhcj_normalizes_unicode_orders_keys_and_formats_decimals() -> None:
    value = {
        "z": Decimal("100.5000"),
        "e\u0301": "Cafe\u0301",
        "a": Decimal("1E-7"),
    }

    assert canonical_json_bytes(value) == ('{"a":0.0000001,"z":100.5,"é":"Café"}'.encode())


def test_fhcj_formats_rfc3339_timestamps_without_exponents() -> None:
    value = {
        "day": date(2026, 7, 28),
        "local": datetime(
            2026,
            7,
            28,
            12,
            30,
            1,
            120000,
            tzinfo=timezone(timedelta(hours=9)),
        ),
        "utc": datetime(2026, 7, 28, 3, 30, tzinfo=UTC),
    }

    assert canonical_json_bytes(value) == (
        b'{"day":"2026-07-28","local":"2026-07-28T12:30:01.12+09:00","utc":"2026-07-28T03:30:00Z"}'
    )


@pytest.mark.parametrize(
    "value",
    [
        Decimal("-0"),
        Decimal("NaN"),
        Decimal("Infinity"),
        0.1,
        float("nan"),
        datetime(2026, 7, 28),
    ],
)
def test_fhcj_rejects_ambiguous_numbers_and_naive_timestamps(value: object) -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes(value)


def test_fhcj_rejects_keys_that_collide_after_nfc() -> None:
    with pytest.raises(CanonicalizationError, match="collide"):
        canonical_json_bytes({"é": 1, "e\u0301": 2})


def test_fhcj_rejects_lone_unicode_surrogates() -> None:
    with pytest.raises(CanonicalizationError, match="Unicode scalar"):
        canonical_json_bytes("\ud800")


def test_canonical_ndjson_sorts_by_declared_unique_key_and_has_final_lf() -> None:
    rows = [
        {
            "metric_name": "steps",
            "statistic": "mean",
            "period_kind": "CALENDAR_WEEK",
            "dimension_key": None,
            "value_num": Decimal(12),
        },
        {
            "metric_name": "steps",
            "statistic": "observed",
            "period_kind": "DAY",
            "dimension_key": None,
            "value_num": Decimal(10),
        },
    ]

    result = canonical_ndjson_bytes(
        rows,
        sort_fields=(
            "metric_name",
            "statistic",
            "period_kind",
            "dimension_key",
        ),
    )

    assert result.endswith(b"\n")
    assert result.count(b"\n") == 2
    assert result.splitlines()[0].startswith(
        b'{"dimension_key":null,"metric_name":"steps","period_kind":"CALENDAR_WEEK"'
    )


def test_gzip_v1_is_deterministic_and_platform_neutral() -> None:
    payload = b'{"health":"private"}'

    first = deterministic_gzip(payload)
    second = deterministic_gzip(payload)

    assert first == second
    assert first[3] == 0  # no filename or comment flags
    assert first[4:8] == b"\x00\x00\x00\x00"  # mtime=0
    assert first[9] == 255  # normalized OS byte
    assert gzip.decompress(first) == payload


def test_content_addressed_uri_uses_canonical_not_gzip_hash() -> None:
    encoded = encode_content(b'{"a":1}')
    snapshot_name = snapshot_object_name(encoded.canonical_sha256)

    assert encoded.canonical_sha256 in snapshot_name
    assert encoded.object_sha256 not in snapshot_name
    assert f"canonicalization={CANONICALIZATION_VERSION}" in snapshot_name
    assert f"encoding={ENCODING_VERSION}" in snapshot_name

    daily_name = daily_revision_object_name(
        "2026-07-28",
        encoded.canonical_sha256,
    )
    assert "metric_date=2026-07-28" in daily_name
    assert daily_name.endswith(f"sha256={encoded.canonical_sha256}.ndjson.gz")


def test_canonical_ndjson_supports_empty_and_explicit_sort_key() -> None:
    assert canonical_ndjson_bytes([]) == b""
    assert (
        canonical_ndjson_bytes(
            [{"order": 2}, {"order": 1}],
            sort_key=lambda row: cast(dict[str, int], row)["order"],
        )
        == b'{"order":1}\n{"order":2}\n'
    )


def test_canonical_ndjson_rejects_invalid_sort_configuration() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        canonical_ndjson_bytes(
            [],
            sort_key=lambda row: row,
            sort_fields=("id",),
        )
    with pytest.raises(CanonicalizationError, match="mapping rows"):
        canonical_ndjson_bytes([1], sort_fields=("id",))
    with pytest.raises(CanonicalizationError, match="missing NDJSON"):
        canonical_ndjson_bytes([{"value": 1}], sort_fields=("id",))


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({1: "value"}, "object key must be str"),
        (object(), "unsupported"),
    ],
)
def test_fhcj_rejects_non_json_value_types(value: object, message: str) -> None:
    with pytest.raises(CanonicalizationError, match=message):
        canonical_json_bytes(value)


def test_generic_content_addressed_path_supports_partitions() -> None:
    digest = "a" * 64
    result = content_addressed_object_name(
        "blobs/normalized",
        digest,
        suffix=".ndjson.gz",
        partitions=("record_kind=SAMPLE", "data_type=heart-rate"),
    )
    assert result.startswith("blobs/normalized/record_kind=SAMPLE/data_type=heart-rate/")
    assert result.endswith(f"/{digest}.ndjson.gz")


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        (lambda: snapshot_object_name("A" * 64), "64 lowercase"),
        (lambda: year_shard_object_name(10_000, "a" * 64), "invalid year"),
        (
            lambda: daily_revision_object_name("not-a-date", "a" * 64),
            "invalid ISO date",
        ),
        (
            lambda: daily_revision_object_name(
                datetime(2026, 7, 28, tzinfo=UTC),
                "a" * 64,
            ),
            "must not be a datetime",
        ),
        (
            lambda: daily_revision_object_name("2026-07-28", "a" * 64, schema_version=0),
            "schema_version",
        ),
        (
            lambda: content_addressed_object_name(
                "blobs//raw",
                "a" * 64,
                suffix=".json.gz",
            ),
            "invalid object prefix",
        ),
        (
            lambda: content_addressed_object_name(
                "blobs/raw",
                "a" * 64,
                suffix="json.gz",
            ),
            "invalid object suffix",
        ),
        (
            lambda: content_addressed_object_name(
                "blobs/raw",
                "a" * 64,
                suffix=".json.gz",
                partitions=("..",),
            ),
            "invalid object path component",
        ),
    ],
)
def test_content_addressed_paths_reject_ambiguous_inputs(
    operation: Callable[[], str],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        operation()
