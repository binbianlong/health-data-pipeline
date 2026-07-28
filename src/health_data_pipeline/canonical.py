"""Canonical serialization and content-addressed object naming.

FHCJ-1 deliberately accepts a smaller value domain than Python's ``json``
module.  In particular, decimal values must be supplied as ``Decimal`` rather
than binary floating point values.  That restriction prevents platform or
library upgrades from changing an object's identity.
"""

from __future__ import annotations

import dataclasses
import gzip
import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import PurePosixPath

CANONICALIZATION_VERSION = "fhcj-1"
ENCODING_VERSION = "gzip-v1"
GZIP_HEADER_SIZE = 10
MAX_YEAR = 9999
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CanonicalizationError(ValueError):
    """Raised when a value cannot be represented by FHCJ-1."""


@dataclass(frozen=True, slots=True)
class EncodedContent:
    """Canonical bytes and their deterministic gzip representation."""

    canonical_bytes: bytes
    object_bytes: bytes
    canonical_sha256: str
    object_sha256: str


def sha256_hex(value: bytes) -> str:
    """Return a lowercase SHA-256 digest."""

    return hashlib.sha256(value).hexdigest()


def canonical_json_bytes(value: object) -> bytes:
    """Serialize a supported value using FHCJ-1.

    Arrays retain their input order.  Callers must sort arrays whose domain
    semantics declare them unordered; canonical NDJSON offers an explicit sort
    hook for that purpose.
    """

    try:
        return _serialize(_model_dump(value)).encode("utf-8")
    except UnicodeError as exc:
        msg = "FHCJ-1 strings must contain valid Unicode scalar values"
        raise CanonicalizationError(msg) from exc


def canonical_ndjson_bytes(
    rows: Iterable[object],
    *,
    sort_key: Callable[[object], object] | None = None,
    sort_fields: Sequence[str] | None = None,
) -> bytes:
    """Serialize and deterministically order NDJSON rows.

    ``sort_key`` and ``sort_fields`` are mutually exclusive.  Without either,
    rows are ordered by their complete canonical representation.  Duplicate
    rows are retained because normalized detail streams can legitimately
    contain equal values; domain-level duplicate checks belong to adapters.
    """

    if sort_key is not None and sort_fields is not None:
        msg = "sort_key and sort_fields are mutually exclusive"
        raise ValueError(msg)

    serialized_rows: list[tuple[bytes, bytes]] = []
    for original_row in rows:
        row = _model_dump(original_row)
        row_bytes = canonical_json_bytes(row)
        if sort_key is not None:
            key_bytes = canonical_json_bytes(sort_key(original_row))
        elif sort_fields is not None:
            if not isinstance(row, Mapping):
                msg = "sort_fields requires mapping rows"
                raise CanonicalizationError(msg)
            try:
                key_bytes = canonical_json_bytes([row[field] for field in sort_fields])
            except KeyError as exc:
                msg = f"missing NDJSON sort field: {exc.args[0]}"
                raise CanonicalizationError(msg) from exc
        else:
            key_bytes = row_bytes
        serialized_rows.append((key_bytes, row_bytes))

    serialized_rows.sort(key=lambda item: (item[0], item[1]))
    if not serialized_rows:
        return b""
    return b"\n".join(row for _, row in serialized_rows) + b"\n"


def deterministic_gzip(canonical_content: bytes) -> bytes:
    """Encode ``canonical_content`` using the design's gzip-v1 format."""

    compressed = bytearray(gzip.compress(canonical_content, compresslevel=9, mtime=0))
    if len(compressed) < GZIP_HEADER_SIZE:  # pragma: no cover - defensive stdlib invariant
        msg = "gzip encoder returned a truncated header"
        raise RuntimeError(msg)
    # RFC 1952 byte 9 is the originating OS.  CPython/zlib differs by platform
    # for mtime=0, so gzip-v1 normalizes it to the unknown value (255).
    compressed[9] = 255
    return bytes(compressed)


def encode_content(canonical_content: bytes) -> EncodedContent:
    """Create a deterministic gzip object and both integrity hashes."""

    object_bytes = deterministic_gzip(canonical_content)
    return EncodedContent(
        canonical_bytes=canonical_content,
        object_bytes=object_bytes,
        canonical_sha256=sha256_hex(canonical_content),
        object_sha256=sha256_hex(object_bytes),
    )


def snapshot_object_name(canonical_sha256: str) -> str:
    """Return the Root Snapshot object name for a canonical content hash."""

    return _hashed_object_name(
        "manifests/snapshots",
        canonical_sha256,
        suffix=".json.gz",
    )


def year_shard_object_name(year: int, canonical_sha256: str) -> str:
    """Return a year-shard object name."""

    if year < 1 or year > MAX_YEAR:
        msg = f"invalid year: {year}"
        raise ValueError(msg)
    return _hashed_object_name(
        f"manifests/snapshot-shards/year={year:04d}",
        canonical_sha256,
        suffix=".json.gz",
    )


def daily_revision_object_name(
    metric_date: date | str,
    canonical_sha256: str,
    *,
    schema_version: int = 1,
) -> str:
    """Return a BigQuery-compatible daily-canonical object name."""

    parsed_date = _parse_date(metric_date)
    if schema_version < 1:
        msg = "schema_version must be positive"
        raise ValueError(msg)
    digest = _validate_sha256(canonical_sha256)
    return (
        f"daily-canonical/schema_version={schema_version}/"
        f"metric_date={parsed_date.isoformat()}/"
        f"canonicalization={CANONICALIZATION_VERSION}/"
        f"encoding={ENCODING_VERSION}/sha256={digest}.ndjson.gz"
    )


def content_addressed_object_name(
    prefix: str,
    canonical_sha256: str,
    *,
    suffix: str,
    partitions: Sequence[str] = (),
) -> str:
    """Build a generic sharded content-addressed object name.

    This helper is intended for API pages, normalized streams, and ingestion
    manifests.  Dedicated helpers above encode the stronger path invariants for
    Root Snapshots, Year Shards, and daily revisions.
    """

    clean_prefix = _validate_prefix(prefix)
    clean_partitions = tuple(_validate_path_component(value) for value in partitions)
    partition_path = "/".join((clean_prefix, *clean_partitions))
    return _hashed_object_name(partition_path, canonical_sha256, suffix=suffix)


def _hashed_object_name(prefix: str, digest: str, *, suffix: str) -> str:
    clean_prefix = _validate_prefix(prefix)
    clean_digest = _validate_sha256(digest)
    if not suffix.startswith(".") or "/" in suffix:
        msg = f"invalid object suffix: {suffix!r}"
        raise ValueError(msg)
    return (
        f"{clean_prefix}/canonicalization={CANONICALIZATION_VERSION}/"
        f"encoding={ENCODING_VERSION}/sha256/{clean_digest[:2]}/"
        f"{clean_digest}{suffix}"
    )


def _validate_sha256(value: str) -> str:
    if not _SHA256_RE.fullmatch(value):
        msg = "SHA-256 must be 64 lowercase hexadecimal characters"
        raise ValueError(msg)
    return value


def _validate_prefix(prefix: str) -> str:
    normalized = prefix.strip("/")
    if not normalized or PurePosixPath(normalized).is_absolute():
        msg = f"invalid object prefix: {prefix!r}"
        raise ValueError(msg)
    components = normalized.split("/")
    if any(component in {"", ".", ".."} for component in components):
        msg = f"invalid object prefix: {prefix!r}"
        raise ValueError(msg)
    return normalized


def _validate_path_component(value: str) -> str:
    if not value or value in {".", ".."} or "/" in value:
        msg = f"invalid object path component: {value!r}"
        raise ValueError(msg)
    return value


def _parse_date(value: date | str) -> date:
    if isinstance(value, datetime):
        msg = "metric_date must not be a datetime"
        raise TypeError(msg)
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        msg = f"invalid ISO date: {value!r}"
        raise ValueError(msg) from exc


def _model_dump(value: object) -> object:
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return dump(mode="python")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    return value


def _serialize(value: object) -> str:  # noqa: PLR0911, PLR0912
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return _serialize_string(value)
    if isinstance(value, Enum):
        return _serialize(value.value)
    if isinstance(value, datetime):
        return _serialize_string(_format_datetime(value))
    if isinstance(value, date):
        return _serialize_string(value.isoformat())
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _format_decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            msg = "NaN and Infinity are not valid FHCJ-1 numbers"
            raise CanonicalizationError(msg)
        msg = "binary float is not valid FHCJ-1; use Decimal"
        raise CanonicalizationError(msg)
    if isinstance(value, Mapping):
        return _serialize_mapping(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_serialize(_model_dump(item)) for item in value) + "]"
    msg = f"unsupported FHCJ-1 value type: {type(value).__name__}"
    raise CanonicalizationError(msg)


def _serialize_string(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value)
    try:
        normalized.encode("utf-8")
    except UnicodeError as exc:
        msg = "FHCJ-1 strings must contain valid Unicode scalar values"
        raise CanonicalizationError(msg) from exc
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


def _serialize_mapping(value: Mapping[object, object]) -> str:
    normalized: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            msg = f"FHCJ-1 object key must be str, got {type(key).__name__}"
            raise CanonicalizationError(msg)
        normalized_key = unicodedata.normalize("NFC", key)
        if normalized_key in normalized:
            msg = f"object keys collide after NFC normalization: {normalized_key!r}"
            raise CanonicalizationError(msg)
        normalized[normalized_key] = item

    members = (
        f"{_serialize_string(key)}:{_serialize(_model_dump(normalized[key]))}"
        for key in sorted(normalized)
    )
    return "{" + ",".join(members) + "}"


def _format_decimal(value: Decimal) -> str:
    if not value.is_finite():
        msg = "NaN and Infinity are not valid FHCJ-1 numbers"
        raise CanonicalizationError(msg)
    if value.is_zero():
        if value.is_signed():
            msg = "negative zero is not valid FHCJ-1"
            raise CanonicalizationError(msg)
        return "0"

    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _format_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = "FHCJ-1 timestamps must be timezone-aware"
        raise CanonicalizationError(msg)
    rendered = value.isoformat(timespec="microseconds")
    date_part, offset_part = rendered.rsplit("+", 1) if "+" in rendered else (rendered, "")
    if not offset_part and rendered[-6:-5] == "-":
        date_part, offset_part = rendered[:-6], rendered[-5:]
        offset_part = f"-{offset_part}"
    if "." in date_part:
        date_part = date_part.rstrip("0").rstrip(".")
    rendered = (
        f"{date_part}+{offset_part}"
        if offset_part and not offset_part.startswith("-")
        else f"{date_part}{offset_part}"
    )
    if value.utcoffset() == UTC.utcoffset(value):
        rendered = rendered[:-6] + "Z"
    return rendered
