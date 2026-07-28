"""Pydantic models for committed GCS state.

Only HEAD is mutable.  Every model referenced by HEAD is frozen so application
code builds a new value instead of accidentally mutating a value that has
already been content-addressed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    field_validator,
    model_validator,
)

Sha256Hex = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{64}$", min_length=64, max_length=64),
]
ObjectName = Annotated[str, StringConstraints(min_length=1, max_length=1024)]
MAX_DAYS_IN_YEAR = 366
MAX_YEAR = 9999
YEAR_TEXT_LENGTH = 4


class FrozenModel(BaseModel):
    """Strict immutable base model for canonical persisted values."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        validate_assignment=True,
    )


class ContentReference(FrozenModel):
    """Integrity metadata for one immutable gzip object."""

    uri: ObjectName
    canonical_sha256: Sha256Hex
    object_sha256: Sha256Hex

    @field_validator("uri")
    @classmethod
    def validate_relative_object_name(cls, value: str) -> str:
        if value.startswith(("/", "gs://")) or "?" in value or "#" in value:
            msg = "uri must be a bucket-relative GCS object name"
            raise ValueError(msg)
        if any(component in {"", ".", ".."} for component in value.split("/")):
            msg = "uri contains an invalid path component"
            raise ValueError(msg)
        return value


class DailyRevisionReference(ContentReference):
    """Current canonical daily NDJSON revision for one civil date."""

    schema_version: int = Field(default=1, ge=1)
    row_count: int = Field(ge=0)
    source_generation: int = Field(ge=1)


class YearShardReference(ContentReference):
    """Reference to a complete year shard."""

    date_count: int = Field(ge=0, le=366)


class YearShard(FrozenModel):
    """Complete map of all currently available daily revisions for one year."""

    schema_version: Literal[1] = 1
    year: int = Field(ge=1, le=9999)
    daily_revisions: dict[str, DailyRevisionReference] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_revision_dates(self) -> Self:
        parsed_dates: list[date] = []
        for value in self.daily_revisions:
            try:
                parsed = date.fromisoformat(value)
            except ValueError as exc:
                msg = f"daily revision key is not an ISO date: {value!r}"
                raise ValueError(msg) from exc
            if parsed.isoformat() != value:
                msg = f"daily revision key is not canonical ISO date: {value!r}"
                raise ValueError(msg)
            if parsed.year != self.year:
                msg = f"daily revision {value} is outside shard year {self.year}"
                raise ValueError(msg)
            parsed_dates.append(parsed)
        if len(parsed_dates) > MAX_DAYS_IN_YEAR:
            msg = "year shard cannot contain more than 366 dates"
            raise ValueError(msg)
        return self


class SnapshotState(FrozenModel):
    """Complete state required to continue from a committed HEAD."""

    daily_year_shards: dict[str, YearShardReference] = Field(default_factory=dict)
    data_type_cursors: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_cursor: dict[str, JsonValue] = Field(default_factory=dict)
    backfill_cursors: dict[str, JsonValue] = Field(default_factory=dict)
    fitbit_last_sync_time: AwareDatetime | None = None
    time_zone: str = "Asia/Tokyo"

    @field_validator("daily_year_shards")
    @classmethod
    def validate_shard_years(
        cls,
        value: dict[str, YearShardReference],
    ) -> dict[str, YearShardReference]:
        for year in value:
            if (
                len(year) != YEAR_TEXT_LENGTH
                or not year.isdigit()
                or not 1 <= int(year) <= MAX_YEAR
            ):
                msg = f"year shard key must be a four-digit year: {year!r}"
                raise ValueError(msg)
        return value

    @field_validator("fitbit_last_sync_time")
    @classmethod
    def normalize_last_sync_time(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        return _as_utc(value)

    @field_validator("time_zone")
    @classmethod
    def validate_time_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            msg = f"unknown IANA time zone: {value}"
            raise ValueError(msg) from exc
        return value


class CommitKind(StrEnum):
    """Why a Root Snapshot was committed."""

    INITIAL = "INITIAL"
    WEBHOOK = "WEBHOOK"
    SCHEDULED = "SCHEDULED"
    BACKFILL = "BACKFILL"
    REBUILD = "REBUILD"
    RECOVERY = "RECOVERY"


class ChangeSet(FrozenModel):
    """Audit-oriented difference between a Root Snapshot and its parent."""

    commit_kind: CommitKind
    changed_metric_dates: tuple[date, ...] = ()
    changed_years: tuple[int, ...] = ()
    raw_replacements: tuple[JsonValue, ...] = ()
    ingestion_manifests: tuple[ContentReference, ...] = ()
    created_at: AwareDatetime

    @field_validator("changed_metric_dates")
    @classmethod
    def canonicalize_changed_dates(cls, value: tuple[date, ...]) -> tuple[date, ...]:
        return tuple(sorted(set(value)))

    @field_validator("changed_years")
    @classmethod
    def canonicalize_changed_years(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        years = tuple(sorted(set(value)))
        if any(year < 1 or year > MAX_YEAR for year in years):
            msg = "changed_years contains an invalid year"
            raise ValueError(msg)
        return years

    @field_validator("ingestion_manifests")
    @classmethod
    def canonicalize_ingestion_manifests(
        cls,
        value: tuple[ContentReference, ...],
    ) -> tuple[ContentReference, ...]:
        uris = [reference.uri for reference in value]
        if len(uris) != len(set(uris)):
            msg = "ingestion_manifests contains duplicate URIs"
            raise ValueError(msg)
        return tuple(sorted(value, key=lambda reference: reference.uri))

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        return _required_utc(value)

    @model_validator(mode="after")
    def validate_changed_date_years(self) -> Self:
        date_years = {value.year for value in self.changed_metric_dates}
        if not date_years.issubset(self.changed_years):
            missing = sorted(date_years.difference(self.changed_years))
            msg = f"changed_years omits years used by changed_metric_dates: {missing}"
            raise ValueError(msg)
        return self


class RetentionReference(ContentReference):
    """A reference whose payload is marked by reachability GC."""

    retain_until: AwareDatetime | None = None

    @field_validator("retain_until")
    @classmethod
    def normalize_retain_until(cls, value: datetime | None) -> datetime | None:
        return _as_utc(value)


class AuditReference(FrozenModel):
    """An audit reference that intentionally does not pin its payload."""

    uri: ObjectName
    canonical_sha256: Sha256Hex
    payload_retain_until: AwareDatetime

    @field_validator("uri")
    @classmethod
    def validate_relative_object_name(cls, value: str) -> str:
        return ContentReference.validate_relative_object_name(value)

    @field_validator("payload_retain_until")
    @classmethod
    def normalize_payload_retain_until(cls, value: datetime) -> datetime:
        return _required_utc(value)


class SnapshotReferences(FrozenModel):
    """GC-retaining references and non-retaining audit references."""

    retention: tuple[RetentionReference, ...] = ()
    audit: tuple[AuditReference, ...] = ()

    @field_validator("retention")
    @classmethod
    def canonicalize_retention(
        cls,
        value: tuple[RetentionReference, ...],
    ) -> tuple[RetentionReference, ...]:
        return _unique_sorted_references(value)

    @field_validator("audit")
    @classmethod
    def canonicalize_audit(
        cls,
        value: tuple[AuditReference, ...],
    ) -> tuple[AuditReference, ...]:
        return _unique_sorted_references(value)


class RootSnapshot(FrozenModel):
    """Immutable root containing complete state and a separate commit delta."""

    schema_version: Literal[3] = 3
    generation: int = Field(ge=1)
    parent_snapshot: ContentReference | None = None
    state: SnapshotState
    change_set: ChangeSet
    references: SnapshotReferences = Field(default_factory=SnapshotReferences)

    @model_validator(mode="after")
    def validate_parent_shape(self) -> Self:
        if self.generation == 1 and self.parent_snapshot is not None:
            msg = "generation 1 must not have a parent snapshot"
            raise ValueError(msg)
        if self.generation > 1 and self.parent_snapshot is None:
            msg = "generation greater than 1 requires a parent snapshot"
            raise ValueError(msg)
        return self


class DataHead(FrozenModel):
    """The only mutable commit pointer in the data store."""

    schema_version: Literal[3] = 3
    generation: int = Field(ge=1)
    root_snapshot: ContentReference
    parent_generation: int = Field(ge=0)
    updated_at: AwareDatetime

    @field_validator("updated_at")
    @classmethod
    def normalize_updated_at(cls, value: datetime) -> datetime:
        return _required_utc(value)

    @model_validator(mode="after")
    def validate_generation_chain(self) -> Self:
        if self.parent_generation != self.generation - 1:
            msg = "parent_generation must equal generation - 1"
            raise ValueError(msg)
        return self


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return _required_utc(value)


def _required_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = "timestamp must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _unique_sorted_references(
    value: tuple[Any, ...],
) -> tuple[Any, ...]:
    uris = [reference.uri for reference in value]
    if len(uris) != len(set(uris)):
        msg = "reference list contains duplicate URIs"
        raise ValueError(msg)
    return tuple(sorted(value, key=lambda reference: reference.uri))
