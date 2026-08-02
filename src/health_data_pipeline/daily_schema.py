"""Strict, versioned validation for canonical daily rows."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

DAILY_SCHEMA_V1 = 1
DAILY_SCHEMA_V2 = 2
DAILY_SCHEMA_V3 = 3
SUPPORTED_DAILY_SCHEMA_VERSIONS = frozenset({DAILY_SCHEMA_V1, DAILY_SCHEMA_V2, DAILY_SCHEMA_V3})

DAILY_UNIQUE_KEY_V1 = (
    "metric_name",
    "statistic",
    "period_kind",
    "dimension_key",
)
DAILY_UNIQUE_KEY_V2 = ("record_kind", *DAILY_UNIQUE_KEY_V1)

type MetricName = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z][a-z0-9_]*$", min_length=1, max_length=128),
]
type ShortText = Annotated[str, StringConstraints(min_length=1, max_length=64)]
type DimensionKey = Annotated[str, StringConstraints(max_length=256)]
type NonEmptyDimensionKey = Annotated[
    str,
    StringConstraints(min_length=1, max_length=256),
]
type ValueText = Annotated[str, StringConstraints(max_length=2048)]
type FlagText = Annotated[str, StringConstraints(min_length=1, max_length=128)]
type SourceDataType = Annotated[str, StringConstraints(min_length=1, max_length=256)]
type SessionText = Annotated[str, StringConstraints(min_length=1, max_length=128)]

type Statistic = Literal[
    "observed",
    "mean",
    "delta",
    "delta_percent",
    "min",
    "max",
    "coverage",
]
type DataStatus = Literal[
    "PROVISIONAL",
    "CANONICAL_SYNCED",
    "CANONICAL_STALE",
    "MISSING",
    "INSUFFICIENT_DATA",
]
type CoverageKind = Literal[
    "UNKNOWN",
    "OBSERVATION_COUNT",
    "CALENDAR_DAYS",
    "NOT_APPLICABLE",
]
type TimeZoneSource = Literal[
    "GOOGLE_HEALTH_SETTINGS",
    "CONFIG_DEFAULT",
    "LEGACY_UNKNOWN",
]
type SessionRole = Literal["MAIN", "NAP", "UNCLASSIFIED"]
type SessionRoleSource = Literal["SOURCE_FLAG", "INFERRED", "UNCLASSIFIED"]


class DailySchemaValidationError(ValueError):
    """A daily row does not match its declared immutable schema version."""


class _DailyRowCommon(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    metric_name: MetricName
    statistic: Statistic
    dimension_key: DimensionKey | None
    value_num: Decimal | None
    value_text: ValueText | None
    unit: ShortText
    aggregation: ShortText
    period_start: date
    period_end: date
    sample_count: Annotated[int, Field(ge=0)]
    coverage_ratio: Annotated[Decimal, Field(ge=0, le=1)] | None
    data_status: DataStatus
    quality_flags: list[FlagText]
    source_data_types: list[SourceDataType]
    fitbit_last_sync_time: AwareDatetime | None
    time_zone: ShortText
    physical_day_start: AwareDatetime
    physical_day_end: AwareDatetime
    logic_version: ShortText

    @field_validator("period_start", "period_end", mode="before")
    @classmethod
    def parse_canonical_date(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("date must use ISO YYYY-MM-DD format") from exc
        if parsed.isoformat() != value:
            raise ValueError("date must use canonical ISO YYYY-MM-DD format")
        return parsed

    @field_validator(
        "fitbit_last_sync_time",
        "physical_day_start",
        "physical_day_end",
        mode="before",
    )
    @classmethod
    def parse_aware_datetime(cls, value: object) -> object:
        if value is None or not isinstance(value, str):
            return value
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("timestamp must use ISO date-time format") from exc

    @field_validator("value_num", "coverage_ratio", mode="before")
    @classmethod
    def require_decimal_number(cls, value: object) -> object:
        if value is None or isinstance(value, Decimal):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return Decimal(value)
        raise ValueError("number must be an int or Decimal")

    @field_validator("quality_flags", "source_data_types")
    @classmethod
    def require_unique_array_items(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("array items must be unique")
        return value

    @model_validator(mode="after")
    def validate_common_intervals(self) -> Self:
        if self.period_start > self.period_end:
            raise ValueError("period_start must not be after period_end")
        if self.physical_day_start >= self.physical_day_end:
            raise ValueError("physical_day_start must be before physical_day_end")
        return self


class DailyRowV1(_DailyRowCommon):
    """The immutable v1 row contract."""

    period_kind: Literal["DAY", "CALENDAR_WEEK", "CALENDAR_MONTH"]
    expected_count: Annotated[int, Field(ge=0)]


class DailyRowV2(_DailyRowCommon):
    """The immutable v2 row contract."""

    record_kind: Literal["METRIC", "SLEEP_SESSION"]
    period_kind: Literal["DAY", "SESSION", "CALENDAR_WEEK", "CALENDAR_MONTH"]
    expected_count: Annotated[int, Field(ge=0)]
    session_start_time: AwareDatetime | None
    session_end_time: AwareDatetime | None
    session_start_utc_offset_seconds: Annotated[int, Field(ge=-64800, le=64800)] | None
    session_end_utc_offset_seconds: Annotated[int, Field(ge=-64800, le=64800)] | None
    session_is_main: bool | None
    session_is_nap: bool | None

    @field_validator("session_start_time", "session_end_time", mode="before")
    @classmethod
    def parse_session_datetime(cls, value: object) -> object:
        return cls.parse_aware_datetime(value)


class _DailyRowV3Base(_DailyRowCommon):
    record_kind: Literal["METRIC", "SLEEP_SESSION", "SOURCE_STATUS"]
    period_kind: Literal["DAY", "SESSION", "CALENDAR_WEEK", "CALENDAR_MONTH"]
    session_start_time: AwareDatetime | None
    session_end_time: AwareDatetime | None
    session_start_utc_offset_seconds: Annotated[int, Field(ge=-64800, le=64800)] | None
    session_end_utc_offset_seconds: Annotated[int, Field(ge=-64800, le=64800)] | None
    session_is_main: bool | None
    session_is_nap: bool | None
    missing_reason: ValueText | None
    coverage_kind: CoverageKind
    expected_count: Annotated[int, Field(ge=0)] | None
    time_zone_source: TimeZoneSource
    session_role: SessionRole | None
    session_role_source: SessionRoleSource | None
    session_processed: bool | None
    session_manually_edited: bool | None
    session_stages_status: SessionText | None
    session_sleep_type: SessionText | None
    session_create_time: AwareDatetime | None
    session_update_time: AwareDatetime | None

    @field_validator("session_start_time", "session_end_time", mode="before")
    @classmethod
    def parse_session_datetime(cls, value: object) -> object:
        return cls.parse_aware_datetime(value)

    @field_validator("session_create_time", "session_update_time", mode="before")
    @classmethod
    def parse_session_metadata_datetime(cls, value: object) -> object:
        return cls.parse_aware_datetime(value)

    @model_validator(mode="after")
    def validate_coverage_contract(self) -> Self:
        if self.coverage_kind in {"UNKNOWN", "NOT_APPLICABLE"}:
            if self.expected_count is not None or self.coverage_ratio is not None:
                raise ValueError(
                    "UNKNOWN and NOT_APPLICABLE coverage require null expected_count and "
                    "coverage_ratio"
                )
        elif self.expected_count is None:
            raise ValueError("counted coverage requires expected_count")
        return self


class MetricRowV3(_DailyRowV3Base):
    """A scalar metric, optionally attached to a separate session identity."""

    record_kind: Literal["METRIC"]
    period_kind: Literal["DAY", "SESSION", "CALENDAR_WEEK", "CALENDAR_MONTH"]
    session_start_time: None
    session_end_time: None
    session_start_utc_offset_seconds: None
    session_end_utc_offset_seconds: None
    session_is_main: None
    session_is_nap: None
    session_role: None
    session_role_source: None
    session_processed: None
    session_manually_edited: None
    session_stages_status: None
    session_sleep_type: None
    session_create_time: None
    session_update_time: None

    @model_validator(mode="after")
    def validate_metric_semantics(self) -> Self:
        if self.metric_name == "source_status":
            raise ValueError("metric_name source_status is reserved for SOURCE_STATUS rows")
        if self.data_status == "MISSING":
            if self.value_num is not None or self.value_text is not None:
                raise ValueError("MISSING metrics require null values")
            if self.missing_reason is None:
                raise ValueError("MISSING metrics require a missing_reason")
        if self.period_kind == "SESSION" and not self.dimension_key:
            raise ValueError("SESSION metrics require a dimension_key")
        if (
            self.period_kind in {"CALENDAR_WEEK", "CALENDAR_MONTH"}
            and self.coverage_kind != "CALENDAR_DAYS"
        ):
            raise ValueError("calendar metrics require CALENDAR_DAYS coverage")
        return self


class SleepSessionRowV3(_DailyRowV3Base):
    """One typed sleep-session boundary and its normalized classification."""

    record_kind: Literal["SLEEP_SESSION"]
    metric_name: Literal["sleep_session"]
    statistic: Literal["observed"]
    period_kind: Literal["SESSION"]
    dimension_key: NonEmptyDimensionKey
    unit: Literal["min"]
    aggregation: Literal["session_value"]
    coverage_kind: Literal["NOT_APPLICABLE"]
    expected_count: None
    coverage_ratio: None
    session_start_time: AwareDatetime
    session_end_time: AwareDatetime
    session_start_utc_offset_seconds: Annotated[int, Field(ge=-64800, le=64800)]
    session_end_utc_offset_seconds: Annotated[int, Field(ge=-64800, le=64800)]
    session_is_main: bool
    session_is_nap: bool
    session_role: SessionRole
    session_role_source: SessionRoleSource

    @model_validator(mode="after")
    def validate_session_semantics(self) -> Self:
        if self.value_num is not None and self.value_num < 0:
            raise ValueError("sleep session value_num must be non-negative")
        if self.session_start_time >= self.session_end_time:
            raise ValueError("session_start_time must be before session_end_time")
        if self.session_role == "MAIN" and not (
            self.session_is_main or self.session_role_source == "INFERRED"
        ):
            raise ValueError("MAIN role must be source-flagged or inferred")
        if self.session_role == "NAP" and not self.session_is_nap:
            raise ValueError("NAP role requires session_is_nap")
        return self


class SourceStatusRowV3(_DailyRowV3Base):
    """One explicit result row for a queried source data type."""

    record_kind: Literal["SOURCE_STATUS"]
    metric_name: Literal["source_status"]
    statistic: Literal["observed"]
    period_kind: Literal["DAY"]
    dimension_key: NonEmptyDimensionKey
    value_num: Decimal
    value_text: Literal[
        "SUCCESS_WITH_DATA",
        "SUCCESS_EMPTY",
        "SOURCE_PROCESSING",
        "INVALID_SOURCE",
    ]
    unit: Literal["records"]
    aggregation: Literal["query_result"]
    coverage_kind: Literal["NOT_APPLICABLE"]
    expected_count: None
    coverage_ratio: None
    session_start_time: None
    session_end_time: None
    session_start_utc_offset_seconds: None
    session_end_utc_offset_seconds: None
    session_is_main: None
    session_is_nap: None
    session_role: None
    session_role_source: None
    session_processed: None
    session_manually_edited: None
    session_stages_status: None
    session_sleep_type: None
    session_create_time: None
    session_update_time: None

    @model_validator(mode="after")
    def validate_source_status_semantics(self) -> Self:
        if self.value_num < 0 or self.value_num != self.value_num.to_integral_value():
            raise ValueError("SOURCE_STATUS value_num must be a non-negative record count")
        if self.period_start != self.period_end:
            raise ValueError("SOURCE_STATUS period must be exactly one day")
        if self.source_data_types != [self.dimension_key]:
            raise ValueError("SOURCE_STATUS source_data_types must match dimension_key")
        if self.value_text == "SUCCESS_EMPTY" and (self.value_num != 0 or self.sample_count != 0):
            raise ValueError("SUCCESS_EMPTY requires zero source and normalized counts")
        if self.value_text == "SUCCESS_WITH_DATA" and (
            self.value_num == 0 or self.sample_count == 0
        ):
            raise ValueError("SUCCESS_WITH_DATA requires positive source and normalized counts")
        if self.value_text == "SOURCE_PROCESSING" and self.value_num == 0:
            raise ValueError("SOURCE_PROCESSING requires a positive source count")
        if self.value_text == "INVALID_SOURCE" and (self.value_num == 0 or self.sample_count != 0):
            raise ValueError(
                "INVALID_SOURCE requires a positive source count and zero normalized count"
            )
        return self


type DailyRowV3 = Annotated[
    MetricRowV3 | SleepSessionRowV3 | SourceStatusRowV3,
    Field(discriminator="record_kind"),
]
_V3_ADAPTER: TypeAdapter[DailyRowV3] = TypeAdapter(DailyRowV3)


def daily_unique_key_fields(schema_version: int) -> tuple[str, ...]:
    """Return the immutable uniqueness contract for a supported row version."""

    require_supported_daily_schema(schema_version)
    return DAILY_UNIQUE_KEY_V1 if schema_version == DAILY_SCHEMA_V1 else DAILY_UNIQUE_KEY_V2


def require_supported_daily_schema(schema_version: int) -> None:
    if isinstance(schema_version, bool) or schema_version not in SUPPORTED_DAILY_SCHEMA_VERSIONS:
        raise DailySchemaValidationError(f"unsupported daily schema version: {schema_version!r}")


def validate_daily_row(
    row: Mapping[str, object],
    *,
    schema_version: int,
    metric_date: date,
) -> None:
    """Validate one Python row without changing its canonical representation."""

    require_supported_daily_schema(schema_version)
    try:
        if schema_version == DAILY_SCHEMA_V1:
            DailyRowV1.model_validate(row)
        elif schema_version == DAILY_SCHEMA_V2:
            DailyRowV2.model_validate(row)
        else:
            validated = _V3_ADAPTER.validate_python(row, strict=True)
    except ValidationError as exc:
        raise DailySchemaValidationError(
            f"daily row does not match schema v{schema_version}: {exc}"
        ) from exc

    if schema_version == DAILY_SCHEMA_V3:
        _validate_v3_partition(validated, metric_date=metric_date)


def _validate_v3_partition(validated: DailyRowV3, *, metric_date: date) -> None:
    if validated.period_end != metric_date:
        raise DailySchemaValidationError(
            "daily row does not match schema v3: "
            "period_end must match the daily partition metric_date"
        )
    if validated.period_kind in {"DAY", "SESSION"} and validated.period_start != metric_date:
        raise DailySchemaValidationError(
            "daily row does not match schema v3: "
            "DAY and SESSION period_start must match the daily partition metric_date"
        )


def validate_daily_rows(
    rows: list[Mapping[str, object]],
    *,
    schema_version: int,
    metric_date: date,
) -> None:
    require_supported_daily_schema(schema_version)
    for row in rows:
        validate_daily_row(row, schema_version=schema_version, metric_date=metric_date)
