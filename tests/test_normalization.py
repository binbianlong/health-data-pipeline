from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from typing import Any

from health_data_pipeline.normalization import normalize_daily_observations


def _civil(day: int) -> dict[str, object]:
    return {"date": {"year": 2026, "month": 7, "day": day}}


def test_normalizes_core_fitbit_metrics() -> None:
    points: list[Mapping[str, Any]] = [
        {
            "steps": {
                "interval": {"civilStartTime": _civil(28)},
                "count": "4000",
            }
        },
        {
            "steps": {
                "interval": {"civilStartTime": _civil(28)},
                "count": "4432",
            }
        },
        {
            "dailyHeartRateVariability": {
                "date": {"year": 2026, "month": 7, "day": 28},
                "averageHeartRateVariabilityMilliseconds": 42.5,
            }
        },
        {
            "dailyOxygenSaturation": {
                "date": {"year": 2026, "month": 7, "day": 28},
                "averagePercentage": 96.2,
            }
        },
        {
            "sleep": {
                "interval": {"civilEndTime": _civil(28)},
                "summary": {"minutesAsleep": "430"},
            }
        },
    ]
    result = normalize_daily_observations(points)[date(2026, 7, 28)]
    by_name = {item.metric_name: item for item in result}
    assert by_name["steps"].value == Decimal(8432)
    assert by_name["hrv_rmssd"].value == Decimal("42.5")
    assert by_name["spo2"].value == Decimal("96.2")
    assert by_name["sleep_minutes"].value == Decimal(430)


def test_sleep_uses_civil_end_date() -> None:
    point = {
        "sleep": {
            "interval": {
                "civilStartTime": _civil(27),
                "civilEndTime": _civil(28),
            },
            "summary": {"minutesAsleep": "400"},
        }
    }
    result = normalize_daily_observations([point])
    assert date(2026, 7, 28) in result
    assert date(2026, 7, 27) not in result


def test_sleep_prefers_civil_end_date_over_physical_end_date() -> None:
    point = {
        "sleep": {
            "interval": {
                "civilEndTime": _civil(28),
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {"minutesAsleep": "400"},
        }
    }
    result = normalize_daily_observations([point])
    assert date(2026, 7, 28) in result
    assert date(2026, 7, 29) not in result


def test_sleep_uses_physical_end_time_and_utc_offset_with_stage_summaries() -> None:
    point = {
        "sleep": {
            "interval": {
                "startTime": "2026-07-28T08:00:00Z",
                "startUtcOffset": "32400s",
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {
                "minutesAsleep": "420",
                "stagesSummary": [
                    {"type": "REM", "minutes": "90"},
                    {"type": "DEEP", "minutes": "60"},
                    {"type": "LIGHT", "minutes": "270"},
                    {"type": "AWAKE", "minutes": "30"},
                ],
            },
        }
    }
    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}
    assert by_name["sleep_minutes"].value == Decimal(420)
    assert by_name["sleep_rem_minutes"].value == Decimal(90)
    assert by_name["sleep_deep_minutes"].value == Decimal(60)
    assert by_name["sleep_light_minutes"].value == Decimal(270)
    assert by_name["sleep_awake_minutes"].value == Decimal(30)


def test_sleep_stage_summary_wins_over_detailed_intervals() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {
                "minutesAsleep": "90",
                "stagesSummary": [{"type": "REM", "minutes": "90"}],
            },
            "stages": [
                {
                    "type": "REM",
                    "startTime": "2026-07-28T14:00:00Z",
                    "endTime": "2026-07-28T15:00:00Z",
                }
            ],
        }
    }
    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    rem = next(item for item in observations if item.metric_name == "sleep_rem_minutes")
    assert rem.value == Decimal(90)
    assert rem.sample_count == 1


def test_sleep_stages_fall_back_to_detailed_intervals() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {"minutesAsleep": "60"},
            "stages": [
                {
                    "type": "DEEP",
                    "startTime": "2026-07-28T14:00:00Z",
                    "endTime": "2026-07-28T14:30:00Z",
                },
                {
                    "type": "LIGHT",
                    "startTime": "2026-07-28T14:30:00Z",
                    "endTime": "2026-07-28T15:00:00Z",
                },
            ],
        }
    }
    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}
    assert by_name["sleep_deep_minutes"].value == Decimal(30)
    assert by_name["sleep_light_minutes"].value == Decimal(30)


def test_sleep_awake_minutes_fall_back_to_summary_total() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {
                "minutesAsleep": "400",
                "minutesAwake": "35",
            },
        }
    }

    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_awake_minutes"].value == Decimal(35)


def test_malformed_physical_sleep_interval_falls_back_to_civil_end_date() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "not-a-duration",
                "civilEndTime": _civil(28),
            },
            "summary": {"minutesAsleep": "400"},
        }
    }
    assert date(2026, 7, 28) in normalize_daily_observations([point])


def test_physical_sleep_interval_without_utc_offset_is_ignored() -> None:
    point = {
        "sleep": {
            "interval": {"endTime": "2026-07-28T16:00:00Z"},
            "summary": {"minutesAsleep": "400"},
        }
    }
    assert normalize_daily_observations([point]) == {}


def test_unknown_union_field_is_ignored() -> None:
    assert normalize_daily_observations([{"futureMetric": {"value": 1}}]) == {}


def test_exercise_duration_converts_seconds_to_minutes() -> None:
    point = {
        "exercise": {
            "interval": {"civilStartTime": _civil(28)},
            "activeDuration": "1800s",
        }
    }
    observation = normalize_daily_observations([point])[date(2026, 7, 28)][0]
    assert observation.value == Decimal(30)


def test_exercise_uses_physical_start_time_and_utc_offset() -> None:
    point = {
        "exercise": {
            "interval": {
                "startTime": "2026-07-28T16:15:00Z",
                "startUtcOffset": "32400s",
                "endTime": "2026-07-28T16:45:00Z",
                "endUtcOffset": "32400s",
            },
            "activeDuration": "1800s",
        }
    }
    observation = normalize_daily_observations([point])[date(2026, 7, 29)][0]
    assert observation.metric_name == "exercise_minutes"
    assert observation.value == Decimal(30)


def test_exercise_prefers_civil_start_date_over_physical_start_date() -> None:
    point = {
        "exercise": {
            "interval": {
                "civilStartTime": _civil(28),
                "startTime": "2026-07-28T16:15:00Z",
                "startUtcOffset": "32400s",
            },
            "activeDuration": "1800s",
        }
    }
    result = normalize_daily_observations([point])
    assert date(2026, 7, 28) in result
    assert date(2026, 7, 29) not in result


def test_daily_summary_wins_over_detail_for_same_metric() -> None:
    observations = normalize_daily_observations(
        [
            {
                "heartRateVariability": {
                    "rootMeanSquareOfSuccessiveDifferencesMilliseconds": 10,
                    "sampleTime": {"civilTime": _civil(28)},
                }
            },
            {
                "dailyHeartRateVariability": {
                    "averageHeartRateVariabilityMilliseconds": 42,
                    "date": {"year": 2026, "month": 7, "day": 28},
                }
            },
        ]
    )[date(2026, 7, 28)]
    hrv = next(item for item in observations if item.metric_name == "hrv_rmssd")
    assert hrv.value == Decimal(42)
    assert hrv.aggregation == "daily_value"
