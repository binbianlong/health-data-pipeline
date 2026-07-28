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
