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


def test_sleep_daily_totals_exclude_a_separate_nap_session() -> None:
    main = {
        "dataPointName": "main",
        "sleep": {
            "interval": {"civilEndTime": _civil(28)},
            "metadata": {"mainSleep": True, "nap": False},
            "summary": {"minutesAsleep": "400"},
        },
    }
    nap = {
        "dataPointName": "nap",
        "sleep": {
            "interval": {"civilEndTime": _civil(28)},
            "metadata": {"mainSleep": False, "nap": True},
            "summary": {"minutesAsleep": "30"},
        },
    }

    observations = normalize_daily_observations([main, nap])[date(2026, 7, 28)]

    assert {item.metric_name: item.value for item in observations}["sleep_minutes"] == Decimal(400)


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


def test_sleep_stage_details_win_over_duplicated_summary() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {
                "minutesAsleep": "61",
                "minutesAwake": "0",
                "minutesInSleepPeriod": "61",
                "stagesSummary": [
                    {"type": "REM", "minutes": "90"},
                    {"type": "REM", "minutes": "90"},
                ],
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
    by_name = {item.metric_name: item for item in observations}
    assert by_name["sleep_minutes"].value == Decimal(60)
    assert by_name["sleep_rem_minutes"].value == Decimal(60)
    assert by_name["sleep_rem_minutes"].sample_count == 1


def test_sleep_stage_details_deduplicate_identical_intervals() -> None:
    interval = {
        "type": "REM",
        "startTime": "2026-07-28T14:00:00Z",
        "endTime": "2026-07-28T15:00:00Z",
    }
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {
                "minutesAsleep": "60",
                "minutesAwake": "0",
                "minutesInSleepPeriod": "60",
            },
            "stages": [interval, interval],
        }
    }

    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_minutes"].value == Decimal(60)
    assert by_name["sleep_rem_minutes"].value == Decimal(60)


def test_sleep_stage_details_fall_back_when_reported_total_differs() -> None:
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
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_minutes"].value == Decimal(90)
    assert by_name["sleep_rem_minutes"].value == Decimal(90)


def test_sleep_stage_summary_deduplicates_identical_stage_totals() -> None:
    duplicated_summary = [
        {"type": "AWAKE", "minutes": "20"},
        {"type": "LIGHT", "minutes": "60"},
        {"type": "DEEP", "minutes": "20"},
        {"type": "REM", "minutes": "40"},
    ] * 2
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {
                "minutesAsleep": "120",
                "minutesAwake": "20",
                "minutesInSleepPeriod": "140",
                "stagesSummary": duplicated_summary,
            },
        }
    }

    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_minutes"].value == Decimal(120)
    assert by_name["sleep_rem_minutes"].value == Decimal(40)
    assert by_name["sleep_deep_minutes"].value == Decimal(20)
    assert by_name["sleep_light_minutes"].value == Decimal(60)
    assert by_name["sleep_awake_minutes"].value == Decimal(20)


def test_sleep_stage_summary_rejects_conflicting_duplicate_totals() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "summary": {
                "minutesAsleep": "120",
                "minutesAwake": "20",
                "minutesInSleepPeriod": "140",
                "stagesSummary": [
                    {"type": "AWAKE", "minutes": "20"},
                    {"type": "LIGHT", "minutes": "60"},
                    {"type": "DEEP", "minutes": "20"},
                    {"type": "REM", "minutes": "40"},
                    {"type": "REM", "minutes": "41"},
                ],
            },
        }
    }

    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_minutes"].value == Decimal(120)
    assert by_name["sleep_awake_minutes"].value == Decimal(20)
    assert "sleep_rem_minutes" not in by_name
    assert "sleep_deep_minutes" not in by_name
    assert "sleep_light_minutes" not in by_name


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


def test_sleep_stage_details_use_only_the_selected_main_session() -> None:
    duplicated_first_summary = [
        {"type": "AWAKE", "minutes": "21"},
        {"type": "LIGHT", "minutes": "229"},
        {"type": "DEEP", "minutes": "54"},
        {"type": "REM", "minutes": "88"},
    ] * 2
    duplicated_second_summary = [
        {"type": "AWAKE", "minutes": "67"},
        {"type": "LIGHT", "minutes": "52"},
        {"type": "DEEP", "minutes": "3"},
        {"type": "REM", "minutes": "17"},
    ] * 2
    points = [
        {
            "sleep": {
                "interval": {
                    "endTime": "2026-07-31T23:34:00+00:00",
                    "endUtcOffset": "32400s",
                },
                "summary": {
                    "minutesAsleep": "372",
                    "minutesAwake": "21",
                    "minutesInSleepPeriod": "393",
                    "stagesSummary": duplicated_first_summary,
                },
                "stages": [
                    {
                        "type": "AWAKE",
                        "startTime": "2026-07-31T17:01:00+00:00",
                        "endTime": "2026-07-31T17:22:00+00:00",
                    },
                    {
                        "type": "LIGHT",
                        "startTime": "2026-07-31T17:22:00+00:00",
                        "endTime": "2026-07-31T21:11:30+00:00",
                    },
                    {
                        "type": "DEEP",
                        "startTime": "2026-07-31T21:11:30+00:00",
                        "endTime": "2026-07-31T22:05:30+00:00",
                    },
                    {
                        "type": "REM",
                        "startTime": "2026-07-31T22:05:30+00:00",
                        "endTime": "2026-07-31T23:34:00+00:00",
                    },
                ],
            }
        },
        {
            "sleep": {
                "interval": {
                    "endTime": "2026-08-01T07:57:00+00:00",
                    "endUtcOffset": "32400s",
                },
                "summary": {
                    "minutesAsleep": "73",
                    "minutesAwake": "67",
                    "minutesInSleepPeriod": "140",
                    "stagesSummary": duplicated_second_summary,
                },
                "stages": [
                    {
                        "type": "AWAKE",
                        "startTime": "2026-08-01T05:37:00+00:00",
                        "endTime": "2026-08-01T06:44:00+00:00",
                    },
                    {
                        "type": "LIGHT",
                        "startTime": "2026-08-01T06:44:00+00:00",
                        "endTime": "2026-08-01T07:36:00+00:00",
                    },
                    {
                        "type": "DEEP",
                        "startTime": "2026-08-01T07:36:00+00:00",
                        "endTime": "2026-08-01T07:39:30+00:00",
                    },
                    {
                        "type": "REM",
                        "startTime": "2026-08-01T07:39:30+00:00",
                        "endTime": "2026-08-01T07:57:00+00:00",
                    },
                ],
            }
        },
    ]

    observations = normalize_daily_observations(points)[date(2026, 8, 1)]
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_minutes"].value == Decimal(372)
    assert by_name["sleep_rem_minutes"].value == Decimal("88.5")
    assert by_name["sleep_deep_minutes"].value == Decimal(54)
    assert by_name["sleep_light_minutes"].value == Decimal("229.5")
    assert by_name["sleep_awake_minutes"].value == Decimal(21)


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


def test_classic_sleep_does_not_invent_rem_deep_or_light_stages() -> None:
    point = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "type": "CLASSIC",
            "summary": {
                "minutesAsleep": "100",
                "minutesAwake": "10",
                "minutesInSleepPeriod": "120",
            },
            "stages": [
                {
                    "type": "ASLEEP",
                    "startTime": "2026-07-28T14:00:00Z",
                    "endTime": "2026-07-28T15:40:00Z",
                },
                {
                    "type": "RESTLESS",
                    "startTime": "2026-07-28T15:40:00Z",
                    "endTime": "2026-07-28T15:50:00Z",
                },
                {
                    "type": "AWAKE",
                    "startTime": "2026-07-28T15:50:00Z",
                    "endTime": "2026-07-28T16:00:00Z",
                },
            ],
        }
    }

    observations = normalize_daily_observations([point])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_minutes"].value == Decimal(100)
    assert by_name["sleep_awake_minutes"].value == Decimal(10)
    assert "sleep_rem_minutes" not in by_name
    assert "sleep_deep_minutes" not in by_name
    assert "sleep_light_minutes" not in by_name


def test_main_session_selection_does_not_mix_classic_and_staged_sleep() -> None:
    staged = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T15:00:00Z",
                "endUtcOffset": "32400s",
            },
            "type": "STAGES",
            "summary": {
                "minutesAsleep": "60",
                "minutesAwake": "0",
                "minutesInSleepPeriod": "60",
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
    classic = {
        "sleep": {
            "interval": {
                "endTime": "2026-07-28T16:00:00Z",
                "endUtcOffset": "32400s",
            },
            "type": "CLASSIC",
            "summary": {
                "minutesAsleep": "100",
                "minutesAwake": "10",
                "minutesInSleepPeriod": "110",
            },
        }
    }

    observations = normalize_daily_observations([staged, classic])[date(2026, 7, 29)]
    by_name = {item.metric_name: item for item in observations}

    assert by_name["sleep_minutes"].value == Decimal(100)
    assert by_name["sleep_awake_minutes"].value == Decimal(10)
    assert "sleep_rem_minutes" not in by_name
    assert "sleep_deep_minutes" not in by_name
    assert "sleep_light_minutes" not in by_name


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
