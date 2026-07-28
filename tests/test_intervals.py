from datetime import UTC, datetime, timedelta

import pytest

from health_data_pipeline.intervals import TimeWindow, chunk_window, union_windows


def _time(hour: int) -> datetime:
    return datetime(2026, 1, 1, hour, tzinfo=UTC)


def test_union_preserves_unrelated_gaps() -> None:
    result = union_windows(
        [
            TimeWindow(_time(0), _time(1)),
            TimeWindow(_time(1), _time(2)),
            TimeWindow(_time(4), _time(5)),
        ]
    )
    assert result == (
        TimeWindow(_time(0), _time(2)),
        TimeWindow(_time(4), _time(5)),
    )


def test_chunk_window_uses_closed_open_boundaries() -> None:
    chunks = list(chunk_window(TimeWindow(_time(0), _time(5)), timedelta(hours=2)))
    assert chunks == [
        TimeWindow(_time(0), _time(2)),
        TimeWindow(_time(2), _time(4)),
        TimeWindow(_time(4), _time(5)),
    ]


def test_naive_window_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        TimeWindow(datetime(2026, 1, 1), datetime(2026, 1, 2))
