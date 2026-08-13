"""Time-window helpers shared by webhook reconciliation and backfill."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True, order=True)
class TimeWindow:
    """A closed-open physical time window."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("time windows require timezone-aware datetimes")
        if self.start >= self.end:
            raise ValueError("window start must be before end")


def union_windows(windows: Iterable[TimeWindow]) -> tuple[TimeWindow, ...]:
    """Merge overlapping or adjacent windows without spanning unrelated gaps."""

    ordered = sorted(windows)
    if not ordered:
        return ()
    merged: list[TimeWindow] = [ordered[0]]
    for window in ordered[1:]:
        previous = merged[-1]
        if window.start <= previous.end:
            merged[-1] = TimeWindow(previous.start, max(previous.end, window.end))
        else:
            merged.append(window)
    return tuple(merged)


def chunk_window(window: TimeWindow, maximum: timedelta) -> Iterator[TimeWindow]:
    """Split a window into API-safe closed-open chunks."""

    if maximum <= timedelta(0):
        raise ValueError("maximum chunk duration must be positive")
    cursor = window.start
    while cursor < window.end:
        chunk_end = min(cursor + maximum, window.end)
        yield TimeWindow(cursor, chunk_end)
        cursor = chunk_end
