"""Deterministic Notion document rendering for daily health views."""

from __future__ import annotations

import hashlib
from datetime import datetime

from health_data_pipeline.application.views.models import (
    DailyMetric,
    DailyView,
    SleepSessionView,
    canonical_view_json,
)
from health_data_pipeline.metrics import METRIC_DEFINITIONS

_NOTION_METRIC_LABELS = {
    definition.name: definition.notion_label for definition in METRIC_DEFINITIONS
}
_NOTION_METRIC_UNITS = {definition.name: definition.unit for definition in METRIC_DEFINITIONS}
_NOTION_METRIC_ORDER = {
    metric_name: index for index, metric_name in enumerate(_NOTION_METRIC_LABELS)
}


def _format_value(value: int | float | None) -> str:
    if value is None:
        return "欠損"
    if isinstance(value, float):
        return format(value, ".6g")
    return str(value)


def _format_coverage(value: float | None) -> str:
    return "unknown" if value is None else f"{value * 100:.1f}%"


def _notion_metric_sort_key(metric: DailyMetric) -> tuple[int, int | str, str]:
    order = _NOTION_METRIC_ORDER.get(metric.metric_name)
    if order is not None:
        return (0, order, metric.statistic)
    return (1, metric.metric_name, metric.statistic)


def _format_session_time(value: datetime) -> str:
    return value.isoformat(timespec="minutes")


def _format_sleep_session(label: str, session: SleepSessionView) -> str:
    duration = (
        ""
        if session.minutes_asleep is None
        else f" / 睡眠時間: {_format_value(session.minutes_asleep)}分"
    )
    details: list[str] = []
    if session.role_source == "INFERRED":
        details.append("主睡眠判定: 推定")
    if session.processed is False:
        details.append("Fitbit処理中")
    if session.stages_status:
        details.append(f"睡眠ステージ状態: {session.stages_status}")
    if session.manually_edited is True:
        details.append("手動編集あり")
    suffix = "" if not details else " / " + " / ".join(details)
    return (
        f"- {label}: 就寝時刻 {_format_session_time(session.start_time)} / "
        f"起床時刻 {_format_session_time(session.end_time)}{duration}{suffix}"
    )


def _sleep_session_lines(view: DailyView) -> list[str]:
    if not view.sleep_sessions_available:
        if view.sleep_source_status == "SUCCESS_EMPTY":
            return ["## 睡眠セッション", "主睡眠: 記録なし", "昼寝: なし", ""]
        if view.sleep_source_status == "SOURCE_PROCESSING":
            return ["## 睡眠セッション", "主睡眠: Fitbit処理中", "昼寝: 判定待ち", ""]
        return ["## 睡眠セッション", "主睡眠: 未取得", "昼寝: 未取得", ""]

    ordered = tuple(sorted(view.sleep_sessions, key=lambda item: (item.start_time, item.end_time)))
    main = tuple(session for session in ordered if session.is_main)
    naps = tuple(session for session in ordered if session.is_nap and not session.is_main)
    unknown = tuple(session for session in ordered if not session.is_main and not session.is_nap)
    lines = ["## 睡眠セッション"]
    if main:
        lines.extend(
            _format_sleep_session(
                "主睡眠" if len(main) == 1 else f"主睡眠 {index}",
                session,
            )
            for index, session in enumerate(main, start=1)
        )
    else:
        lines.append("主睡眠: 未取得")
    if naps:
        lines.extend(
            _format_sleep_session(f"昼寝 {index}", session)
            for index, session in enumerate(naps, start=1)
        )
    else:
        lines.append("昼寝: なし")
    lines.extend(
        _format_sleep_session(f"未分類の睡眠 {index}", session)
        for index, session in enumerate(unknown, start=1)
    )
    lines.append("")
    return lines


def render_notion_body(view: DailyView) -> str:
    """Render a stable, compact table intended for ChatGPT consumption."""

    metrics = list(view.metrics)
    present = {(metric.metric_name, metric.statistic) for metric in metrics}
    metrics.extend(
        DailyMetric(
            metric_name=metric_name,
            statistic="value",
            value=None,
            unit=_NOTION_METRIC_UNITS[metric_name],
            status="MISSING",
            missing_reason="NOT_QUERIED",
        )
        for metric_name in _NOTION_METRIC_LABELS
        if (metric_name, "value") not in present
    )

    lines = [
        f"Date: {view.metric_date.isoformat()}",
        f"Data status: {view.data_status}",
        f"Source HEAD generation: {view.source_generation}",
        "Fitbit last sync time: "
        + (
            view.fitbit_last_sync_time.isoformat()
            if view.fitbit_last_sync_time is not None
            else "unknown"
        ),
        f"Time zone: {view.time_zone or 'unknown'}",
        f"Time zone source: {view.time_zone_source or 'unknown'}",
        "Physical day: "
        + (
            f"{view.physical_day_start.isoformat()} / {view.physical_day_end.isoformat()}"
            if view.physical_day_start is not None and view.physical_day_end is not None
            else "unknown"
        ),
        "",
        *_sleep_session_lines(view),
        (
            "| Metric | Value | Unit | Calendar week avg "
            "(Mon-date; final Sun) | Week coverage | Week status | "
            "Calendar month avg (1st-date) | Month coverage | Month status | "
            "Daily status | Daily coverage |"
        ),
        "|---|---:|---|---:|---:|---|---:|---:|---|---|---:|",
    ]
    for metric in sorted(metrics, key=_notion_metric_sort_key):
        metric_label = _NOTION_METRIC_LABELS.get(metric.metric_name, metric.metric_name)
        label = (
            metric_label if metric.statistic == "value" else f"{metric_label} ({metric.statistic})"
        )
        coverage = f"{_format_coverage(metric.coverage)} ({metric.coverage_kind})"
        flags = ",".join(sorted(metric.quality_flags))
        missing = "" if metric.missing_reason is None else f"; {metric.missing_reason}"
        status = metric.status + missing if not flags else f"{metric.status}{missing}; {flags}"
        weekly_flags = ",".join(sorted(metric.weekly_quality_flags))
        weekly_status = metric.weekly_status or "unknown"
        if weekly_flags:
            weekly_status += f"; {weekly_flags}"
        monthly_flags = ",".join(sorted(metric.monthly_quality_flags))
        monthly_status = metric.monthly_status or "unknown"
        if monthly_flags:
            monthly_status += f"; {monthly_flags}"
        lines.append(
            "| "
            + " | ".join(
                (
                    label,
                    _format_value(metric.value),
                    metric.unit or "",
                    _format_value(metric.weekly_average),
                    _format_coverage(metric.weekly_coverage),
                    weekly_status,
                    _format_value(metric.monthly_average),
                    _format_coverage(metric.monthly_coverage),
                    monthly_status,
                    status,
                    coverage,
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def notion_content_hash(view: DailyView, body: str) -> str:
    """Hash only deterministic view content, never a write timestamp."""

    payload = {
        "metric_date": view.metric_date.isoformat(),
        "source_generation": view.source_generation,
        "data_status": view.data_status,
        "fitbit_last_sync_time": (
            view.fitbit_last_sync_time.isoformat() if view.fitbit_last_sync_time else None
        ),
        "body": body,
    }
    return hashlib.sha256(canonical_view_json(payload)).hexdigest()


class DefaultNotionRenderer:
    """Default deterministic renderer injected by the compatibility facade."""

    def render_body(self, view: DailyView) -> str:
        return render_notion_body(view)

    def content_hash(self, view: DailyView, body: str) -> str:
        return notion_content_hash(view, body)
