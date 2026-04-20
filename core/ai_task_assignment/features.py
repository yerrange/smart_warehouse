from __future__ import annotations

from datetime import datetime

from django.utils import timezone

from core.models import Task, Shift, EmployeeShiftStats


def build_feature_dict(*, task: Task, shift: Shift, stats: EmployeeShiftStats, ts: datetime) -> dict:
    """
    Собираем ровно те фичи, которые были в датасете.
    Порядок потом выставим по meta.feature_cols.
    """
    # task_age_minutes – сколько минут задача “в системе” до назначения
    if task.created_at:
        task_age_minutes = int((ts - task.created_at).total_seconds() // 60)
        if task_age_minutes < 0:
            task_age_minutes = 0
    else:
        task_age_minutes = 0

    # minutes_into_shift – сколько минут идёт смена на момент назначения
    if shift.start_time:
        minutes_into_shift = int((ts - shift.start_time).total_seconds() // 60)
        if minutes_into_shift < 0:
            minutes_into_shift = 0
    else:
        minutes_into_shift = 0

    return {
        "task_type": task.task_type,
        "priority": int(task.priority or 0),
        "difficulty": int(task.difficulty or 1),
        "estimated_minutes": int(task.estimated_minutes or 0),
        "task_age_minutes": int(task_age_minutes),
        "emp_task_assigned_count": int(stats.task_assigned_count or 0),
        "emp_task_completed_count": int(stats.task_completed_count or 0),
        "emp_shift_score": int(stats.shift_score or 0),
        "minutes_into_shift": int(minutes_into_shift),
    }


def to_row(feature_dict: dict, feature_cols: list[str]) -> list:
    """Собираем вектор фич строго в порядке обучения."""
    return [feature_dict.get(col) for col in feature_cols]
