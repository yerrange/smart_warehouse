from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from core.models import (
    EmployeeShiftStats,
    EmployeeTaskProfile,
    EmployeeTaskQualificationModifier,
    Shift,
    Task,
)


FEATURE_COLS = [
    "task_type",
    "priority",
    "difficulty",
    "estimated_minutes",
    "task_age_minutes",
    "emp_task_assigned_count",
    "emp_task_completed_count",
    "emp_shift_score",
    "minutes_into_shift",
    "emp_profile_performance_factor",
    "emp_profile_sigma",
    "emp_profile_sample_count",
    "emp_profile_mean_minutes",
    "emp_profile_has_mean",
    "emp_profile_source_kind",
    "task_required_qual_count",
    "emp_modifier_factor_product",
    "emp_modifier_sigma_bonus_sum",
    "emp_modifier_sample_count_sum",
    "emp_required_modifier_count",
]

CAT_COLS = ["task_type", "emp_profile_source_kind"]

DEFAULT_REQUIRED_QUAL_CODES_BY_TASK_TYPE = {
    Task.TaskType.RECEIVE_TO_INBOUND: ("RECEIVE",),
    Task.TaskType.PUTAWAY_TO_RACK: ("MOVE",),
    Task.TaskType.MOVE_BETWEEN_SLOTS: ("MOVE",),
    Task.TaskType.DISPATCH_CARGO: ("DISPATCH",),
    Task.TaskType.GENERAL: (),
}


def build_feature_dict(
    *,
    task: Task,
    shift: Shift,
    stats: EmployeeShiftStats,
    ts: datetime,
) -> dict[str, Any]:
    """
    Собирает признаки для первой рабочей ML-модели назначения задач.

    Профиль сотрудника по типу задачи считается обязательной частью
    признакового описания. Если профиль отсутствует, функция выбрасывает
    ошибку, а runtime переведёт назначение в heuristic_fallback.
    """
    profile = _get_employee_task_profile(task=task, stats=stats)
    required_codes = _get_required_qualification_codes(task)
    modifier_features = _build_modifier_features(
        profile=profile,
        required_codes=required_codes,
    )

    mean_minutes = profile.mean_minutes
    has_mean = _is_finite_number(mean_minutes)

    return {
        "task_type": str(task.task_type or "unknown"),
        "priority": int(task.priority or 0),
        "difficulty": int(task.difficulty or 1),
        "estimated_minutes": int(task.estimated_minutes or 0),
        "task_age_minutes": _minutes_between(task.created_at, ts),
        "emp_task_assigned_count": int(stats.task_assigned_count or 0),
        "emp_task_completed_count": int(stats.task_completed_count or 0),
        "emp_shift_score": int(stats.shift_score or 0),
        "minutes_into_shift": _minutes_between(shift.start_time, ts),
        "emp_profile_performance_factor": _safe_float(
            profile.performance_factor,
            default=1.0,
        ),
        "emp_profile_sigma": _safe_float(profile.sigma, default=0.10),
        "emp_profile_sample_count": int(profile.sample_count or 0),
        "emp_profile_mean_minutes": (
            _safe_float(mean_minutes, default=0.0)
            if has_mean
            else 0.0
        ),
        "emp_profile_has_mean": 1 if has_mean else 0,
        "emp_profile_source_kind": str(profile.source_kind or "unknown"),
        "task_required_qual_count": len(required_codes),
        **modifier_features,
    }


def to_row(feature_dict: dict[str, Any], feature_cols: list[str]) -> list[Any]:
    """Собирает вектор признаков строго в порядке, сохранённом в meta.json."""
    missing = [col for col in feature_cols if col not in feature_dict]

    if missing:
        raise KeyError(
            "Feature dict does not contain required model columns: "
            f"{', '.join(missing)}"
        )

    return [feature_dict[col] for col in feature_cols]


def _get_employee_task_profile(
    *,
    task: Task,
    stats: EmployeeShiftStats,
) -> EmployeeTaskProfile:
    profile = (
        EmployeeTaskProfile.objects
        .filter(employee_id=stats.employee_id, task_type=task.task_type)
        .only(
            "id",
            "employee_id",
            "task_type",
            "performance_factor",
            "sigma",
            "sample_count",
            "mean_minutes",
            "source_kind",
        )
        .first()
    )

    if profile is None:
        employee_code = getattr(stats.employee, "employee_code", stats.employee_id)
        raise ValueError(
            "EmployeeTaskProfile not found for "
            f"employee={employee_code}, task_type={task.task_type}"
        )

    return profile


def _build_modifier_features(
    *,
    profile: EmployeeTaskProfile,
    required_codes: set[str],
) -> dict[str, Any]:
    if not required_codes:
        return {
            "emp_modifier_factor_product": 1.0,
            "emp_modifier_sigma_bonus_sum": 0.0,
            "emp_modifier_sample_count_sum": 0,
            "emp_required_modifier_count": 0,
        }

    modifiers = (
        EmployeeTaskQualificationModifier.objects
        .select_related("employee_qualification__qualification")
        .filter(
            profile_id=profile.id,
            employee_qualification__qualification__code__in=required_codes,
        )
        .only(
            "factor",
            "sigma_bonus",
            "sample_count",
            "employee_qualification__qualification__code",
        )
    )

    factor_product = 1.0
    sigma_bonus_sum = 0.0
    sample_count_sum = 0
    modifier_count = 0

    for modifier in modifiers:
        factor_product *= _safe_float(modifier.factor, default=1.0)
        sigma_bonus_sum += _safe_float(modifier.sigma_bonus, default=0.0)
        sample_count_sum += int(modifier.sample_count or 0)
        modifier_count += 1

    return {
        "emp_modifier_factor_product": factor_product,
        "emp_modifier_sigma_bonus_sum": sigma_bonus_sum,
        "emp_modifier_sample_count_sum": sample_count_sum,
        "emp_required_modifier_count": modifier_count,
    }


def _get_required_qualification_codes(task: Task) -> set[str]:
    cached_attr = "_ai_required_qualification_codes"
    cached = getattr(task, cached_attr, None)

    if cached is not None:
        return set(cached)

    codes = {
        str(qualification.code)
        for qualification in task.required_qualifications.all()
        if getattr(qualification, "code", None)
    }

    if not codes:
        codes = set(
            DEFAULT_REQUIRED_QUAL_CODES_BY_TASK_TYPE.get(task.task_type, ())
        )

    setattr(task, cached_attr, tuple(sorted(codes)))
    return codes


def _minutes_between(start: datetime | None, end: datetime) -> int:
    if start is None:
        return 0

    minutes = int((end - start).total_seconds() // 60)
    return max(minutes, 0)


def _safe_float(value: Any, *, default: float) -> float:
    try:
        if value is None:
            return default

        parsed = float(value)

        if not math.isfinite(parsed):
            return default

        return parsed
    except Exception:
        return default


def _is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except Exception:
        return False
