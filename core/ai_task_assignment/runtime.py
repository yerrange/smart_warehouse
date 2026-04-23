from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

from django.conf import settings

from core.models import Task, Shift, EmployeeShiftStats
from .features import build_feature_dict, to_row
from .model_store import load_model_and_meta


@dataclass(frozen=True)
class PickResult:
    selected_stats: Optional[EmployeeShiftStats]
    mode: str  # "ml" | "heuristic" | "heuristic_fallback"
    predicted_minutes: Optional[float] = None
    adjusted_score: Optional[float] = None
    error: Optional[str] = None
    topk: Optional[list[dict[str, Any]]] = None


def _heuristic_pick(eligible_stats: list[EmployeeShiftStats]) -> PickResult:
    eligible_stats.sort(key=lambda s: (s.shift_score or 0, s.task_completed_count or 0))
    best = eligible_stats[0]
    return PickResult(
        selected_stats=best,
        mode="heuristic",
        topk=[
            {
                "employee_id": best.employee_id,
                "employee_code": getattr(best.employee, "employee_code", None),
                "shift_score": int(best.shift_score or 0),
                "task_completed_count": int(best.task_completed_count or 0),
            }
        ],
    )


def _safe_norm(value: int, max_value: int) -> float:
    if max_value <= 0:
        return 0.0
    return float(value) / float(max_value)


def _balance_penalty(
    *,
    stats: EmployeeShiftStats,
    eligible_stats: list[EmployeeShiftStats],
) -> float:
    max_assigned = max(int(s.task_assigned_count or 0) for s in eligible_stats) if eligible_stats else 0
    max_completed = max(int(s.task_completed_count or 0) for s in eligible_stats) if eligible_stats else 0
    max_score = max(int(s.shift_score or 0) for s in eligible_stats) if eligible_stats else 0

    assigned_norm = _safe_norm(int(stats.task_assigned_count or 0), max_assigned)
    completed_norm = _safe_norm(int(stats.task_completed_count or 0), max_completed)
    score_norm = _safe_norm(int(stats.shift_score or 0), max_score)

    assigned_w = float(getattr(settings, "TASK_ASSIGNER_BALANCE_ASSIGNED_WEIGHT", 0.20))
    completed_w = float(getattr(settings, "TASK_ASSIGNER_BALANCE_COMPLETED_WEIGHT", 0.10))
    score_w = float(getattr(settings, "TASK_ASSIGNER_BALANCE_SCORE_WEIGHT", 0.25))
    max_penalty = float(getattr(settings, "TASK_ASSIGNER_BALANCE_MAX_PENALTY", 0.35))

    raw_penalty = (
        assigned_norm * assigned_w
        + completed_norm * completed_w
        + score_norm * score_w
    )
    return min(raw_penalty, max_penalty)


def pick_assignee(
    *,
    task: Task,
    shift: Shift,
    eligible_stats: list[EmployeeShiftStats],
    ts: datetime,
) -> PickResult:
    mode = str(getattr(settings, "TASK_ASSIGNER_MODE", "heuristic")).lower()
    topk_n = int(getattr(settings, "TASK_ASSIGNER_LOG_TOPK", 3))

    if mode != "ml":
        return _heuristic_pick(eligible_stats)

    try:
        model, meta = load_model_and_meta()

        rows: list[list] = []
        for s in eligible_stats:
            f = build_feature_dict(task=task, shift=shift, stats=s, ts=ts)
            rows.append(to_row(f, meta.feature_cols))

        from catboost import Pool  # type: ignore

        pool = Pool(rows, cat_features=meta.cat_idx)
        preds = model.predict(pool)

        scored = []
        for s, pred in zip(eligible_stats, preds):
            pred_minutes = float(pred)
            balance_penalty = _balance_penalty(
                stats=s,
                eligible_stats=eligible_stats,
            )
            adjusted_score = pred_minutes * (1.0 + balance_penalty)

            scored.append(
                (
                    adjusted_score,
                    pred_minutes,
                    s,
                    {
                        "employee_id": s.employee_id,
                        "employee_code": getattr(s.employee, "employee_code", None),
                        "predicted_minutes": pred_minutes,
                        "adjusted_score": adjusted_score,
                        "balance_penalty": round(balance_penalty, 4),
                        "emp_task_assigned_count": int(s.task_assigned_count or 0),
                        "emp_task_completed_count": int(s.task_completed_count or 0),
                        "emp_shift_score": int(s.shift_score or 0),
                    },
                )
            )

        scored.sort(key=lambda x: x[0])  # меньше adjusted_score – лучше
        best_adjusted, best_pred, best_stats, _ = scored[0]

        topk = [item[3] for item in scored[: max(1, topk_n)]]

        return PickResult(
            selected_stats=best_stats,
            mode="ml",
            predicted_minutes=float(best_pred),
            adjusted_score=float(best_adjusted),
            topk=topk,
        )

    except Exception as e:
        h = _heuristic_pick(eligible_stats)
        return PickResult(
            selected_stats=h.selected_stats,
            mode="heuristic_fallback",
            error=str(e),
            topk=h.topk,
        )