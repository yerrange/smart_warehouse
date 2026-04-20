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
    error: Optional[str] = None
    topk: Optional[list[dict[str, Any]]] = None  # список лучших кандидатов


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
            scored.append(
                (
                    float(pred),
                    s,
                    {
                        "employee_id": s.employee_id,
                        "employee_code": getattr(s.employee, "employee_code", None),
                        "predicted_minutes": float(pred),
                        "emp_task_assigned_count": int(s.task_assigned_count or 0),
                        "emp_task_completed_count": int(s.task_completed_count or 0),
                        "emp_shift_score": int(s.shift_score or 0),
                    },
                )
            )

        scored.sort(key=lambda x: x[0])  # меньше минут – лучше
        best_pred, best_stats, _ = scored[0]

        topk = [item[2] for item in scored[: max(1, topk_n)]]

        return PickResult(
            selected_stats=best_stats,
            mode="ml",
            predicted_minutes=float(best_pred),
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
