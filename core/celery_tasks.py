from __future__ import annotations

import os
from pathlib import Path
from time import perf_counter
from typing import Optional, Dict, List

from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.timezone import now

from core.models import Task, Shift
from core.services.tasks import assign_task_to_best_employee

# Фиксированное имя логгера (под него настроен LOGGING в settings.py)
logger = get_task_logger("core.celery_tasks")


TICK_LOG_PATH = Path(settings.BASE_DIR) / "background_tick_results.txt"


def _append_tick_result_line(line: str) -> None:
    TICK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TICK_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

# Параметры можно переопределять через переменные окружения
BATCH_SIZE = int(os.getenv("ASSIGN_BATCH_SIZE", "100"))
SLOW_TICK_MS = int(os.getenv("ASSIGN_SLOW_TICK_MS", "150"))  # порог «медленного» тика для 🐢

def _eligible_active_shifts(ts):
    """
    Активные смены, которые ещё не закончились, и в них есть сотрудники.
    """
    return list(
        Shift.objects.filter(is_active=True)
        .filter(Q(end_time__isnull=True) | Q(end_time__gt=ts))
        .filter(employees__isnull=False)
        .distinct()
        .order_by("end_time")
    )

def _pool_tasks(batch_size=BATCH_SIZE):
    """
    Пул задач: pending и без назначенной смены.
    """
    return list(
        Task.objects.filter(status="pending", shift__isnull=True)
        .order_by("id")[:batch_size]
    )

def _try_assign_task_to_some_shift(task_id: int, shifts: list[Shift], stats: Dict[str, int | List[int]]) -> bool:
    """
    Попробовать пристроить задачу в какую-то смену.
    Обновляет счётчики в stats. Возвращает True при успешном назначении.
    """
    for sh in shifts:
        sh.refresh_from_db()  # смена могла завершиться
        if not sh.is_active or (sh.end_time and sh.end_time <= now()):
            stats["shifts_skipped"] += 1
            continue

        with transaction.atomic():
            # Блокируем задачу от гонок
            task = (
                Task.objects.select_for_update()
                .select_related("shift")
                .get(pk=task_id)
            )

            # Инварианты под блокировкой
            if task.status != "pending" or task.shift_id is not None:
                stats["status_changed"] += 1
                return False

            # Условный UPDATE — если кто-то успел раньше, updated=0
            updated = (
                Task.objects
                .filter(pk=task.pk, status="pending", shift__isnull=True)
                .update(shift=sh)
            )
            if not updated:
                stats["lost_race"] += 1
                return False

            # Синхронизируем объект
            task.shift_id = sh.id
            task.shift = sh

            # Доменная логика: выбор сотрудника
            ok = assign_task_to_best_employee(task, sh)
            if ok:
                stats["assigned"] += 1
                if len(stats["assigned_task_ids"]) < 3:
                    stats["assigned_task_ids"].append(task.id)
                return True

            # Вернуть в пул (сотрудника не нашли)
            Task.objects.filter(pk=task.pk, status="pending", shift=sh).update(shift=None)
            stats["returned_to_pool"] += 1
            return False
    return False

def _summary_line(
    task_id: Optional[str],
    stats: Dict[str, int | List[int]],
    shifts_total: int,
    pool_total: int,
    dur_ms: float,
    ts_iso: str,
) -> str:
    """
    Одна короткая сводка на тик — без «стены текста».
    """
    if shifts_total == 0:
        head = "⛔ NO_SHIFTS"
    elif stats["assigned"] > 0:
        head = f"✅ ASSIGNED x{stats['assigned']}"
    elif pool_total == 0:
        head = "💤 IDLE"
    else:
        head = "⚠️ NO_MATCH"

    slow = " 🐢" if dur_ms >= SLOW_TICK_MS else ""
    parts = [
        head + slow,
        f"tick={task_id}",
        f"shifts={shifts_total}",
        f"pool={pool_total}",
        f"tried={stats['attempted']}",
        f"assigned={stats['assigned']}",
        f"back={stats['returned_to_pool']}",
        f"lost={stats['lost_race']}",
        f"changed={stats['status_changed']}",
        f"skip={stats['shifts_skipped']}",
        f"dur={dur_ms:.1f}ms",
        f"ts={ts_iso}",
    ]
    if stats["assigned"] > 0 and stats["assigned_task_ids"]:
        parts.append(f"sample_tasks={stats['assigned_task_ids']}")
    return " | ".join(parts)

@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
)
def assign_pending_tasks_tick(self):
    """
    Один «тик»: берёт активные смены и пытается из пула назначить задачи.
    Возвращает число успешно назначенных задач.
    """
    started = perf_counter()
    ts = now()

    shifts = _eligible_active_shifts(ts)
    stats: Dict[str, int | List[int]] = {
        "attempted": 0,
        "assigned": 0,
        "returned_to_pool": 0,
        "lost_race": 0,
        "status_changed": 0,
        "shifts_skipped": 0,
        "assigned_task_ids": [],
    }

    if not shifts:
        dur_ms = (perf_counter() - started) * 1000.0
        summary = _summary_line(
            getattr(self.request, "id", None),
            stats,
            0,
            0,
            dur_ms,
            ts.isoformat(),
        )
        logger.info(summary)
        _append_tick_result_line(summary)
        return 0

    tasks = _pool_tasks()
    for t in tasks:
        stats["attempted"] += 1
        try:
            _try_assign_task_to_some_shift(t.id, shifts, stats)
        except Task.DoesNotExist:
            # задачу могли удалить между выборкой и lock'ом
            continue

    dur_ms = (perf_counter() - started) * 1000.0
    summary = _summary_line(
        getattr(self.request, "id", None),
        stats,
        len(shifts),
        len(tasks),
        dur_ms,
        ts.isoformat(),
    )
    logger.info(summary)
    _append_tick_result_line(summary)
    return int(stats["assigned"])  # на всякий случай приводим к int

@shared_task
def assign_pending_tasks_loop_once():
    """
    Обёртка для Beat: ставит «тик» в очередь и сразу возвращается.
    Не логируем тут, чтобы не дублировать — «тик» сам пишет сводку.
    """
    res = assign_pending_tasks_tick.delay()
    return res.id
