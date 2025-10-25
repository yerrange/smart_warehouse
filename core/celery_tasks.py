# smart_warehouse/core/celery_tasks.py
from __future__ import annotations

from time import perf_counter

from celery import shared_task
from celery.utils.log import get_task_logger
from django.db import transaction
from django.db.models import Q
from django.utils.timezone import now

from core.models import Task, Shift
from core.services.tasks import assign_task_to_best_employee

logger = get_task_logger(__name__)

BATCH_SIZE = 100


def _eligible_active_shifts(ts):
    """
    Вернёт список активных смен, которые ещё не закончились и в которых есть сотрудники.
    Отсортированы по времени завершения.
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
    Возьмём батч задач из пула: статус pending и смена ещё не установлена.
    """
    return list(
        Task.objects.filter(status="pending", shift__isnull=True)
        .order_by("id")[:batch_size]
    )


def _try_assign_task_to_some_shift(task_id: int, shifts: list[Shift]) -> bool:
    """
    Попробовать назначить задачу в одну из подходящих смен.
    Возвращает True, если задача успешно назначена сотруднику; иначе False.
    """
    for sh in shifts:
        sh.refresh_from_db()  # смена могла завершиться между выборкой и попыткой назначения
        if not sh.is_active or (sh.end_time and sh.end_time <= now()):
            continue

        with transaction.atomic():
            # Блокируем строку задачи, чтобы избежать гонок при одновременных тиках
            task = (
                Task.objects.select_for_update()
                .select_related("shift")
                .get(pk=task_id)
            )

            # Повторная проверка инвариантов под блокировкой
            if task.status != "pending" or task.shift_id is not None:
                return False

            # Условный UPDATE: выставляем смену только если она всё ещё не проставлена
            updated = (
                Task.objects
                .filter(pk=task.pk, status="pending", shift__isnull=True)
                .update(shift=sh)
            )
            if not updated:
                # Кто-то уже успел изменить задачу
                return False

            # Синхронизируем объект в памяти
            task.shift_id = sh.id
            task.shift = sh

            # Доменная логика: выбрать лучшего сотрудника в смене
            ok = assign_task_to_best_employee(task, sh)
            if ok:
                return True

            # Не нашли сотрудника — вернём задачу в общий пул (очистим shift)
            Task.objects.filter(pk=task.pk, status="pending", shift=sh).update(shift=None)
            return False

    return False


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,       # экспоненциальная задержка между ретраями
    retry_backoff_max=60,     # максимум 60 секунд
    retry_jitter=True,        # небольшой случайный сдвиг
    max_retries=5
)
def assign_pending_tasks_tick(self):
    """
    Один «тик»: берёт активные смены и пытается из пула назначить задачи.
    Благодаря транзакциям и условным UPDATE операция идемпотентна.
    Возвращает число успешно назначенных задач.
    """
    started = perf_counter()
    ts = now()

    shifts = _eligible_active_shifts(ts)
    if not shifts:
        logger.info(
            "assign_tick: task_id=%s shifts=0 tasks_in_pool=0 assigned=0 reason=no_active_shifts ts=%s",
            getattr(self.request, "id", None), ts.isoformat()
        )
        return 0

    tasks = _pool_tasks()
    attempted = 0
    assigned = 0

    for t in tasks:
        attempted += 1
        try:
            if _try_assign_task_to_some_shift(t.id, shifts):
                assigned += 1
        except Task.DoesNotExist:
            logger.debug("assign_tick: task_disappeared id=%s", t.id)
            continue

    dur_ms = (perf_counter() - started) * 1000.0
    logger.info(
        "assign_tick: task_id=%s shifts=%d tasks_in_pool=%d attempted=%d assigned=%d duration_ms=%.1f ts=%s",
        getattr(self.request, "id", None), len(shifts), len(tasks), attempted, assigned, dur_ms, ts.isoformat()
    )
    return assigned


@shared_task
def assign_pending_tasks_loop_once():
    """
    Обёртка для Celery Beat: поставить один «тик» в очередь и сразу вернуться.
    Удобно планировать каждые N секунд.
    """
    res = assign_pending_tasks_tick.delay()
    logger.debug("loop_once queued subtask_id=%s", getattr(res, "id", None))
    return res.id
