from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Count, Q
from django.utils.timezone import now
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from rest_framework.exceptions import NotFound
from core.ai_task_assignment.runtime import pick_assignee

from core.models import (
    Task,
    Employee,
    EmployeeShiftStats,
    TaskAssignmentLog,
    Shift,
    Cargo
)
from core.serializers import TaskReadSerializer
from core.services import cargos as cargo_service
from audit.services import record_event
from celery import current_task

import logging
logger = logging.getLogger("core.task_assigner")


CARGO_TYPES = {
    Task.TaskType.RECEIVE_TO_INBOUND,
    Task.TaskType.PUTAWAY_TO_RACK,
    Task.TaskType.MOVE_BETWEEN_SLOTS,
    Task.TaskType.DISPATCH_CARGO,
}

MOVE_LIKE = {
    Task.TaskType.RECEIVE_TO_INBOUND,
    Task.TaskType.PUTAWAY_TO_RACK,
    Task.TaskType.MOVE_BETWEEN_SLOTS,
}

FUNC_TO_CALL = {
    Task.TaskType.RECEIVE_TO_INBOUND: cargo_service.arrive,
    Task.TaskType.PUTAWAY_TO_RACK: cargo_service.store,
    Task.TaskType.MOVE_BETWEEN_SLOTS: cargo_service.move,
    Task.TaskType.DISPATCH_CARGO: cargo_service.dispatch,
}


def employee_has_all_qualifications(employee: Employee, task: Task) -> bool:
    required = set(task.required_qualifications.all())
    actual = set(employee.qualifications.all())
    return required.issubset(actual)


@transaction.atomic
def assign_task_to_best_employee(task: Task, shift: Shift | None):
    """
    Автоназначение: выбираем сотрудника без параллельных pending-задач.
    Возвращает Employee или None.
    """
    if task.assigned_to or task.status != Task.Status.PENDING or not shift:
        return None

    # Если задача уже привязана к смене,
    # назначать её можно только в рамках этой же смены
    if task.shift_id is not None and task.shift_id != shift.id:
        return None

    # Подходят только не занятые сотрудники смены
    # И без уже назначенных pending-задач в этой смене
    stats_qs = (
        EmployeeShiftStats.objects
        .select_related("employee")
        .filter(shift=shift, is_busy=False)
        .annotate(
            pending_assigned=Count(
                "employee__task",
                filter=Q(
                    employee__task__status="pending",
                    employee__task__shift=shift,
                ),
                distinct=True,
            )
        )
        .filter(pending_assigned=0)
    )

    # Отфильтруем по квалификациям
    eligible = [
        s for s
        in stats_qs
        if employee_has_all_qualifications(s.employee, task)
    ]
    if not eligible:
        return None

    # Сортировка: меньше выполнял / меньше набрал очков
    # eligible.sort(key=lambda s: (s.shift_score or 0, s.task_completed_count or 0))
    # selected = eligible[0]
    # employee = selected.employee


    timestamp = now()  # единый таймштамп – и для ML-фич, и для БД/аудита

    pick = pick_assignee(
        task=task,
        shift=shift,
        eligible_stats=eligible,
        ts=timestamp
    )
    if not pick.selected_stats:
        return None

    selected = pick.selected_stats
    employee = selected.employee

    if pick.mode == "ml":
        top = pick.topk or []
        top_str = "; ".join(
            (
                f"{c.get('employee_code') or c['employee_id']}"
                f"=pred:{c.get('predicted_minutes'):.2f}m"
                f"/adj:{c.get('adjusted_score'):.2f}"
            )
            for c in top
            if c.get("predicted_minutes") is not None and c.get("adjusted_score") is not None
        )
        note = (
            f"Автоназначение (ML) – "
            f"best_pred={pick.predicted_minutes:.2f}m – "
            f"best_adj={pick.adjusted_score:.2f} – "
            f"top{len(top)}: {top_str}"
        )
    elif pick.mode == "heuristic_fallback":
        note = f"Автоназначение (fallback→эвристика) – причина: {pick.error}"
    else:
        note = "Автоназначение (эвристика)"


    # Снимок "до" — фиксируем ДО апдейта
    before_shift_id = task.shift_id
    before = {
        "shift_id": str(before_shift_id) if before_shift_id is not None else None,
        "assignee_id": None,
        "assigned_at": None,
    }

    # Атомарно назначаем только если задача всё ещё pending и без исполнителя
    updated = (
        Task.objects
        .filter(
            pk=task.pk,
            status=Task.Status.PENDING,
            assigned_to__isnull=True
        )
        .update(
            assigned_to=employee,
            shift=shift if task.shift_id is None else task.shift,
            assigned_at=timestamp,
            updated_at=timestamp,
        )
    )
    if not updated:
        return None  # гонка: кто-то уже изменил задачу

    # Обновим инстанс task для WS/логов без повторного запроса
    task.assigned_to = employee
    if task.shift_id is None:
        task.shift = shift
        task.shift_id = shift.id
    task.assigned_at = timestamp

    # Обновим счётчики
    selected.task_assigned_count = (selected.task_assigned_count or 0) + 1
    selected.last_task_at = timestamp
    selected.save(update_fields=["task_assigned_count", "last_task_at"])

    # Blockchain Audit — готовим неизменяемые снимки для on_commit
    after = {
        "shift_id": str(task.shift_id) if task.shift_id is not None else None,
        "assignee_id": str(task.assigned_to_id) if task.assigned_to_id else None,
        "assignee_code": getattr(task.assigned_to, "employee_code", None),
        "assigned_at": timestamp.isoformat(),
        "mode": pick.mode,
    }

    if pick.predicted_minutes is not None:
        after["ml_predicted_minutes"] = round(float(pick.predicted_minutes), 3)
    if pick.adjusted_score is not None:
        after["ml_adjusted_score"] = round(float(pick.adjusted_score), 3)
    if pick.error:
        after["ml_error"] = pick.error

    req = getattr(current_task, "request", None)
    meta = {
        "source": "celery",
        "celery_task_id": getattr(req, "id", None),
    }

    entity_id = str(task.id)

    transaction.on_commit(
        lambda eid=entity_id, b=before, a=after, m=meta: record_event(
            actor_type="system",
            actor_id="celery",
            entity_type="Task",
            entity_id=eid,
            action="ASSIGN",          # единое действие, режим в after.mode
            before=b,
            after=a,
            meta=m,
        )
    )

    TaskAssignmentLog.objects.create(
        task=task,
        employee=employee,
        timestamp=timestamp,
        note=note
    )

    logger.info(
        "ASSIGN – task_id=%s type=%s mode=%s employee=%s pred=%s topk=%s err=%s",
        task.id,
        task.task_type,
        pick.mode,
        getattr(employee, "employee_code", employee.id),
        f"{pick.predicted_minutes:.3f}" if pick.predicted_minutes is not None else None,
        pick.topk,
        pick.error,
    )
    # WS: шлём полную задачу (статус остаётся "pending")
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_assigned", "message": TaskReadSerializer(task).data},
    )

    return employee


@transaction.atomic
def assign_task_manually(task: Task, employee_code: str) -> None:
    if task.status not in (
        Task.Status.PENDING,
        Task.Status.IN_PROGRESS,
        Task.Status.PAUSED,
    ):
        raise ValueError("Нельзя назначить задачу с текущим статусом.")

    if not task.shift_id:
        raise ValueError(
            "Нельзя вручную назначить задачу, не привязанную к смене."
        )

    try:
        employee = Employee.objects.get(
            employee_code=employee_code,
            is_active=True
        )
    except Employee.DoesNotExist:
        raise NotFound("Сотрудник не найден")

    if task.assigned_to_id == employee.id:
        raise ValueError("Задача уже назначена этому сотруднику.")

    if not employee_has_all_qualifications(employee, task):
        raise ValueError(
            "У сотрудника нет требуемых квалификаций для этой задачи."
        )

    try:
        new_stats = EmployeeShiftStats.objects.get(
            employee=employee,
            shift=task.shift
        )
    except EmployeeShiftStats.DoesNotExist:
        raise ValueError("Сотрудник не входит в состав этой смены.")

    prev_assignee_id = task.assigned_to_id
    prev_shift_id = task.shift_id
    prev_assigned_at = getattr(task, "assigned_at", None)

    timestamp = now()
    was_active = task.status in (Task.Status.IN_PROGRESS, Task.Status.PAUSED)

    # Если задача была в работе у другого сотрудника, освобождаем его
    if prev_assignee_id and prev_assignee_id != employee.id:
        try:
            prev_stats = EmployeeShiftStats.objects.get(
                employee_id=prev_assignee_id,
                shift=task.shift
            )
            if was_active:
                prev_stats.is_busy = False
                prev_stats.last_task_at = timestamp
                prev_stats.save(update_fields=["is_busy", "last_task_at"])
        except EmployeeShiftStats.DoesNotExist:
            pass

    # Переназначение всегда возвращает задачу в pending
    task.assigned_to = employee
    task.status = Task.Status.PENDING
    task.started_at = None
    task.assigned_at = timestamp
    task.updated_at = timestamp
    task.save(
        update_fields=[
            "assigned_to",
            "status",
            "started_at",
            "assigned_at",
            "updated_at",
        ]
    )

    # Если это грузовая задача и после переназначения не осталось активных задач по грузу,
    # возвращаем груз в IDLE
    if task.cargo_id and was_active:
        has_active = task.cargo.task_set.filter(
            status__in=[Task.Status.IN_PROGRESS, Task.Status.PAUSED]
        ).exists()
        if not has_active and task.cargo.handling_state != Cargo.HandlingState.IDLE:
            task.cargo.handling_state = Cargo.HandlingState.IDLE
            task.cargo.updated_at = timestamp
            task.cargo.save(update_fields=["handling_state", "updated_at"])

    new_stats.task_assigned_count = (new_stats.task_assigned_count or 0) + 1
    new_stats.last_task_at = timestamp
    new_stats.save(update_fields=["task_assigned_count", "last_task_at"])

    TaskAssignmentLog.objects.create(
        task=task,
        employee=employee,
        timestamp=timestamp,
        note="Ручное назначение через API"
    )

    # Blockchain Audit
    before = {
        "shift_id": str(prev_shift_id) if prev_shift_id is not None else None,
        "assignee_id": str(prev_assignee_id) if prev_assignee_id else None,
        "assigned_at": prev_assigned_at.isoformat() if prev_assigned_at else None,
    }
    after = {
        "shift_id": str(task.shift_id) if task.shift_id is not None else None,
        "assignee_id": str(task.assigned_to_id) if task.assigned_to_id else None,
        "assignee_code": getattr(task.assigned_to, "employee_code", None),
        "assigned_at": timestamp.isoformat(),
        "mode": "manual",
    }
    meta = {
        "source": "api",
        "func": "assign_task_manually"
    }

    entity_id = str(task.id)
    transaction.on_commit(
        lambda eid=entity_id,
        b=before,
        a=after,
        m=meta: record_event(
            actor_type="system",
            actor_id="api",
            entity_type="Task",
            entity_id=eid,
            action="ASSIGN",
            before=b,
            after=a,
            meta=m,
        )
    )

    # WS
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_assigned", "message": TaskReadSerializer(task).data},
    )


@transaction.atomic
def start_task(task: Task) -> bool:
    if (
        task.status != Task.Status.PENDING
        or not task.assigned_to
        or not task.shift_id
    ):
        return False

    timestamp = now()

    before = {
        "status": Task.Status.PENDING,
        "started_at": None,
    }

    task.status = Task.Status.IN_PROGRESS
    task.started_at = timestamp
    task.updated_at = timestamp
    task.save(update_fields=["status", "started_at", "updated_at"])

    if task.task_type in CARGO_TYPES:
        # task.cargo_id, а не task.cargo.id - быстрее и не нагружает БД
        if (
            task.cargo_id
            and task.cargo.handling_state != Cargo.HandlingState.PROCESSING
        ):
            task.cargo.handling_state = Cargo.HandlingState.PROCESSING
            task.cargo.save(update_fields=["handling_state", "updated_at"])

    try:
        stats = task.shift.employee_stats.get(employee=task.assigned_to)
        stats.is_busy = True
        stats.last_task_at = timestamp
        stats.save(update_fields=["is_busy", "last_task_at"])
    except EmployeeShiftStats.DoesNotExist:
        raise ValidationError(
            "Для сотрудника не инициализирована статистика смены"
        )

    after = {
        "status": Task.Status.IN_PROGRESS,
        "started_at": timestamp.isoformat(),
    }
    meta = {
        "source": "api",
        "func": "start_task"
    }

    entity_id = str(task.id)
    transaction.on_commit(
        lambda eid=entity_id,
        b=before,
        a=after,
        m=meta: record_event(
            actor_type="system",
            actor_id="api",
            entity_type="Task",
            entity_id=eid,
            action="START",
            before=b,
            after=a,
            meta=m,
        )
    )

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_started", "message": TaskReadSerializer(task).data},
    )
    return True


@transaction.atomic
def complete_task(task: Task) -> bool:
    if task.status != Task.Status.IN_PROGRESS or not task.assigned_to:
        return False

    # Валидация и доменные операции над Cargo (как было)
    if task.task_type in CARGO_TYPES:
        # Грузовая задача без cargo — завершать нельзя
        if not task.cargo_id:
            return False

        operation = FUNC_TO_CALL.get(task.task_type)
        if not operation:
            return False

        payload = task.payload or {}
        employee_code = getattr(task.assigned_to, "employee_code", None)

        if task.task_type in MOVE_LIKE:
            to_slot = payload.get("to_slot_code")
            if not to_slot:
                return False  # обязательный параметр отсутствует
            operation(task.cargo.cargo_code, to_slot, employee_code, payload.get("note"))
        else:  # DISPATCH_CARGO
            operation(task.cargo.cargo_code, employee_code, payload.get("note"))

    timestamp = now()

    before = {
        "status": Task.Status.IN_PROGRESS,
        "completed_at": None,
    }

    task.status = Task.Status.COMPLETED
    task.completed_at = timestamp
    task.updated_at = timestamp
    task.task_pool = None
    task.save(
        update_fields=[
            "status",
            "completed_at",
            "updated_at",
            "task_pool"
        ]
    )

    if task.cargo_id:
        has_active = task.cargo.task_set.filter(status__in=["in_progress", "paused"]).exists()
        if not has_active and task.cargo.handling_state != Cargo.HandlingState.IDLE:
            task.cargo.handling_state = Cargo.HandlingState.IDLE
            task.cargo.save(update_fields=["handling_state", "updated_at"])

    if task.shift_id:
        try:
            stats = task.shift.employee_stats.get(employee=task.assigned_to)
            stats.is_busy = False
            stats.shift_score = (stats.shift_score or 0) + (task.difficulty or 1)
            stats.task_completed_count = (stats.task_completed_count or 0) + 1
            stats.last_task_at = timestamp
            stats.save(
                update_fields=[
                    "is_busy",
                    "shift_score",
                    "task_completed_count",
                    "last_task_at"
                ]
            )
        except EmployeeShiftStats.DoesNotExist:
            pass

    after = {
        "status": Task.Status.COMPLETED,
        "completed_at": timestamp.isoformat(),
    }
    meta = {
        "source": "api",
        "func": "complete_task"
    }

    entity_id = str(task.id)
    transaction.on_commit(
        lambda eid=entity_id,
        b=before,
        a=after,
        m=meta: record_event(
            actor_type="system",
            actor_id="api",
            entity_type="Task",
            entity_id=eid,
            action="COMPLETE",
            before=b,
            after=a,
            meta=m,
        )
    )

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_completed", "message": TaskReadSerializer(task).data},
    )
    return True
