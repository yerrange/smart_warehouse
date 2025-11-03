from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Count, Q
from django.utils.timezone import now
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from rest_framework.exceptions import NotFound

from core.models import (
    Task,
    Employee,
    EmployeeShiftStats,
    TaskAssignmentLog,
    Shift,
    Cargo
)
from core.serializers import TaskReadSerializer
from core.services import cargo as cargo_service
from audit.services import record_event
from celery import current_task


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
    if task.assigned_to or task.status != "pending" or not shift:
        return None

    # Подходят только не занятые сотрудники смены И без уже назначенных pending-задач в этой смене
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
    eligible = [s for s in stats_qs if employee_has_all_qualifications(s.employee, task)]
    if not eligible:
        return None

    # Сортировка: меньше выполнял / меньше набрал очков
    eligible.sort(key=lambda s: (s.shift_score or 0, s.task_completed_count or 0))
    selected = eligible[0]
    employee = selected.employee

    # Снимок "до" — фиксируем ДО апдейта
    before_shift_id = task.shift_id
    before = {
        "shift_id": str(before_shift_id) if before_shift_id is not None else None,
        "assignee_id": None,
        "assigned_at": None,
    }

    timestamp = now()  # единый таймштамп для БД и аудита

    # Атомарно назначаем только если задача всё ещё pending и без исполнителя
    updated = (
        Task.objects
        .filter(pk=task.pk, status="pending", assigned_to__isnull=True)
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
    selected.save(update_fields=["task_assigned_count"])

    # Blockchain Audit — готовим неизменяемые снимки для on_commit
    after = {
        "shift_id": str(task.shift_id) if task.shift_id is not None else None,
        "assignee_id": str(task.assigned_to_id) if task.assigned_to_id else None,
        "assignee_code": getattr(task.assigned_to, "employee_code", None),
        "assigned_at": timestamp.isoformat(),
        "mode": "auto",
    }

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

    # TaskAssignmentLog — оставим для UI/быстрых отчётов
    TaskAssignmentLog.objects.create(
        task=task,
        employee=employee,
        timestamp=timestamp,
        note="Автоназначение (эвристика)"
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
    if task.status not in ("pending", "in_progress"):
        raise ValueError("Нельзя назначить задачу с текущим статусом.")

    try:
        employee = Employee.objects.get(employee_code=employee_code)
    except Employee.DoesNotExist:
        raise NotFound("Сотрудник не найден")

    prev_assignee_id = task.assigned_to_id
    prev_shift_id = task.shift_id
    prev_assigned_at = getattr(task, "assigned_at", None)

    timestamp = now()

    # Приведём задачу к pending на выбранного сотрудника
    task.assigned_to = employee
    task.status = "pending"
    task.assigned_at = timestamp
    task.updated_at = timestamp
    task.save(update_fields=["assigned_to", "status", "assigned_at", "updated_at"])

    # Если задача в смене — обновить статистику
    if task.shift_id:
        stats, _ = EmployeeShiftStats.objects.get_or_create(employee=employee, shift=task.shift)
        stats.task_assigned_count = (stats.task_assigned_count or 0) + 1
        stats.save(update_fields=["task_assigned_count"])

    # Человекочитаемый лог для UI
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
    meta = {"source": "api", "func": "assign_task_manually"}

    entity_id = str(task.id)
    transaction.on_commit(lambda eid=entity_id, b=before, a=after, m=meta: record_event(
        actor_type="system",
        actor_id="api",
        entity_type="Task",
        entity_id=eid,
        action="ASSIGN",
        before=b,
        after=a,
        meta=m,
    ))

    # WS
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_assigned", "message": TaskReadSerializer(task).data},
    )


@transaction.atomic
def start_task(task: Task) -> bool:
    if task.status != "pending" or not task.assigned_to:
        return False

    timestamp = now()

    before = {
        "status": "pending",
        "started_at": None,
    }

    task.status = "in_progress"
    task.started_at = timestamp
    task.updated_at = timestamp
    task.save(update_fields=["status", "started_at", "updated_at"])

    if task.task_type in CARGO_TYPES:
        # task.cargo_id, а не task.cargo.id - быстрее и не нагружает БД
        if task.cargo_id and task.cargo.handling_state != Cargo.HandlingState.PROCESSING:
            task.cargo.handling_state = Cargo.HandlingState.PROCESSING
            task.cargo.save(update_fields=["handling_state", "updated_at"])

    try:
        stats = task.shift.employee_stats.get(employee=task.assigned_to)
        stats.is_busy = True
        stats.save(update_fields=["is_busy"])
    except EmployeeShiftStats.DoesNotExist:
        raise ValidationError("Для сотрудника не инициализирована статистика смены")

    after = {
        "status": "in_progress",
        "started_at": timestamp.isoformat(),
    }
    meta = {"source": "api", "func": "start_task"}

    entity_id = str(task.id)
    transaction.on_commit(lambda eid=entity_id, b=before, a=after, m=meta: record_event(
        actor_type="system",
        actor_id="api",
        entity_type="Task",
        entity_id=eid,
        action="START",
        before=b,
        after=a,
        meta=m,
    ))

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_started", "message": TaskReadSerializer(task).data},
    )
    return True


@transaction.atomic
def complete_task(task: Task) -> bool:
    if task.status != "in_progress" or not task.assigned_to:
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
        "status": "in_progress",
        "completed_at": None,
    }

    task.status = "completed"
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
            stats.save(update_fields=["is_busy", "shift_score", "task_completed_count"])
        except EmployeeShiftStats.DoesNotExist:
            pass

    after = {
        "status": "completed",
        "completed_at": timestamp.isoformat(),
    }
    meta = {"source": "api", "func": "complete_task"}

    entity_id = str(task.id)
    transaction.on_commit(lambda eid=entity_id, b=before, a=after, m=meta: record_event(
        actor_type="system",
        actor_id="api",
        entity_type="Task",
        entity_id=eid,
        action="COMPLETE",
        before=b,
        after=a,
        meta=m,
    ))

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_completed", "message": TaskReadSerializer(task).data},
    )
    return True
