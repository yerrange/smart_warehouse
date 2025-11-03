from datetime import date as _date
from django.db import transaction
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from core.models import Shift, Employee, EmployeeShiftStats, TaskPool, TaskAssignmentLog
from core.services.tasks import assign_task_to_best_employee
from rest_framework.exceptions import ValidationError

from audit.services import record_event


def create_shift(name, date, start_time, end_time) -> Shift:
    shift, created = Shift.objects.get_or_create(
        name=name,
        date=date,
        start_time=start_time,
        end_time=end_time,
    )
    if created:
        # Логируем создание смены
        transaction.on_commit(lambda: record_event(
            actor_type="system",
            actor_id="service:shifts",
            entity_type="Shift",
            entity_id=str(shift.id),
            action="CREATE",
            before=None,
            after={
                "name": shift.name,
                "date": shift.date.isoformat() if hasattr(shift.date, "isoformat") else str(shift.date),
                "start_time": shift.start_time.isoformat() if shift.start_time else None,
                "end_time": shift.end_time.isoformat() if shift.end_time else None,
                "is_active": shift.is_active,
            },
            meta={"source": "service", "func": "create_shift"},
        ))
    return shift


@transaction.atomic
def add_employees_to_shift(shift: Shift, employee_codes: list[str]) -> None:
    # Снимок "до"
    before_codes = set(shift.employees.values_list("employee_code", flat=True))

    employees = Employee.objects.filter(employee_code__in=employee_codes, is_active=True)
    for employee in employees:
        EmployeeShiftStats.objects.get_or_create(employee=employee, shift=shift)

    # Снимок "после"
    after_codes = set(shift.employees.values_list("employee_code", flat=True))
    added = sorted(list(after_codes - before_codes))

    if added:
        transaction.on_commit(lambda: record_event(
            actor_type="system",
            actor_id="service:shifts",
            entity_type="Shift",
            entity_id=str(shift.id),
            action="ADD_EMPLOYEES",
            before={"employees": sorted(list(before_codes))},
            after={"employees": sorted(list(after_codes))},
            meta={"source": "service", "func": "add_employees_to_shift", "added": added, "added_count": len(added)},
        ))


@transaction.atomic
def create_shift_with_employees(name, date: _date, start_time, end_time, employee_codes: list[str]) -> Shift:
    shift = create_shift(name, date, start_time, end_time)
    add_employees_to_shift(shift, employee_codes)
    return shift


def get_active_shift(for_date: _date | None = None) -> Shift | None:
    target_date = for_date or _date.today()
    return Shift.objects.filter(date=target_date, is_active=True).first()


def assign_tasks_from_pool_to_shift(shift: Shift) -> int:
    """
    Назначает из пула «pending» задачи в смену по эвристике.
    Пер-тасковые события «ASSIGN» пишет сама assign_task_to_best_employee(...).
    Тут — только сводное событие по смене (count + превью id), чтобы не дублировать низкоуровневые события.
    """
    pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
    tasks = pool.tasks.filter(status="pending").order_by("-priority", "id")

    assigned_count = 0
    assigned_ids: list[str] = []

    for task in tasks:
        employee = assign_task_to_best_employee(task, shift)
        if employee:
            TaskAssignmentLog.objects.create(
                task=task,
                employee=employee,
                note="Назначено из пула при старте смены",
            )
            assigned_count += 1
            assigned_ids.append(str(task.id))

    if assigned_count:
        preview_limit = 20
        preview_ids = assigned_ids[:preview_limit]
        more = assigned_count - len(preview_ids)
        transaction.on_commit(lambda: record_event(
            actor_type="system",
            actor_id="service:shifts",
            entity_type="Shift",
            entity_id=str(shift.id),
            action="ASSIGN_FROM_POOL",
            before=None,
            after={"assigned_count": assigned_count},
            meta={
                "source": "service",
                "func": "assign_tasks_from_pool_to_shift",
                "preview_task_ids": preview_ids,
                "more": max(0, more),
            },
        ))

    return assigned_count


def start_shift(shift: Shift) -> int:
    """Запускает смену и назначает задачи из пула. Возвращает число назначенных задач."""
    if not shift.employees.exists():
        raise ValidationError("Нельзя запустить смену без сотрудников.")

    # Снимок "до"
    before_state = {
        "is_active": shift.is_active,
        "start_time": shift.start_time.isoformat() if shift.start_time else None,
        "actual_start_time": None,
    }

    # доменный метод (у модели) устанавливает start_time и флаги
    shift.start()

    assigned = assign_tasks_from_pool_to_shift(shift)

    # Снимок "после"
    after_state = {
        "is_active": shift.is_active,
        "start_time": shift.start_time.isoformat() if shift.start_time else None,
        "actual_start_time": shift.actual_start_time.isoformat() if shift.start_time else None,
    }

    transaction.on_commit(lambda: record_event(
        actor_type="system",
        actor_id="service:shifts",
        entity_type="Shift",
        entity_id=str(shift.id),
        action="START",
        before=before_state,
        after=after_state,
        meta={"source": "service", "func": "start_shift", "assigned_from_pool": assigned},
    ))
    return assigned


def close_shift(shift: Shift) -> int:
    """
    Закрывает смену: возвращает незавершённые задачи в пул, освобождает сотрудников, шлёт событие.
    Возвращает число возвращённых задач.
    """
    if not shift.is_active:
        raise ValueError("Смена уже закрыта.")

    # Снимок "до" для смены
    before_shift = {
        "is_active": shift.is_active,
        "end_time": shift.end_time.isoformat() if shift.end_time else None,
        "actual_end_time": None,
    }

    task_pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
    unfinished = shift.tasks.filter(status__in=["pending", "in_progress"])  # QuerySet
    returned_count = 0

    # Будущие события по возвратам задач
    task_events: list[tuple[str, dict, dict]] = []

    for task in unfinished:
        prev_employee = task.assigned_to

        # Снимок "до" для задачи
        before = {
            "shift_id": str(shift.id),
            "assignee_id": str(prev_employee.id) if prev_employee else None,
            "status": task.status,
        }

        # Изменяем задачу
        task.assigned_to = None
        task.status = "pending"
        task.shift = None
        task.task_pool = task_pool
        task.save(update_fields=["assigned_to", "status", "shift", "task_pool", "updated_at"])
        returned_count += 1

        # Снимок "после" для задачи
        after = {
            "shift_id": None,
            "assignee_id": None,
            "status": "pending",
            "task_pool_id": str(task_pool.id),
        }

        task_events.append((str(task.id), before, after))

        if prev_employee:
            TaskAssignmentLog.objects.create(
                task=task,
                employee=prev_employee,
                note="Снята и возвращена в пул при завершении смены",
            )

    # Освободить всех сотрудников по этой смене
    EmployeeShiftStats.objects.filter(shift=shift).update(is_busy=False)

    # доменный метод модели: проставит end_time/is_active
    shift.close()

    # Снимок "после" для смены
    after_shift = {
        "is_active": shift.is_active,
        "end_time": shift.end_time.isoformat() if shift.end_time else None,
        "actual_end_time": shift.actual_end_time.isoformat() if shift.end_time else None,
    }

    # Планируем аудит после коммита
    def _enqueue_audit():
        # События по задачам
        for eid, b, a in task_events:
            record_event(
                actor_type="system",
                actor_id="service:shifts",
                entity_type="Task",
                entity_id=eid,
                action="RETURN_TO_POOL",
                before=b,
                after=a,
                meta={"source": "service", "func": "close_shift", "shift_id": str(shift.id)},
            )
        # Итоговое событие закрытия смены
        record_event(
            actor_type="system",
            actor_id="service:shifts",
            entity_type="Shift",
            entity_id=str(shift.id),
            action="CLOSE",
            before=before_shift,
            after=after_shift,
            meta={"source": "service", "func": "close_shift", "returned_count": returned_count},
        )

    transaction.on_commit(_enqueue_audit)

    # WS уведомление
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "shift_closed", "message": {"reason": "смена завершена", "shift_id": shift.id}},
    )
    return returned_count
