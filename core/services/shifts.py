from datetime import date as _date
from django.db import transaction
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from core.models import Shift, Employee, EmployeeShiftStats, TaskPool, TaskAssignmentLog
from core.services.tasks import assign_task_to_best_employee


def create_shift(name, date, start_time, end_time) -> Shift:
    shift, _ = Shift.objects.get_or_create(
        name=name,
        date=date,
        start_time=start_time,
        end_time=end_time
    )
    return shift


@transaction.atomic
def add_employees_to_shift(shift: Shift, employee_codes: list[str]) -> None:
    employees = Employee.objects.filter(employee_code__in=employee_codes, is_active=True)
    shift.employees.add(*employees)
    for employee in employees:
        EmployeeShiftStats.objects.get_or_create(employee=employee, shift=shift)


@transaction.atomic
def create_shift_with_employees(name, date: _date, start_time, end_time, employee_codes: list[str]) -> Shift:
    shift = create_shift(name, date, start_time, end_time)
    add_employees_to_shift(shift, employee_codes)
    return shift


def get_active_shift(for_date: _date | None = None) -> Shift | None:
    target_date = for_date or _date.today()
    return Shift.objects.filter(date=target_date, is_active=True).first()


def assign_tasks_from_pool_to_shift(shift: Shift) -> int:
    pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
    tasks = pool.tasks.filter(status="pending").order_by("-priority", "id")

    assigned_count = 0
    for task in tasks:
        employee = assign_task_to_best_employee(task, shift)
        if employee:
            TaskAssignmentLog.objects.create(
                task=task,
                employee=employee,
                note="Назначено из пула при старте смены"
            )
            assigned_count += 1
    return assigned_count


def start_shift(shift: Shift) -> int:
    """Запускает смену и назначает задачи из пула. Возвращает число назначенных задач."""
    if shift.is_active or shift.start_time:
        raise ValueError("Смена уже активна.")
    # доменный метод (у модели) устанавливает start_time и флаги
    shift.start()
    return assign_tasks_from_pool_to_shift(shift)


def close_shift(shift: Shift) -> int:
    """Закрывает смену: возвращает незавершённые задачи в пул, освобождает сотрудников, шлёт событие.
    Возвращает число возвращённых задач."""
    if not shift.is_active:
        raise ValueError("Смена уже закрыта.")

    task_pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
    unfinished = shift.tasks.filter(status__in=["pending", "in_progress"])  # QuerySet
    returned_count = 0

    for task in unfinished:
        prev_employee = task.assigned_to
        task.assigned_to = None
        task.status = "pending"
        task.shift = None
        task.task_pool = task_pool
        task.save(update_fields=["assigned_to", "status", "shift", "task_pool", "updated_at"])
        returned_count += 1

        if prev_employee:
            TaskAssignmentLog.objects.create(
                task=task,
                employee=prev_employee,
                note="Снята и возвращена в пул при завершении смены"
            )

    # Освободить всех сотрудников по этой смене
    EmployeeShiftStats.objects.filter(shift=shift).update(is_busy=False)

    # доменный метод модели: проставит end_time/is_active
    shift.close()

    # WS уведомление
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "shift_closed", "message": {"reason": "смена завершена", "shift_id": shift.id}},
    )
    return returned_count
