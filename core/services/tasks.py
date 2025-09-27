from django.db import transaction
from django.utils.timezone import now
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from rest_framework.exceptions import NotFound

from core.models import Task, Employee, EmployeeShiftStats, TaskAssignmentLog, Shift
from core.serializers import TaskReadSerializer


def employee_has_all_qualifications(employee: Employee, task: Task) -> bool:
    required = set(task.required_qualifications.all())
    actual = set(employee.qualifications.all())
    return required.issubset(actual)


@transaction.atomic
def assign_task_to_best_employee(task: Task, shift: Shift | None):
    if task.assigned_to or task.status != "pending" or not shift:
        return None

    stats = EmployeeShiftStats.objects.filter(shift=shift, is_busy=False)
    eligible = [s for s in stats if employee_has_all_qualifications(s.employee, task)]
    if not eligible:
        return None

    eligible.sort(key=lambda s: (s.task_count, s.shift_score))
    selected = eligible[0]
    employee = selected.employee

    # назначаем
    task.assigned_to = employee
    task.status = "pending"
    task.shift = shift
    task.assigned_at = now()
    task.save(update_fields=["assigned_to", "status", "shift", "assigned_at", "updated_at"])

    TaskAssignmentLog.objects.create(task=task, employee=employee, note="Автоназначение (эвристика)")

    selected.task_count = (selected.task_count or 0) + 1
    selected.save(update_fields=["task_count"])

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

    # Приведём задачу к in_progress на выбранного сотрудника
    task.assigned_to = employee
    task.status = "pending"
    task.assigned_at = now()
    task.save(update_fields=["assigned_to", "status", "assigned_at", "updated_at"])

    # Если задача в смене — обновить статистику
    if task.shift_id:
        stats, _ = EmployeeShiftStats.objects.get_or_create(employee=employee, shift=task.shift)
        stats.task_count = (stats.task_count or 0) + 1
        stats.save(update_fields=["task_count"])

    TaskAssignmentLog.objects.create(task=task, employee=employee, note="Ручное назначение через API")

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_assigned", "message": TaskReadSerializer(task).data},
    )


@transaction.atomic
def complete_task(task: Task) -> bool:
    if task.status != "in_progress" or not task.assigned_to:
        return False

    task.status = "completed"
    task.completed_at = now()
    task.save(update_fields=["status", "completed_at", "updated_at"])

    if task.shift_id:
        try:
            stats = task.shift.employee_stats.get(employee=task.assigned_to)
            stats.is_busy = False
            stats.shift_score = (stats.shift_score or 0) + (task.difficulty or 1)
            stats.save(update_fields=["is_busy", "shift_score"])
        except EmployeeShiftStats.DoesNotExist:
            pass

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_completed", "message": TaskReadSerializer(task).data},
    )
    return True


@transaction.atomic
def start_task(task: Task) -> bool:
    if task.status != "pending" or not task.assigned_to:
        return False

    task.status = "in_progress"
    task.started_at = now()
    task.save(update_fields=["status", "started_at", "updated_at"])

    stats = task.shift.employee_stats.get(employee=task.assigned_to)
    stats.is_busy = True
    stats.save(update_fields=["is_busy"])

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "task_updates",
        {"type": "task_started", "message": TaskReadSerializer(task).data},
    )
    return True
