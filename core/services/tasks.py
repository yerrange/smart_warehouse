from core.models import Task, EmployeeShiftStats, TaskAssignmentLog
from django.db import transaction


def employee_has_all_qualifications(employee, task):
    required = task.required_qualifications.all()
    actual = employee.qualifications.all()
    return all(q in actual for q in required)


@transaction.atomic
def assign_task_to_best_employee(task: Task):
    if task.assigned_to or task.status != "pending":
        return None  # уже назначено

    shift = task.shift
    stats = EmployeeShiftStats.objects.filter(shift=shift, is_busy=False)

    # фильтруем по квалификациям
    eligible_stats = []
    for stat in stats:
        if employee_has_all_qualifications(stat.employee, task):
            eligible_stats.append(stat)

    if not eligible_stats:
        return None

    # сортируем по task_count, затем по shift_score
    eligible_stats.sort(key=lambda s: (s.task_count, s.shift_score))

    selected = eligible_stats[0]
    employee = selected.employee

    # назначаем задачу
    task.assigned_to = employee
    task.status = "in_progress"
    task.save()

    # логируем назначение
    TaskAssignmentLog.objects.create(
        task=task,
        employee=employee,
        note="Автоматическое назначение (эвристика)"
    )

    # обновляем stats
    selected.task_count += 1
    selected.is_busy = True
    selected.save()

    return employee


def complete_task(task: Task):
    """Завершает задачу и обновляет статистику сотрудника"""
    if task.status != "in_progress" or not task.assigned_to:
        return False

    task.status = "completed"
    task.save()

    # обновляем статистику сотрудника
    try:
        stats = task.shift.employee_stats.get(employee=task.assigned_to)
        stats.is_busy = False
        stats.shift_score += task.difficulty or 1
        stats.save()
    except EmployeeShiftStats.DoesNotExist:
        pass  # можно логировать

    return True
