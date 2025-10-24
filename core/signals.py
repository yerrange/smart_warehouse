from django.db.models.signals import pre_save
from django.dispatch import receiver
from core.models import Task, TaskAssignmentLog

@receiver(pre_save, sender=Task)
def log_task_assignment_change(sender, instance: Task, **kwargs):
    """
    Создаём TaskAssignmentLog, когда у задачи меняется assigned_to
    (в том числе при первом назначении). Срабатывает для любых путей изменения:
    админка, API, сервисные функции.
    """
    if not instance.pk:
        # новая задача — назначение может прийти позже
        return

    # читаем старое значение из БД без лишних полей
    try:
        old = sender.objects.only("assigned_to").get(pk=instance.pk)
    except sender.DoesNotExist:
        return

    old_emp_id = getattr(old, "assigned_to_id", None)
    new_emp_id = getattr(instance, "assigned_to_id", None)

    # логируем ТОЛЬКО если стало непусто и кто-то другой
    if new_emp_id and new_emp_id != old_emp_id:
        TaskAssignmentLog.objects.create(task=instance, employee_id=new_emp_id)