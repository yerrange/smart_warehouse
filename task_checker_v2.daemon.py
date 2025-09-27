import os
import time
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "smart_warehouse.settings")
django.setup()

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from core.models import Task, Shift
from core.services.tasks import assign_task_to_best_employee  # <-- важный импорт

CHECK_INTERVAL_SEC = int(os.getenv("AI_DEMON_INTERVAL", "5"))


def eligible_active_shifts(now):
    """Активные смены, у которых end_time не прошёл (или нет) и есть сотрудники."""
    qs = (
        Shift.objects.filter(is_active=True)
        .filter(Q(end_time__isnull=True) | Q(end_time__gt=now))
        .filter(employees__isnull=False)
        .distinct()
        .order_by("end_time")
    )
    return list(qs)


def pool_tasks(batch_size=100):
    """Задачи из пула: ожидают назначения и без смены."""
    return list(
        Task.objects.filter(status="pending", shift__isnull=True)
        .order_by("id")[:batch_size]
    )


def try_assign_task_to_some_shift(task_id: int, shifts: list[Shift]) -> bool:
    """
    Пытается назначить задачу в одну из подходящих смен и сразу подобрать исполнителя.
    Возвращает True, если получилось (смена + сотрудник назначены).
    """
    # Перебираем смены по очереди, пока не получится назначить сотрудника
    for sh in shifts:
        # На всякий случай обновим актуальность смены (могла завершиться за это время)
        if sh.end_time and sh.end_time <= timezone.now():
            continue

        try:
            with transaction.atomic():
                # Блокируем задачу и убеждаемся, что она всё ещё в пуле
                task = (
                    Task.objects.select_for_update()
                    .select_related("shift")
                    .get(pk=task_id)
                )
                if task.status != "pending" or task.shift_id is not None:
                    return False  # кто-то уже занялся этой задачей

                # Назначаем смену
                updated = (
                    Task.objects
                    .filter(pk=task.pk, status="pending", shift__isnull=True)
                    .update(shift=sh)
                )
                if not updated:
                    return False  # гонка, задача изменилась

                # Обновим инстанс (у task.shift ещё None)
                task.shift_id = sh.id
                task.shift = sh

                # Пробуем назначить лучшего сотрудника
                ok = assign_task_to_best_employee(task, sh)
                if ok:
                    print(f"✅ Задача #{task.id}: смена #{sh.id} и сотрудник назначены")
                    return True

                # Не получилось подобрать сотрудника → вернём задачу в пул
                Task.objects.filter(pk=task.pk, shift=sh).update(shift=None)
                print(f"⏸ Задача #{task.id}: в смене #{sh.id} нет подходящего сотрудника — возвращаю в пул")
                # и пробуем следующую смену

        except Exception as e:
            print(f"❌ Ошибка при обработке задачи #{task_id} со сменой #{sh.id}: {e}")

    # Ни в одной смене не удалось подобрать сотрудника
    return False


def tick_once() -> int:
    now = timezone.now()
    shifts = eligible_active_shifts(now)
    if not shifts:
        print("ℹ️  Нет подходящих активных смен — пропуск тика.")
        return 0

    tasks = pool_tasks()
    if not tasks:
        print("ℹ️  Пул пуст — пропуск тика.")
        return 0

    success = 0
    for t in tasks:
        if try_assign_task_to_some_shift(t.id, shifts):
            success += 1
    return success


def loop():
    print(f"🧠 AI-демон запущен. Период проверки: {CHECK_INTERVAL_SEC} сек.")
    while True:
        try:
            n = tick_once()
            if n:
                print(f"📦 Успешно назначено задач (смена+сотрудник): {n}")
        except Exception as e:
            print("❌ Критическая ошибка цикла:", e)
        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    loop()
