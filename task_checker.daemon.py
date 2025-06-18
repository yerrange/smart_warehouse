import time
from django.core.wsgi import get_wsgi_application
import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "smart_warehouse.settings")
django.setup()

from core.models import Task
from core.services.tasks import assign_task_to_best_employee

def ping_and_assign():
    pending_tasks = Task.objects.filter(status="pending", assigned_to=None)

    for task in pending_tasks:
        print(f"🔍 Пытаемся назначить задачу #{task.id}")

        # Проверка: есть ли смена и активна ли она
        if task.shift and task.shift.is_active:
            result = assign_task_to_best_employee(task, task.shift)
            if result:
                print(f"✅ Назначена: {task.id}")
            else:
                print(f"⏸ Не удалось назначить: {task.id}")
        else:
            print(f"⛔ Задача #{task.id} — смена отсутствует или неактивна")

            
if __name__ == "__main__":
    print("🧠 AI-демон запущен. Проверка пула задач каждые 5 сек.")
    while True:
        try:
            ping_and_assign()
        except Exception as e:
            print("❌ Ошибка:", e)
        time.sleep(5)
