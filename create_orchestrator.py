import os
import sys
import subprocess
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "smart_warehouse.settings")

import django  # noqa: E402
django.setup()

from django.core.management import call_command  # noqa: E402
from django.contrib.auth import get_user_model  # noqa: E402


BASE_DIR = Path(__file__).resolve().parent
PY = sys.executable  # текущий интерпретатор Python


def run_seed_script(name: str):
    script_path = BASE_DIR / name
    if not script_path.exists():
        raise SystemExit(f"Не найден скрипт {name} ({script_path})")
    print(f"→ Запуск {name} ...")
    subprocess.run([PY, str(script_path)], check=True)
    print(f"✓ {name} завершён")


def ensure_superuser(username="admin", email="admin@admin.com", password="admin"):
    User = get_user_model()
    username_field = User.USERNAME_FIELD

    # определяем, чем искать пользователя (по username или по email)
    if username_field == "email":
        lookup = {"email": email}
    else:
        lookup = {username_field: username}

    user = User.objects.filter(**lookup).first()
    if user:
        # обновим пароль для предсказуемой локальной разработки
        user.set_password(password)
        user.is_staff = True
        user.is_superuser = True
        user.save(update_fields=["password", "is_staff", "is_superuser"])
        print(f"✓ Суперпользователь уже был ({lookup}), пароль обновлён")
        return

    # создаём нового суперпользователя
    if username_field == "email":
        user = User.objects.create_superuser(email=email, password=password)
    else:
        # стандартный случай: username + email
        user = User.objects.create_superuser(username=username, email=email, password=password)
    print(f"✓ Суперпользователь создан: {username} / {email} / <пароль задан>")


def main():
    print("=== Шаг 1. makemigrations ===")
    call_command("makemigrations", interactive=False)

    print("=== Шаг 2. migrate ===")
    call_command("migrate", interactive=False)

    print("=== Шаг 3. createsuperuser ===")
    ensure_superuser(username="admin", email="admin@admin.com", password="admin")

    print("=== Шаг 4–6. Скрипты инициализации данных ===")
    run_seed_script("create_employees.py")
    run_seed_script("create_locations.py")
    run_seed_script("create_cargos.py")

    print("🎉 Готово!")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Ошибка при выполнении команды: {e}") from e
    except Exception as e:
        raise SystemExit(f"Сбой инициализации: {e}") from e
