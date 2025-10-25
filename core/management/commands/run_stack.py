# core/management/commands/run_stack.py
from django.core.management import BaseCommand
import subprocess
import sys
import os
import signal
import time
import shlex

PROJECT_MODULE = "smart_warehouse"  # если у тебя другой пакет с settings/asgi — поправь здесь

def _terminate(proc: subprocess.Popen):
    """Аккуратно останавливаем подпроцесс на любой платформе."""
    if proc and proc.poll() is None:
        try:
            if os.name == "nt":
                proc.terminate()  # Windows
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)  # POSIX
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

def _spawn(cmd_list, cwd=None, env=None, title=None):
    """Запускает процесс и возвращает Popen. Печатает команду для отладки."""
    pretty = " ".join(shlex.quote(x) for x in cmd_list)
    print(f"[run_stack] start {title or cmd_list[0]}: {pretty}")
    creationflags = 0
    preexec_fn = None
    if os.name != "nt":
        # создать отдельную группу процессов, чтобы корректно гасить по Ctrl+C
        preexec_fn = os.setsid
    return subprocess.Popen(
        cmd_list,
        cwd=cwd,
        env=env or os.environ.copy(),
        creationflags=creationflags,
        preexec_fn=preexec_fn,
    )

class Command(BaseCommand):
    help = "Запускает ASGI-сервер Django, Celery worker и Celery beat одним вызовом."

    def add_arguments(self, parser):
        parser.add_argument("--host", default="127.0.0.1", help="Адрес ASGI-сервера (по умолчанию 127.0.0.1).")
        parser.add_argument("--port", default=8000, type=int, help="Порт ASGI-сервера (по умолчанию 8000).")
        parser.add_argument("--daphne", action="store_true",
                            help="Использовать daphne вместо runserver (нужно: pip install daphne).")
        parser.add_argument("--reload", action="store_true",
                            help="Релоадер для ASGI-сервера (uvicorn/daphne могут плодить процессы — в проде не используйте).")
        parser.add_argument("--concurrency", type=int, default=(1 if os.name == "nt" else 4),
                            help="Количество воркеров Celery (на Windows обычно 1).")
        parser.add_argument("--pool", default=("solo" if os.name == "nt" else "prefork"),
                            help="Пул Celery: solo|threads|prefork. На Windows используйте solo/threads.")
        parser.add_argument("--loglevel", default="info", help="Уровень логов Celery/ASGI.")

    def handle(self, *args, **opts):
        host = opts["host"]
        port = str(opts["port"])
        use_daphne = bool(opts["daphne"])
        reload_flag = bool(opts["reload"])
        conc = str(opts["concurrency"])
        pool = opts["pool"]
        loglevel = opts["loglevel"].lower()

        env = os.environ.copy()
        # здесь при желании можно прокинуть брокер/бэкенд, если не задаёшь их в settings/.env:
        # env.setdefault("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
        # env.setdefault("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1")

        processes = []

        try:
            # 1) ASGI-сервер
            if use_daphne:
                # daphne нужен для Channels; запускаем модульно, чтобы не зависеть от PATH
                web_cmd = [sys.executable, "-m", "daphne", "-b", host, "-p", port, f"{PROJECT_MODULE}.asgi:application"]
                if reload_flag:
                    # У daphne нет нативного --reload; для дев-режима можно оставить runserver/uvicorn
                    print("[run_stack] Внимание: daphne не поддерживает --reload. Игнорируем флаг.")
            else:
                # Просто dev-сервер Django (при наличии Channels — это ASGI)
                web_cmd = [sys.executable, "manage.py", "runserver", f"{host}:{port}"]
                # runserver сам делает reload в dev-режиме — отдельный флаг не нужен

            p_web = _spawn(web_cmd, env=env, title="asgi")
            processes.append(("web", p_web))

            # 2) Celery worker
            worker_cmd = [sys.executable, "-m", "celery", "-A", PROJECT_MODULE, "worker", "-l", loglevel]
            # Пул на Windows — solo/threads. prefork на Windows неустойчив.
            if pool:
                worker_cmd += ["-P", pool]
            if pool in ("solo", "threads"):
                worker_cmd += ["--concurrency", conc]
            p_worker = _spawn(worker_cmd, env=env, title="celery-worker")
            processes.append(("worker", p_worker))

            # 3) Celery beat (расписания)
            beat_cmd = [sys.executable, "-m", "celery", "-A", PROJECT_MODULE, "beat", "-l", loglevel]
            p_beat = _spawn(beat_cmd, env=env, title="celery-beat")
            processes.append(("beat", p_beat))

            # Немного подождать, показать PID’ы
            time.sleep(1.0)
            self.stdout.write(self.style.SUCCESS(
                f"Запущено: web pid={p_web.pid}, worker pid={p_worker.pid}, beat pid={p_beat.pid}. Нажми Ctrl+C для остановки."
            ))

            # Ждём падения любого процесса
            while True:
                dead = [(name, p) for (name, p) in processes if p.poll() is not None]
                if dead:
                    for name, p in dead:
                        self.stderr.write(self.style.ERROR(f"[run_stack] {name} exited with code {p.returncode}"))
                    break
                time.sleep(0.5)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Останавливаю процессы..."))
        finally:
            # Гасим всё
            for name, p in processes:
                _terminate(p)
            # Даём время завершиться
            time.sleep(1.0)
            for name, p in processes:
                if p and p.poll() is None:
                    try:
                        p.kill()
                    except Exception:
                        pass
            self.stdout.write(self.style.SUCCESS("Стек остановлен."))
