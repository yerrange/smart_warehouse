# Минимальный запуск
# python manage.py export_execution_dataset

# Выгрузка всех задач, не только completed
# python manage.py export_execution_dataset --include-non-completed

# Выгрузка за конкретный период
# python manage.py export_execution_dataset --date-from 2026-01-01 --date-to 2026-04-30


from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Optional

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count
from django.utils import timezone

from core.models import Task


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise CommandError(
            f"Некорректная дата '{value}'. Используй формат YYYY-MM-DD."
        ) from e


def _iso(dt) -> str:
    return dt.isoformat() if dt else ""


def _minutes_between(start, end) -> Optional[int]:
    if not start or not end:
        return None
    return int((end - start).total_seconds() // 60)


class Command(BaseCommand):
    help = (
        "Выгружает эксплуатационный датасет по задачам и назначениям из БД "
        "в CSV и печатает первичную сводку по качеству данных."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--out",
            default="ml_data/execution_dataset.csv",
            help="Путь к выходному CSV-файлу.",
        )
        parser.add_argument(
            "--include-non-completed",
            action="store_true",
            help="Если указан, выгружать не только completed-задачи, но и все задачи.",
        )
        parser.add_argument(
            "--date-from",
            help="Нижняя граница по task.created_at (YYYY-MM-DD).",
        )
        parser.add_argument(
            "--date-to",
            help="Верхняя граница по task.created_at (YYYY-MM-DD).",
        )

    def handle(self, *args, **options):
        out_path = Path(options["out"])
        include_non_completed = bool(options["include_non_completed"])
        date_from = _parse_date(options.get("date_from"))
        date_to = _parse_date(options.get("date_to"))

        qs = (
            Task.objects
            .select_related("assigned_to", "shift", "cargo")
            .prefetch_related("required_qualifications")
            .annotate(assignment_log_count=Count("assignment_history", distinct=True))
            .order_by("id")
        )

        if not include_non_completed:
            qs = qs.filter(status=Task.Status.COMPLETED)

        if date_from:
            qs = qs.filter(created_at__date__gte=date_from)
        if date_to:
            qs = qs.filter(created_at__date__lte=date_to)

        fieldnames = [
            "task_id",
            "task_type",
            "status",
            "source",
            "priority",
            "difficulty",
            "estimated_minutes",
            "actual_minutes_db",
            "assignment_log_count",
            "required_qualification_codes",
            "shift_id",
            "shift_date",
            "shift_start_time",
            "shift_actual_start_time",
            "shift_actual_end_time",
            "employee_id",
            "employee_code",
            "cargo_id",
            "cargo_code",
            "created_at",
            "assigned_at",
            "started_at",
            "completed_at",
            "due_at",
            "queue_wait_minutes",
            "assignment_to_start_minutes",
            "fact_execution_minutes",
            "total_cycle_minutes",
        ]

        out_path.parent.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        completed_rows = 0
        rows_with_fact_execution = 0

        missing_assigned_at_for_completed = 0
        missing_started_at_for_completed = 0
        missing_completed_at_for_completed = 0

        negative_queue_wait = 0
        negative_assignment_to_start = 0
        negative_fact_execution = 0
        negative_total_cycle = 0

        actual_minutes_mismatch = 0

        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for task in qs.iterator(chunk_size=200):
                total_rows += 1

                is_completed = task.status == Task.Status.COMPLETED
                if is_completed:
                    completed_rows += 1

                queue_wait_minutes = _minutes_between(task.created_at, task.assigned_at)
                assignment_to_start_minutes = _minutes_between(task.assigned_at, task.started_at)
                fact_execution_minutes = _minutes_between(task.started_at, task.completed_at)
                total_cycle_minutes = _minutes_between(task.created_at, task.completed_at)

                if fact_execution_minutes is not None:
                    rows_with_fact_execution += 1

                if is_completed and task.assigned_at is None:
                    missing_assigned_at_for_completed += 1
                if is_completed and task.started_at is None:
                    missing_started_at_for_completed += 1
                if is_completed and task.completed_at is None:
                    missing_completed_at_for_completed += 1

                if queue_wait_minutes is not None and queue_wait_minutes < 0:
                    negative_queue_wait += 1
                if assignment_to_start_minutes is not None and assignment_to_start_minutes < 0:
                    negative_assignment_to_start += 1
                if fact_execution_minutes is not None and fact_execution_minutes < 0:
                    negative_fact_execution += 1
                if total_cycle_minutes is not None and total_cycle_minutes < 0:
                    negative_total_cycle += 1

                actual_minutes_db = int(task.actual_minutes or 0)
                if (
                    fact_execution_minutes is not None
                    and actual_minutes_db > 0
                    and actual_minutes_db != fact_execution_minutes
                ):
                    actual_minutes_mismatch += 1

                writer.writerow(
                    {
                        "task_id": task.id,
                        "task_type": task.task_type,
                        "status": task.status,
                        "source": task.source,
                        "priority": int(task.priority or 0),
                        "difficulty": int(task.difficulty or 1),
                        "estimated_minutes": int(task.estimated_minutes or 0),
                        "actual_minutes_db": actual_minutes_db,
                        "assignment_log_count": int(task.assignment_log_count or 0),
                        "required_qualification_codes": ";".join(
                            sorted(q.code for q in task.required_qualifications.all())
                        ),
                        "shift_id": task.shift_id or "",
                        "shift_date": task.shift.date.isoformat() if task.shift else "",
                        "shift_start_time": _iso(task.shift.start_time) if task.shift else "",
                        "shift_actual_start_time": _iso(task.shift.actual_start_time) if task.shift else "",
                        "shift_actual_end_time": _iso(task.shift.actual_end_time) if task.shift else "",
                        "employee_id": task.assigned_to_id or "",
                        "employee_code": task.assigned_to.employee_code if task.assigned_to else "",
                        "cargo_id": task.cargo_id or "",
                        "cargo_code": task.cargo.cargo_code if task.cargo else "",
                        "created_at": _iso(task.created_at),
                        "assigned_at": _iso(task.assigned_at),
                        "started_at": _iso(task.started_at),
                        "completed_at": _iso(task.completed_at),
                        "due_at": _iso(task.due_at),
                        "queue_wait_minutes": "" if queue_wait_minutes is None else queue_wait_minutes,
                        "assignment_to_start_minutes": "" if assignment_to_start_minutes is None else assignment_to_start_minutes,
                        "fact_execution_minutes": "" if fact_execution_minutes is None else fact_execution_minutes,
                        "total_cycle_minutes": "" if total_cycle_minutes is None else total_cycle_minutes,
                    }
                )

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"CSV сохранён: {out_path.resolve()}"))
        self.stdout.write(f"Экспортировано строк: {total_rows}")
        self.stdout.write(f"Из них completed-задач: {completed_rows}")
        self.stdout.write(f"Строк с вычислимой fact_execution_minutes: {rows_with_fact_execution}")

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("Первичная проверка качества данных:"))
        self.stdout.write(
            f"  completed без assigned_at: {missing_assigned_at_for_completed}"
        )
        self.stdout.write(
            f"  completed без started_at: {missing_started_at_for_completed}"
        )
        self.stdout.write(
            f"  completed без completed_at: {missing_completed_at_for_completed}"
        )
        self.stdout.write(f"  отрицательный queue_wait_minutes: {negative_queue_wait}")
        self.stdout.write(
            f"  отрицательный assignment_to_start_minutes: {negative_assignment_to_start}"
        )
        self.stdout.write(
            f"  отрицательный fact_execution_minutes: {negative_fact_execution}"
        )
        self.stdout.write(
            f"  отрицательный total_cycle_minutes: {negative_total_cycle}"
        )
        self.stdout.write(
            f"  несовпадение actual_minutes_db и fact_execution_minutes: {actual_minutes_mismatch}"
        )

        if total_rows == 0:
            self.stdout.write("")
            self.stdout.write(
                self.style.WARNING(
                    "По выбранным фильтрам не найдено ни одной задачи для выгрузки."
                )
            )
