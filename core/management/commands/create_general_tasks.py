from __future__ import annotations

import random
from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from core.models import Task
from core.serializers import TaskCreateSerializer


class Command(BaseCommand):
    help = (
        "Create N GENERAL tasks through the same serializer that is used by "
        "POST /api/tasks/. Tasks are placed into the common task pool and can "
        "then be assigned by the background assignment loop."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "count",
            type=int,
            help="Количество GENERAL-задач, которые нужно создать.",
        )
        parser.add_argument(
            "--name-prefix",
            default="Manual GENERAL task",
            help="Префикс названия задачи.",
        )
        parser.add_argument(
            "--description",
            default=(
                "GENERAL task created by management command through "
                "TaskCreateSerializer, equivalent to API task creation."
            ),
            help="Описание задачи.",
        )
        parser.add_argument(
            "--priority",
            type=int,
            default=1,
            help="Приоритет задачи. Чем выше, тем раньше задача будет взята из пула.",
        )
        parser.add_argument(
            "--difficulty",
            type=int,
            default=1,
            help="Сложность задачи от 1 до 5.",
        )
        parser.add_argument(
            "--estimated-minutes",
            type=int,
            default=10,
            help=(
                "Оценочная длительность задачи в минутах. "
                "Поле не входит в текущий API-сериализатор, поэтому команда "
                "дописывает его после serializer.save()."
            ),
        )
        parser.add_argument(
            "--due-in-minutes",
            type=int,
            default=None,
            help="Если указано, задаёт due_at как now() + N минут.",
        )
        parser.add_argument(
            "--payload-note",
            default="",
            help="Необязательная заметка, которая попадёт в payload.note.",
        )
        parser.add_argument(
            "--source",
            choices=["manual", "auto"],
            default="manual",
            help=(
                "Значение поля Task.source. Для задач, созданных человеком, "
                "логичнее manual."
            ),
        )
        parser.add_argument(
            "--external-prefix",
            default="",
            help=(
                "Необязательный префикс external_ref. Если не указан, "
                "external_ref остаётся пустым, как при обычном создании через API."
            ),
        )
        parser.add_argument(
            "--vary",
            action="store_true",
            help=(
                "Слегка варьировать priority, difficulty и estimated_minutes "
                "для более живого набора задач."
            ),
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Seed для режима --vary.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Проверить входные параметры и показать пример без записи в БД.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        count = int(options["count"])
        priority = int(options["priority"])
        difficulty = int(options["difficulty"])
        estimated_minutes = int(options["estimated_minutes"])
        due_in_minutes = options.get("due_in_minutes")
        dry_run = bool(options["dry_run"])
        rng = random.Random(int(options["seed"]))

        if count <= 0:
            raise CommandError("count должен быть положительным числом.")
        if priority < 0:
            raise CommandError("--priority должен быть >= 0.")
        if not 1 <= difficulty <= 5:
            raise CommandError("--difficulty должен быть в диапазоне 1..5.")
        if estimated_minutes <= 0:
            raise CommandError("--estimated-minutes должен быть > 0.")
        if due_in_minutes is not None and int(due_in_minutes) <= 0:
            raise CommandError("--due-in-minutes должен быть > 0.")

        preview_data, preview_estimated = self._build_task_data(
            index=1,
            options=options,
            priority=priority,
            difficulty=difficulty,
            estimated_minutes=estimated_minutes,
            rng=rng,
        )

        if dry_run:
            serializer = TaskCreateSerializer(data=preview_data)
            serializer.is_valid(raise_exception=True)
            self.stdout.write(self.style.WARNING("DRY RUN: задачи не будут созданы."))
            self.stdout.write(f"Пример данных для TaskCreateSerializer: {preview_data}")
            self.stdout.write(
                "После serializer.save() команда дополнительно выставила бы: "
                f"estimated_minutes={preview_estimated}, "
                f"source={options['source']!r}, "
                f"due_at={'now() + ' + str(due_in_minutes) + ' min' if due_in_minutes else None}, "
                f"external_ref_prefix={options['external_prefix']!r}"
            )
            return

        created_tasks: list[Task] = []
        now_ts = timezone.now()

        with transaction.atomic():
            for index in range(1, count + 1):
                task_data, task_estimated_minutes = self._build_task_data(
                    index=index,
                    options=options,
                    priority=priority,
                    difficulty=difficulty,
                    estimated_minutes=estimated_minutes,
                    rng=rng,
                )

                serializer = TaskCreateSerializer(data=task_data)
                serializer.is_valid(raise_exception=True)
                task: Task = serializer.save()

                update_fields = ["estimated_minutes", "source", "updated_at"]
                task.estimated_minutes = task_estimated_minutes
                task.source = str(options["source"])
                task.updated_at = now_ts

                if due_in_minutes is not None:
                    task.due_at = now_ts + timedelta(minutes=int(due_in_minutes))
                    update_fields.append("due_at")

                external_prefix = str(options["external_prefix"] or "").strip()
                if external_prefix:
                    task.external_ref = f"{external_prefix}-{index:04d}"
                    update_fields.append("external_ref")

                task.save(update_fields=update_fields)
                created_tasks.append(task)

        pool_ids = sorted({task.task_pool_id for task in created_tasks})
        task_ids = [task.id for task in created_tasks]

        self.stdout.write(
            self.style.SUCCESS(
                f"Создано GENERAL-задач: {len(created_tasks)}. "
                f"Пулы: {pool_ids}. "
                f"ID первых задач: {task_ids[:10]}"
            )
        )
        self.stdout.write(
            "Задачи находятся в статусе pending, shift=None, assigned_to=None. "
            "Если активная смена уже запущена и Celery Beat/worker работают, "
            "они будут назначены автоматически ближайшим тиком."
        )

    def _build_task_data(
        self,
        *,
        index: int,
        options: dict[str, Any],
        priority: int,
        difficulty: int,
        estimated_minutes: int,
        rng: random.Random,
    ) -> tuple[dict[str, Any], int]:
        if bool(options["vary"]):
            priority_value = max(0, priority + rng.choice([-1, 0, 0, 1]))
            difficulty_value = max(1, min(5, difficulty + rng.choice([-1, 0, 0, 1])))
            estimated_value = max(
                1,
                int(round(estimated_minutes * rng.uniform(0.8, 1.25))),
            )
        else:
            priority_value = priority
            difficulty_value = difficulty
            estimated_value = estimated_minutes

        payload: dict[str, Any] = {}
        payload_note = str(options.get("payload_note") or "").strip()
        if payload_note:
            payload["note"] = payload_note

        data = {
            "name": f"{options['name_prefix']} #{index:04d}",
            "description": str(options["description"]),
            "difficulty": difficulty_value,
            "priority": priority_value,
            "required_qualification_codes": [],
            "task_type": Task.TaskType.GENERAL,
            "payload": payload,
        }
        return data, estimated_value
