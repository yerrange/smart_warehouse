from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Employee, Qualification


BASE_QUALS: list[tuple[str, str, str]] = [
    ("RECEIVE", "Приёмка", "Базовая квалификация на приёмку груза."),
    ("DISPATCH", "Отгрузка", "Базовая квалификация на отгрузку груза."),
    ("MOVE", "Перемещение", "Базовая квалификация на перемещение груза внутри склада."),
]

EXTRA_QUALS: list[tuple[str, str, str]] = [
    ("FORKLIFT", "Водитель погрузчика", "Допуск к операциям, требующим работу с погрузчиком."),
]

FIRST_NAMES = [
    "Иван", "Пётр", "Алексей", "Дмитрий", "Сергей",
    "Максим", "Егор", "Никита", "Михаил", "Андрей",
    "Роман", "Константин", "Олег", "Антон", "Павел",
    "Владимир", "Илья", "Кирилл", "Арсений", "Денис",
]

LAST_NAMES = [
    "Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов",
    "Попов", "Васильев", "Соколов", "Михайлов", "Новиков",
    "Фёдоров", "Морозов", "Волков", "Алексеев", "Лебедев",
    "Семёнов", "Егоров", "Павлов", "Козлов", "Степанов",
    "Николаев", "Орлов", "Андреев", "Макаров", "Никитин",
    "Захаров", "Зайцев", "Соловьёв", "Борисов", "Яковлев",
    "Григорьев", "Романов", "Воробьёв", "Сергеев", "Крылов",
    "Максимов", "Лазарев", "Голубев", "Беляев", "Тарасов",
    "Белов", "Комаров", "Киселёв", "Ильин", "Гусев",
    "Титов", "Калинин", "Королёв", "Чернов", "Жуков",
]

CODE_RE = re.compile(r"^(?P<prefix>[A-Za-z]+)(?P<num>\d+)$")
DEFAULT_PREFIX = "E"
DEFAULT_FORKLIFT_RATIO = 0.20
DEFAULT_CODE_WIDTH = 4


@dataclass(frozen=True)
class EmployeeIdentity:
    first_name: str
    last_name: str


class Command(BaseCommand):
    help = (
        "Поддерживает фиксированный размер пула сотрудников склада. "
        "Если целевое число меньше текущего числа активных сотрудников, часть сотрудников деактивируется. "
        "Если больше, скрипт сначала реактивирует неактивных сотрудников из того же пула, а затем при необходимости "
        "создаёт новых. Всем сотрудникам пула выдаются базовые квалификации RECEIVE/DISPATCH/MOVE, "
        "а немногим активным дополнительно назначается FORKLIFT."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "count",
            type=int,
            help="Желаемое количество активных сотрудников склада.",
        )
        parser.add_argument(
            "--prefix",
            default=DEFAULT_PREFIX,
            help=f"Префикс кода управляемых сотрудников. По умолчанию: {DEFAULT_PREFIX}.",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Seed для воспроизводимого выбора сотрудников на деактивацию/реактивацию и выдачи FORKLIFT. По умолчанию: 42.",
        )
        parser.add_argument(
            "--forklift-ratio",
            type=float,
            default=DEFAULT_FORKLIFT_RATIO,
            help=f"Доля активных сотрудников, которым будет выдан FORKLIFT. По умолчанию: {DEFAULT_FORKLIFT_RATIO:.2f}.",
        )
        parser.add_argument(
            "--code-width",
            type=int,
            default=DEFAULT_CODE_WIDTH,
            help=f"Ширина числовой части кода сотрудника. По умолчанию: {DEFAULT_CODE_WIDTH}.",
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        target_count = int(opts["count"])
        prefix = str(opts["prefix"] or DEFAULT_PREFIX).strip()
        seed = int(opts["seed"])
        forklift_ratio = float(opts["forklift_ratio"])
        code_width = int(opts["code_width"])

        if target_count < 0:
            raise CommandError("Количество сотрудников не может быть отрицательным.")
        if not prefix:
            raise CommandError("Префикс сотрудников не должен быть пустым.")
        if code_width < 1:
            raise CommandError("--code-width должен быть >= 1.")
        if forklift_ratio < 0 or forklift_ratio > 1:
            raise CommandError("--forklift-ratio должен находиться в диапазоне от 0 до 1.")

        quals = self._ensure_qualifications()
        rng = random.Random(seed)

        pool = self._get_managed_pool(prefix)
        active = [e for e in pool if e.is_active]
        inactive = [e for e in pool if not e.is_active]

        created = 0
        reactivated = 0
        deactivated = 0

        active_count = len(active)

        if active_count > target_count:
            to_deactivate_count = active_count - target_count
            picked = self._sample_employees(active, to_deactivate_count, rng)
            for emp in picked:
                if emp.is_active:
                    emp.is_active = False
                    emp.save(update_fields=["is_active", "updated_at"])
                    deactivated += 1

        elif active_count < target_count:
            shortage = target_count - active_count
            if inactive:
                picked = self._sample_employees(inactive, min(shortage, len(inactive)), rng)
                for emp in picked:
                    if not emp.is_active:
                        emp.is_active = True
                        emp.save(update_fields=["is_active", "updated_at"])
                        reactivated += 1
                shortage -= reactivated

            if shortage > 0:
                used_numbers = self._used_employee_numbers(self._get_managed_pool(prefix), prefix)
                for _ in range(shortage):
                    new_number = self._next_free_number(used_numbers)
                    used_numbers.add(new_number)
                    employee_code = f"{prefix}{new_number:0{code_width}d}"
                    identity = self._identity_for_number(new_number)
                    Employee.objects.create(
                        employee_code=employee_code,
                        first_name=identity.first_name,
                        last_name=identity.last_name,
                        is_active=True,
                    )
                    created += 1

        pool = self._get_managed_pool(prefix)
        active = [e for e in pool if e.is_active]
        active_count = len(active)

        forklift_count = self._calculate_forklift_count(active_count, forklift_ratio)
        forklift_holders = set()
        if forklift_count > 0:
            picked_forklift = self._sample_employees(active, forklift_count, random.Random(seed + 1009))
            forklift_holders = {emp.employee_code for emp in picked_forklift}

        updated_quals = 0
        for emp in pool:
            target_codes = {"RECEIVE", "DISPATCH", "MOVE"}
            if emp.is_active and emp.employee_code in forklift_holders:
                target_codes.add("FORKLIFT")
            if self._sync_employee_qualifications(emp, target_codes, quals):
                updated_quals += 1

        self.stdout.write(self.style.SUCCESS(
            "Готово. "
            f"target_active={target_count}, active_now={active_count}, total_managed={len(pool)}, "
            f"created={created}, reactivated={reactivated}, deactivated={deactivated}, quals_updated={updated_quals}."
        ))

        self.stdout.write(
            f"Управляемый пул: сотрудники с префиксом '{prefix}'."
        )
        self.stdout.write(
            "Базовые квалификации RECEIVE, DISPATCH и MOVE выданы всему управляемому пулу."
        )
        self.stdout.write(
            f"FORKLIFT назначен {forklift_count} активным сотрудникам из {active_count} "
            f"({forklift_ratio:.0%} с округлением вверх, но не меньше 1 при наличии активных сотрудников)."
        )
        if forklift_holders:
            self.stdout.write("Текущие сотрудники с FORKLIFT: " + ", ".join(sorted(forklift_holders)))
        else:
            self.stdout.write("Сейчас активных сотрудников с FORKLIFT нет.")

    def _ensure_qualifications(self) -> dict[str, Qualification]:
        out: dict[str, Qualification] = {}
        for code, name, description in [*BASE_QUALS, *EXTRA_QUALS]:
            qual, _ = Qualification.objects.get_or_create(
                code=code,
                defaults={
                    "name": name,
                    "description": description,
                },
            )
            changed = False
            if qual.name != name:
                qual.name = name
                changed = True
            if qual.description != description:
                qual.description = description
                changed = True
            if changed:
                qual.save(update_fields=["name", "description", "updated_at"])
            out[code] = qual
        return out

    def _get_managed_pool(self, prefix: str) -> list[Employee]:
        employees = list(Employee.objects.filter(employee_code__startswith=prefix).order_by("employee_code"))
        return sorted(employees, key=self._sort_key)

    def _sort_key(self, employee: Employee) -> tuple[int, str]:
        return self._employee_code_sort_key(employee.employee_code)

    def _employee_code_sort_key(self, code: str) -> tuple[int, str]:
        m = CODE_RE.match(code)
        if not m:
            return (10**9, code)
        return (int(m.group("num")), code)

    def _sample_employees(self, employees: list[Employee], count: int, rng: random.Random) -> list[Employee]:
        ordered = sorted(employees, key=self._sort_key)
        if count <= 0:
            return []
        if count >= len(ordered):
            return ordered
        return rng.sample(ordered, count)

    def _used_employee_numbers(self, employees: list[Employee], prefix: str) -> set[int]:
        out: set[int] = set()
        for emp in employees:
            m = CODE_RE.match(emp.employee_code)
            if not m:
                continue
            if m.group("prefix") != prefix:
                continue
            out.add(int(m.group("num")))
        return out

    def _next_free_number(self, used_numbers: set[int]) -> int:
        n = 1
        while n in used_numbers:
            n += 1
        return n

    def _identity_for_number(self, number: int) -> EmployeeIdentity:
        idx = number - 1
        first_name = FIRST_NAMES[idx % len(FIRST_NAMES)]
        # Фамилии распределяем псевдослучайно, но детерминированно по номеру сотрудника,
        # чтобы даже в первых 20–30 сотрудниках не было эффекта "все Ивановы и Петровы".
        # Множитель 7 взаимно прост с длиной списка фамилий (50), поэтому ранние номера
        # дают хорошо перемешанную последовательность без короткого повтора.
        last_name = LAST_NAMES[(idx * 7) % len(LAST_NAMES)]
        return EmployeeIdentity(first_name=first_name, last_name=last_name)

    def _calculate_forklift_count(self, active_count: int, ratio: float) -> int:
        if active_count <= 0:
            return 0
        return max(1, math.ceil(active_count * ratio))

    def _sync_employee_qualifications(
        self,
        employee: Employee,
        target_codes: set[str],
        quals: dict[str, Qualification],
    ) -> bool:
        target_quals = [quals[code] for code in sorted(target_codes) if code in quals]
        existing_codes = set(employee.qualifications.values_list("code", flat=True))
        if existing_codes == target_codes:
            return False
        employee.qualifications.set(target_quals)
        return True
