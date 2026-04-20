# core/management/commands/simulate_history.py
"""
Что делает команда:
1) Создаёт (или переиспользует) Shift.
2) Создаёт Qualifications (если их нет).
3) Создаёт/берёт SIM-сотрудников и раздаёт им квалификации.
4) Гарантирует, что для каждого профиля задач есть минимум K eligible сотрудников.
5) Генерирует задачи в течение смены, назначает их подходящим сотрудникам, завершает.
6) Пишет TaskAssignmentLog.
7) Опционально экспортирует CSV-датасет (task, employee) + label.
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Iterable

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from core.models import (
    Employee,
    Qualification,
    Shift,
    EmployeeShiftStats,
    Task,
    TaskAssignmentLog,
)

# --------------------------- Presets ----------------------------

DEFAULT_QUALS: list[tuple[str, str]] = [
    ("FORKLIFT", "Водитель погрузчика"),
    ("QC", "Контроль качества"),
    ("DISPATCH", "Отгрузка"),
    ("RECEIVE", "Приёмка"),
]


@dataclass(frozen=True)
class TaskProfile:
    task_type: str
    base_minutes: int
    default_priority: int
    required_qual_codes: tuple[str, ...]


TASK_PROFILES: dict[str, TaskProfile] = {
    Task.TaskType.RECEIVE_TO_INBOUND: TaskProfile(
        task_type=Task.TaskType.RECEIVE_TO_INBOUND,
        base_minutes=18,
        default_priority=3,
        required_qual_codes=("RECEIVE", "FORKLIFT"),
    ),
    Task.TaskType.PUTAWAY_TO_RACK: TaskProfile(
        task_type=Task.TaskType.PUTAWAY_TO_RACK,
        base_minutes=22,
        default_priority=2,
        required_qual_codes=("FORKLIFT",),
    ),
    Task.TaskType.MOVE_BETWEEN_SLOTS: TaskProfile(
        task_type=Task.TaskType.MOVE_BETWEEN_SLOTS,
        base_minutes=15,
        default_priority=1,
        required_qual_codes=("FORKLIFT",),
    ),
    Task.TaskType.DISPATCH_CARGO: TaskProfile(
        task_type=Task.TaskType.DISPATCH_CARGO,
        base_minutes=20,
        default_priority=4,
        required_qual_codes=("DISPATCH",),
    ),
    Task.TaskType.GENERAL: TaskProfile(
        task_type=Task.TaskType.GENERAL,
        base_minutes=10,
        default_priority=0,
        required_qual_codes=(),
    ),
}


def _aware_datetime(d, t) -> datetime:
    """Make timezone-aware datetime for the project's timezone."""
    tz = timezone.get_current_timezone()
    return timezone.make_aware(datetime.combine(d, t), tz)


def _ensure_qualifications() -> dict[str, Qualification]:
    out: dict[str, Qualification] = {}
    for code, name in DEFAULT_QUALS:
        q, _ = Qualification.objects.get_or_create(
            code=code,
            defaults={"name": name, "description": "SIM qualification"},
        )
        out[code] = q
    return out


def _ensure_sim_employees(count: int) -> list[Employee]:
    """
    Ensure at least `count` active SIM employees exist.
    IMPORTANT: we only use SIM employees so we don't mutate real seed data.
    """
    existing_sim = list(
        Employee.objects.filter(is_active=True, employee_code__startswith="SIM").order_by("id")
    )
    if len(existing_sim) >= count:
        return existing_sim[:count]

    to_create = count - len(existing_sim)
    created: list[Employee] = []
    # Continue numbering
    max_idx = 0
    for e in existing_sim:
        try:
            max_idx = max(max_idx, int(e.employee_code.replace("SIM", "")))
        except Exception:
            continue

    for i in range(to_create):
        idx = max_idx + 1 + i
        emp, _ = Employee.objects.get_or_create(
            employee_code=f"SIM{idx:04d}",
            defaults={
                "first_name": f"Sim{idx}",
                "last_name": "Employee",
                "is_active": True,
            },
        )
        created.append(emp)

    return existing_sim + created


def _assign_random_qualifications(
    *,
    employees: list[Employee],
    quals: dict[str, Qualification],
    rng: random.Random,
) -> None:
    """
    Randomly assigns qualifications for SIM employees.

    Guarantees:
    – repeatable runs: clear quals first
    – every qualification exists on at least one employee
    """
    for e in employees:
        e.qualifications.clear()

    pool = employees[:]
    rng.shuffle(pool)

    # coverage: every qual -> at least one employee
    for _, q in quals.items():
        if pool:
            pool.pop().qualifications.add(q)

    qual_list = list(quals.values())
    for e in employees:
        extra_n = rng.randint(0, 2)
        if extra_n:
            for q in rng.sample(qual_list, k=min(extra_n, len(qual_list))):
                e.qualifications.add(q)


def _has_required_quals(employee: Employee, required: set[Qualification]) -> bool:
    if not required:
        return True
    actual = set(employee.qualifications.all())
    return required.issubset(actual)


def _ensure_min_eligible_for_profiles(
    *,
    employees: list[Employee],
    quals: dict[str, Qualification],
    profiles: Iterable[TaskProfile],
    min_eligible: int,
    rng: random.Random,
) -> None:
    """
    Ensures that for each profile with required_qual_codes there are at least `min_eligible`
    employees that satisfy all required qualifications.

    This is crucial for ML: if eligible size == 1, the model has nothing to choose from.
    """
    if min_eligible <= 1:
        return

    for profile in profiles:
        codes = tuple(profile.required_qual_codes)
        if not codes:
            continue

        required_qs = {quals[c] for c in codes if c in quals}
        if not required_qs:
            continue

        current = [e for e in employees if _has_required_quals(e, required_qs)]
        need = min_eligible - len(current)
        if need <= 0:
            continue

        current_ids = {e.id for e in current}
        candidates = [e for e in employees if e.id not in current_ids]
        rng.shuffle(candidates)

        # Add missing qualifications to some employees.
        for e in candidates[:need]:
            missing = required_qs - set(e.qualifications.all())
            if missing:
                e.qualifications.add(*missing)


def _ensure_shift(*, shift_date, name: str, start_time: datetime, end_time: datetime) -> Shift:
    """Create or reuse shift. If shift exists, update its time window to be consistent."""
    shift, _ = Shift.objects.get_or_create(
        date=shift_date,
        name=name,
        defaults={
            "start_time": start_time,
            "end_time": end_time,
            "is_active": False,
            "actual_start_time": start_time,
            "actual_end_time": end_time,
        },
    )

    need_update = (shift.start_time != start_time) or (shift.end_time != end_time)
    if need_update:
        shift.start_time = start_time
        shift.end_time = end_time
        shift.actual_start_time = start_time
        shift.actual_end_time = end_time
        shift.is_active = False
        shift.save(
            update_fields=[
                "start_time",
                "end_time",
                "actual_start_time",
                "actual_end_time",
                "is_active",
                "updated_at",
            ]
        )
    return shift


def _ensure_shift_stats(shift: Shift, employees: Iterable[Employee]) -> dict[int, EmployeeShiftStats]:
    stats_by_emp: dict[int, EmployeeShiftStats] = {}
    for e in employees:
        s, _ = EmployeeShiftStats.objects.get_or_create(employee=e, shift=shift)
        stats_by_emp[e.id] = s
    return stats_by_emp


# --------------------------- Command ---------------------------------------


class Command(BaseCommand):
    help = "Generate synthetic task assignment history (for ML training)."

    def add_arguments(self, parser):
        parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible simulation")
        parser.add_argument("--employees", type=int, default=12, help="How many employees to include (SIM only)")
        parser.add_argument("--tasks", type=int, default=200, help="How many tasks to generate")
        parser.add_argument(
            "--shift-date",
            default=None,
            help="Shift date in YYYY-MM-DD (default: today in project timezone)",
        )
        parser.add_argument("--shift-name", default="SIM shift", help="Shift name to create/reuse")

        parser.add_argument(
            "--min-eligible",
            type=int,
            default=3,
            help="Minimum number of eligible employees per task profile (important for ML).",
        )

        parser.add_argument(
            "--explore",
            type=float,
            default=0.15,
            help="Exploration rate: with this probability we pick not-the-best candidate",
        )
        parser.add_argument(
            "--topk",
            type=int,
            default=3,
            help="If exploring, choose randomly among top-k candidates (k>=1)",
        )
        parser.add_argument(
            "--purge",
            action="store_true",
            help="Delete previously simulated tasks/logs (Task.external_ref starts with SIM-)",
        )
        parser.add_argument(
            "--reset-stats",
            action="store_true",
            help="Reset EmployeeShiftStats counters for this simulation shift (recommended with --purge).",
        )
        parser.add_argument("--dataset-out", default=None, help="Optional CSV path to export training dataset")
        parser.add_argument(
            "--pairs-per-task",
            type=int,
            default=6,
            help="How many (task, employee) rows to export per task (samples from eligible).",
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        rng = random.Random(int(opts["seed"]))

        # ---- Shift date ----
        if opts["shift_date"]:
            shift_date = datetime.strptime(opts["shift_date"], "%Y-%m-%d").date()
        else:
            shift_date = timezone.localdate()

        if opts["purge"]:
            self._purge_previous()

        # ---- Ensure reference data ----
        quals = _ensure_qualifications()
        employees = _ensure_sim_employees(int(opts["employees"]))

        _assign_random_qualifications(employees=employees, quals=quals, rng=rng)

        # IMPORTANT: make sure we have multiple eligible employees for each profile
        _ensure_min_eligible_for_profiles(
            employees=employees,
            quals=quals,
            profiles=TASK_PROFILES.values(),
            min_eligible=int(opts["min_eligible"]),
            rng=rng,
        )

        # ---- Shift timeline 09:00–18:00 ----
        shift_start = _aware_datetime(shift_date, time(9, 0))
        shift_end = _aware_datetime(shift_date, time(18, 0))

        shift = _ensure_shift(
            shift_date=shift_date,
            name=str(opts["shift_name"]),
            start_time=shift_start,
            end_time=shift_end,
        )

        stats_by_emp = _ensure_shift_stats(shift, employees)

        if opts["reset_stats"] or opts["purge"]:
            EmployeeShiftStats.objects.filter(shift=shift, employee__in=employees).update(
                is_busy=False,
                task_assigned_count=0,
                task_completed_count=0,
                shift_score=0,
                last_task_at=None,
            )
            for s in stats_by_emp.values():
                s.is_busy = False
                s.task_assigned_count = 0
                s.task_completed_count = 0
                s.shift_score = 0
                s.last_task_at = None

        # in-memory stats – состояние ДО назначения каждой задачи
        mem_stats = {
            e.id: {
                "task_assigned_count": stats_by_emp[e.id].task_assigned_count or 0,
                "task_completed_count": stats_by_emp[e.id].task_completed_count or 0,
                "shift_score": stats_by_emp[e.id].shift_score or 0,
                "last_task_at": stats_by_emp[e.id].last_task_at,
            }
            for e in employees
        }

        # Optional dataset writer
        dataset_path = Path(opts["dataset_out"]) if opts.get("dataset_out") else None
        dataset_fh = None
        dataset_writer = None
        if dataset_path:
            dataset_path.parent.mkdir(parents=True, exist_ok=True)
            dataset_fh = dataset_path.open("w", newline="", encoding="utf-8")
            dataset_writer = csv.DictWriter(dataset_fh, fieldnames=self._dataset_fields())
            dataset_writer.writeheader()

        # hidden "truth" – per-employee speed by task type
        speed = self._make_speed_profiles(employees, rng)

        cursor = shift_start
        created_tasks = 0

        explore = float(opts["explore"])
        topk = max(1, int(opts["topk"]))
        pairs_per_task = int(opts["pairs_per_task"])

        for i in range(int(opts["tasks"])):
            # keep tasks inside the shift window (для правдоподобия)
            if cursor > (shift_end - timedelta(minutes=30)):
                cursor = shift_start + timedelta(minutes=rng.randint(0, 240))

            profile = rng.choice(list(TASK_PROFILES.values()))
            difficulty = rng.randint(1, 5)  # constraint 1..5
            priority = max(0, profile.default_priority + rng.choice([-1, 0, 0, 1]))

            required_qs = {quals[c] for c in profile.required_qual_codes if c in quals}

            eligible = [e for e in employees if _has_required_quals(e, required_qs)]
            if not eligible:
                # Fallback: should almost never happen now, but keep it safe
                profile = TASK_PROFILES[Task.TaskType.GENERAL]
                required_qs = set()
                eligible = employees[:]

            # score eligible by "true" minutes (true_mins depends on current load BEFORE assignment)
            scored: list[tuple[float, Employee]] = []
            for e in eligible:
                true_mins = self._true_minutes(
                    rng=rng,
                    task_profile=profile,
                    difficulty=difficulty,
                    speed_factor=speed[e.id][profile.task_type],
                    current_load=mem_stats[e.id]["task_assigned_count"],
                )
                scored.append((true_mins, e))
            scored.sort(key=lambda x: x[0])

            # choose: best or explore among top-k
            if rng.random() < explore and len(scored) > 1:
                k = min(topk, len(scored))
                chosen_true_mins, chosen_emp = rng.choice(scored[:k])
                choice_mode = "explore"
            else:
                chosen_true_mins, chosen_emp = scored[0]
                choice_mode = "best"

            assigned_at = cursor
            started_at = assigned_at + timedelta(minutes=rng.randint(1, 7))
            completed_at = started_at + timedelta(minutes=int(round(chosen_true_mins)))
            created_at = assigned_at - timedelta(minutes=rng.randint(0, 10))

            cursor = cursor + timedelta(minutes=rng.randint(2, 8))

            # Create Task (due_at=None to satisfy constraint)
            task = Task.objects.create(
                name=f"SIM task #{i + 1}",
                description=f"Synthetic task for ML training (choice={choice_mode}).",
                task_type=profile.task_type,
                payload={"sim": True, "choice_mode": choice_mode, "seed": int(opts["seed"])},
                status=Task.Status.COMPLETED,
                priority=priority,
                difficulty=difficulty,
                estimated_minutes=int(profile.base_minutes * (1 + 0.25 * (difficulty - 1))),
                actual_minutes=int(round(chosen_true_mins)),
                due_at=None,
                assigned_at=assigned_at,
                started_at=started_at,
                completed_at=completed_at,
                shift=shift,
                assigned_to=chosen_emp,
                external_ref=f"SIM-{shift_date.isoformat()}-{i + 1}",
                source="auto",
            )

            if required_qs:
                task.required_qualifications.set(required_qs)

            # Align created_at/updated_at with simulated timeline
            Task.objects.filter(pk=task.pk).update(
                created_at=created_at,
                updated_at=completed_at,
            )
            task.created_at = created_at
            task.updated_at = completed_at

            TaskAssignmentLog.objects.create(
                task=task,
                employee=chosen_emp,
                timestamp=assigned_at,
                note=f"SIM assignment ({choice_mode})",
            )

            # IMPORTANT: write dataset rows BEFORE updating mem_stats (state at decision time)
            if dataset_writer:
                self._write_dataset_rows(
                    writer=dataset_writer,
                    rng=rng,
                    task=task,
                    profile=profile,
                    eligible=scored,
                    mem_stats=mem_stats,  # still pre-update
                    shift_start=shift_start,
                    pairs_per_task=pairs_per_task,
                )

            created_tasks += 1

            # update in-memory stats AFTER logging dataset rows
            ms = mem_stats[chosen_emp.id]
            ms["task_assigned_count"] += 1
            ms["task_completed_count"] += 1
            ms["shift_score"] += difficulty
            ms["last_task_at"] = completed_at

        if dataset_fh:
            dataset_fh.close()

        self._apply_mem_stats(stats_by_emp=stats_by_emp, mem_stats=mem_stats)

        self.stdout.write(
            self.style.SUCCESS(
                f"Simulation done. shift_id={shift.id}, employees={len(employees)}, tasks_created={created_tasks}"
            )
        )
        if dataset_path:
            self.stdout.write(self.style.SUCCESS(f"Dataset exported to: {dataset_path.resolve()}"))

    # ------------------------- Purge & stats --------------------------------

    def _purge_previous(self) -> None:
        logs = TaskAssignmentLog.objects.filter(task__external_ref__startswith="SIM-")
        tasks = Task.objects.filter(external_ref__startswith="SIM-")

        deleted_logs = logs.count()
        deleted_tasks = tasks.count()

        logs.delete()
        tasks.delete()

        self.stdout.write(self.style.WARNING(f"Purged previous simulation: logs={deleted_logs}, tasks={deleted_tasks}"))

    def _apply_mem_stats(
        self,
        *,
        stats_by_emp: dict[int, EmployeeShiftStats],
        mem_stats: dict[int, dict],
    ) -> None:
        to_update: list[EmployeeShiftStats] = []
        for emp_id, stats in stats_by_emp.items():
            ms = mem_stats.get(emp_id)
            if not ms:
                continue
            stats.task_assigned_count = ms["task_assigned_count"]
            stats.task_completed_count = ms["task_completed_count"]
            stats.shift_score = ms["shift_score"]
            stats.last_task_at = ms["last_task_at"]
            stats.is_busy = False
            to_update.append(stats)

        if to_update:
            EmployeeShiftStats.objects.bulk_update(
                to_update,
                ["task_assigned_count", "task_completed_count", "shift_score", "last_task_at", "is_busy"],
            )

    # ------------------------- Hidden truth ---------------------------------

    def _make_speed_profiles(self, employees: list[Employee], rng: random.Random) -> dict[int, dict[str, float]]:
        speeds: dict[int, dict[str, float]] = {}
        for e in employees:
            per_type: dict[str, float] = {}
            base = rng.uniform(0.85, 1.15)
            for t in TASK_PROFILES.keys():
                per_type[t] = base * rng.uniform(0.80, 1.25)
            speeds[e.id] = per_type
        return speeds

    def _true_minutes(
        self,
        *,
        rng: random.Random,
        task_profile: TaskProfile,
        difficulty: int,
        speed_factor: float,
        current_load: int,
    ) -> float:
        diff_mult = 1.0 + 0.25 * max(0, difficulty - 1)
        load_penalty = 1.0 + 0.03 * min(current_load, 25)
        noise = rng.gauss(mu=1.0, sigma=0.08)
        minutes = (task_profile.base_minutes * diff_mult * load_penalty * noise) / max(speed_factor, 0.35)
        return max(2.0, minutes)

    # ------------------------- Dataset export -------------------------------

    def _dataset_fields(self) -> list[str]:
        return [
            "task_id",
            "employee_id",
            "task_type",
            "priority",
            "difficulty",
            "estimated_minutes",
            "task_age_minutes",
            "emp_task_assigned_count",
            "emp_task_completed_count",
            "emp_shift_score",
            "minutes_into_shift",
            "label_true_minutes",
            "was_assigned",
        ]

    def _write_dataset_rows(
        self,
        *,
        writer: csv.DictWriter,
        rng: random.Random,
        task: Task,
        profile: TaskProfile,
        eligible: list[tuple[float, Employee]],
        mem_stats: dict[int, dict],
        shift_start: datetime,
        pairs_per_task: int,
    ) -> None:
        if pairs_per_task <= 0:
            return

        assigned_id = task.assigned_to_id

        # Always include the assigned employee row + sample a few others
        rows: list[tuple[float, Employee]] = []
        for m, e in eligible:
            if e.id == assigned_id:
                rows.append((m, e))
                break

        others = [(m, e) for (m, e) in eligible if e.id != assigned_id]
        rng.shuffle(others)
        need = max(0, pairs_per_task - len(rows))
        rows.extend(others[:need])

        minutes_into_shift = 0
        if task.assigned_at:
            minutes_into_shift = int((task.assigned_at - shift_start).total_seconds() // 60)

        task_age_minutes = 0
        if task.assigned_at and task.created_at:
            task_age_minutes = int((task.assigned_at - task.created_at).total_seconds() // 60)
            if task_age_minutes < 0:
                task_age_minutes = 0

        for true_mins, e in rows:
            ms = mem_stats[e.id]  # IMPORTANT: state at decision time (pre-update)
            writer.writerow(
                {
                    "task_id": task.id,
                    "employee_id": e.id,
                    "task_type": profile.task_type,
                    "priority": task.priority,
                    "difficulty": task.difficulty,
                    "estimated_minutes": task.estimated_minutes,
                    "task_age_minutes": task_age_minutes,
                    "emp_task_assigned_count": ms["task_assigned_count"],
                    "emp_task_completed_count": ms["task_completed_count"],
                    "emp_shift_score": ms["shift_score"],
                    "minutes_into_shift": minutes_into_shift,
                    "label_true_minutes": round(float(true_mins), 3),
                    "was_assigned": 1 if e.id == assigned_id else 0,
                }
            )
