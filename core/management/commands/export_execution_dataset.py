from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count

from core.ai_task_assignment.features import (
    CAT_COLS,
    DEFAULT_REQUIRED_QUAL_CODES_BY_TASK_TYPE,
    FEATURE_COLS,
)
from core.models import (
    EmployeeTaskProfile,
    EmployeeTaskQualificationModifier,
    Task,
)


TARGET_COL = "label_true_minutes"

RAW_COLS = [
    "task_id",
    "status",
    "source",
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
    "actual_minutes_db",
]

# Эти поля уже есть в FEATURE_COLS, но оставлены в RAW_COLS через уникальную
# сборку заголовка: так CSV одновременно читаем человеком и готов для обучения.
HUMAN_READABLE_FEATURE_COLS = [
    "task_type",
    "priority",
    "difficulty",
    "estimated_minutes",
]


@dataclass(frozen=True)
class PreAssignmentStats:
    assigned_count: int = 0
    completed_count: int = 0
    shift_score: int = 0


@dataclass
class ExportCounters:
    total_seen: int = 0
    rows_written: int = 0
    completed_seen: int = 0
    rows_with_fact_execution: int = 0
    skipped_non_completed: int = 0
    skipped_without_employee: int = 0
    skipped_without_shift: int = 0
    skipped_without_assigned_at: int = 0
    skipped_without_started_at: int = 0
    skipped_without_completed_at: int = 0
    skipped_without_positive_estimate: int = 0
    skipped_without_positive_label: int = 0
    skipped_missing_profile: int = 0
    negative_queue_wait: int = 0
    negative_assignment_to_start: int = 0
    negative_fact_execution: int = 0
    negative_total_cycle: int = 0
    actual_minutes_mismatch: int = 0
    profile_rows_matched: int = 0
    modifier_rows_matched: int = 0


class Command(BaseCommand):
    help = (
        "Exports a training-ready execution dataset for the task assignment model. "
        "The CSV contains FEATURE_COLS from core.ai_task_assignment.features and "
        "label_true_minutes as the target column."
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
            help=(
                "Выгружать не только completed-задачи. Для обучения обычно не нужно: "
                "у незавершённых задач будет пустая целевая переменная."
            ),
        )
        parser.add_argument(
            "--date-from",
            help="Нижняя граница по task.created_at, формат YYYY-MM-DD.",
        )
        parser.add_argument(
            "--date-to",
            help="Верхняя граница по task.created_at, формат YYYY-MM-DD.",
        )
        parser.add_argument(
            "--only-simulated",
            action="store_true",
            help="Выгружать только задачи с external_ref, начинающимся на SIMSHIFT.",
        )
        parser.add_argument(
            "--skip-missing-profiles",
            action="store_true",
            help=(
                "Не останавливать экспорт, если для пары сотрудник-тип задачи нет "
                "EmployeeTaskProfile, а пропускать такие строки. По умолчанию экспорт строгий."
            ),
        )

    def handle(self, *args, **options):
        out_path = Path(options["out"])
        include_non_completed = bool(options["include_non_completed"])
        only_simulated = bool(options["only_simulated"])
        skip_missing_profiles = bool(options["skip_missing_profiles"])
        date_from = _parse_date(options.get("date_from"))
        date_to = _parse_date(options.get("date_to"))

        tasks = self._load_tasks(
            include_non_completed=include_non_completed,
            only_simulated=only_simulated,
            date_from=date_from,
            date_to=date_to,
        )

        counters = ExportCounters(total_seen=len(tasks))

        if not tasks:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            self._write_empty_csv(out_path)
            self.stdout.write(self.style.WARNING("По выбранным фильтрам задачи не найдены."))
            self.stdout.write(self.style.SUCCESS(f"CSV сохранён: {out_path.resolve()}"))
            return

        pre_assignment_stats = self._build_pre_assignment_stats(tasks)
        profile_by_key, modifier_by_profile_and_code = self._load_profile_maps(tasks)

        fieldnames = _unique(
            HUMAN_READABLE_FEATURE_COLS
            + RAW_COLS
            + FEATURE_COLS
            + [TARGET_COL]
            + CAT_COLS
        )

        out_path.parent.mkdir(parents=True, exist_ok=True)

        with out_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for task in tasks:
                row = self._build_row(
                    task=task,
                    pre_assignment_stats=pre_assignment_stats.get(
                        task.id,
                        PreAssignmentStats(),
                    ),
                    profile_by_key=profile_by_key,
                    modifier_by_profile_and_code=modifier_by_profile_and_code,
                    counters=counters,
                    include_non_completed=include_non_completed,
                    skip_missing_profiles=skip_missing_profiles,
                )

                if row is None:
                    continue

                writer.writerow({name: row.get(name, "") for name in fieldnames})
                counters.rows_written += 1

        self._print_report(out_path, counters, fieldnames)

    def _load_tasks(
        self,
        *,
        include_non_completed: bool,
        only_simulated: bool,
        date_from: Optional[date],
        date_to: Optional[date],
    ) -> list[Task]:
        qs = (
            Task.objects
            .select_related("assigned_to", "shift", "cargo")
            .prefetch_related("required_qualifications")
            .annotate(assignment_log_count=Count("assignment_history", distinct=True))
            .order_by("id")
        )

        if not include_non_completed:
            qs = qs.filter(status=Task.Status.COMPLETED)

        if only_simulated:
            qs = qs.filter(external_ref__startswith="SIMSHIFT")

        if date_from:
            qs = qs.filter(created_at__date__gte=date_from)

        if date_to:
            qs = qs.filter(created_at__date__lte=date_to)

        return list(qs)

    def _build_pre_assignment_stats(self, tasks: list[Task]) -> dict[int, PreAssignmentStats]:
        result: dict[int, PreAssignmentStats] = {}
        assigned_counts: dict[tuple[int, int], int] = {}
        completed_counts: dict[tuple[int, int], int] = {}
        score_counts: dict[tuple[int, int], int] = {}

        sortable_tasks = [
            task
            for task in tasks
            if task.shift_id and task.assigned_to_id and task.assigned_at
        ]

        sortable_tasks.sort(
            key=lambda task: (
                int(task.shift_id or 0),
                task.assigned_at,
                int(task.id),
            )
        )

        for task in sortable_tasks:
            key = (int(task.shift_id), int(task.assigned_to_id))

            result[task.id] = PreAssignmentStats(
                assigned_count=assigned_counts.get(key, 0),
                completed_count=completed_counts.get(key, 0),
                shift_score=score_counts.get(key, 0),
            )

            assigned_counts[key] = assigned_counts.get(key, 0) + 1

            if task.status == Task.Status.COMPLETED:
                completed_counts[key] = completed_counts.get(key, 0) + 1
                score_counts[key] = score_counts.get(key, 0) + max(
                    1,
                    int(task.difficulty or 1),
                )

        return result

    def _load_profile_maps(self, tasks: list[Task]):
        employee_ids = {
            int(task.assigned_to_id)
            for task in tasks
            if task.assigned_to_id
        }
        task_types = {
            str(task.task_type)
            for task in tasks
            if task.task_type
        }

        profiles = list(
            EmployeeTaskProfile.objects
            .filter(employee_id__in=employee_ids, task_type__in=task_types)
            .values(
                "id",
                "employee_id",
                "task_type",
                "performance_factor",
                "sigma",
                "sample_count",
                "mean_minutes",
                "source_kind",
            )
        )

        profile_by_key: dict[tuple[int, str], dict[str, Any]] = {}
        profile_ids: list[int] = []

        for profile in profiles:
            profile_id = int(profile["id"])
            profile_ids.append(profile_id)
            profile_by_key[(int(profile["employee_id"]), str(profile["task_type"]))] = profile

        modifiers = list(
            EmployeeTaskQualificationModifier.objects
            .filter(profile_id__in=profile_ids)
            .values(
                "profile_id",
                "factor",
                "sigma_bonus",
                "sample_count",
                "employee_qualification__qualification__code",
            )
        )

        modifier_by_profile_and_code: dict[tuple[int, str], dict[str, Any]] = {}

        for modifier in modifiers:
            profile_id = int(modifier["profile_id"])
            qual_code = str(modifier["employee_qualification__qualification__code"])
            modifier_by_profile_and_code[(profile_id, qual_code)] = modifier

        return profile_by_key, modifier_by_profile_and_code

    def _build_row(
        self,
        *,
        task: Task,
        pre_assignment_stats: PreAssignmentStats,
        profile_by_key: dict[tuple[int, str], dict[str, Any]],
        modifier_by_profile_and_code: dict[tuple[int, str], dict[str, Any]],
        counters: ExportCounters,
        include_non_completed: bool,
        skip_missing_profiles: bool,
    ) -> dict[str, Any] | None:
        is_completed = task.status == Task.Status.COMPLETED

        if is_completed:
            counters.completed_seen += 1
        elif not include_non_completed:
            counters.skipped_non_completed += 1
            return None

        if not task.assigned_to_id:
            counters.skipped_without_employee += 1
            return None

        if not task.shift_id or not task.shift:
            counters.skipped_without_shift += 1
            return None

        if not task.assigned_at:
            counters.skipped_without_assigned_at += 1
            return None

        if not task.started_at:
            counters.skipped_without_started_at += 1
            if not include_non_completed:
                return None

        if not task.completed_at:
            counters.skipped_without_completed_at += 1
            if not include_non_completed:
                return None

        if not task.estimated_minutes or int(task.estimated_minutes) <= 0:
            counters.skipped_without_positive_estimate += 1
            return None

        queue_wait_minutes = _minutes_between(task.created_at, task.assigned_at)
        assignment_to_start_minutes = _minutes_between(task.assigned_at, task.started_at)
        fact_execution_minutes = _minutes_between(task.started_at, task.completed_at)
        total_cycle_minutes = _minutes_between(task.created_at, task.completed_at)
        minutes_into_shift = _minutes_between(task.shift.start_time, task.assigned_at)

        _count_negative(queue_wait_minutes, "queue_wait", counters)
        _count_negative(assignment_to_start_minutes, "assignment_to_start", counters)
        _count_negative(fact_execution_minutes, "fact_execution", counters)
        _count_negative(total_cycle_minutes, "total_cycle", counters)

        actual_minutes_db = int(task.actual_minutes or 0)

        if fact_execution_minutes is not None:
            counters.rows_with_fact_execution += 1

        if (
            fact_execution_minutes is not None
            and actual_minutes_db > 0
            and actual_minutes_db != fact_execution_minutes
        ):
            counters.actual_minutes_mismatch += 1

        label_true_minutes = fact_execution_minutes
        if label_true_minutes is None and actual_minutes_db > 0:
            label_true_minutes = actual_minutes_db

        if not include_non_completed:
            if label_true_minutes is None or label_true_minutes <= 0:
                counters.skipped_without_positive_label += 1
                return None

        required_codes = _required_qualification_codes(task)
        profile_key = (int(task.assigned_to_id), str(task.task_type))
        profile = profile_by_key.get(profile_key)

        if profile is None:
            counters.skipped_missing_profile += 1
            if skip_missing_profiles:
                return None
            employee_code = task.assigned_to.employee_code if task.assigned_to else task.assigned_to_id
            raise CommandError(
                "EmployeeTaskProfile not found while exporting training dataset: "
                f"employee={employee_code}, task_type={task.task_type}. "
                "Run init_employee_task_profiles/update_employee_task_profiles first, "
                "or rerun export with --skip-missing-profiles."
            )

        counters.profile_rows_matched += 1

        modifier_features = self._build_modifier_features(
            profile=profile,
            required_codes=required_codes,
            modifier_by_profile_and_code=modifier_by_profile_and_code,
            counters=counters,
        )

        mean_minutes = profile.get("mean_minutes")
        has_mean = _is_finite_number(mean_minutes)

        row: dict[str, Any] = {
            "task_id": task.id,
            "task_type": str(task.task_type or "unknown"),
            "status": task.status,
            "source": task.source,
            "priority": int(task.priority or 0),
            "difficulty": int(task.difficulty or 1),
            "estimated_minutes": int(task.estimated_minutes or 0),
            "actual_minutes_db": actual_minutes_db,
            "assignment_log_count": int(getattr(task, "assignment_log_count", 0) or 0),
            "required_qualification_codes": ";".join(sorted(required_codes)),
            "shift_id": task.shift_id,
            "shift_date": task.shift.date.isoformat() if task.shift and task.shift.date else "",
            "shift_start_time": _iso(task.shift.start_time) if task.shift else "",
            "shift_actual_start_time": _iso(task.shift.actual_start_time) if task.shift else "",
            "shift_actual_end_time": _iso(task.shift.actual_end_time) if task.shift else "",
            "employee_id": task.assigned_to_id,
            "employee_code": task.assigned_to.employee_code if task.assigned_to else "",
            "cargo_id": task.cargo_id or "",
            "cargo_code": task.cargo.cargo_code if task.cargo else "",
            "created_at": _iso(task.created_at),
            "assigned_at": _iso(task.assigned_at),
            "started_at": _iso(task.started_at),
            "completed_at": _iso(task.completed_at),
            "due_at": _iso(task.due_at),
            "queue_wait_minutes": _blank_if_none(queue_wait_minutes),
            "assignment_to_start_minutes": _blank_if_none(assignment_to_start_minutes),
            "fact_execution_minutes": _blank_if_none(fact_execution_minutes),
            "total_cycle_minutes": _blank_if_none(total_cycle_minutes),
            TARGET_COL: _blank_if_none(label_true_minutes),
            "task_age_minutes": _non_negative_or_zero(queue_wait_minutes),
            "emp_task_assigned_count": int(pre_assignment_stats.assigned_count),
            "emp_task_completed_count": int(pre_assignment_stats.completed_count),
            "emp_shift_score": int(pre_assignment_stats.shift_score),
            "minutes_into_shift": _non_negative_or_zero(minutes_into_shift),
            "emp_profile_performance_factor": _safe_float(
                profile.get("performance_factor"),
                default=1.0,
            ),
            "emp_profile_sigma": _safe_float(profile.get("sigma"), default=0.10),
            "emp_profile_sample_count": int(profile.get("sample_count") or 0),
            "emp_profile_mean_minutes": (
                _safe_float(mean_minutes, default=0.0)
                if has_mean
                else 0.0
            ),
            "emp_profile_has_mean": 1 if has_mean else 0,
            "emp_profile_source_kind": str(profile.get("source_kind") or "unknown"),
            "task_required_qual_count": len(required_codes),
            **modifier_features,
        }

        return row

    def _build_modifier_features(
        self,
        *,
        profile: dict[str, Any],
        required_codes: set[str],
        modifier_by_profile_and_code: dict[tuple[int, str], dict[str, Any]],
        counters: ExportCounters,
    ) -> dict[str, Any]:
        factor_product = 1.0
        sigma_bonus_sum = 0.0
        sample_count_sum = 0
        modifier_count = 0

        profile_id = int(profile["id"])

        for qual_code in required_codes:
            modifier = modifier_by_profile_and_code.get((profile_id, qual_code))
            if modifier is None:
                continue

            factor_product *= _safe_float(modifier.get("factor"), default=1.0)
            sigma_bonus_sum += _safe_float(modifier.get("sigma_bonus"), default=0.0)
            sample_count_sum += int(modifier.get("sample_count") or 0)
            modifier_count += 1

        if modifier_count:
            counters.modifier_rows_matched += 1

        return {
            "emp_modifier_factor_product": factor_product,
            "emp_modifier_sigma_bonus_sum": sigma_bonus_sum,
            "emp_modifier_sample_count_sum": sample_count_sum,
            "emp_required_modifier_count": modifier_count,
        }

    def _write_empty_csv(self, out_path: Path) -> None:
        fieldnames = _unique(
            HUMAN_READABLE_FEATURE_COLS
            + RAW_COLS
            + FEATURE_COLS
            + [TARGET_COL]
            + CAT_COLS
        )
        with out_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

    def _print_report(
        self,
        out_path: Path,
        counters: ExportCounters,
        fieldnames: list[str],
    ) -> None:
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"CSV сохранён: {out_path.resolve()}"))
        self.stdout.write(f"Всего задач найдено: {counters.total_seen}")
        self.stdout.write(f"Строк выгружено: {counters.rows_written}")
        self.stdout.write(f"Completed-задач среди найденных: {counters.completed_seen}")
        self.stdout.write(f"Строк с вычислимой fact_execution_minutes: {counters.rows_with_fact_execution}")
        self.stdout.write(f"Колонок в CSV: {len(fieldnames)}")

        self.stdout.write("")
        self.stdout.write(self.style.NOTICE("Признаки модели:"))
        self.stdout.write(f"  feature_cols: {len(FEATURE_COLS)}")
        self.stdout.write(f"  cat_cols: {', '.join(CAT_COLS)}")
        self.stdout.write(f"  target_col: {TARGET_COL}")

        self.stdout.write("")
        self.stdout.write(self.style.NOTICE("Профильные признаки:"))
        self.stdout.write(f"  profile_rows_matched: {counters.profile_rows_matched}")
        self.stdout.write(f"  modifier_rows_matched: {counters.modifier_rows_matched}")
        self.stdout.write(f"  skipped_missing_profile: {counters.skipped_missing_profile}")

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("Первичная проверка качества данных:"))
        self.stdout.write(f"  пропущено без сотрудника: {counters.skipped_without_employee}")
        self.stdout.write(f"  пропущено без смены: {counters.skipped_without_shift}")
        self.stdout.write(f"  пропущено без assigned_at: {counters.skipped_without_assigned_at}")
        self.stdout.write(f"  пропущено без started_at: {counters.skipped_without_started_at}")
        self.stdout.write(f"  пропущено без completed_at: {counters.skipped_without_completed_at}")
        self.stdout.write(
            f"  пропущено без положительного estimated_minutes: "
            f"{counters.skipped_without_positive_estimate}"
        )
        self.stdout.write(
            f"  пропущено без положительного label_true_minutes: "
            f"{counters.skipped_without_positive_label}"
        )
        self.stdout.write(f"  отрицательный queue_wait_minutes: {counters.negative_queue_wait}")
        self.stdout.write(
            f"  отрицательный assignment_to_start_minutes: "
            f"{counters.negative_assignment_to_start}"
        )
        self.stdout.write(f"  отрицательный fact_execution_minutes: {counters.negative_fact_execution}")
        self.stdout.write(f"  отрицательный total_cycle_minutes: {counters.negative_total_cycle}")
        self.stdout.write(
            f"  несовпадение actual_minutes_db и fact_execution_minutes: "
            f"{counters.actual_minutes_mismatch}"
        )

        if counters.rows_written == 0:
            self.stdout.write("")
            self.stdout.write(
                self.style.WARNING(
                    "Не выгружено ни одной обучающей строки. Проверь фильтры и наличие "
                    "completed-задач с assigned_at/started_at/completed_at."
                )
            )


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise CommandError(
            f"Некорректная дата '{value}'. Используй формат YYYY-MM-DD."
        ) from exc


def _iso(dt) -> str:
    return dt.isoformat() if dt else ""


def _minutes_between(start, end) -> Optional[int]:
    if not start or not end:
        return None

    return int((end - start).total_seconds() // 60)


def _safe_float(value: Any, *, default: float) -> float:
    try:
        if value is None:
            return default

        parsed = float(value)

        if parsed != parsed:
            return default

        if parsed in (float("inf"), float("-inf")):
            return default

        return parsed
    except Exception:
        return default


def _is_finite_number(value: Any) -> bool:
    try:
        parsed = float(value)
        return parsed == parsed and parsed not in (float("inf"), float("-inf"))
    except Exception:
        return False


def _blank_if_none(value: Any) -> Any:
    return "" if value is None else value


def _non_negative_or_zero(value: Optional[int]) -> int:
    if value is None:
        return 0

    return max(int(value), 0)


def _required_qualification_codes(task: Task) -> set[str]:
    codes = {
        str(qualification.code)
        for qualification in task.required_qualifications.all()
        if getattr(qualification, "code", None)
    }

    if not codes:
        codes = set(DEFAULT_REQUIRED_QUAL_CODES_BY_TASK_TYPE.get(task.task_type, ()))

    return codes


def _count_negative(value: Optional[int], kind: str, counters: ExportCounters) -> None:
    if value is None or value >= 0:
        return

    if kind == "queue_wait":
        counters.negative_queue_wait += 1
    elif kind == "assignment_to_start":
        counters.negative_assignment_to_start += 1
    elif kind == "fact_execution":
        counters.negative_fact_execution += 1
    elif kind == "total_cycle":
        counters.negative_total_cycle += 1


def _unique(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()

    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)

    return result
