from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from statistics import mean, pstdev
from typing import Optional

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q

from core.models import (
    Employee,
    EmployeeTaskProfile,
    EmployeeTaskQualificationModifier,
    Task,
)


@dataclass
class Observation:
    employee_id: int
    task_type: str
    shift_id: int
    actual_minutes: float
    estimated_minutes: float
    ratio: float
    required_codes: set[str]


@dataclass
class ProfileStats:
    observations_count: int
    shifts_count: int
    mean_minutes: float
    factor: float
    sigma: float


@dataclass
class ModifierStats:
    observations_count: int
    shifts_count: int
    factor: float
    sigma_bonus: float


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise CommandError(
            f"Некорректная дата '{value}'. Используй формат YYYY-MM-DD."
        ) from exc


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_fact_minutes(task: Task) -> Optional[float]:
    if task.actual_minutes and task.actual_minutes > 0:
        return float(task.actual_minutes)

    if task.started_at and task.completed_at:
        minutes = (task.completed_at - task.started_at).total_seconds() / 60.0
        if minutes > 0:
            return minutes

    return None


class Command(BaseCommand):
    help = (
        "Recalculate EmployeeTaskProfile and EmployeeTaskQualificationModifier "
        "from completed task execution history."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--min-shifts",
            type=int,
            default=3,
            help="Минимальное число разных смен для обновления профиля.",
        )
        parser.add_argument(
            "--min-tasks",
            type=int,
            default=5,
            help="Минимальное число завершённых задач для обновления профиля.",
        )
        parser.add_argument(
            "--min-modifier-shifts",
            type=int,
            default=2,
            help="Минимальное число разных смен для обновления поправки квалификации.",
        )
        parser.add_argument(
            "--min-modifier-tasks",
            type=int,
            default=4,
            help="Минимальное число задач для обновления поправки квалификации.",
        )
        parser.add_argument(
            "--date-from",
            default=None,
            help="Использовать задачи, завершённые не раньше этой даты, YYYY-MM-DD.",
        )
        parser.add_argument(
            "--date-to",
            default=None,
            help="Использовать задачи, завершённые не позже этой даты, YYYY-MM-DD.",
        )
        parser.add_argument(
            "--employee-codes",
            nargs="*",
            default=None,
            help="Ограничить пересчёт конкретными employee_code.",
        )
        parser.add_argument(
            "--only-simulated",
            action="store_true",
            help="Использовать только задачи с external_ref, начинающимся на SIMSHIFT.",
        )
        parser.add_argument(
            "--source-kind",
            choices=[
                EmployeeTaskProfile.SourceKind.SYNTHETIC,
                EmployeeTaskProfile.SourceKind.REAL,
                EmployeeTaskProfile.SourceKind.MIXED,
            ],
            default=EmployeeTaskProfile.SourceKind.MIXED,
            help="Какой source_kind записывать после пересчёта.",
        )
        parser.add_argument(
            "--min-factor",
            type=float,
            default=0.60,
            help="Нижняя граница performance_factor.",
        )
        parser.add_argument(
            "--max-factor",
            type=float,
            default=1.60,
            help="Верхняя граница performance_factor.",
        )
        parser.add_argument(
            "--min-ratio",
            type=float,
            default=0.50,
            help="Нижняя граница отношения actual/estimated для одной задачи.",
        )
        parser.add_argument(
            "--max-ratio",
            type=float,
            default=2.00,
            help="Верхняя граница отношения actual/estimated для одной задачи.",
        )
        parser.add_argument(
            "--min-sigma",
            type=float,
            default=0.03,
            help="Нижняя граница sigma.",
        )
        parser.add_argument(
            "--max-sigma",
            type=float,
            default=0.30,
            help="Верхняя граница sigma.",
        )
        parser.add_argument(
            "--min-modifier-factor",
            type=float,
            default=0.75,
            help="Нижняя граница factor для qualification modifier.",
        )
        parser.add_argument(
            "--max-modifier-factor",
            type=float,
            default=1.25,
            help="Верхняя граница factor для qualification modifier.",
        )
        parser.add_argument(
            "--max-sigma-bonus",
            type=float,
            default=0.15,
            help="Верхняя граница sigma_bonus для qualification modifier.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показать изменения без записи в БД.",
        )

    def handle(self, *args, **options):
        date_from = _parse_date(options.get("date_from"))
        date_to = _parse_date(options.get("date_to"))
        employee_codes = options.get("employee_codes") or None
        dry_run = bool(options["dry_run"])

        observations = self._load_observations(
            date_from=date_from,
            date_to=date_to,
            employee_codes=employee_codes,
            only_simulated=bool(options["only_simulated"]),
            min_ratio=float(options["min_ratio"]),
            max_ratio=float(options["max_ratio"]),
        )

        if not observations:
            raise CommandError(
                "Не найдено completed-задач, пригодных для пересчёта профилей."
            )

        observations_by_profile: dict[tuple[int, str], list[Observation]] = defaultdict(list)

        for observation in observations:
            key = (observation.employee_id, observation.task_type)
            observations_by_profile[key].append(observation)

        employee_ids = {
            employee_id
            for employee_id, _ in observations_by_profile.keys()
        }

        if employee_codes:
            employee_ids = set(
                Employee.objects
                .filter(employee_code__in=employee_codes)
                .values_list("id", flat=True)
            )

        profiles = list(
            EmployeeTaskProfile.objects
            .select_related("employee")
            .filter(employee_id__in=employee_ids)
            .order_by("employee__employee_code", "task_type", "id")
        )

        profile_by_key = {
            (profile.employee_id, profile.task_type): profile
            for profile in profiles
        }

        changed_profiles = 0
        skipped_profiles = 0
        missing_profiles = 0

        profile_stats_by_key: dict[tuple[int, str], ProfileStats] = {}

        with transaction.atomic():
            for key, profile_observations in sorted(observations_by_profile.items()):
                profile = profile_by_key.get(key)

                if profile is None:
                    missing_profiles += 1
                    continue

                stats = self._build_profile_stats(profile_observations, options)

                if (
                    stats.shifts_count < int(options["min_shifts"])
                    or stats.observations_count < int(options["min_tasks"])
                ):
                    skipped_profiles += 1
                    continue

                profile_stats_by_key[key] = stats

                before = (
                    float(profile.performance_factor),
                    float(profile.sigma),
                    int(profile.sample_count),
                    profile.mean_minutes,
                    profile.source_kind,
                )

                after = (
                    stats.factor,
                    stats.sigma,
                    stats.observations_count,
                    stats.mean_minutes,
                    options["source_kind"],
                )

                self.stdout.write(
                    f"PROFILE {profile.employee.employee_code} {profile.task_type}: "
                    f"factor {before[0]:.4f} -> {after[0]:.4f}, "
                    f"sigma {before[1]:.4f} -> {after[1]:.4f}, "
                    f"tasks={stats.observations_count}, "
                    f"shifts={stats.shifts_count}, "
                    f"mean_minutes={stats.mean_minutes:.2f}"
                )

                if before != after:
                    changed_profiles += 1

                    if not dry_run:
                        profile.performance_factor = stats.factor
                        profile.sigma = stats.sigma
                        profile.sample_count = stats.observations_count
                        profile.mean_minutes = stats.mean_minutes
                        profile.source_kind = options["source_kind"]

                        profile.save(
                            update_fields=[
                                "performance_factor",
                                "sigma",
                                "sample_count",
                                "mean_minutes",
                                "source_kind",
                                "updated_at",
                            ]
                        )

            changed_modifiers = self._update_modifiers(
                profiles=profiles,
                profile_stats_by_key=profile_stats_by_key,
                observations_by_profile=observations_by_profile,
                options=options,
                dry_run=dry_run,
            )

            if dry_run:
                transaction.set_rollback(True)

        self.stdout.write("")
        style = self.style.WARNING if dry_run else self.style.SUCCESS

        self.stdout.write(
            style(
                "Employee task profile recalculation finished: "
                f"observations={len(observations)}, "
                f"profiles_changed={changed_profiles}, "
                f"modifiers_changed={changed_modifiers}, "
                f"profiles_skipped={skipped_profiles}, "
                f"profiles_missing={missing_profiles}, "
                f"dry_run={dry_run}"
            )
        )

    def _load_observations(
        self,
        *,
        date_from: Optional[date],
        date_to: Optional[date],
        employee_codes: Optional[list[str]],
        only_simulated: bool,
        min_ratio: float,
        max_ratio: float,
    ) -> list[Observation]:
        qs = (
            Task.objects
            .select_related("assigned_to", "shift")
            .prefetch_related("required_qualifications")
            .filter(
                status=Task.Status.COMPLETED,
                assigned_to__isnull=False,
                shift__isnull=False,
                estimated_minutes__gt=0,
            )
            .filter(
                Q(actual_minutes__gt=0)
                | Q(started_at__isnull=False, completed_at__isnull=False)
            )
            .order_by(
                "assigned_to__employee_code",
                "task_type",
                "completed_at",
                "id",
            )
        )

        if date_from:
            qs = qs.filter(completed_at__date__gte=date_from)

        if date_to:
            qs = qs.filter(completed_at__date__lte=date_to)

        if employee_codes:
            qs = qs.filter(assigned_to__employee_code__in=employee_codes)

        if only_simulated:
            qs = qs.filter(external_ref__startswith="SIMSHIFT")

        observations: list[Observation] = []

        for task in qs.iterator(chunk_size=300):
            actual_minutes = _safe_fact_minutes(task)

            if actual_minutes is None or actual_minutes <= 0:
                continue

            estimated_minutes = float(task.estimated_minutes or 0)

            if estimated_minutes <= 0:
                continue

            raw_ratio = actual_minutes / estimated_minutes
            ratio = _clip(raw_ratio, min_ratio, max_ratio)

            required_codes = {
                qualification.code
                for qualification in task.required_qualifications.all()
            }

            observations.append(
                Observation(
                    employee_id=task.assigned_to_id,
                    task_type=task.task_type,
                    shift_id=task.shift_id,
                    actual_minutes=actual_minutes,
                    estimated_minutes=estimated_minutes,
                    ratio=ratio,
                    required_codes=required_codes,
                )
            )

        return observations

    def _build_profile_stats(
        self,
        observations: list[Observation],
        options: dict,
    ) -> ProfileStats:
        ratios = [
            _clip(
                observation.ratio,
                float(options["min_ratio"]),
                float(options["max_ratio"]),
            )
            for observation in observations
        ]

        factor = _clip(
            mean(ratios),
            float(options["min_factor"]),
            float(options["max_factor"]),
        )

        raw_sigma = (
            pstdev(ratios)
            if len(ratios) > 1
            else float(options["min_sigma"])
        )

        sigma = _clip(
            raw_sigma,
            float(options["min_sigma"]),
            float(options["max_sigma"]),
        )

        mean_minutes = mean(
            observation.actual_minutes
            for observation in observations
        )

        return ProfileStats(
            observations_count=len(observations),
            shifts_count=len({
                observation.shift_id
                for observation in observations
            }),
            mean_minutes=round(mean_minutes, 4),
            factor=round(factor, 4),
            sigma=round(sigma, 4),
        )

    def _update_modifiers(
        self,
        *,
        profiles: list[EmployeeTaskProfile],
        profile_stats_by_key: dict[tuple[int, str], ProfileStats],
        observations_by_profile: dict[tuple[int, str], list[Observation]],
        options: dict,
        dry_run: bool,
    ) -> int:
        profile_ids = [profile.id for profile in profiles]

        if not profile_ids:
            return 0

        modifiers = (
            EmployeeTaskQualificationModifier.objects
            .select_related(
                "profile",
                "profile__employee",
                "employee_qualification__qualification",
            )
            .filter(profile_id__in=profile_ids)
            .order_by(
                "profile__employee__employee_code",
                "profile__task_type",
                "employee_qualification__qualification__code",
                "id",
            )
        )

        changed = 0

        for modifier in modifiers:
            profile = modifier.profile
            key = (profile.employee_id, profile.task_type)
            base_stats = profile_stats_by_key.get(key)

            if base_stats is None:
                continue

            qualification_code = modifier.employee_qualification.qualification.code

            modifier_observations = [
                observation
                for observation in observations_by_profile.get(key, [])
                if qualification_code in observation.required_codes
            ]

            if len(modifier_observations) < int(options["min_modifier_tasks"]):
                continue

            modifier_shift_count = len({
                observation.shift_id
                for observation in modifier_observations
            })

            if modifier_shift_count < int(options["min_modifier_shifts"]):
                continue

            modifier_stats = self._build_modifier_stats(
                observations=modifier_observations,
                base_stats=base_stats,
                options=options,
            )

            before = (
                float(modifier.factor),
                float(modifier.sigma_bonus),
                int(modifier.sample_count),
                modifier.source_kind,
            )

            after = (
                modifier_stats.factor,
                modifier_stats.sigma_bonus,
                modifier_stats.observations_count,
                options["source_kind"],
            )

            self.stdout.write(
                f"MODIFIER {profile.employee.employee_code} "
                f"{profile.task_type} + {qualification_code}: "
                f"factor {before[0]:.4f} -> {after[0]:.4f}, "
                f"sigma_bonus {before[1]:.4f} -> {after[1]:.4f}, "
                f"tasks={modifier_stats.observations_count}, "
                f"shifts={modifier_stats.shifts_count}"
            )

            if before != after:
                changed += 1

                if not dry_run:
                    modifier.factor = modifier_stats.factor
                    modifier.sigma_bonus = modifier_stats.sigma_bonus
                    modifier.sample_count = modifier_stats.observations_count
                    modifier.source_kind = options["source_kind"]

                    modifier.save(
                        update_fields=[
                            "factor",
                            "sigma_bonus",
                            "sample_count",
                            "source_kind",
                            "updated_at",
                        ]
                    )

        return changed

    def _build_modifier_stats(
        self,
        *,
        observations: list[Observation],
        base_stats: ProfileStats,
        options: dict,
    ) -> ModifierStats:
        ratios = [
            _clip(
                observation.ratio,
                float(options["min_ratio"]),
                float(options["max_ratio"]),
            )
            for observation in observations
        ]

        qual_mean_ratio = mean(ratios)

        raw_factor = qual_mean_ratio / max(base_stats.factor, 0.01)

        factor = _clip(
            raw_factor,
            float(options["min_modifier_factor"]),
            float(options["max_modifier_factor"]),
        )

        qual_sigma = (
            pstdev(ratios)
            if len(ratios) > 1
            else base_stats.sigma
        )

        sigma_bonus = _clip(
            max(0.0, qual_sigma - base_stats.sigma),
            0.0,
            float(options["max_sigma_bonus"]),
        )

        return ModifierStats(
            observations_count=len(observations),
            shifts_count=len({
                observation.shift_id
                for observation in observations
            }),
            factor=round(factor, 4),
            sigma_bonus=round(sigma_bonus, 4),
        )
