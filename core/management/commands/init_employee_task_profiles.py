
from __future__ import annotations

import random

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import (
    Employee,
    EmployeeQualification,
    EmployeeTaskProfile,
    EmployeeTaskQualificationModifier,
    Task,
)


# Базовые квалификации, уже "вшитые" в сам тип задачи.
# Для них отдельный modifier создавать не нужно, иначе получится двойной учёт:
# 1) через EmployeeTaskProfile по task_type
# 2) ещё раз через EmployeeTaskQualificationModifier
BASE_TASK_QUAL_MAP: dict[str, set[str]] = {
    Task.TaskType.RECEIVE_TO_INBOUND: {"RECEIVE"},
    Task.TaskType.PUTAWAY_TO_RACK: {"MOVE"},
    Task.TaskType.MOVE_BETWEEN_SLOTS: {"MOVE"},
    Task.TaskType.DISPATCH_CARGO: {"DISPATCH"},
    Task.TaskType.GENERAL: set(),
}

# Базовые "складские" квалификации. Если квалификация сотрудника входит в этот набор,
# но не является базовой для конкретного task_type, мы тоже не создаём modifier по умолчанию.
# Иначе профиль быстро превращается в избыточную матрицу вроде:
# DISPATCH_CARGO + RECEIVE, DISPATCH_CARGO + MOVE и т.п., что обычно не несёт смысла.
BASE_OPERATIONAL_QUAL_CODES = {"RECEIVE", "MOVE", "DISPATCH"}


class Command(BaseCommand):
    help = (
        "Create missing EmployeeTaskProfile and EmployeeTaskQualificationModifier records "
        "for existing employees. By default, existing profiles/modifiers are left unchanged, "
        "but invalid/obsolete modifiers are pruned to keep the schema consistent."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed for reproducible synthetic profile initialization",
        )
        parser.add_argument(
            "--employee-codes",
            nargs="*",
            default=None,
            help="Optional list of employee_code values to limit initialization",
        )
        parser.add_argument(
            "--include-inactive",
            action="store_true",
            help="Include inactive employees too (default: only active employees)",
        )
        parser.add_argument(
            "--reset-existing",
            action="store_true",
            help="Re-generate existing profiles and allowed modifiers for the selected employees",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        rng = random.Random(int(options["seed"]))
        reset_existing = bool(options["reset_existing"])

        employees = self._get_employees(
            employee_codes=options.get("employee_codes"),
            include_inactive=bool(options["include_inactive"]),
        )
        if not employees:
            raise CommandError("Не найдено сотрудников для генерации профилей.")

        task_types = [value for value, _ in Task.TaskType.choices]

        created_profiles = 0
        updated_profiles = 0
        created_modifiers = 0
        updated_modifiers = 0
        deleted_modifiers = 0

        for employee in employees:
            employee_quals = list(
                EmployeeQualification.objects.select_related("qualification")
                .filter(employee=employee)
                .order_by("qualification__code", "id")
            )

            for task_type in task_types:
                profile_defaults = self._build_profile_defaults(rng=rng)
                profile, created = EmployeeTaskProfile.objects.get_or_create(
                    employee=employee,
                    task_type=task_type,
                    defaults=profile_defaults,
                )
                if created:
                    created_profiles += 1
                elif reset_existing:
                    if self._apply_updates(profile, profile_defaults):
                        updated_profiles += 1

                allowed_employee_quals = self._allowed_modifier_qualifications(
                    employee_quals=employee_quals,
                    task_type=task_type,
                )
                allowed_eq_ids = {eq.id for eq in allowed_employee_quals}

                # Подчищаем уже существующие, но теперь невалидные modifiers.
                qs_to_delete = EmployeeTaskQualificationModifier.objects.filter(profile=profile)
                if allowed_eq_ids:
                    qs_to_delete = qs_to_delete.exclude(employee_qualification_id__in=allowed_eq_ids)
                to_delete_count = qs_to_delete.count()
                if to_delete_count:
                    qs_to_delete.delete()
                    deleted_modifiers += to_delete_count

                for employee_qualification in allowed_employee_quals:
                    modifier_defaults = self._build_modifier_defaults(
                        qualification_code=employee_qualification.qualification.code,
                        task_type=task_type,
                        rng=rng,
                    )
                    modifier, created = EmployeeTaskQualificationModifier.objects.get_or_create(
                        profile=profile,
                        employee_qualification=employee_qualification,
                        defaults=modifier_defaults,
                    )
                    if created:
                        created_modifiers += 1
                    elif reset_existing:
                        if self._apply_updates(modifier, modifier_defaults):
                            updated_modifiers += 1

        self.stdout.write(
            self.style.SUCCESS(
                "Employee task profiles initialized: "
                f"employees={len(employees)}, "
                f"profiles_created={created_profiles}, profiles_updated={updated_profiles}, "
                f"modifiers_created={created_modifiers}, modifiers_updated={updated_modifiers}, "
                f"modifiers_deleted={deleted_modifiers}"
            )
        )

    def _get_employees(self, *, employee_codes: list[str] | None, include_inactive: bool) -> list[Employee]:
        qs = Employee.objects.order_by("employee_code", "id")
        if not include_inactive:
            qs = qs.filter(is_active=True)
        if employee_codes:
            qs = qs.filter(employee_code__in=employee_codes)
        return list(qs)

    def _build_profile_defaults(self, *, rng: random.Random) -> dict:
        return {
            "performance_factor": round(rng.uniform(0.90, 1.10), 4),
            "sigma": round(rng.uniform(0.05, 0.14), 4),
            "sample_count": 0,
            "mean_minutes": None,
            "source_kind": EmployeeTaskProfile.SourceKind.SYNTHETIC,
        }

    def _allowed_modifier_qualifications(
        self,
        *,
        employee_quals: list[EmployeeQualification],
        task_type: str,
    ) -> list[EmployeeQualification]:
        """
        Возвращает только те квалификации, которые имеют смысл как ДОПОЛНИТЕЛЬНЫЕ
        modifiers поверх базового EmployeeTaskProfile по task_type.
        """
        base_codes_for_task = BASE_TASK_QUAL_MAP.get(task_type, set())

        allowed: list[EmployeeQualification] = []
        for employee_qualification in employee_quals:
            code = employee_qualification.qualification.code

            # Базовая квалификация конкретного task_type уже учтена самим EmployeeTaskProfile.
            if code in base_codes_for_task:
                continue

            # Прочие базовые складские квалификации по умолчанию тоже не создаём как modifiers,
            # чтобы не плодить искусственные сочетания вроде DISPATCH_CARGO + RECEIVE.
            if code in BASE_OPERATIONAL_QUAL_CODES:
                continue

            allowed.append(employee_qualification)

        return allowed

    def _build_modifier_defaults(
        self,
        *,
        qualification_code: str,
        task_type: str,
        rng: random.Random,
    ) -> dict:
        factor_min, factor_max = 0.97, 1.03
        sigma_bonus_min, sigma_bonus_max = 0.0, 0.02

        # Более выразительные стартовые поправки для реально осмысленных дополнительных кейсов.
        if qualification_code == "FORKLIFT":
            if task_type in {
                Task.TaskType.PUTAWAY_TO_RACK,
                Task.TaskType.MOVE_BETWEEN_SLOTS,
                Task.TaskType.RECEIVE_TO_INBOUND,
            }:
                factor_min, factor_max = 0.88, 1.00
                sigma_bonus_min, sigma_bonus_max = 0.00, 0.03
            else:
                factor_min, factor_max = 0.97, 1.02
                sigma_bonus_min, sigma_bonus_max = 0.00, 0.02
        elif qualification_code == "QC":
            if task_type == Task.TaskType.GENERAL:
                factor_min, factor_max = 0.93, 1.02
                sigma_bonus_min, sigma_bonus_max = 0.00, 0.02

        return {
            "factor": round(rng.uniform(factor_min, factor_max), 4),
            "sigma_bonus": round(rng.uniform(sigma_bonus_min, sigma_bonus_max), 4),
            "sample_count": 0,
            "source_kind": EmployeeTaskQualificationModifier.SourceKind.SYNTHETIC,
        }

    def _apply_updates(self, instance, values: dict) -> bool:
        changed_fields: list[str] = []
        for field, value in values.items():
            if getattr(instance, field) != value:
                setattr(instance, field, value)
                changed_fields.append(field)
        if changed_fields:
            instance.save(update_fields=changed_fields + ["updated_at"])
            return True
        return False
