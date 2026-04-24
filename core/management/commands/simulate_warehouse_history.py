# пример запуска
# python manage.py simulate_warehouse_history \
#   --ensure-topology \
#   --cargos 25 \
#   --general-tasks 8 \
#   --seed 42 \
#   --shift-date 2026-04-24 \
#   --final-seal \
#   --verify-chain \
#   --export-execution-dataset ml_data/execution_dataset.csv


from __future__ import annotations

import random
import string
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import date as date_cls
from datetime import datetime, time, timedelta
from typing import Callable, Iterable, Optional
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

import audit.services as audit_services
import core.models as core_models
import core.services.cargos as cargo_services
import core.services.shifts as shift_services
import core.services.tasks as task_services
from audit.services import seal_block, verify_chain
from core.models import (
    Cargo,
    CargoEvent,
    Employee,
    EmployeeShiftStats,
    LocationSlot,
    Qualification,
    SKU,
    Shift,
    StorageLocation,
    Task,
    TaskPool,
    TaskAssignmentLog,
)
from core.services.shifts import create_shift_with_employees, start_shift, close_shift
from core.services.tasks import assign_task_to_best_employee, start_task, complete_task


DEFAULT_QUALS: list[tuple[str, str]] = [
    ("FORKLIFT", "Водитель погрузчика"),
    ("QC", "Контроль качества"),
    ("DISPATCH", "Отгрузка"),
    ("RECEIVE", "Приёмка"),
]

SKU_SEED: dict[str, str] = {
    "SKU-0001": "Болт М8x30",
    "SKU-0002": "Гайка М8",
    "SKU-0003": "Шайба 8мм",
    "SKU-0004": "Кабель ПВС 3x1.5",
    "SKU-0005": "Труба ПНД 25мм",
    "SKU-0006": "Скотч 48мм",
    "SKU-0007": "Поддон EUR",
    "SKU-0008": "Плёнка стрейч",
}


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
        base_minutes=16,
        default_priority=2,
        required_qual_codes=("FORKLIFT",),
    ),
    Task.TaskType.DISPATCH_CARGO: TaskProfile(
        task_type=Task.TaskType.DISPATCH_CARGO,
        base_minutes=14,
        default_priority=4,
        required_qual_codes=("DISPATCH",),
    ),
    Task.TaskType.GENERAL: TaskProfile(
        task_type=Task.TaskType.GENERAL,
        base_minutes=10,
        default_priority=1,
        required_qual_codes=(),
    ),
}


class DummyChannelLayer:
    async def group_send(self, *args, **kwargs):
        return None


@dataclass
class TopologyContext:
    inbound_sizes: set[str]
    rack_sizes: set[str]
    outbound_sizes: set[str]
    staging_sizes: set[str]
    qc_sizes: set[str]
    supported_flow_sizes: list[str]


class Command(BaseCommand):
    help = "Generate realistic warehouse history with cargos, topology-aware tasks and audit trail."

    def add_arguments(self, parser):
        parser.add_argument("--seed", type=int, default=42, help="Random seed")
        parser.add_argument("--employees", type=int, default=12, help="How many SIM employees to use")
        parser.add_argument("--cargos", type=int, default=25, help="How many cargo units to simulate")
        parser.add_argument(
            "--general-tasks",
            type=int,
            default=8,
            help="Additional non-cargo tasks to generate during the shift",
        )
        parser.add_argument(
            "--dispatch-rate",
            type=float,
            default=0.70,
            help="Probability that a stored cargo will be moved to OUTBOUND and dispatched",
        )
        parser.add_argument(
            "--internal-move-rate",
            type=float,
            default=0.35,
            help="Probability of an extra move between storage slots before dispatch/retention",
        )
        parser.add_argument(
            "--shift-date",
            default=None,
            help="Shift date in YYYY-MM-DD (default: today in project timezone)",
        )
        parser.add_argument("--shift-name", default="SIM warehouse shift", help="Shift name")
        parser.add_argument(
            "--seal-every",
            type=int,
            default=20,
            help="Seal audit block every N finished tasks (0 disables intermediate sealing)",
        )
        parser.add_argument(
            "--final-seal",
            action="store_true",
            help="Seal remaining audit events once at the end",
        )
        parser.add_argument(
            "--verify-chain",
            action="store_true",
            help="Verify blockchain chain at the end",
        )
        parser.add_argument(
            "--export-execution-dataset",
            default=None,
            help="Optional path for export_execution_dataset after simulation",
        )
        parser.add_argument(
            "--purge",
            action="store_true",
            help="Delete previously generated SIMWH tasks/cargos/logs before run (audit is kept intact)",
        )
        parser.add_argument(
            "--ensure-topology",
            action="store_true",
            help="If topology is missing, run create_locations.py logic before simulation",
        )
        parser.add_argument(
            "--min-eligible",
            type=int,
            default=3,
            help="Minimum number of eligible employees for each task profile",
        )

    def handle(self, *args, **opts):
        rng = random.Random(int(opts["seed"]))
        dispatch_rate = float(opts["dispatch_rate"])
        internal_move_rate = float(opts["internal_move_rate"])
        seal_every = int(opts["seal_every"])

        if not 0.0 <= dispatch_rate <= 1.0:
            raise CommandError("--dispatch-rate must be between 0 and 1")
        if not 0.0 <= internal_move_rate <= 1.0:
            raise CommandError("--internal-move-rate must be between 0 and 1")

        if opts["shift_date"]:
            try:
                shift_date = date_cls.fromisoformat(opts["shift_date"])
            except ValueError as exc:
                raise CommandError("Некорректная дата. Используй YYYY-MM-DD.") from exc
        else:
            shift_date = timezone.localdate()

        self.run_id = f"SIMWH-{shift_date.isoformat()}-{int(opts['seed'])}"
        self.external_prefix = f"SIMWH-{shift_date.isoformat()}"

        if opts["purge"]:
            self._purge_previous(self.external_prefix)

        topology = self._ensure_topology_if_needed(bool(opts["ensure_topology"]))
        self.stdout.write(
            self.style.NOTICE(
                "Topology detected – "
                f"IN={sorted(topology.inbound_sizes)}; "
                f"RACK={sorted(topology.rack_sizes)}; "
                f"OUT={sorted(topology.outbound_sizes)}; "
                f"flow_sizes={topology.supported_flow_sizes}"
            )
        )

        quals = self._ensure_qualifications()
        employees = self._ensure_sim_employees(int(opts["employees"]))
        self._assign_random_qualifications(employees=employees, quals=quals, rng=rng)
        self._ensure_min_eligible_for_profiles(
            employees=employees,
            quals=quals,
            profiles=TASK_PROFILES.values(),
            min_eligible=int(opts["min_eligible"]),
            rng=rng,
        )
        self._ensure_skus()

        shift_start = self._aware_datetime(shift_date, time(9, 0))
        shift_end = self._aware_datetime(shift_date, time(18, 0))

        requested_shift_name = str(opts["shift_name"])
        shift_name = requested_shift_name
        if Shift.objects.filter(date=shift_date, name=shift_name).exists() and not opts["purge"]:
            shift_name = f"{requested_shift_name} [{self.run_id}]"

        employee_codes = [e.employee_code for e in employees]
        with self._patched_runtime(shift_start):
            shift = create_shift_with_employees(
                shift_name,
                shift_date,
                shift_start,
                shift_end,
                employee_codes,
            )
            start_shift(shift)

        task_pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
        speed_profiles = self._make_speed_profiles(employees, rng)

        now_cursor = shift_start + timedelta(minutes=5)
        completed_tasks = 0
        created_cargos = 0
        dispatched_cargos = 0
        created_general = 0

        for idx in range(int(opts["cargos"])):
            if now_cursor > shift_end - timedelta(minutes=45):
                self.stdout.write(
                    self.style.WARNING(
                        f"Shift time window exhausted after {created_cargos} cargos. Stopping cargo simulation."
                    )
                )
                break

            container_type = rng.choice(topology.supported_flow_sizes)
            cargo_created_at = now_cursor
            cargo = self._create_cargo(created_at=cargo_created_at, container_type=container_type, rng=rng)
            created_cargos += 1
            current_time = cargo_created_at + timedelta(minutes=rng.randint(1, 4))

            inbound_slot = self._pick_free_slot(
                location_type=StorageLocation.LocationType.INBOUND,
                size_class=container_type,
                rng=rng,
            )
            if not inbound_slot:
                self.stdout.write(self.style.WARNING("No free INBOUND slot. Stopping simulation."))
                break

            current_time = self._execute_task_lifecycle(
                created_at=current_time,
                shift=shift,
                task_pool=task_pool,
                profile=TASK_PROFILES[Task.TaskType.RECEIVE_TO_INBOUND],
                cargo=cargo,
                payload={
                    "to_slot_code": inbound_slot.code,
                    "note": "SIMWH receive to inbound",
                    "sim": True,
                    "simulation_run": self.run_id,
                },
                quals=quals,
                rng=rng,
                speed_profiles=speed_profiles,
            )
            completed_tasks += 1
            self._maybe_seal(completed_tasks, seal_every)

            rack_slot = self._pick_free_slot(
                location_type=StorageLocation.LocationType.RACK,
                size_class=container_type,
                rng=rng,
            )
            if not rack_slot:
                self.stdout.write(self.style.WARNING("No free RACK slot. Stopping simulation."))
                break

            current_time += timedelta(minutes=rng.randint(3, 10))
            current_time = self._execute_task_lifecycle(
                created_at=current_time,
                shift=shift,
                task_pool=task_pool,
                profile=TASK_PROFILES[Task.TaskType.PUTAWAY_TO_RACK],
                cargo=cargo,
                payload={
                    "to_slot_code": rack_slot.code,
                    "note": "SIMWH putaway to rack",
                    "sim": True,
                    "simulation_run": self.run_id,
                },
                quals=quals,
                rng=rng,
                speed_profiles=speed_profiles,
            )
            completed_tasks += 1
            self._maybe_seal(completed_tasks, seal_every)

            if rng.random() < internal_move_rate:
                next_rack_slot = self._pick_free_slot(
                    location_type=StorageLocation.LocationType.RACK,
                    size_class=container_type,
                    rng=rng,
                    exclude_slot_ids={cargo.current_slot_id} if cargo.current_slot_id else set(),
                )
                if next_rack_slot:
                    current_time += timedelta(minutes=rng.randint(5, 15))
                    current_time = self._execute_task_lifecycle(
                        created_at=current_time,
                        shift=shift,
                        task_pool=task_pool,
                        profile=TASK_PROFILES[Task.TaskType.MOVE_BETWEEN_SLOTS],
                        cargo=cargo,
                        payload={
                            "to_slot_code": next_rack_slot.code,
                            "note": "SIMWH internal move",
                            "sim": True,
                            "simulation_run": self.run_id,
                        },
                        quals=quals,
                        rng=rng,
                        speed_profiles=speed_profiles,
                    )
                    completed_tasks += 1
                    self._maybe_seal(completed_tasks, seal_every)

            if rng.random() < dispatch_rate:
                outbound_slot = self._pick_free_slot(
                    location_type=StorageLocation.LocationType.OUTBOUND,
                    size_class=container_type,
                    rng=rng,
                )
                if outbound_slot:
                    current_time += timedelta(minutes=rng.randint(10, 25))
                    current_time = self._execute_task_lifecycle(
                        created_at=current_time,
                        shift=shift,
                        task_pool=task_pool,
                        profile=TASK_PROFILES[Task.TaskType.MOVE_BETWEEN_SLOTS],
                        cargo=cargo,
                        payload={
                            "to_slot_code": outbound_slot.code,
                            "note": "SIMWH move to outbound",
                            "sim": True,
                            "simulation_run": self.run_id,
                        },
                        quals=quals,
                        rng=rng,
                        speed_profiles=speed_profiles,
                    )
                    completed_tasks += 1
                    self._maybe_seal(completed_tasks, seal_every)

                    current_time += timedelta(minutes=rng.randint(2, 8))
                    current_time = self._execute_task_lifecycle(
                        created_at=current_time,
                        shift=shift,
                        task_pool=task_pool,
                        profile=TASK_PROFILES[Task.TaskType.DISPATCH_CARGO],
                        cargo=cargo,
                        payload={
                            "note": "SIMWH dispatch cargo",
                            "sim": True,
                            "simulation_run": self.run_id,
                        },
                        quals=quals,
                        rng=rng,
                        speed_profiles=speed_profiles,
                    )
                    completed_tasks += 1
                    dispatched_cargos += 1
                    self._maybe_seal(completed_tasks, seal_every)

            now_cursor = current_time + timedelta(minutes=rng.randint(2, 12))

        for idx in range(int(opts["general_tasks"])):
            if now_cursor > shift_end - timedelta(minutes=25):
                break
            current_time = self._execute_task_lifecycle(
                created_at=now_cursor,
                shift=shift,
                task_pool=task_pool,
                profile=TASK_PROFILES[Task.TaskType.GENERAL],
                cargo=None,
                payload={
                    "note": "SIMWH general task",
                    "sim": True,
                    "simulation_run": self.run_id,
                },
                quals=quals,
                rng=rng,
                speed_profiles=speed_profiles,
            )
            created_general += 1
            completed_tasks += 1
            self._maybe_seal(completed_tasks, seal_every)
            now_cursor = current_time + timedelta(minutes=rng.randint(3, 14))

        close_time = min(now_cursor + timedelta(minutes=5), shift_end)
        with self._patched_runtime(close_time):
            shift.refresh_from_db()
            if shift.is_active:
                close_shift(shift)

        if opts["final_seal"]:
            sealed = 0
            while True:
                block = seal_block(max_events=512)
                if not block:
                    break
                sealed += 1
            self.stdout.write(self.style.SUCCESS(f"Final sealing done. blocks_created={sealed}"))

        if opts["verify_chain"]:
            res = verify_chain()
            if res.get("ok"):
                self.stdout.write(self.style.SUCCESS(f"Audit chain OK. blocks={res['blocks']}"))
            else:
                self.stdout.write(self.style.ERROR(f"Audit chain FAILED: {res}"))

        if opts.get("export_execution_dataset"):
            call_command("export_execution_dataset", out=opts["export_execution_dataset"])
            self.stdout.write(
                self.style.SUCCESS(
                    f"Execution dataset exported to {opts['export_execution_dataset']}"
                )
            )

        self.stdout.write(
            self.style.SUCCESS(
                "Simulation done – "
                f"run_id={self.run_id}, cargos_created={created_cargos}, cargos_dispatched={dispatched_cargos}, "
                f"general_tasks={created_general}, completed_tasks={completed_tasks}"
            )
        )

    # ----------------------- Patching runtime -----------------------

    @contextmanager
    def _patched_runtime(self, ts: datetime):
        dummy_layer = DummyChannelLayer()
        base_record_event = audit_services.record_event

        def wrapped_record_event(*, actor_type, actor_id=None, entity_type, entity_id, action, before, after, meta=None):
            meta_payload = dict(meta or {})
            meta_payload.setdefault("simulation_kind", "warehouse_history")
            meta_payload.setdefault("simulation_run", self.run_id)
            meta_payload.setdefault("simulation_external_prefix", self.external_prefix)
            return base_record_event(
                actor_type=actor_type,
                actor_id=actor_id,
                entity_type=entity_type,
                entity_id=entity_id,
                action=action,
                before=before,
                after=after,
                meta=meta_payload,
            )

        with ExitStack() as stack:
            stack.enter_context(patch("django.utils.timezone.now", new=lambda: ts))
            stack.enter_context(patch.object(core_models, "now", new=lambda: ts))
            stack.enter_context(patch.object(task_services, "now", new=lambda: ts))
            stack.enter_context(patch.object(cargo_services, "now", new=lambda: ts))
            stack.enter_context(patch.object(shift_services.timezone, "now", new=lambda: ts))
            stack.enter_context(patch.object(audit_services.timezone, "now", new=lambda: ts))
            stack.enter_context(patch.object(task_services, "get_channel_layer", new=lambda: dummy_layer))
            stack.enter_context(patch.object(task_services, "record_event", new=wrapped_record_event))
            stack.enter_context(patch.object(cargo_services, "record_event", new=wrapped_record_event))
            stack.enter_context(patch.object(shift_services, "record_event", new=wrapped_record_event))
            stack.enter_context(patch.object(audit_services, "record_event", new=wrapped_record_event))
            yield

    # ----------------------- Support: reference data -----------------------

    def _aware_datetime(self, d: date_cls, t: time) -> datetime:
        tz = timezone.get_current_timezone()
        return timezone.make_aware(datetime.combine(d, t), tz)

    def _ensure_topology_if_needed(self, ensure_topology: bool) -> TopologyContext:
        has_required = all([
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.INBOUND).exists(),
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.RACK).exists(),
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.OUTBOUND).exists(),
        ])

        if not has_required and ensure_topology:
            import create_locations
            create_locations.main()

        inbound_sizes = set(
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.INBOUND)
            .values_list("slot_size_class", flat=True)
            .distinct()
        )
        rack_sizes = set(
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.RACK)
            .values_list("slot_size_class", flat=True)
            .distinct()
        )
        outbound_sizes = set(
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.OUTBOUND)
            .values_list("slot_size_class", flat=True)
            .distinct()
        )
        staging_sizes = set(
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.STAGING)
            .values_list("slot_size_class", flat=True)
            .distinct()
        )
        qc_sizes = set(
            StorageLocation.objects.filter(location_type=StorageLocation.LocationType.QC)
            .values_list("slot_size_class", flat=True)
            .distinct()
        )

        supported_flow_sizes = sorted(inbound_sizes & rack_sizes & outbound_sizes)
        if not supported_flow_sizes:
            raise CommandError(
                "Topology does not contain a common size_class across INBOUND/RACK/OUTBOUND. "
                "Generate topology with create_locations.py first."
            )

        return TopologyContext(
            inbound_sizes=inbound_sizes,
            rack_sizes=rack_sizes,
            outbound_sizes=outbound_sizes,
            staging_sizes=staging_sizes,
            qc_sizes=qc_sizes,
            supported_flow_sizes=supported_flow_sizes,
        )

    def _ensure_qualifications(self) -> dict[str, Qualification]:
        out: dict[str, Qualification] = {}
        for code, name in DEFAULT_QUALS:
            q, _ = Qualification.objects.get_or_create(
                code=code,
                defaults={"name": name, "description": "SIM warehouse qualification"},
            )
            out[code] = q
        return out

    def _ensure_sim_employees(self, count: int) -> list[Employee]:
        existing_sim = list(
            Employee.objects.filter(is_active=True, employee_code__startswith="SIM")
            .order_by("id")
        )
        if len(existing_sim) >= count:
            return existing_sim[:count]

        to_create = count - len(existing_sim)
        max_idx = 0
        for e in existing_sim:
            try:
                max_idx = max(max_idx, int(e.employee_code.replace("SIM", "")))
            except Exception:
                continue

        created: list[Employee] = []
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

    def _assign_random_qualifications(self, *, employees: list[Employee], quals: dict[str, Qualification], rng: random.Random) -> None:
        for e in employees:
            e.qualifications.clear()

        pool = employees[:]
        rng.shuffle(pool)
        for _, q in quals.items():
            if pool:
                pool.pop().qualifications.add(q)

        qual_list = list(quals.values())
        for e in employees:
            extra_n = rng.randint(0, 2)
            if extra_n:
                for q in rng.sample(qual_list, k=min(extra_n, len(qual_list))):
                    e.qualifications.add(q)

    def _ensure_min_eligible_for_profiles(
        self,
        *,
        employees: list[Employee],
        quals: dict[str, Qualification],
        profiles: Iterable[TaskProfile],
        min_eligible: int,
        rng: random.Random,
    ) -> None:
        if min_eligible <= 1:
            return

        for profile in profiles:
            codes = tuple(profile.required_qual_codes)
            if not codes:
                continue
            required_qs = {quals[c] for c in codes if c in quals}
            current = [e for e in employees if required_qs.issubset(set(e.qualifications.all()))]
            need = min_eligible - len(current)
            if need <= 0:
                continue
            candidates = [e for e in employees if e.id not in {x.id for x in current}]
            rng.shuffle(candidates)
            for e in candidates[:need]:
                missing = required_qs - set(e.qualifications.all())
                if missing:
                    e.qualifications.add(*missing)

    def _ensure_skus(self) -> None:
        for code, name in SKU_SEED.items():
            SKU.objects.get_or_create(code=code, defaults={"name": name})

    # ----------------------- Domain creation helpers -----------------------

    def _generate_cargo_code(self, *, rng: random.Random) -> str:
        alphabet = string.ascii_uppercase + string.digits
        while True:
            suffix = "".join(rng.choice(alphabet) for _ in range(8))
            code = f"{self.external_prefix}-C-{suffix}"
            if not Cargo.objects.filter(cargo_code=code).exists():
                return code

    def _pick_random_sku(self, *, rng: random.Random) -> SKU:
        skus = list(SKU.objects.filter(is_active=True).order_by("id"))
        if not skus:
            self._ensure_skus()
            skus = list(SKU.objects.filter(is_active=True).order_by("id"))
        return rng.choice(skus)

    def _create_cargo(self, *, created_at: datetime, container_type: str, rng: random.Random) -> Cargo:
        sku = self._pick_random_sku(rng=rng)
        units = rng.randint(4, 30)
        weight_kg = round(max(80.0, units * rng.uniform(8.0, 22.0)), 2)
        volume_m3 = round(max(0.25, units * rng.uniform(0.015, 0.045)), 3)
        cargo_code = self._generate_cargo_code(rng=rng)

        with self._patched_runtime(created_at):
            cargo = Cargo.objects.create(
                cargo_code=cargo_code,
                sku=sku,
                sku_name_snapshot=sku.name,
                container_type=container_type,
                units=units,
                weight_kg=weight_kg,
                volume_m3=volume_m3,
                status=Cargo.Status.CREATED,
                handling_state=Cargo.HandlingState.IDLE,
                current_slot=None,
            )
            event = CargoEvent.objects.create(
                cargo=cargo,
                event_type=CargoEvent.EventType.CREATED,
                timestamp=created_at,
                quantity=units,
                note="SIM warehouse history: cargo created",
            )
            audit_services.record_event(
                actor_type="system",
                actor_id="simulation:warehouse_history",
                entity_type="Cargo",
                entity_id=str(cargo.id),
                action="CREATE",
                before=None,
                after={
                    "cargo_code": cargo.cargo_code,
                    "sku_code": cargo.sku.code,
                    "container_type": cargo.container_type,
                    "units": cargo.units,
                    "weight_kg": cargo.weight_kg,
                    "volume_m3": cargo.volume_m3,
                    "status": cargo.status,
                },
                meta={
                    "source": "simulate_warehouse_history",
                    "func": "_create_cargo",
                    "cargo_event_id": str(event.id),
                },
            )
        return cargo

    def _task_external_ref(self, task_type: str, seq: int) -> str:
        suffix = task_type.replace("_", "-")
        return f"{self.external_prefix}-T-{seq:05d}-{suffix}"

    def _estimate_minutes(self, *, profile: TaskProfile, cargo: Optional[Cargo], difficulty: int, is_outbound_move: bool = False) -> int:
        base = profile.base_minutes + 3 * max(0, difficulty - 1)
        if cargo:
            units_penalty = cargo.units * 0.20
            weight_penalty = cargo.weight_kg * 0.015
            volume_penalty = cargo.volume_m3 * 8.0
            base = base + units_penalty + weight_penalty + volume_penalty
        if profile.task_type == Task.TaskType.MOVE_BETWEEN_SLOTS and is_outbound_move:
            base += 8
        return max(4, int(round(base)))

    def _make_speed_profiles(self, employees: list[Employee], rng: random.Random) -> dict[int, dict[str, float]]:
        speeds: dict[int, dict[str, float]] = {}
        for employee in employees:
            per_type: dict[str, float] = {}
            base = rng.uniform(0.88, 1.12)
            for task_type in TASK_PROFILES:
                per_type[task_type] = base * rng.uniform(0.82, 1.20)
            speeds[employee.id] = per_type
        return speeds

    def _duration_minutes(
        self,
        *,
        profile: TaskProfile,
        task: Task,
        cargo: Optional[Cargo],
        employee: Employee,
        shift: Shift,
        speed_profiles: dict[int, dict[str, float]],
        rng: random.Random,
    ) -> int:
        stats = EmployeeShiftStats.objects.get(employee=employee, shift=shift)
        base = float(task.estimated_minutes or profile.base_minutes)
        if cargo:
            base += cargo.units * 0.06
            base += cargo.weight_kg * 0.008
            base += cargo.volume_m3 * 5.5
            if profile.task_type == Task.TaskType.DISPATCH_CARGO:
                base += 2.0
        load_penalty = 1.0 + min(stats.task_assigned_count or 0, 20) * 0.025
        score_penalty = 1.0 + min(stats.shift_score or 0, 40) * 0.01
        noise = rng.gauss(1.0, 0.10)
        speed = max(0.45, speed_profiles[employee.id][profile.task_type])
        minutes = (base * load_penalty * score_penalty * noise) / speed
        return max(3, int(round(minutes)))

    def _create_pending_task(
        self,
        *,
        created_at: datetime,
        shift: Shift,
        task_pool: TaskPool,
        profile: TaskProfile,
        cargo: Optional[Cargo],
        payload: dict,
        quals: dict[str, Qualification],
        rng: random.Random,
        seq: int,
    ) -> Task:
        difficulty = rng.randint(1, 5)
        is_outbound_move = (
            profile.task_type == Task.TaskType.MOVE_BETWEEN_SLOTS and payload.get("to_slot_code", "").startswith("OUT-")
        )
        estimated_minutes = self._estimate_minutes(
            profile=profile,
            cargo=cargo,
            difficulty=difficulty,
            is_outbound_move=is_outbound_move,
        )
        due_at = created_at + timedelta(minutes=estimated_minutes + rng.randint(20, 120))

        with self._patched_runtime(created_at):
            task = Task.objects.create(
                name=f"SIMWH {profile.task_type}",
                description="Realistic simulated warehouse task",
                task_type=profile.task_type,
                payload=payload,
                status=Task.Status.PENDING,
                priority=max(0, profile.default_priority + rng.choice([-1, 0, 0, 1])),
                difficulty=difficulty,
                estimated_minutes=estimated_minutes,
                due_at=due_at,
                shift=None,
                task_pool=task_pool,
                assigned_to=None,
                cargo=cargo,
                external_ref=self._task_external_ref(profile.task_type, seq),
                source="auto",
            )
            required_qs = [quals[c] for c in profile.required_qual_codes if c in quals]
            if required_qs:
                task.required_qualifications.set(required_qs)
            audit_services.record_event(
                actor_type="system",
                actor_id="simulation:warehouse_history",
                entity_type="Task",
                entity_id=str(task.id),
                action="CREATE",
                before=None,
                after={
                    "task_type": task.task_type,
                    "status": task.status,
                    "priority": task.priority,
                    "difficulty": task.difficulty,
                    "estimated_minutes": task.estimated_minutes,
                    "task_pool_id": str(task_pool.id),
                    "cargo_code": cargo.cargo_code if cargo else None,
                    "required_qualifications": [q.code for q in required_qs],
                },
                meta={
                    "source": "simulate_warehouse_history",
                    "func": "_create_pending_task",
                    "shift_id": str(shift.id),
                    "external_ref": task.external_ref,
                },
            )
        return task

    def _pick_free_slot(
        self,
        *,
        location_type: str,
        size_class: str,
        rng: random.Random,
        exclude_slot_ids: Optional[set[int]] = None,
    ) -> Optional[LocationSlot]:
        qs = (
            LocationSlot.objects.select_related("location")
            .filter(location__location_type=location_type, location__slot_size_class=size_class, cargo__isnull=True)
            .order_by("location__zone", "location__aisle", "location__rack", "location__shelf", "location__bin", "index", "id")
        )
        if exclude_slot_ids:
            qs = qs.exclude(id__in=list(exclude_slot_ids))
        slots = list(qs[:40])
        if not slots:
            return None
        return rng.choice(slots)

    # ----------------------- Task lifecycle -----------------------

    def _execute_task_lifecycle(
        self,
        *,
        created_at: datetime,
        shift: Shift,
        task_pool: TaskPool,
        profile: TaskProfile,
        cargo: Optional[Cargo],
        payload: dict,
        quals: dict[str, Qualification],
        rng: random.Random,
        speed_profiles: dict[int, dict[str, float]],
    ) -> datetime:
        seq = Task.objects.filter(external_ref__startswith=self.external_prefix).count() + 1
        task = self._create_pending_task(
            created_at=created_at,
            shift=shift,
            task_pool=task_pool,
            profile=profile,
            cargo=cargo,
            payload=payload,
            quals=quals,
            rng=rng,
            seq=seq,
        )

        assigned_at = created_at + timedelta(minutes=rng.randint(0, 8))
        with self._patched_runtime(assigned_at):
            shift.refresh_from_db()
            employee = assign_task_to_best_employee(task, shift)
        if not employee:
            raise CommandError(f"Could not assign task {task.id} ({task.task_type})")

        task.refresh_from_db()
        started_at = assigned_at + timedelta(minutes=rng.randint(1, 7))
        with self._patched_runtime(started_at):
            ok = start_task(task)
        if not ok:
            raise CommandError(f"Could not start task {task.id} ({task.task_type})")

        task.refresh_from_db()
        true_minutes = self._duration_minutes(
            profile=profile,
            task=task,
            cargo=cargo,
            employee=employee,
            shift=shift,
            speed_profiles=speed_profiles,
            rng=rng,
        )
        completed_at = started_at + timedelta(minutes=true_minutes)
        with self._patched_runtime(completed_at):
            ok = complete_task(task)
        if not ok:
            raise CommandError(f"Could not complete task {task.id} ({task.task_type})")

        task.refresh_from_db()
        task.actual_minutes = true_minutes
        with self._patched_runtime(completed_at):
            task.save(update_fields=["actual_minutes", "updated_at"])
        return completed_at

    # ----------------------- Cleanup and block ops -----------------------

    def _maybe_seal(self, completed_tasks: int, seal_every: int) -> None:
        if seal_every > 0 and completed_tasks % seal_every == 0:
            seal_block(max_events=512)

    def _purge_previous(self, prefix: str) -> None:
        task_ids = list(Task.objects.filter(external_ref__startswith=prefix).values_list("id", flat=True))
        cargo_ids = list(Cargo.objects.filter(cargo_code__startswith=prefix).values_list("id", flat=True))
        shift_ids = list(Shift.objects.filter(name__startswith="SIM warehouse shift").values_list("id", flat=True))

        logs_count = TaskAssignmentLog.objects.filter(task_id__in=task_ids).count()
        cargo_events_count = CargoEvent.objects.filter(cargo_id__in=cargo_ids).count()
        tasks_count = len(task_ids)
        cargos_count = len(cargo_ids)

        Task.objects.filter(id__in=task_ids).delete()
        Cargo.objects.filter(id__in=cargo_ids).delete()

        for shift in Shift.objects.filter(id__in=shift_ids):
            if not shift.tasks.exists():
                shift.delete()

        self.stdout.write(
            self.style.WARNING(
                f"Purged previous SIMWH domain data – tasks={tasks_count}, logs={logs_count}, cargos={cargos_count}, cargo_events={cargo_events_count}. "
                "Audit tables were not touched."
            )
        )
