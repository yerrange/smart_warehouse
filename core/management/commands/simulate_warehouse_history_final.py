from __future__ import annotations

import random
import string
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import date as date_cls
from datetime import datetime, time, timedelta
from typing import Iterable, Optional
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

import audit.services as audit_services
import core.models as core_models
import core.services.cargos as cargo_services
import core.services.shifts as shift_services
import core.services.tasks as task_services
from audit.models import AuditEvent
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

SKU_SEED: list[tuple[str, str, str]] = [
    ("SKU-0001", "Болт М8x30", "pcs"),
    ("SKU-0002", "Гайка М8", "pcs"),
    ("SKU-0003", "Шайба 8 мм", "pcs"),
    ("SKU-0004", "Саморез 4.2x19", "pcs"),
    ("SKU-0005", "Анкер клиновой 10x100", "pcs"),
    ("SKU-0006", "Кронштейн монтажный усиленный", "pcs"),
    ("SKU-0007", "Кабель ПВС 3x1.5", "m"),
    ("SKU-0008", "Кабель ВВГнг 3x2.5", "m"),
    ("SKU-0009", "Гофра ПНД 20 мм", "m"),
    ("SKU-0010", "Труба ПНД 25 мм", "m"),
    ("SKU-0011", "Труба ПП 32 мм", "m"),
    ("SKU-0012", "Фитинг угловой 25 мм", "pcs"),
    ("SKU-0013", "Краска акриловая белая 10 л", "pcs"),
    ("SKU-0014", "Грунтовка универсальная 5 л", "pcs"),
    ("SKU-0015", "Герметик силиконовый 280 мл", "pcs"),
    ("SKU-0016", "Клей монтажный", "pcs"),
    ("SKU-0017", "Скотч упаковочный 48 мм", "roll"),
    ("SKU-0018", "Стрейч-плёнка 500 мм", "roll"),
    ("SKU-0019", "Термоэтикетка 58x40", "roll"),
    ("SKU-0020", "Картонный короб 600x400x400", "pcs"),
    ("SKU-0021", "Паллет деревянный EUR", "pcs"),
    ("SKU-0022", "Пластиковый контейнер 600x400", "pcs"),
    ("SKU-0023", "Перчатки защитные", "pair"),
    ("SKU-0024", "Каска строительная", "pcs"),
    ("SKU-0025", "Респиратор FFP2", "pcs"),
    ("SKU-0026", "Светильник LED 36W", "pcs"),
    ("SKU-0027", "Блок питания 24V", "pcs"),
    ("SKU-0028", "Датчик температуры складской", "pcs"),
    ("SKU-0029", "Маркер промышленный", "pcs"),
    ("SKU-0030", "Трос крепёжный 6 мм", "m"),
    ("SKU-0031", "Лента сигнальная", "roll"),
    ("SKU-0032", "Комплект крепежа для стеллажа", "set"),
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


@dataclass
class TaskExecutionResult:
    task: Task
    state: str
    at_time: datetime
    employee: Optional[Employee] = None

    @property
    def is_completed(self) -> bool:
        return self.state == "completed"


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

        cargo_schedule = self._build_arrival_schedule(
            shift_start=shift_start,
            shift_end=shift_end,
            count=int(opts["cargos"]),
            rng=rng,
            kind="cargo",
        )
        general_schedule = self._build_arrival_schedule(
            shift_start=shift_start,
            shift_end=shift_end,
            count=int(opts["general_tasks"]),
            rng=rng,
            kind="general",
        )

        now_cursor = shift_start + timedelta(minutes=5)
        next_general_idx = 0
        completed_tasks = 0
        created_tasks = 0
        unfinished_tasks = 0
        created_cargos = 0
        dispatched_cargos = 0
        created_general = 0

        for cargo_arrival in cargo_schedule:
            next_general_idx, created_general, created_tasks, completed_tasks, unfinished_tasks, now_cursor = self._run_due_general_tasks(
                stop_at=cargo_arrival,
                now_cursor=now_cursor,
                shift=shift,
                task_pool=task_pool,
                quals=quals,
                rng=rng,
                speed_profiles=speed_profiles,
                general_schedule=general_schedule,
                next_general_idx=next_general_idx,
                created_general=created_general,
                created_tasks=created_tasks,
                completed_tasks=completed_tasks,
                unfinished_tasks=unfinished_tasks,
                seal_every=seal_every,
                shift_end=shift_end,
            )

            now_cursor = max(now_cursor, cargo_arrival)
            if now_cursor > shift_end - timedelta(minutes=5):
                break

            chain_result = self._run_cargo_chain(
                arrival_at=now_cursor,
                shift=shift,
                task_pool=task_pool,
                topology=topology,
                quals=quals,
                rng=rng,
                speed_profiles=speed_profiles,
                dispatch_rate=dispatch_rate,
                internal_move_rate=internal_move_rate,
                seal_every=seal_every,
                shift_end=shift_end,
                completed_tasks=completed_tasks,
            )
            created_cargos += 1
            created_tasks += chain_result["created_tasks"]
            completed_tasks = chain_result["completed_tasks"]
            unfinished_tasks += chain_result["unfinished_tasks"]
            if chain_result["dispatched"]:
                dispatched_cargos += 1
            now_cursor = max(now_cursor, chain_result["ended_at"] + timedelta(minutes=rng.randint(2, 10)))

        next_general_idx, created_general, created_tasks, completed_tasks, unfinished_tasks, now_cursor = self._run_due_general_tasks(
            stop_at=shift_end - timedelta(minutes=5),
            now_cursor=now_cursor,
            shift=shift,
            task_pool=task_pool,
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            general_schedule=general_schedule,
            next_general_idx=next_general_idx,
            created_general=created_general,
            created_tasks=created_tasks,
            completed_tasks=completed_tasks,
            unfinished_tasks=unfinished_tasks,
            seal_every=seal_every,
            shift_end=shift_end,
            force_remaining=True,
        )

        close_time = min(max(now_cursor, shift_start + timedelta(minutes=5)), shift_end)
        with self._patched_runtime(close_time):
            shift.refresh_from_db()
            if shift.is_active:
                close_shift(shift)

        if opts["final_seal"]:
            sealed = 0
            while True:
                block = self._seal_one_block_at_simulated_time()
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
                f"general_tasks={created_general}, tasks_created={created_tasks}, "
                f"completed_tasks={completed_tasks}, unfinished_tasks={unfinished_tasks}"
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

    # ----------------------- Arrival schedule -----------------------

    def _build_arrival_schedule(
        self,
        *,
        shift_start: datetime,
        shift_end: datetime,
        count: int,
        rng: random.Random,
        kind: str,
    ) -> list[datetime]:
        if count <= 0:
            return []

        if kind == "cargo":
            warmup = 10
            latest_fraction = 0.58 if count <= 3 else 0.72
            alpha, beta = 1.35, 3.2
            min_gap = 18
        else:
            warmup = 25
            latest_fraction = 0.90
            alpha, beta = 1.4, 1.8
            min_gap = 12

        usable_start = shift_start + timedelta(minutes=warmup)
        usable_end = shift_start + timedelta(
            seconds=(shift_end - shift_start).total_seconds() * latest_fraction
        )
        usable_end = min(usable_end, shift_end - timedelta(minutes=20))
        if usable_end <= usable_start:
            usable_end = shift_end - timedelta(minutes=20)
        if usable_end <= usable_start:
            return [shift_start + timedelta(minutes=10)] * count

        span_minutes = max(1, int((usable_end - usable_start).total_seconds() // 60))
        sampled: list[datetime] = []
        for _ in range(count):
            ratio = rng.betavariate(alpha, beta)
            sampled.append(usable_start + timedelta(minutes=int(ratio * span_minutes)))
        sampled.sort()

        adjusted: list[datetime] = []
        for ts in sampled:
            if adjusted:
                ts = max(ts, adjusted[-1] + timedelta(minutes=min_gap))
            if ts > usable_end:
                ts = usable_end
            adjusted.append(ts)
        return adjusted

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
        for code, name, unit in SKU_SEED:
            sku, created = SKU.objects.get_or_create(
                code=code,
                defaults={"name": name, "unit_of_measurement": unit},
            )
            if not created:
                update_fields: list[str] = []
                if sku.name != name:
                    sku.name = name
                    update_fields.append("name")
                if sku.unit_of_measurement != unit:
                    sku.unit_of_measurement = unit
                    update_fields.append("unit_of_measurement")
                if update_fields:
                    sku.save(update_fields=update_fields + ["updated_at"])

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
        shift_end: datetime,
    ) -> TaskExecutionResult:
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

        if created_at > shift_end - timedelta(minutes=15):
            return TaskExecutionResult(task=task, state="pending", at_time=created_at)

        assigned_at = created_at + timedelta(minutes=rng.randint(0, 8))
        if assigned_at >= shift_end - timedelta(minutes=10):
            return TaskExecutionResult(task=task, state="pending", at_time=created_at)

        with self._patched_runtime(assigned_at):
            shift.refresh_from_db()
            employee = assign_task_to_best_employee(task, shift)
        task.refresh_from_db()
        if not employee:
            return TaskExecutionResult(task=task, state="pending", at_time=created_at)

        started_at = assigned_at + timedelta(minutes=rng.randint(1, 7))
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

        if completed_at > shift_end - timedelta(minutes=3):
            return TaskExecutionResult(task=task, state="assigned", at_time=assigned_at, employee=employee)

        with self._patched_runtime(started_at):
            ok = start_task(task)
        if not ok:
            raise CommandError(f"Could not start task {task.id} ({task.task_type})")

        task.refresh_from_db()
        with self._patched_runtime(completed_at):
            ok = complete_task(task)
        if not ok:
            raise CommandError(f"Could not complete task {task.id} ({task.task_type})")

        task.refresh_from_db()
        task.actual_minutes = true_minutes
        with self._patched_runtime(completed_at):
            task.save(update_fields=["actual_minutes", "updated_at"])
        return TaskExecutionResult(task=task, state="completed", at_time=completed_at, employee=employee)

    def _run_cargo_chain(
        self,
        *,
        arrival_at: datetime,
        shift: Shift,
        task_pool: TaskPool,
        topology: TopologyContext,
        quals: dict[str, Qualification],
        rng: random.Random,
        speed_profiles: dict[int, dict[str, float]],
        dispatch_rate: float,
        internal_move_rate: float,
        seal_every: int,
        shift_end: datetime,
        completed_tasks: int,
    ) -> dict[str, object]:
        container_type = rng.choice(topology.supported_flow_sizes)
        cargo = self._create_cargo(created_at=arrival_at, container_type=container_type, rng=rng)
        current_time = arrival_at + timedelta(minutes=rng.randint(1, 4))
        created_tasks = 0
        unfinished_tasks = 0
        dispatched = False

        inbound_slot = self._pick_free_slot(
            location_type=StorageLocation.LocationType.INBOUND,
            size_class=container_type,
            rng=rng,
        )
        if not inbound_slot:
            return {
                "ended_at": current_time,
                "created_tasks": created_tasks,
                "completed_tasks": completed_tasks,
                "unfinished_tasks": unfinished_tasks,
                "dispatched": dispatched,
            }

        receive_result = self._execute_task_lifecycle(
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
            shift_end=shift_end,
        )
        created_tasks += 1
        if not receive_result.is_completed:
            unfinished_tasks += 1
            return {
                "ended_at": receive_result.at_time,
                "created_tasks": created_tasks,
                "completed_tasks": completed_tasks,
                "unfinished_tasks": unfinished_tasks,
                "dispatched": dispatched,
            }
        completed_tasks += 1
        self._maybe_seal(completed_tasks, seal_every, receive_result.at_time)

        rack_slot = self._pick_free_slot(
            location_type=StorageLocation.LocationType.RACK,
            size_class=container_type,
            rng=rng,
        )
        if not rack_slot:
            return {
                "ended_at": receive_result.at_time,
                "created_tasks": created_tasks,
                "completed_tasks": completed_tasks,
                "unfinished_tasks": unfinished_tasks,
                "dispatched": dispatched,
            }

        current_time = receive_result.at_time + timedelta(minutes=rng.randint(3, 10))
        putaway_result = self._execute_task_lifecycle(
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
            shift_end=shift_end,
        )
        created_tasks += 1
        if not putaway_result.is_completed:
            unfinished_tasks += 1
            return {
                "ended_at": putaway_result.at_time,
                "created_tasks": created_tasks,
                "completed_tasks": completed_tasks,
                "unfinished_tasks": unfinished_tasks,
                "dispatched": dispatched,
            }
        completed_tasks += 1
        self._maybe_seal(completed_tasks, seal_every, putaway_result.at_time)

        last_completed_at = putaway_result.at_time

        if rng.random() < internal_move_rate:
            next_rack_slot = self._pick_free_slot(
                location_type=StorageLocation.LocationType.RACK,
                size_class=container_type,
                rng=rng,
                exclude_slot_ids={cargo.current_slot_id} if cargo.current_slot_id else set(),
            )
            if next_rack_slot:
                move_result = self._execute_task_lifecycle(
                    created_at=last_completed_at + timedelta(minutes=rng.randint(5, 15)),
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
                    shift_end=shift_end,
                )
                created_tasks += 1
                if not move_result.is_completed:
                    unfinished_tasks += 1
                    return {
                        "ended_at": move_result.at_time,
                        "created_tasks": created_tasks,
                        "completed_tasks": completed_tasks,
                        "unfinished_tasks": unfinished_tasks,
                        "dispatched": dispatched,
                    }
                completed_tasks += 1
                self._maybe_seal(completed_tasks, seal_every, move_result.at_time)
                last_completed_at = move_result.at_time

        if rng.random() < dispatch_rate:
            outbound_slot = self._pick_free_slot(
                location_type=StorageLocation.LocationType.OUTBOUND,
                size_class=container_type,
                rng=rng,
            )
            if outbound_slot:
                move_out_result = self._execute_task_lifecycle(
                    created_at=last_completed_at + timedelta(minutes=rng.randint(10, 25)),
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
                    shift_end=shift_end,
                )
                created_tasks += 1
                if not move_out_result.is_completed:
                    unfinished_tasks += 1
                    return {
                        "ended_at": move_out_result.at_time,
                        "created_tasks": created_tasks,
                        "completed_tasks": completed_tasks,
                        "unfinished_tasks": unfinished_tasks,
                        "dispatched": dispatched,
                    }
                completed_tasks += 1
                self._maybe_seal(completed_tasks, seal_every, move_out_result.at_time)
                last_completed_at = move_out_result.at_time

                dispatch_result = self._execute_task_lifecycle(
                    created_at=last_completed_at + timedelta(minutes=rng.randint(2, 8)),
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
                    shift_end=shift_end,
                )
                created_tasks += 1
                if not dispatch_result.is_completed:
                    unfinished_tasks += 1
                    return {
                        "ended_at": dispatch_result.at_time,
                        "created_tasks": created_tasks,
                        "completed_tasks": completed_tasks,
                        "unfinished_tasks": unfinished_tasks,
                        "dispatched": dispatched,
                    }
                completed_tasks += 1
                self._maybe_seal(completed_tasks, seal_every, dispatch_result.at_time)
                last_completed_at = dispatch_result.at_time
                dispatched = True

        return {
            "ended_at": last_completed_at,
            "created_tasks": created_tasks,
            "completed_tasks": completed_tasks,
            "unfinished_tasks": unfinished_tasks,
            "dispatched": dispatched,
        }

    def _run_due_general_tasks(
        self,
        *,
        stop_at: datetime,
        now_cursor: datetime,
        shift: Shift,
        task_pool: TaskPool,
        quals: dict[str, Qualification],
        rng: random.Random,
        speed_profiles: dict[int, dict[str, float]],
        general_schedule: list[datetime],
        next_general_idx: int,
        created_general: int,
        created_tasks: int,
        completed_tasks: int,
        unfinished_tasks: int,
        seal_every: int,
        shift_end: datetime,
        force_remaining: bool = False,
    ) -> tuple[int, int, int, int, int, datetime]:
        latest_create_at = shift_end - timedelta(minutes=5)
        while next_general_idx < len(general_schedule):
            scheduled_at = general_schedule[next_general_idx]
            if not force_remaining and scheduled_at > stop_at:
                break
            created_at = max(now_cursor, scheduled_at)
            created_at = min(created_at, latest_create_at)
            result = self._execute_task_lifecycle(
                created_at=created_at,
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
                shift_end=shift_end,
            )
            created_general += 1
            created_tasks += 1
            if result.is_completed:
                completed_tasks += 1
                self._maybe_seal(completed_tasks, seal_every, result.at_time)
            else:
                unfinished_tasks += 1
            now_cursor = max(now_cursor, result.at_time) + timedelta(minutes=rng.randint(3, 14))
            next_general_idx += 1
        return next_general_idx, created_general, created_tasks, completed_tasks, unfinished_tasks, now_cursor

    # ----------------------- Cleanup and block ops -----------------------

    def _latest_unblocked_event_ts(self) -> Optional[datetime]:
        return (
            AuditEvent.objects.filter(in_block=False)
            .order_by("-created_at")
            .values_list("created_at", flat=True)
            .first()
        )

    def _seal_one_block_at_simulated_time(self):
        seal_ts = self._latest_unblocked_event_ts()
        if seal_ts is None:
            return None
        with self._patched_runtime(seal_ts):
            return seal_block(max_events=512)

    def _maybe_seal(self, completed_tasks: int, seal_every: int, current_time: datetime) -> None:
        if seal_every > 0 and completed_tasks % seal_every == 0:
            with self._patched_runtime(current_time):
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
