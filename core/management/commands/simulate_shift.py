# python manage.py simulate_shift --employees-on-shift 15 --cargos 35 --general-tasks 15 --forklift-mix-rate 0.2 --seed 42 --shift-date 2026-04-28 --final-seal


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
from django.db import transaction

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
    EmployeeQualification,
    EmployeeShiftStats,
    EmployeeTaskProfile,
    EmployeeTaskQualificationModifier,
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
    ("MOVE", "Перемещение"),
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
        base_minutes=12,
        default_priority=4,
        required_qual_codes=("RECEIVE",),
    ),
    Task.TaskType.PUTAWAY_TO_RACK: TaskProfile(
        task_type=Task.TaskType.PUTAWAY_TO_RACK,
        base_minutes=18,
        default_priority=5,
        required_qual_codes=("MOVE",),
    ),
    Task.TaskType.MOVE_BETWEEN_SLOTS: TaskProfile(
        task_type=Task.TaskType.MOVE_BETWEEN_SLOTS,
        base_minutes=14,
        default_priority=3,
        required_qual_codes=("MOVE",),
    ),
    Task.TaskType.DISPATCH_CARGO: TaskProfile(
        task_type=Task.TaskType.DISPATCH_CARGO,
        base_minutes=16,
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
    help = "Simulate one warehouse shift using existing employees, topology-aware tasks and audit trail."

    def add_arguments(self, parser):
        parser.add_argument("--seed", type=int, default=42, help="Random seed")
        parser.add_argument("--employees-on-shift", "--employees", dest="employees_on_shift", type=int, default=15, help="How many existing active employees to sample into the shift")
        parser.add_argument("--cargos", type=int, default=22, help="How many cargo units to simulate")
        parser.add_argument(
            "--general-tasks",
            type=int,
            default=20,
            help="Additional non-cargo tasks to generate during the shift",
        )
        parser.add_argument(
            "--dispatch-rate",
            type=float,
            default=0.8,
            help="Probability that a stored cargo will be moved to OUTBOUND and dispatched",
        )
        parser.add_argument(
            "--internal-move-rate",
            type=float,
            default=0.45,
            help="Probability of an extra move between storage slots before dispatch/retention",
        )
        parser.add_argument(
            "--shift-date",
            default=None,
            help="Shift date in YYYY-MM-DD (default: today in project timezone)",
        )
        parser.add_argument(
            "--shifts-count",
            type=int,
            default=1,
            help="How many consecutive shifts to simulate starting from --shift-date",
        )
        parser.add_argument("--shift-name", default="SIM shift", help="Shift name")
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
            help="Delete previously generated simulated tasks/cargos/logs before run (audit is kept intact)",
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
        parser.add_argument(
            "--forklift-mix-rate",
            type=float,
            default=0.35,
            help="Probability to additionally require FORKLIFT for a cargo task",
        )



    def handle(self, *args, **opts):
        shifts_count = int(opts.get("shifts_count") or 1)
        if shifts_count <= 0:
            raise CommandError("--shifts-count must be a positive integer")

        if opts.get("shift_date"):
            try:
                base_shift_date = date_cls.fromisoformat(opts["shift_date"])
            except ValueError as exc:
                raise CommandError("Некорректная дата. Используй YYYY-MM-DD.") from exc
        else:
            base_shift_date = timezone.localdate()

        base_seed = int(opts["seed"])

        if shifts_count == 1:
            single_opts = dict(opts)
            single_opts["shift_date"] = base_shift_date.isoformat()
            single_opts["seed"] = base_seed
            return self._simulate_one_shift(*args, **single_opts)

        self.stdout.write(
            self.style.NOTICE(
                f"Запуск серии смен: count={shifts_count}, first_date={base_shift_date.isoformat()}, base_seed={base_seed}"
            )
        )

        for offset in range(shifts_count):
            current_opts = dict(opts)
            current_date = base_shift_date + timedelta(days=offset)
            current_seed = base_seed + offset

            current_opts["shift_date"] = current_date.isoformat()
            current_opts["seed"] = current_seed
            current_opts["final_seal"] = False
            current_opts["verify_chain"] = False
            current_opts["export_execution_dataset"] = None

            self.stdout.write(
                self.style.NOTICE(
                    f"=== Симуляция смены {offset + 1}/{shifts_count}: date={current_date.isoformat()}, seed={current_seed} ==="
                )
            )
            self._simulate_one_shift(*args, **current_opts)

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
                raise CommandError(f"Audit chain FAILED: {res}")

        if opts.get("export_execution_dataset"):
            call_command("export_execution_dataset", out=opts["export_execution_dataset"])
            self.stdout.write(
                self.style.SUCCESS(
                    f"Execution dataset exported to {opts['export_execution_dataset']}"
                )
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Simulation series done – shifts={shifts_count}, first_date={base_shift_date.isoformat()}, last_date={(base_shift_date + timedelta(days=shifts_count - 1)).isoformat()}"
            )
        )

    def _simulate_one_shift(self, *args, **opts):
        rng = random.Random(int(opts["seed"]))
        dispatch_rate = float(opts["dispatch_rate"])
        seal_every = int(opts["seal_every"])
        forklift_mix_rate = float(opts["forklift_mix_rate"])

        if not 0.0 <= dispatch_rate <= 1.0:
            raise CommandError("--dispatch-rate must be between 0 and 1")
        if not 0.0 <= forklift_mix_rate <= 1.0:
            raise CommandError("--forklift-mix-rate must be between 0 and 1")

        if opts["shift_date"]:
            try:
                shift_date = date_cls.fromisoformat(opts["shift_date"])
            except ValueError as exc:
                raise CommandError("Некорректная дата. Используй YYYY-MM-DD.") from exc
        else:
            shift_date = timezone.localdate()

        self.run_id = f"SIMSHIFT-{shift_date.isoformat()}-{int(opts['seed'])}"
        self.external_prefix = f"SIMSHIFT-{shift_date.isoformat()}"

        requested_employees = int(opts["employees_on_shift"])
        if requested_employees <= 0:
            raise CommandError("--employees-on-shift must be a positive integer")

        with transaction.atomic():
            active_employee_pool = self._get_active_employee_pool()
            if requested_employees > len(active_employee_pool):
                raise CommandError(
                    "Недостаточно активных сотрудников в БД: "
                    f"запрошено {requested_employees}, доступно {len(active_employee_pool)}."
                )

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
            employees = self._sample_shift_employees(
                employee_pool=active_employee_pool,
                count=requested_employees,
                rng=rng,
            )
            self.stdout.write(
                self.style.NOTICE(
                    "Employees sampled for shift – " + ", ".join(e.employee_code for e in employees)
                )
            )
            self._ensure_employee_profiles(employees)
            self._ensure_skus()

            shift_start = self._aware_datetime(shift_date, time(hour=9, minute=0))
            shift_end = self._aware_datetime(shift_date, time(hour=18, minute=0))
            employee_codes = [e.employee_code for e in employees]

            with self._patched_runtime(shift_start):
                shift = create_shift_with_employees(
                    name=opts["shift_name"],
                    date=shift_date,
                    start_time=shift_start,
                    end_time=shift_end,
                    employee_codes=employee_codes,
                )
                start_shift(shift)

            task_pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
            speed_profiles = self._make_speed_profiles(employees)
            employee_available_at = {
                e.id: shift_start + timedelta(minutes=rng.randint(0, 6)) for e in employees
            }

            cargo_seed_schedule = self._build_cargo_seed_schedule(
                shift_start=shift_start,
                shift_end=shift_end,
                count=int(opts["cargos"]),
                rng=rng,
            )

            general_schedule = self._build_arrival_schedule(
                shift_start=shift_start,
                shift_end=shift_end,
                count=int(opts["general_tasks"]),
                rng=rng,
                kind="general",
            )
            target_dispatch_cargos = self._dispatch_attempt_count(
                requested_cargos=int(opts["cargos"]),
                dispatch_rate=dispatch_rate,
                rng=rng,
            )
            dispatch_schedule = self._build_dispatch_schedule(
                shift_start=shift_start,
                shift_end=shift_end,
                count=target_dispatch_cargos,
                rng=rng,
            )

            latest_new_cargo_at = min(shift_start + timedelta(minutes=500), shift_end - timedelta(minutes=55))
            wave_size_minutes = 16
            cargo_seed_idx = 0
            next_general_idx = 0
            next_dispatch_idx = 0
            now_cursor = shift_start + timedelta(minutes=6)

            created_tasks = 0
            completed_tasks = 0
            unfinished_tasks = 0
            dispatched_cargos = 0
            created_general = 0
            created_cargos = 0

            while now_cursor < shift_end - timedelta(minutes=24):
                local_cursor = now_cursor
                wave_end = min(now_cursor + timedelta(minutes=wave_size_minutes), shift_end - timedelta(minutes=18))
                wave_progress = False
                shift_progress = (now_cursor - shift_start).total_seconds() / max(60.0, (shift_end - shift_start).total_seconds())
                if shift_progress < 0.22:
                    putaway_quota = max(2, min(3, requested_employees // 6 + 1))
                    receive_quota = max(2, min(3, requested_employees // 6 + 1))
                    dispatch_quota = max(1, min(2, requested_employees // 10 + 1))
                    general_quota_per_wave = 1
                elif shift_progress < 0.68:
                    putaway_quota = max(2, min(4, requested_employees // 5 + 1))
                    receive_quota = max(2, min(3, requested_employees // 7 + 1))
                    dispatch_quota = max(1, min(3, requested_employees // 7 + 1))
                    general_quota_per_wave = max(1, min(2, requested_employees // 10 + 1))
                else:
                    putaway_quota = max(1, min(3, requested_employees // 7 + 1))
                    receive_quota = max(1, min(2, requested_employees // 12 + 1))
                    dispatch_quota = max(2, min(3, requested_employees // 6 + 1))
                    general_quota_per_wave = max(1, min(2, requested_employees // 8 + 1))

                if rng.random() < 0.18:
                    receive_quota = max(1, receive_quota - 1)
                if rng.random() < 0.15:
                    putaway_quota = min(4, putaway_quota + 1)
                if shift_progress > 0.45 and rng.random() < 0.25:
                    dispatch_quota = min(4, dispatch_quota + 1)

                # Materialize newly arriving cargos only in the first ~2/3 of the shift.
                while (
                    cargo_seed_idx < len(cargo_seed_schedule)
                    and cargo_seed_schedule[cargo_seed_idx] <= wave_end
                    and cargo_seed_schedule[cargo_seed_idx] <= latest_new_cargo_at
                ):
                    created_at = cargo_seed_schedule[cargo_seed_idx]
                    container_type = rng.choice(topology.supported_flow_sizes)
                    self._create_cargo(created_at=created_at, container_type=container_type, rng=rng)
                    created_cargos += 1
                    cargo_seed_idx += 1

                # General work should appear during the day, not only at the very end.
                (
                    next_general_idx,
                    created_general,
                    created_tasks,
                    completed_tasks,
                    unfinished_tasks,
                    local_cursor,
                ) = self._run_due_general_tasks(
                    stop_at=wave_end,
                    now_cursor=local_cursor,
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
                    forklift_mix_rate=forklift_mix_rate,
                    employee_available_at=employee_available_at,
                )

                # Highest operational priority: empty INBOUND into long-term storage.
                for _ in range(putaway_quota):
                    res = self._run_putaway_for_inbound_cargo(
                        current_time=local_cursor,
                        shift=shift,
                        task_pool=task_pool,
                        topology=topology,
                        quals=quals,
                        rng=rng,
                        speed_profiles=speed_profiles,
                        shift_end=shift_end,
                        forklift_mix_rate=forklift_mix_rate,
                        employee_available_at=employee_available_at,
                    )
                    if not res["handled"]:
                        break
                    wave_progress = True
                    created_tasks += res["created_tasks"]
                    completed_tasks += res["completed_tasks"]
                    unfinished_tasks += res["unfinished_tasks"]
                    if res["completed_tasks"]:
                        self._maybe_seal(completed_tasks, seal_every, res["ended_at"])
                    local_cursor = min(wave_end, local_cursor + timedelta(minutes=rng.randint(1, 3)))
                    if local_cursor >= shift_end - timedelta(minutes=15):
                        break

                # If storage already has cargo, proactively launch a few dispatch tasks before admitting too much new flow.
                rack_pressure = Cargo.objects.filter(
                    status=Cargo.Status.STORED,
                    current_slot__isnull=False,
                    current_slot__location__location_type=StorageLocation.LocationType.RACK,
                ).count()
                if (
                    dispatched_cargos < target_dispatch_cargos
                    and rack_pressure >= 3
                    and now_cursor >= shift_start + timedelta(minutes=210)
                    and local_cursor < shift_end - timedelta(minutes=52)
                ):
                    proactive_dispatch_runs = min(
                        dispatch_quota + 1,
                        max(1, rack_pressure // 2 + 1),
                        max(0, target_dispatch_cargos - dispatched_cargos),
                    )
                    for _ in range(proactive_dispatch_runs):
                        res = self._run_dispatch_wave(
                            current_time=local_cursor,
                            shift=shift,
                            task_pool=task_pool,
                            topology=topology,
                            quals=quals,
                            rng=rng,
                            speed_profiles=speed_profiles,
                            shift_end=shift_end,
                            forklift_mix_rate=forklift_mix_rate,
                            employee_available_at=employee_available_at,
                        )
                        if not res["handled"]:
                            break
                        wave_progress = True
                        created_tasks += res["created_tasks"]
                        completed_tasks += res["completed_tasks"]
                        unfinished_tasks += res["unfinished_tasks"]
                        dispatched_cargos += res["dispatched_cargos"]
                        if res["completed_tasks"]:
                            self._maybe_seal(completed_tasks, seal_every, res["ended_at"])
                        local_cursor = min(wave_end, local_cursor + timedelta(minutes=rng.randint(1, 3)))
                        if local_cursor >= shift_end - timedelta(minutes=15):
                            break

                # Keep draining INBOUND once more before admitting more new cargo.
                for _ in range(max(2, putaway_quota // 2)):
                    if local_cursor >= shift_end - timedelta(minutes=15):
                        break
                    res = self._run_putaway_for_inbound_cargo(
                        current_time=local_cursor,
                        shift=shift,
                        task_pool=task_pool,
                        topology=topology,
                        quals=quals,
                        rng=rng,
                        speed_profiles=speed_profiles,
                        shift_end=shift_end,
                        forklift_mix_rate=forklift_mix_rate,
                        employee_available_at=employee_available_at,
                    )
                    if not res["handled"]:
                        break
                    wave_progress = True
                    created_tasks += res["created_tasks"]
                    completed_tasks += res["completed_tasks"]
                    unfinished_tasks += res["unfinished_tasks"]
                    if res["completed_tasks"]:
                        self._maybe_seal(completed_tasks, seal_every, res["ended_at"])
                    local_cursor = min(wave_end, local_cursor + timedelta(minutes=rng.randint(1, 3)))

                # Then keep feeding cargo into INBOUND when slots exist.
                for _ in range(receive_quota):
                    if local_cursor >= shift_end - timedelta(minutes=15):
                        break
                    res = self._run_receive_for_created_cargo(
                        current_time=local_cursor,
                        shift=shift,
                        task_pool=task_pool,
                        topology=topology,
                        quals=quals,
                        rng=rng,
                        speed_profiles=speed_profiles,
                        shift_end=shift_end,
                        forklift_mix_rate=forklift_mix_rate,
                        employee_available_at=employee_available_at,
                    )
                    if not res["handled"]:
                        break
                    wave_progress = True
                    created_tasks += res["created_tasks"]
                    completed_tasks += res["completed_tasks"]
                    unfinished_tasks += res["unfinished_tasks"]
                    if res["completed_tasks"]:
                        self._maybe_seal(completed_tasks, seal_every, res["ended_at"])
                    local_cursor = min(wave_end, local_cursor + timedelta(minutes=rng.randint(1, 4)))

                # Dispatch waves from already stored cargo should happen regularly in the second half.
                rack_pressure = Cargo.objects.filter(
                    status=Cargo.Status.STORED,
                    current_slot__isnull=False,
                    current_slot__location__location_type=StorageLocation.LocationType.RACK,
                ).count()
                dispatch_due = next_dispatch_idx < len(dispatch_schedule) and dispatch_schedule[next_dispatch_idx] <= wave_end
                if (
                    dispatched_cargos < target_dispatch_cargos
                    and (dispatch_due or (rack_pressure >= max(3, requested_employees // 4) and now_cursor >= shift_start + timedelta(minutes=255)))
                ):
                    dispatch_runs = min(
                        dispatch_quota + (1 if rack_pressure >= max(6, requested_employees // 3) else 0),
                        max(0, target_dispatch_cargos - dispatched_cargos),
                    )
                    for _ in range(dispatch_runs):
                        if next_dispatch_idx < len(dispatch_schedule) and dispatch_schedule[next_dispatch_idx] <= wave_end:
                            next_dispatch_idx += 1
                        res = self._run_dispatch_wave(
                            current_time=local_cursor,
                            shift=shift,
                            task_pool=task_pool,
                            topology=topology,
                            quals=quals,
                            rng=rng,
                            speed_profiles=speed_profiles,
                            shift_end=shift_end,
                            forklift_mix_rate=forklift_mix_rate,
                            employee_available_at=employee_available_at,
                        )
                        if not res["handled"]:
                            break
                        wave_progress = True
                        created_tasks += res["created_tasks"]
                        completed_tasks += res["completed_tasks"]
                        unfinished_tasks += res["unfinished_tasks"]
                        dispatched_cargos += res["dispatched_cargos"]
                        if res["completed_tasks"]:
                            self._maybe_seal(completed_tasks, seal_every, res["ended_at"])
                        local_cursor = min(wave_end, local_cursor + timedelta(minutes=rng.randint(1, 3)))
                        if local_cursor >= shift_end - timedelta(minutes=15):
                            break

                # Final short dispatch sprint if rack still has stock in this wave.
                rack_pressure = Cargo.objects.filter(
                    status=Cargo.Status.STORED,
                    current_slot__isnull=False,
                    current_slot__location__location_type=StorageLocation.LocationType.RACK,
                ).count()
                if (
                    dispatched_cargos < target_dispatch_cargos
                    and rack_pressure >= 2
                    and now_cursor >= shift_start + timedelta(minutes=300)
                    and local_cursor < shift_end - timedelta(minutes=28)
                ):
                    for _ in range(min(3, rack_pressure, max(0, target_dispatch_cargos - dispatched_cargos))):
                        res = self._run_dispatch_wave(
                            current_time=local_cursor,
                            shift=shift,
                            task_pool=task_pool,
                            topology=topology,
                            quals=quals,
                            rng=rng,
                            speed_profiles=speed_profiles,
                            shift_end=shift_end,
                            forklift_mix_rate=forklift_mix_rate,
                            employee_available_at=employee_available_at,
                        )
                        if not res["handled"]:
                            break
                        wave_progress = True
                        created_tasks += res["created_tasks"]
                        completed_tasks += res["completed_tasks"]
                        unfinished_tasks += res["unfinished_tasks"]
                        dispatched_cargos += res["dispatched_cargos"]
                        if res["completed_tasks"]:
                            self._maybe_seal(completed_tasks, seal_every, res["ended_at"])
                        local_cursor = min(wave_end, local_cursor + timedelta(minutes=rng.randint(1, 3)))

                # One more chance for overdue general work inside the same wave.
                (
                    next_general_idx,
                    created_general,
                    created_tasks,
                    completed_tasks,
                    unfinished_tasks,
                    local_cursor,
                ) = self._run_due_general_tasks(
                    stop_at=min(local_cursor + timedelta(minutes=8), shift_end - timedelta(minutes=20)),
                    now_cursor=local_cursor,
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
                    forklift_mix_rate=forklift_mix_rate,
                    employee_available_at=employee_available_at,
                )

                if wave_progress or local_cursor > now_cursor:
                    now_cursor = wave_end + timedelta(minutes=1)
                    continue

                next_candidates = [shift_end - timedelta(minutes=12)]
                if cargo_seed_idx < len(cargo_seed_schedule):
                    next_candidates.append(cargo_seed_schedule[cargo_seed_idx])
                if next_general_idx < len(general_schedule):
                    next_candidates.append(general_schedule[next_general_idx])
                if next_dispatch_idx < len(dispatch_schedule):
                    next_candidates.append(dispatch_schedule[next_dispatch_idx])
                next_created_ts = self._next_waiting_created_ts(now_cursor)
                if next_created_ts:
                    next_candidates.append(next_created_ts)

                next_ts = min(next_candidates)
                if next_ts <= now_cursor:
                    now_cursor = now_cursor + timedelta(minutes=2)
                else:
                    now_cursor = min(next_ts, now_cursor + timedelta(minutes=6))

            # Final overdue general tasks should still happen before the end-window, but never in the last hour.
            (
                next_general_idx,
                created_general,
                created_tasks,
                completed_tasks,
                unfinished_tasks,
                now_cursor,
            ) = self._run_due_general_tasks(
                stop_at=shift_end - timedelta(minutes=28),
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
                forklift_mix_rate=forklift_mix_rate,
                employee_available_at=employee_available_at,
                force_remaining=True,
            )
            with self._patched_runtime(shift_end):
                shift.refresh_from_db()
                if shift.is_active:
                    close_shift(shift)


            self.stdout.write(
                self.style.SUCCESS(
                    "Simulation done – "
                    f"run_id={self.run_id}, cargos_created={created_cargos}, cargos_dispatched={dispatched_cargos}/{target_dispatch_cargos}, "
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
            meta_payload.setdefault("simulation_kind", "simulate_shift")
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
            usable_start = shift_start + timedelta(minutes=12)
            usable_end = min(shift_end - timedelta(minutes=55), shift_start + timedelta(minutes=500))
            min_gap = 4
            ratios = [rng.betavariate(1.0, 1.0) for _ in range(count)]
        else:
            usable_start = shift_start + timedelta(minutes=45)
            usable_end = shift_end - timedelta(minutes=22)
            min_gap = 8
            ratios = []
            late_bias = max(1, int(round(count * 0.35)))
            for i in range(count):
                if i < late_bias:
                    ratios.append(rng.betavariate(1.35, 0.9))
                else:
                    ratios.append(rng.betavariate(1.0, 1.05))

        if usable_end <= usable_start:
            return [shift_start + timedelta(minutes=15)] * count

        span_minutes = max(1, int((usable_end - usable_start).total_seconds() // 60))
        sampled = [usable_start + timedelta(minutes=int(r * span_minutes)) for r in ratios]
        sampled.sort()

        adjusted: list[datetime] = []
        for ts in sampled:
            if adjusted:
                ts = max(ts, adjusted[-1] + timedelta(minutes=min_gap))
            if ts > usable_end:
                ts = usable_end
            adjusted.append(ts)
        return adjusted


    def _build_dispatch_schedule(
        self,
        *,
        shift_start: datetime,
        shift_end: datetime,
        count: int,
        rng: random.Random,
    ) -> list[datetime]:
        if count <= 0:
            return []
        usable_start = shift_start + timedelta(minutes=230)
        usable_end = shift_end - timedelta(minutes=20)
        if usable_end <= usable_start:
            return []
        span_minutes = max(1, int((usable_end - usable_start).total_seconds() // 60))
        sampled: list[datetime] = []
        for _ in range(count):
            ratio = rng.betavariate(1.22, 0.88)
            sampled.append(usable_start + timedelta(minutes=int(ratio * span_minutes)))
        sampled.sort()

        adjusted: list[datetime] = []
        for ts in sampled:
            if adjusted:
                ts = max(ts, adjusted[-1] + timedelta(minutes=8))
            if ts > usable_end:
                ts = usable_end
            adjusted.append(ts)
        return adjusted

    def _dispatch_attempt_count(self, *, requested_cargos: int, dispatch_rate: float, rng: random.Random) -> int:
        if requested_cargos <= 0 or dispatch_rate <= 0:
            return 0
        effective_rate = max(0.35, min(0.92, dispatch_rate * rng.uniform(0.72, 0.9)))
        return max(2, int(round(requested_cargos * effective_rate)))

    def _build_cargo_seed_schedule(
        self,
        *,
        shift_start: datetime,
        shift_end: datetime,
        count: int,
        rng: random.Random,
    ) -> list[datetime]:
        return self._build_arrival_schedule(
            shift_start=shift_start,
            shift_end=shift_end,
            count=count,
            rng=rng,
            kind="cargo",
        )

    def _seed_cargo_backlog(
        self,
        *,
        count: int,
        shift_start: datetime,
        shift_end: datetime,
        topology: TopologyContext,
        rng: random.Random,
    ) -> list[Cargo]:
        cargos: list[Cargo] = []
        for created_at in self._build_cargo_seed_schedule(
            shift_start=shift_start,
            shift_end=shift_end,
            count=count,
            rng=rng,
        ):
            container_type = rng.choice(topology.supported_flow_sizes)
            cargos.append(self._create_cargo(created_at=created_at, container_type=container_type, rng=rng))
        return cargos


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

    def _get_active_employee_pool(self) -> list[Employee]:
        return list(
            Employee.objects.filter(is_active=True)
            .prefetch_related(
                "qualifications",
                "task_profiles__qualification_modifiers__employee_qualification__qualification",
            )
            .order_by("id")
        )

    def _sample_shift_employees(
        self,
        *,
        employee_pool: list[Employee],
        count: int,
        rng: random.Random,
    ) -> list[Employee]:
        if count > len(employee_pool):
            raise CommandError(
                f"Requested {count} employees, but only {len(employee_pool)} active employees exist."
            )
        sampled = rng.sample(employee_pool, count)
        sampled.sort(key=lambda e: (e.employee_code, e.id))
        return sampled

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
        units = rng.randint(1, 4)
        weight_kg = round(max(10.0, units * rng.uniform(3.5, 7.5)), 2)
        volume_m3 = round(max(0.03, units * rng.uniform(0.004, 0.010)), 3)
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
                note="SIMSHIFT: cargo created",
            )
            audit_services.record_event(
                actor_type="system",
                actor_id="simulation:simulate_shift",
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
                    "source": "simulate_shift",
                    "func": "_create_cargo",
                    "cargo_event_id": str(event.id),
                },
            )
        return cargo

    def _task_external_ref(self, task_type: str, seq: int) -> str:
        suffix = task_type.replace("_", "-")
        return f"{self.external_prefix}-T-{seq:05d}-{suffix}"

    def _estimate_minutes(self, *, profile: TaskProfile, cargo: Optional[Cargo], difficulty: int, is_outbound_move: bool = False) -> int:
        base = float(profile.base_minutes) + 0.8 * max(0, difficulty - 1)
        if cargo:
            units_penalty = cargo.units * 0.20
            weight_penalty = cargo.weight_kg * 0.012
            volume_penalty = cargo.volume_m3 * 8.0
            base = base + units_penalty + weight_penalty + volume_penalty
        if profile.task_type == Task.TaskType.MOVE_BETWEEN_SLOTS and is_outbound_move:
            base += 1.5
        return max(3, int(round(base)))

    def _ensure_employee_profiles(self, employees: list[Employee]) -> None:
        expected_task_types = {value for value, _ in Task.TaskType.choices}
        missing_by_employee: dict[str, list[str]] = {}

        for employee in employees:
            profiles = list(employee.task_profiles.all())
            present = {profile.task_type for profile in profiles}
            missing = sorted(expected_task_types - present)
            if missing:
                missing_by_employee[employee.employee_code] = missing

        if missing_by_employee:
            parts = [
                f"{employee_code}: {', '.join(task_types)}"
                for employee_code, task_types in sorted(missing_by_employee.items())
            ]
            raise CommandError(
                "Не у всех выбранных сотрудников есть профили EmployeeTaskProfile по всем типам задач. "
                "Отсутствуют профили у: " + " | ".join(parts)
            )

    def _make_speed_profiles(self, employees: list[Employee]) -> dict[int, dict[str, dict[str, object]]]:
        speeds: dict[int, dict[str, dict[str, object]]] = {}
        for employee in employees:
            per_type: dict[str, dict[str, object]] = {}
            for profile in employee.task_profiles.all():
                modifiers: dict[str, dict[str, float]] = {}
                for modifier in profile.qualification_modifiers.all():
                    qual = modifier.employee_qualification.qualification
                    modifiers[qual.code] = {
                        "factor": float(modifier.factor),
                        "sigma_bonus": float(modifier.sigma_bonus),
                    }

                per_type[profile.task_type] = {
                    "performance_factor": float(profile.performance_factor),
                    "sigma": float(profile.sigma),
                    "modifiers": modifiers,
                }
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
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        rng: random.Random,
    ) -> int:
        stats = EmployeeShiftStats.objects.get(employee=employee, shift=shift)
        base = float(task.estimated_minutes or profile.base_minutes)
        if cargo:
            base += cargo.units * 0.10
            base += cargo.weight_kg * 0.006
            base += cargo.volume_m3 * 4.0
            if profile.task_type == Task.TaskType.DISPATCH_CARGO:
                base += 0.5
        load_penalty = 1.0 + min(stats.task_assigned_count or 0, 20) * 0.008
        score_penalty = 1.0 + min(stats.shift_score or 0, 40) * 0.002

        employee_profile = speed_profiles[employee.id][profile.task_type]
        performance_factor = max(0.10, float(employee_profile["performance_factor"]))
        sigma = max(0.01, float(employee_profile["sigma"]))

        required_codes = set(task.required_qualifications.values_list("code", flat=True))
        modifier_factor = 1.0
        sigma_bonus = 0.0
        for qual_code in required_codes:
            modifier = employee_profile["modifiers"].get(qual_code)
            if modifier:
                modifier_factor *= max(0.10, float(modifier["factor"]))
                sigma_bonus += max(0.0, float(modifier["sigma_bonus"]))

        effective_sigma = min(0.35, sigma + sigma_bonus)
        noise = max(0.70, rng.gauss(1.0, effective_sigma))

        minutes = base * performance_factor * modifier_factor * load_penalty * score_penalty * noise
        return max(2, int(round(minutes)))

    def _resolve_required_qualification_codes(
        self,
        *,
        profile: TaskProfile,
        cargo: Optional[Cargo],
        rng: random.Random,
        forklift_mix_rate: float,
    ) -> list[str]:
        if not cargo:
            return list(profile.required_qual_codes)

        cargo_base_map = {
            Task.TaskType.RECEIVE_TO_INBOUND: "RECEIVE",
            Task.TaskType.PUTAWAY_TO_RACK: "MOVE",
            Task.TaskType.MOVE_BETWEEN_SLOTS: "MOVE",
            Task.TaskType.DISPATCH_CARGO: "DISPATCH",
        }
        base_code = cargo_base_map.get(profile.task_type)
        if not base_code:
            return list(profile.required_qual_codes)

        required_codes = [base_code]
        if rng.random() < forklift_mix_rate:
            required_codes.append("FORKLIFT")
        return required_codes

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
        forklift_mix_rate: float,
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
                name=f"SIMSHIFT {profile.task_type}",
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
            required_codes = self._resolve_required_qualification_codes(
                profile=profile,
                cargo=cargo,
                rng=rng,
                forklift_mix_rate=forklift_mix_rate,
            )
            required_qs = [quals[c] for c in required_codes if c in quals]
            if required_qs:
                task.required_qualifications.set(required_qs)
            audit_services.record_event(
                actor_type="system",
                actor_id="simulation:simulate_shift",
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
                    "source": "simulate_shift",
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

    @contextmanager
    def _temporarily_block_unavailable_employees(
        self,
        *,
        shift: Shift,
        employee_available_at: dict[int, datetime],
        probe_time: datetime,
    ):
        blocked_ids = [eid for eid, free_at in employee_available_at.items() if free_at > probe_time]
        if not blocked_ids:
            yield
            return
        changed_ids = list(
            EmployeeShiftStats.objects.filter(shift=shift, employee_id__in=blocked_ids, is_busy=False).values_list('id', flat=True)
        )
        if changed_ids:
            EmployeeShiftStats.objects.filter(id__in=changed_ids).update(is_busy=True)
        try:
            yield
        finally:
            if changed_ids:
                EmployeeShiftStats.objects.filter(id__in=changed_ids).update(is_busy=False)

    def _candidate_assignment_times(
        self,
        *,
        created_at: datetime,
        employee_available_at: dict[int, datetime],
        shift_end: datetime,
        rng: random.Random,
    ) -> list[datetime]:
        base_probe = created_at + timedelta(minutes=rng.randint(1, 6))
        probes = {base_probe}
        for free_at in employee_available_at.values():
            if free_at >= created_at and free_at <= shift_end - timedelta(minutes=12):
                probes.add(free_at)
                probes.add(max(created_at, free_at + timedelta(minutes=1)))
        return sorted(probes)[:40]

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
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        shift_end: datetime,
        forklift_mix_rate: float,
        employee_available_at: dict[int, datetime],
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
            forklift_mix_rate=forklift_mix_rate,
        )

        if created_at > shift_end - timedelta(minutes=18):
            return TaskExecutionResult(task=task, state="pending", at_time=created_at)

        employee = None
        assigned_at = None
        shift.refresh_from_db()
        for probe_time in self._candidate_assignment_times(
            created_at=created_at,
            employee_available_at=employee_available_at,
            shift_end=shift_end,
            rng=rng,
        ):
            if probe_time >= shift_end - timedelta(minutes=10):
                break
            with self._temporarily_block_unavailable_employees(
                shift=shift,
                employee_available_at=employee_available_at,
                probe_time=probe_time,
            ):
                with self._patched_runtime(probe_time):
                    shift.refresh_from_db()
                    employee = assign_task_to_best_employee(task, shift)
            task.refresh_from_db()
            if employee:
                assigned_at = max(created_at, probe_time)
                break

        if not employee or not assigned_at:
            return TaskExecutionResult(task=task, state="pending", at_time=created_at)

        started_at = max(assigned_at + timedelta(minutes=rng.randint(2, 7)), employee_available_at.get(employee.id, assigned_at), created_at)
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

        if completed_at > shift_end - timedelta(minutes=4):
            employee_available_at[employee.id] = max(employee_available_at.get(employee.id, assigned_at), assigned_at + timedelta(minutes=2))
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
        employee_available_at[employee.id] = completed_at + timedelta(minutes=rng.randint(1, 3))
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
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        dispatch_rate: float,
        internal_move_rate: float,
        seal_every: int,
        shift_end: datetime,
        completed_tasks: int,
        general_schedule: list[datetime],
        next_general_idx: int,
        created_general: int,
    ) -> dict[str, object]:
        def drain_generals(
            current_time: datetime,
            created_tasks_local: int,
            completed_local: int,
            unfinished_local: int,
            created_general_local: int,
            next_general_local: int,
        ) -> tuple[datetime, int, int, int, int, int]:
            (
                next_general_local,
                created_general_local,
                created_tasks_local,
                completed_local,
                unfinished_local,
                new_cursor,
            ) = self._run_due_general_tasks(
                stop_at=current_time,
                now_cursor=current_time,
                shift=shift,
                task_pool=task_pool,
                quals=quals,
                rng=rng,
                speed_profiles=speed_profiles,
                general_schedule=general_schedule,
                next_general_idx=next_general_local,
                created_general=created_general_local,
                created_tasks=created_tasks_local,
                completed_tasks=completed_local,
                unfinished_tasks=unfinished_local,
                seal_every=seal_every,
                shift_end=shift_end,
                forklift_mix_rate=forklift_mix_rate,
            )
            return (
                new_cursor,
                created_tasks_local,
                completed_local,
                unfinished_local,
                created_general_local,
                next_general_local,
            )

        container_type = rng.choice(topology.supported_flow_sizes)
        cargo = self._create_cargo(created_at=arrival_at, container_type=container_type, rng=rng)
        current_time = arrival_at + timedelta(minutes=rng.randint(1, 4))
        created_tasks = 0
        unfinished_tasks = 0
        dispatched = False

        (
            current_time,
            created_tasks,
            completed_tasks,
            unfinished_tasks,
            created_general,
            next_general_idx,
        ) = drain_generals(
            current_time,
            created_tasks,
            completed_tasks,
            unfinished_tasks,
            created_general,
            next_general_idx,
        )

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
                "next_general_idx": next_general_idx,
                "created_general": created_general,
            }

        receive_result = self._execute_task_lifecycle(
            created_at=current_time,
            shift=shift,
            task_pool=task_pool,
            profile=TASK_PROFILES[Task.TaskType.RECEIVE_TO_INBOUND],
            cargo=cargo,
            payload={
                "to_slot_code": inbound_slot.code,
                "note": "SIMSHIFT receive to inbound",
                "sim": True,
                "simulation_run": self.run_id,
            },
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            shift_end=shift_end,
            forklift_mix_rate=forklift_mix_rate,
            employee_available_at=employee_available_at,
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
                "next_general_idx": next_general_idx,
                "created_general": created_general,
            }
        completed_tasks += 1
        self._maybe_seal(completed_tasks, seal_every, receive_result.at_time)

        current_time = receive_result.at_time + timedelta(minutes=rng.randint(3, 10))
        (
            current_time,
            created_tasks,
            completed_tasks,
            unfinished_tasks,
            created_general,
            next_general_idx,
        ) = drain_generals(
            current_time,
            created_tasks,
            completed_tasks,
            unfinished_tasks,
            created_general,
            next_general_idx,
        )

        rack_slot = self._pick_free_slot(
            location_type=StorageLocation.LocationType.RACK,
            size_class=container_type,
            rng=rng,
        )
        if not rack_slot:
            return {
                "ended_at": current_time,
                "created_tasks": created_tasks,
                "completed_tasks": completed_tasks,
                "unfinished_tasks": unfinished_tasks,
                "dispatched": dispatched,
                "next_general_idx": next_general_idx,
                "created_general": created_general,
            }

        putaway_result = self._execute_task_lifecycle(
            created_at=current_time,
            shift=shift,
            task_pool=task_pool,
            profile=TASK_PROFILES[Task.TaskType.PUTAWAY_TO_RACK],
            cargo=cargo,
            payload={
                "to_slot_code": rack_slot.code,
                "note": "SIMSHIFT putaway to rack",
                "sim": True,
                "simulation_run": self.run_id,
            },
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            shift_end=shift_end,
            forklift_mix_rate=forklift_mix_rate,
            employee_available_at=employee_available_at,
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
                "next_general_idx": next_general_idx,
                "created_general": created_general,
            }
        completed_tasks += 1
        self._maybe_seal(completed_tasks, seal_every, putaway_result.at_time)

        last_completed_at = putaway_result.at_time

        if rng.random() < internal_move_rate:
            current_time = last_completed_at + timedelta(minutes=rng.randint(5, 15))
            (
                current_time,
                created_tasks,
                completed_tasks,
                unfinished_tasks,
                created_general,
                next_general_idx,
            ) = drain_generals(
                current_time,
                created_tasks,
                completed_tasks,
                unfinished_tasks,
                created_general,
                next_general_idx,
            )
            next_rack_slot = self._pick_free_slot(
                location_type=StorageLocation.LocationType.RACK,
                size_class=container_type,
                rng=rng,
                exclude_slot_ids={cargo.current_slot_id} if cargo.current_slot_id else set(),
            )
            if next_rack_slot:
                move_result = self._execute_task_lifecycle(
                    created_at=current_time,
                    shift=shift,
                    task_pool=task_pool,
                    profile=TASK_PROFILES[Task.TaskType.MOVE_BETWEEN_SLOTS],
                    cargo=cargo,
                    payload={
                        "to_slot_code": next_rack_slot.code,
                        "note": "SIMSHIFT internal move",
                        "sim": True,
                        "simulation_run": self.run_id,
                    },
                    quals=quals,
                    rng=rng,
                    speed_profiles=speed_profiles,
                    shift_end=shift_end,
                    forklift_mix_rate=forklift_mix_rate,
                    employee_available_at=employee_available_at,
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
                        "next_general_idx": next_general_idx,
                        "created_general": created_general,
                    }
                completed_tasks += 1
                self._maybe_seal(completed_tasks, seal_every, move_result.at_time)
                last_completed_at = move_result.at_time

        if rng.random() < dispatch_rate:
            current_time = last_completed_at + timedelta(minutes=rng.randint(10, 25))
            (
                current_time,
                created_tasks,
                completed_tasks,
                unfinished_tasks,
                created_general,
                next_general_idx,
            ) = drain_generals(
                current_time,
                created_tasks,
                completed_tasks,
                unfinished_tasks,
                created_general,
                next_general_idx,
            )
            outbound_slot = self._pick_free_slot(
                location_type=StorageLocation.LocationType.OUTBOUND,
                size_class=container_type,
                rng=rng,
            )
            if outbound_slot:
                move_out_result = self._execute_task_lifecycle(
                    created_at=current_time,
                    shift=shift,
                    task_pool=task_pool,
                    profile=TASK_PROFILES[Task.TaskType.MOVE_BETWEEN_SLOTS],
                    cargo=cargo,
                    payload={
                        "to_slot_code": outbound_slot.code,
                        "note": "SIMSHIFT move to outbound",
                        "sim": True,
                        "simulation_run": self.run_id,
                    },
                    quals=quals,
                    rng=rng,
                    speed_profiles=speed_profiles,
                    shift_end=shift_end,
                    forklift_mix_rate=forklift_mix_rate,
                    employee_available_at=employee_available_at,
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
                        "next_general_idx": next_general_idx,
                        "created_general": created_general,
                    }
                completed_tasks += 1
                self._maybe_seal(completed_tasks, seal_every, move_out_result.at_time)
                last_completed_at = move_out_result.at_time

                current_time = last_completed_at + timedelta(minutes=rng.randint(2, 8))
                (
                    current_time,
                    created_tasks,
                    completed_tasks,
                    unfinished_tasks,
                    created_general,
                    next_general_idx,
                ) = drain_generals(
                    current_time,
                    created_tasks,
                    completed_tasks,
                    unfinished_tasks,
                    created_general,
                    next_general_idx,
                )
                dispatch_result = self._execute_task_lifecycle(
                    created_at=current_time,
                    shift=shift,
                    task_pool=task_pool,
                    profile=TASK_PROFILES[Task.TaskType.DISPATCH_CARGO],
                    cargo=cargo,
                    payload={
                        "note": "SIMSHIFT dispatch cargo",
                        "sim": True,
                        "simulation_run": self.run_id,
                    },
                    quals=quals,
                    rng=rng,
                    speed_profiles=speed_profiles,
                    shift_end=shift_end,
                    forklift_mix_rate=forklift_mix_rate,
                    employee_available_at=employee_available_at,
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
                        "next_general_idx": next_general_idx,
                        "created_general": created_general,
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
            "next_general_idx": next_general_idx,
            "created_general": created_general,
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
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        general_schedule: list[datetime],
        next_general_idx: int,
        created_general: int,
        created_tasks: int,
        completed_tasks: int,
        unfinished_tasks: int,
        seal_every: int,
        shift_end: datetime,
        forklift_mix_rate: float,
        employee_available_at: dict[int, datetime],
        force_remaining: bool = False,
    ) -> tuple[int, int, int, int, int, datetime]:
        latest_create_at = shift_end - timedelta(minutes=42)
        while next_general_idx < len(general_schedule):
            scheduled_at = general_schedule[next_general_idx]
            if not force_remaining and scheduled_at > stop_at:
                break
            created_at = max(now_cursor, scheduled_at)
            if created_at > latest_create_at:
                break
            created_at = min(created_at, latest_create_at)
            result = self._execute_task_lifecycle(
                created_at=created_at,
                shift=shift,
                task_pool=task_pool,
                profile=TASK_PROFILES[Task.TaskType.GENERAL],
                cargo=None,
                payload={
                    "note": "SIMSHIFT general task",
                    "sim": True,
                    "simulation_run": self.run_id,
                },
                quals=quals,
                rng=rng,
                speed_profiles=speed_profiles,
                shift_end=shift_end,
                forklift_mix_rate=forklift_mix_rate,
                employee_available_at=employee_available_at,
            )
            created_general += 1
            created_tasks += 1
            if result.is_completed:
                completed_tasks += 1
                self._maybe_seal(completed_tasks, seal_every, result.at_time)
            else:
                unfinished_tasks += 1
            now_cursor = max(now_cursor, created_at) + timedelta(minutes=rng.randint(1, 4))
            next_general_idx += 1
        return next_general_idx, created_general, created_tasks, completed_tasks, unfinished_tasks, now_cursor


    def _cargo_has_active_tasks(self, cargo: Cargo) -> bool:
        return cargo.task_set.filter(
            status__in=[Task.Status.PENDING, Task.Status.IN_PROGRESS, Task.Status.PAUSED]
        ).exists()

    def _pick_waiting_created_cargo(self, *, current_time: datetime) -> Optional[Cargo]:
        qs = (
            Cargo.objects.select_related("current_slot", "current_slot__location")
            .filter(status=Cargo.Status.CREATED, current_slot__isnull=True, created_at__lte=current_time)
            .order_by("created_at", "id")
        )
        for cargo in qs[:50]:
            if not self._cargo_has_active_tasks(cargo):
                return cargo
        return None

    def _next_waiting_created_ts(self, current_time: datetime) -> Optional[datetime]:
        return (
            Cargo.objects.filter(
                status=Cargo.Status.CREATED,
                current_slot__isnull=True,
                created_at__gt=current_time,
            )
            .order_by("created_at")
            .values_list("created_at", flat=True)
            .first()
        )

    def _pick_arrived_cargo_for_putaway(self, *, current_time: datetime) -> Optional[Cargo]:
        qs = (
            Cargo.objects.select_related("current_slot", "current_slot__location")
            .filter(
                status=Cargo.Status.ARRIVED,
                current_slot__isnull=False,
                current_slot__location__location_type=StorageLocation.LocationType.INBOUND,
                updated_at__lte=current_time,
            )
            .order_by("updated_at", "created_at", "id")
        )
        for cargo in qs[:50]:
            if not self._cargo_has_active_tasks(cargo):
                return cargo
        return None

    def _pick_stored_cargo_for_dispatch(self, *, current_time: datetime, rng: random.Random) -> Optional[Cargo]:
        qs = list(
            Cargo.objects.select_related("current_slot", "current_slot__location")
            .filter(
                status=Cargo.Status.STORED,
                current_slot__isnull=False,
                current_slot__location__location_type=StorageLocation.LocationType.RACK,
                updated_at__lte=current_time,
            )
            .order_by("created_at", "id")[:80]
        )
        qs = [cargo for cargo in qs if not self._cargo_has_active_tasks(cargo)]
        if not qs:
            return None
        return rng.choice(qs)

    def _empty_op_result(self, current_time: datetime) -> dict[str, object]:
        return {
            "handled": False,
            "ended_at": current_time,
            "created_tasks": 0,
            "completed_tasks": 0,
            "unfinished_tasks": 0,
            "dispatched_cargos": 0,
            "next_general_idx": None,
        }

    def _run_receive_for_created_cargo(
        self,
        *,
        current_time: datetime,
        shift: Shift,
        task_pool: TaskPool,
        topology: TopologyContext,
        quals: dict[str, Qualification],
        rng: random.Random,
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        shift_end: datetime,
        forklift_mix_rate: float,
        employee_available_at: dict[int, datetime],
    ) -> dict[str, object]:
        cargo = self._pick_waiting_created_cargo(current_time=current_time)
        if not cargo:
            return self._empty_op_result(current_time)

        inbound_slot = self._pick_free_slot(
            location_type=StorageLocation.LocationType.INBOUND,
            size_class=cargo.container_type,
            rng=rng,
        )
        if not inbound_slot:
            return self._empty_op_result(current_time)

        result = self._execute_task_lifecycle(
            created_at=max(current_time, cargo.created_at),
            shift=shift,
            task_pool=task_pool,
            profile=TASK_PROFILES[Task.TaskType.RECEIVE_TO_INBOUND],
            cargo=cargo,
            payload={
                "to_slot_code": inbound_slot.code,
                "note": "SIMSHIFT receive to inbound",
                "sim": True,
                "simulation_run": self.run_id,
            },
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            shift_end=shift_end,
            forklift_mix_rate=forklift_mix_rate,
            employee_available_at=employee_available_at,
        )
        return {
            "handled": True,
            "ended_at": result.at_time,
            "created_tasks": 1,
            "completed_tasks": 1 if result.is_completed else 0,
            "unfinished_tasks": 0 if result.is_completed else 1,
            "dispatched_cargos": 0,
        }

    def _run_putaway_for_inbound_cargo(
        self,
        *,
        current_time: datetime,
        shift: Shift,
        task_pool: TaskPool,
        topology: TopologyContext,
        quals: dict[str, Qualification],
        rng: random.Random,
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        shift_end: datetime,
        forklift_mix_rate: float,
        employee_available_at: dict[int, datetime],
    ) -> dict[str, object]:
        cargo = self._pick_arrived_cargo_for_putaway(current_time=current_time)
        if not cargo:
            return self._empty_op_result(current_time)

        rack_slot = self._pick_free_slot(
            location_type=StorageLocation.LocationType.RACK,
            size_class=cargo.container_type,
            rng=rng,
        )
        if not rack_slot:
            return self._empty_op_result(current_time)

        result = self._execute_task_lifecycle(
            created_at=current_time,
            shift=shift,
            task_pool=task_pool,
            profile=TASK_PROFILES[Task.TaskType.PUTAWAY_TO_RACK],
            cargo=cargo,
            payload={
                "to_slot_code": rack_slot.code,
                "note": "SIMSHIFT putaway to rack",
                "sim": True,
                "simulation_run": self.run_id,
            },
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            shift_end=shift_end,
            forklift_mix_rate=forklift_mix_rate,
            employee_available_at=employee_available_at,
        )
        return {
            "handled": True,
            "ended_at": result.at_time,
            "created_tasks": 1,
            "completed_tasks": 1 if result.is_completed else 0,
            "unfinished_tasks": 0 if result.is_completed else 1,
            "dispatched_cargos": 0,
        }

    def _run_dispatch_wave(
        self,
        *,
        current_time: datetime,
        shift: Shift,
        task_pool: TaskPool,
        topology: TopologyContext,
        quals: dict[str, Qualification],
        rng: random.Random,
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        shift_end: datetime,
        forklift_mix_rate: float,
        employee_available_at: dict[int, datetime],
    ) -> dict[str, object]:
        # Conservative time buffer so undelivered cargo stays in long-term storage rather than ending the shift in OUTBOUND.
        if current_time > shift_end - timedelta(minutes=40):
            return self._empty_op_result(current_time)

        cargo = self._pick_stored_cargo_for_dispatch(current_time=current_time, rng=rng)
        if not cargo:
            return self._empty_op_result(current_time)

        outbound_slot = self._pick_free_slot(
            location_type=StorageLocation.LocationType.OUTBOUND,
            size_class=cargo.container_type,
            rng=rng,
        )
        if not outbound_slot:
            return self._empty_op_result(current_time)

        created_tasks = 0
        completed_tasks = 0
        unfinished_tasks = 0
        flow_time = current_time

        move_result = self._execute_task_lifecycle(
            created_at=flow_time,
            shift=shift,
            task_pool=task_pool,
            profile=TASK_PROFILES[Task.TaskType.MOVE_BETWEEN_SLOTS],
            cargo=cargo,
            payload={
                "to_slot_code": outbound_slot.code,
                "note": "SIMSHIFT move to outbound",
                "sim": True,
                "simulation_run": self.run_id,
            },
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            shift_end=shift_end,
            forklift_mix_rate=forklift_mix_rate,
            employee_available_at=employee_available_at,
        )
        created_tasks += 1
        if not move_result.is_completed:
            unfinished_tasks += 1
            return {
                "handled": True,
                "ended_at": move_result.at_time,
                "created_tasks": created_tasks,
                "completed_tasks": completed_tasks,
                "unfinished_tasks": unfinished_tasks,
                "dispatched_cargos": 0,
            }

        completed_tasks += 1
        flow_time = move_result.at_time + timedelta(minutes=rng.randint(2, 6))

        dispatch_result = self._execute_task_lifecycle(
            created_at=flow_time,
            shift=shift,
            task_pool=task_pool,
            profile=TASK_PROFILES[Task.TaskType.DISPATCH_CARGO],
            cargo=cargo,
            payload={
                "note": "SIMSHIFT dispatch cargo",
                "sim": True,
                "simulation_run": self.run_id,
            },
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            shift_end=shift_end,
            forklift_mix_rate=forklift_mix_rate,
            employee_available_at=employee_available_at,
        )
        created_tasks += 1
        if not dispatch_result.is_completed:
            unfinished_tasks += 1
            return {
                "handled": True,
                "ended_at": dispatch_result.at_time,
                "created_tasks": created_tasks,
                "completed_tasks": completed_tasks,
                "unfinished_tasks": unfinished_tasks,
                "dispatched_cargos": 0,
            }

        completed_tasks += 1
        return {
            "handled": True,
            "ended_at": dispatch_result.at_time,
            "created_tasks": created_tasks,
            "completed_tasks": completed_tasks,
            "unfinished_tasks": unfinished_tasks,
            "dispatched_cargos": 1,
        }

    def _run_one_general_task_if_due(
        self,
        *,
        current_time: datetime,
        shift: Shift,
        task_pool: TaskPool,
        quals: dict[str, Qualification],
        rng: random.Random,
        speed_profiles: dict[int, dict[str, dict[str, object]]],
        general_schedule: list[datetime],
        next_general_idx: int,
        shift_end: datetime,
        forklift_mix_rate: float,
        employee_available_at: dict[int, datetime],
    ) -> dict[str, object]:
        if next_general_idx >= len(general_schedule):
            result = self._empty_op_result(current_time)
            result["next_general_idx"] = next_general_idx
            return result

        scheduled_at = general_schedule[next_general_idx]
        latest_create_at = shift_end - timedelta(minutes=42)
        if scheduled_at > current_time or scheduled_at > latest_create_at:
            result = self._empty_op_result(current_time)
            result["next_general_idx"] = next_general_idx
            return result

        result = self._execute_task_lifecycle(
            created_at=max(current_time, scheduled_at),
            shift=shift,
            task_pool=task_pool,
            profile=TASK_PROFILES[Task.TaskType.GENERAL],
            cargo=None,
            payload={
                "note": "SIMSHIFT general task",
                "sim": True,
                "simulation_run": self.run_id,
            },
            quals=quals,
            rng=rng,
            speed_profiles=speed_profiles,
            shift_end=shift_end,
            forklift_mix_rate=forklift_mix_rate,
            employee_available_at=employee_available_at,
        )
        return {
            "handled": True,
            "ended_at": result.at_time,
            "created_tasks": 1,
            "completed_tasks": 1 if result.is_completed else 0,
            "unfinished_tasks": 0 if result.is_completed else 1,
            "dispatched_cargos": 0,
            "next_general_idx": next_general_idx + 1,
        }


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
        shift_ids = list(Shift.objects.filter(name__startswith="SIM shift").values_list("id", flat=True))

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
                f"Purged previous SIMSHIFT domain data – tasks={tasks_count}, logs={logs_count}, cargos={cargos_count}, cargo_events={cargo_events_count}. "
                "Audit tables were not touched."
            )
        )
