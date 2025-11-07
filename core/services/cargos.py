# core/services/cargo.py
from __future__ import annotations

from typing import Optional

from django.db import transaction
from rest_framework.exceptions import ValidationError, NotFound

from core.models import (
    Cargo,
    CargoEvent,
    Employee,
    LocationSlot,
    StorageLocation,
)
from audit.services import record_event
from django.utils.timezone import now


__all__ = ["arrive", "store", "move", "dispatch"]


# ========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ========================

def _get_employee(employee_code: Optional[str]) -> Optional[Employee]:
    """Возвращает сотрудника по коду или None, если код не передан."""
    if not employee_code:
        return None
    try:
        return Employee.objects.get(employee_code=employee_code)
    except Employee.DoesNotExist:
        raise NotFound("Сотрудник не найден")


def _get_slot(slot_code: str) -> LocationSlot:
    """Берёт слот с блокировкой на время транзакции."""
    try:
        return LocationSlot.objects.select_for_update().get(code=slot_code)
    except LocationSlot.DoesNotExist:
        raise NotFound("Ячейка (слот) не найдена")


def _get_cargo(cargo_code: str) -> Cargo:
    """Берёт груз с блокировкой на время транзакции."""
    try:
        return Cargo.objects.select_for_update().get(cargo_code=cargo_code)
    except Cargo.DoesNotExist:
        raise NotFound("Груз не найден")


def _slot_is_free(slot: LocationSlot) -> bool:
    """
    Проверяет, свободен ли слот.
    В модели занятость выражается через Cargo.current_slot -> LocationSlot (OneToOne).
    """
    from core.models import Cargo as CargoModel  # локальный импорт, чтобы избежать циклов при миграциях
    return not CargoModel.objects.filter(current_slot=slot).exists()


def _check_slot_free_and_compatible(slot: LocationSlot, cargo: Cargo) -> None:
    """Проверка, что слот свободен и подходит по классу размера/контейнеру."""
    if not _slot_is_free(slot):
        raise ValidationError("Слот занят другим грузом")
    if slot.location.slot_size_class != cargo.container_type:
        raise ValidationError("Неверный класс ячейки для типа контейнера груза")


# =================
# ДОМЕННЫЕ ОПЕРАЦИИ
# =================

@transaction.atomic
def arrive(
    cargo_code: str,
    to_slot_code: str,
    employee_code: Optional[str] = None,
    note: Optional[str] = None,
) -> CargoEvent:
    """
    Помечает фактическое прибытие груза и кладёт его в ячейку зоны INBOUND.
    Требования:
      - груз не отгружен;
      - у груза нет ячейки (он ещё «вне склада»);
      - статус груза — created;
      - целевой слот свободен, совместим и принадлежит INBOUND.
    Результат:
      - cargo.status = ARRIVED
      - cargo.current_slot = INBOUND слот
      - событие CargoEvent.ARRIVED
    """
    cargo = _get_cargo(cargo_code)

    if cargo.status == Cargo.Status.DISPATCHED:
        raise ValidationError("Груз уже отгружен")

    if cargo.current_slot_id:
        raise ValidationError("Груз уже находится в ячейке")

    if cargo.status != Cargo.Status.CREATED:
        raise ValidationError("Отметить прибытие можно только для груза в статусе 'created'")

    slot = _get_slot(to_slot_code)
    _check_slot_free_and_compatible(slot, cargo)

    if slot.location.location_type != StorageLocation.LocationType.INBOUND:
        raise ValidationError("Прибытие возможно только в зону INBOUND")
    
    before = {
        "status": cargo.status,
        "slot_code": None,
        "units": cargo.units,
    }

    timestamp = now()
    cargo.current_slot = slot
    cargo.status = Cargo.Status.ARRIVED
    cargo.updated_at = timestamp
    cargo.save(update_fields=["current_slot", "status", "updated_at"])

    event = CargoEvent.objects.create(
        cargo=cargo,
        event_type=CargoEvent.EventType.ARRIVED,
        from_slot=None,
        to_slot=slot,
        quantity=cargo.units,
        employee=_get_employee(employee_code),
        timestamp=timestamp,
        note=note or "Arrived at inbound dock",
    )

    after = {
        "status": cargo.status,
        "slot_code": slot.code,
        "units": cargo.units,
        "cargo_code": cargo.cargo_code,
    }
    meta = {
        "source": "core.services.cargos",
        "func": "arrive",
        "to_slot_code": to_slot_code,
        "employee_code": employee_code,
        "cargo_event_id": str(event.id),
        "timestamp": timestamp.isoformat(),
    }
    entity_id = str(cargo.id)
    transaction.on_commit(lambda eid=entity_id, b=before, a=after, m=meta: record_event(
        actor_type="system",
        actor_id="api",
        entity_type="Cargo",
        entity_id=eid,
        action="ARRIVE",
        before=b,
        after=a,
        meta=m,
    ))
    return event


@transaction.atomic
def store(
    cargo_code: str,
    to_slot_code: str,
    employee_code: Optional[str] = None,
    note: Optional[str] = None,
) -> CargoEvent:
    """
    Размещение (putaway) из INBOUND в зону хранения RACK.
    Требования:
      - груз не отгружен;
      - груз находится в ячейке INBOUND и в статусе 'arrived';
      - целевой слот свободен, совместим и принадлежит RACK.
    Результат:
      - cargo.status = STORED
      - cargo.current_slot = RACK слот
      - событие CargoEvent.STORED
    """
    cargo = _get_cargo(cargo_code)

    if cargo.status == Cargo.Status.DISPATCHED:
        raise ValidationError("Груз уже отгружен")

    if not cargo.current_slot_id:
        raise ValidationError("Груз ещё не прибыл (нет ячейки INBOUND)")

    if cargo.status != Cargo.Status.ARRIVED:
        raise ValidationError("Размещение разрешено только из статуса 'arrived' (после прибытия в INBOUND)")

    from_slot = cargo.current_slot
    if from_slot.location.location_type != StorageLocation.LocationType.INBOUND:
        raise ValidationError("Размещать можно только из зоны INBOUND")

    to_slot = _get_slot(to_slot_code)
    _check_slot_free_and_compatible(to_slot, cargo)

    if to_slot.location.location_type != StorageLocation.LocationType.RACK:
        raise ValidationError("Размещать можно только в зону хранения RACK")

    before = {
        "status": cargo.status,
        "slot_code": from_slot.code if from_slot else None,
        "units": cargo.units,
    }

    timestamp = now()
    cargo.current_slot = to_slot
    cargo.status = Cargo.Status.STORED
    cargo.updated_at = timestamp
    cargo.save(update_fields=["current_slot", "status", "updated_at"])

    event = CargoEvent.objects.create(
        cargo=cargo,
        event_type=CargoEvent.EventType.STORED,
        from_slot=from_slot,
        to_slot=to_slot,
        quantity=cargo.units,
        employee=_get_employee(employee_code),
        timestamp=timestamp,
        note=note or "Putaway to storage",
    )
    after = {
        "status": cargo.status,
        "slot_code": to_slot.code,
        "units": cargo.units,
        "cargo_code": cargo.cargo_code,
    }
    meta = {
        "source": "core.services.cargos",
        "func": "store",
        "from_slot_code": from_slot.code if from_slot else None,
        "to_slot_code": to_slot_code,
        "employee_code": employee_code,
        "cargo_event_id": str(event.id),
        "timestamp": timestamp.isoformat(),
    }
    entity_id = str(cargo.id)
    transaction.on_commit(lambda eid=entity_id, b=before, a=after, m=meta: record_event(
        actor_type="system",
        actor_id="api",
        entity_type="Cargo",
        entity_id=eid,
        action="STORE",
        before=b,
        after=a,
        meta=m,
    ))
    return event


@transaction.atomic
def move(
    cargo_code: str,
    to_slot_code: str,
    employee_code: Optional[str] = None,
    note: Optional[str] = None,
) -> CargoEvent:
    """
    Перемещение между ячейками после размещения (например, RACK → OUTBOUND).
    Требования:
      - груз не отгружен;
      - груз находится в ячейке;
      - груз в статусе 'stored' (движения по складу после putaway);
      - целевой слот свободен и совместим.
    Результат:
      - cargo.status остаётся 'stored'
      - cargo.current_slot = целевой слот
      - событие CargoEvent.MOVED
    """
    cargo = _get_cargo(cargo_code)

    if cargo.status == Cargo.Status.DISPATCHED:
        raise ValidationError("Груз уже отгружен")

    if not cargo.current_slot_id:
        raise ValidationError("Груз не находится в ячейке")

    if cargo.status != Cargo.Status.STORED:
        raise ValidationError("Перемещение доступно только после размещения (статус 'stored')")

    to_slot = _get_slot(to_slot_code)
    _check_slot_free_and_compatible(to_slot, cargo)

    from_slot = cargo.current_slot
    before = {
        "status": cargo.status,
        "slot_code": from_slot.code if from_slot else None,
        "units": cargo.units,
    }
    timestamp = now()
    cargo.current_slot = to_slot
    cargo.updated_at = timestamp
    cargo.save(update_fields=["current_slot", "updated_at"])

    event = CargoEvent.objects.create(
        cargo=cargo,
        event_type=CargoEvent.EventType.MOVED,
        from_slot=from_slot,
        to_slot=to_slot,
        quantity=cargo.units,
        employee=_get_employee(employee_code),
        timestamp=timestamp,
        note=note or f"Move to {to_slot.location.location_type}",
    )
    after = {
        "status": cargo.status,
        "slot_code": to_slot.code,
        "units": cargo.units,
        "cargo_code": cargo.cargo_code,
    }
    meta = {
        "source": "core.services.cargos",
        "func": "move",
        "from_slot_code": from_slot.code if from_slot else None,
        "to_slot_code": to_slot_code,
        "employee_code": employee_code,
        "cargo_event_id": str(event.id),
        "timestamp": timestamp.isoformat(),
    }
    entity_id = str(cargo.id)
    transaction.on_commit(lambda eid=entity_id, b=before, a=after, m=meta: record_event(
        actor_type="system",
        actor_id="api",
        entity_type="Cargo",
        entity_id=eid,
        action="MOVE",
        before=b,
        after=a,
        meta=m,
    ))
    return event


@transaction.atomic
def dispatch(
    cargo_code: str,
    employee_code: Optional[str] = None,
    note: Optional[str] = None,
) -> CargoEvent:
    """
    Полная отгрузка целого груза.
    Требования:
      - груз не отгружен ранее;
      - груз находится в ячейке OUTBOUND;
      - отгружается целиком (частичные списания запрещены).
    Результат:
      - cargo.status = DISPATCHED
      - cargo.current_slot = NULL
      - событие CargoEvent.DISPATCHED
    """
    cargo = _get_cargo(cargo_code)

    if cargo.status == Cargo.Status.DISPATCHED:
        raise ValidationError("Груз уже отгружен ранее")

    if not cargo.current_slot_id:
        raise ValidationError("Груз не находится в ячейке")

    # Разрешаем отгрузку только из зоны OUTBOUND
    if cargo.current_slot.location.location_type != StorageLocation.LocationType.OUTBOUND:
        raise ValidationError("Отгрузка разрешена только из зоны OUTBOUND. Переместите груз через /move")

    from_slot = cargo.current_slot
    full_qty = cargo.units
    if full_qty <= 0:
        # На случай неконсистентных данных
        raise ValidationError("У груза отсутствуют единицы для отгрузки")
    
    before = {
        "status": cargo.status,
        "slot_code": from_slot.code if from_slot else None,
        "units": full_qty,
    }

    # cargo.units = 0
    timestamp = now()
    cargo.status = Cargo.Status.DISPATCHED
    cargo.current_slot = None
    cargo.handling_state = Cargo.HandlingState.IDLE
    cargo.updated_at = timestamp
    cargo.save(update_fields=["status", "current_slot", "handling_state", "updated_at"])

    event = CargoEvent.objects.create(
        cargo=cargo,
        event_type=CargoEvent.EventType.DISPATCHED,
        from_slot=from_slot,
        to_slot=None,
        quantity=full_qty,
        employee=_get_employee(employee_code),
        timestamp=timestamp,
        note=note or "",
    )
    
    after = {
        "status": cargo.status,
        "slot_code": None,
        "units": full_qty,            # доменная логика units не меняет — отражаем фактическое
        "cargo_code": cargo.cargo_code,
    }
    meta = {
        "source": "core.services.cargos",
        "func": "dispatch",
        "from_slot_code": from_slot.code if from_slot else None,
        "employee_code": employee_code,
        "cargo_event_id": str(event.id),
        "timestamp": timestamp.isoformat(),
    }
    entity_id = str(cargo.id)
    transaction.on_commit(lambda eid=entity_id, b=before, a=after, m=meta: record_event(
        actor_type="system",
        actor_id="api",
        entity_type="Cargo",
        entity_id=eid,
        action="DISPATCH",
        before=b,
        after=a,
        meta=m,
    ))
    return event
