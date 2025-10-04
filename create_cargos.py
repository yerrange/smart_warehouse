import os
import argparse
import random
import string

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "smart_warehouse.settings")

import django  # noqa: E402
django.setup()

from django.db import transaction, models  # noqa: E402
from core.models import Cargo, CargoEvent, SKU  # noqa: E402

# --------- Параметры по умолчанию ----------
DEFAULT_COUNT = 20
DEFAULT_UNITS_MIN = 5
DEFAULT_UNITS_MAX = 25

# Если у тебя есть enum контейнеров в Cargo.Container — используем его;
# иначе подставь строку по умолчанию, которая совпадает с size_class слотов.
DEFAULT_CONTAINER_TYPE = getattr(Cargo, "Container", None)
DEFAULT_CONTAINER_TYPE = getattr(DEFAULT_CONTAINER_TYPE, "PALLET", "pallet")

# 10 базовых SKU: code -> name (uom возьмётся как default="pcs")
SKU_SEED = {
    "SKU-0001": "Болт М8x30",
    "SKU-0002": "Гайка М8",
    "SKU-0003": "Шайба 8мм",
    "SKU-0004": "Кабель ПВС 3x1.5",
    "SKU-0005": "Труба ПНД 25мм",
    "SKU-0006": "Скотч 48мм",
    "SKU-0007": "Поддон EUR",
    "SKU-0008": "Плёнка стрейч",
    "SKU-0009": "Короб 400x300x250",
    "SKU-0010": "Маркер перманентный",
}


# --------- SKU: ensure seed ----------
def ensure_skus(seed: dict[str, str]) -> None:
    for code, name in seed.items():
        SKU.objects.get_or_create(code=code, defaults={"name": name})


def pick_random_sku() -> SKU:
    sku = SKU.objects.filter(is_active=True).order_by("?").first()
    if not sku:
        ensure_skus(SKU_SEED)
        sku = SKU.objects.filter(is_active=True).order_by("?").first()
    if not sku:
        # На всякий случай (если все is_active=False)
        code, name = next(iter(SKU_SEED.items()))
        sku, _ = SKU.objects.get_or_create(code=code, defaults={"name": name, "is_active": True})
    return sku


# --------- Генерация уникального cargo_code ----------
def generate_cargo_code(prefix: str = "C", length: int = 6, max_tries: int = 1000) -> str:
    """
    Генерирует уникальный cargo_code вида C-ABC123.
    Если код уже есть в базе — пробует снова (до max_tries).
    """
    alphabet = string.ascii_uppercase + string.digits
    sep = "-" if prefix else ""
    for _ in range(max_tries):
        body = "".join(random.choice(alphabet) for _ in range(length))
        code = f"{prefix}{sep}{body}" if prefix else body
        if not Cargo.objects.filter(cargo_code=code).exists():
            return code
    raise RuntimeError("Не удалось сгенерировать уникальный cargo_code — превышен лимит попыток")


# --------- Создание одного груза (status='created') ----------
def create_created_cargo(
    *,
    container_type: str,
    units: int,
    prefix: str,
    code_length: int,
) -> Cargo:
    sku = pick_random_sku()
    cargo_code = generate_cargo_code(prefix=prefix, length=code_length)

    # Готовим общие поля
    base_kwargs = dict(
        cargo_code=cargo_code,
        units=units,
        container_type=container_type,
        status=Cargo.Status.CREATED,
        handling_state=getattr(Cargo.HandlingState, "IDLE", "idle"),
        current_slot=None,
    )

    # Пытаемся сохранить SKU как FK (если в модели Cargo поле sku — ForeignKey на SKU)
    try:
        field = Cargo._meta.get_field("sku")
        if isinstance(field, models.ForeignKey) and field.related_model is SKU:
            cargo = Cargo.objects.create(sku=sku, **base_kwargs)
        else:
            # Если sku — не FK: пишем код (и, при наличии, name-снапшот)
            base_kwargs["sku"] = sku.code
            if "name" in [f.name for f in Cargo._meta.get_fields()]:
                base_kwargs["name"] = sku.name
            cargo = Cargo.objects.create(**base_kwargs)
    except Exception:
        # На всякий случай, если поля отличаются — пишем только обязательное
        base_kwargs["sku"] = getattr(sku, "code", "SKU-UNKNOWN")
        cargo = Cargo.objects.create(**base_kwargs)

    # Событие "created" — для таймлайна
    try:
        CargoEvent.objects.create(
            cargo=cargo,
            event_type=CargoEvent.EventType.CREATED,
            quantity=cargo.units,
            note="Создано скриптом create_cargo_created.py",
        )
    except Exception:
        # если ENUM без created — fallback
        CargoEvent.objects.create(
            cargo=cargo,
            event_type=getattr(CargoEvent.EventType, "NOTE", "note"),
            quantity=cargo.units,
            note="Cargo created (fallback event)",
        )

    return cargo


# --------- Пакетная генерация ----------
@transaction.atomic
def generate_batch(
    *,
    count: int,
    container_type: str,
    units_min: int,
    units_max: int,
    prefix: str,
    code_length: int,
):
    ensure_skus(SKU_SEED)

    created = []
    for _ in range(count):
        units = random.randint(units_min, units_max)
        c = create_created_cargo(
            container_type=container_type,
            units=units,
            prefix=prefix,
            code_length=code_length,
        )
        created.append(c)

    print(f"Создано грузов (status=created): {len(created)}")
    for c in created[:5]:
        # Печать краткой строки (с учётом разных вариантов поля sku)
        sku_code = None
        try:
            # если FK — покажем code
            if isinstance(Cargo._meta.get_field("sku"), models.ForeignKey):
                sku_code = getattr(getattr(c, "sku", None), "code", None)
        except Exception:
            pass
        sku_code = sku_code or getattr(c, "sku", None)
        print(f"  {c.cargo_code} | SKU={sku_code} | units={c.units} | {c.container_type}")


def parse_args():
    p = argparse.ArgumentParser(description="Генерация грузов на стадии 'created' + SKU seed")
    p.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Сколько грузов создать")
    p.add_argument("--container-type", default=DEFAULT_CONTAINER_TYPE, help="Тип контейнера (должен совпадать с size_class слотов)")
    p.add_argument("--units-min", type=int, default=DEFAULT_UNITS_MIN)
    p.add_argument("--units-max", type=int, default=DEFAULT_UNITS_MAX)
    p.add_argument("--prefix", default="C", help="Префикс для cargo_code (по умолчанию 'C')")
    p.add_argument("--code-length", type=int, default=6, help="Длина случайной части cargo_code")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.units_min <= 0 or args.units_max < args.units_min:
        raise SystemExit("Неверные параметры: units-min/units-max")
    try:
        generate_batch(
            count=args.count,
            container_type=args.container_type,
            units_min=args.units_min,
            units_max=args.units_max,
            prefix=args.prefix,
            code_length=args.code_length,
        )
    except Exception as e:
        raise SystemExit(f"Ошибка генерации: {e}")
