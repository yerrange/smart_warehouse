# create_locations.py
import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "smart_warehouse.settings")
django.setup()

from django.db import transaction
from core.models import StorageLocation, LocationSlot

# ----------------------------
# НАСТРОЙКИ ПО УМОЛЧАНИЮ
# Можно смело менять под себя
# ----------------------------

CONFIG = {
    # Приёмка: несколько доков, по 2 слота паллетного размера
    "receiving": {
        "count": 2,              # REC-01..REC-02
        "slot_count": 10,
        "slot_size": StorageLocation.SlotSize.PALLET,
    },
    # Буфер: несколько буферных позиций
    "staging": {
        "count": 4,              # STG-01..STG-04
        "slot_count": 1,
        "slot_size": StorageLocation.SlotSize.PALLET,
    },
    # Стеллажи: зона Z1, два пролёта, по 4 стойки, 3 полки, 5 "бинов" на полку
    "racks": {
        "zone": "Z1",
        "aisles": ["A01", "A02"],
        "racks_per_aisle": 5,    # R01..R04
        "shelves": ["S1", "S2", "S3"],
        "bins_per_shelf": 10,     # B01..B05
        "slot_size": StorageLocation.SlotSize.PALLET,
    },
    # Отборочная "фейс"-зона: мелкая тара
    "pick_face": {
        "zone": "PICK",
        "aisles": ["P01"],
        "racks_per_aisle": 2,
        "shelves": ["S1"],
        "bins_per_shelf": 10,
        "slot_size": StorageLocation.SlotSize.BOX,
    },
    # Отгрузка: несколько доков
    "outbound": {
        "count": 2,              # OUT-01..OUT-02
        "slot_count": 5,
        "slot_size": StorageLocation.SlotSize.PALLET,
    },
    # QC-зона
    "qc": {
        "count": 1,              # QC-01
        "slot_count": 1,
        "slot_size": StorageLocation.SlotSize.PALLET
    },
}


def _make_slot_code(location_code: str, idx: int) -> str:
    # В твоих моделях слоты обычно кодируются как "{location.code}-#<n>"
    return f"{location_code}-#{idx}"


@transaction.atomic
def ensure_location_with_slots(
    *,
    code: str,
    location_type: str,
    slot_count: int,
    slot_size_class: str,
    zone: str = "",
    aisle: str = "",
    rack: str = "",
    shelf: str = "",
    bin_: str = "",
):
    """Создаёт StorageLocation (если нет) и добивает нужное число слотов."""
    loc, created = StorageLocation.objects.get_or_create(
        code=code,
        defaults=dict(
            location_type=location_type,
            zone=zone,
            aisle=aisle,
            rack=rack,
            shelf=shelf,
            bin=bin_,
            slot_count=slot_count,
            slot_size_class=slot_size_class,
        ),
    )

    # Если локация уже существовала, актуализируем параметры (безопасно)
    to_update = []
    if loc.slot_count != slot_count:
        loc.slot_count = slot_count
        to_update.append("slot_count")
    if loc.slot_size_class != slot_size_class:
        loc.slot_size_class = slot_size_class
        to_update.append("slot_size_class")
    # Адресные поля (на случай, если менялись шаблоны)
    if loc.zone != zone:
        loc.zone = zone; to_update.append("zone")
    if loc.aisle != aisle:
        loc.aisle = aisle; to_update.append("aisle")
    if loc.rack != rack:
        loc.rack = rack; to_update.append("rack")
    if loc.shelf != shelf:
        loc.shelf = shelf; to_update.append("shelf")
    if loc.bin != bin_:
        loc.bin = bin_; to_update.append("bin")
    if to_update:
        loc.save(update_fields=to_update + ["updated_at"])

    created_slots = 0
    # Гарантируем наличие слотов 1..slot_count (лишние НЕ трогаем)
    for i in range(1, slot_count + 1):
        slot_code = _make_slot_code(loc.code, i)
        _, slot_created = LocationSlot.objects.get_or_create(
            location=loc,
            index=i,
            defaults=dict(
                code=slot_code,
                size_class=slot_size_class,
            ),
        )
        if slot_created:
            created_slots += 1

    return created, created_slots


def main():
    total_locations_created = 0
    total_slots_created = 0

    # 1) RECEIVING
    rec = CONFIG["receiving"]
    for i in range(1, rec["count"] + 1):
        code = f"REC-{i:02d}"
        created, slots = ensure_location_with_slots(
            code=code,
            location_type=StorageLocation.LocationType.RECEIVING,
            slot_count=rec["slot_count"],
            slot_size_class=rec["slot_size"],
            zone="REC",
        )
        total_locations_created += int(created)
        total_slots_created += slots

    # 2) STAGING
    stg = CONFIG["staging"]
    for i in range(1, stg["count"] + 1):
        code = f"STG-{i:02d}"
        created, slots = ensure_location_with_slots(
            code=code,
            location_type=StorageLocation.LocationType.STAGING,
            slot_count=stg["slot_count"],
            slot_size_class=stg["slot_size"],
            zone="STG",
        )
        total_locations_created += int(created)
        total_slots_created += slots

    # 3) RACKS
    racks_cfg = CONFIG["racks"]
    for aisle_idx, aisle in enumerate(racks_cfg["aisles"], start=1):
        for r in range(1, racks_cfg["racks_per_aisle"] + 1):
            rack_code = f"R{r:02d}"
            for shelf in racks_cfg["shelves"]:
                for b in range(1, racks_cfg["bins_per_shelf"] + 1):
                    bin_code = f"B{b:02d}"
                    loc_code = f"{racks_cfg['zone']}-{aisle}-{rack_code}-{shelf}-{bin_code}"
                    created, slots = ensure_location_with_slots(
                        code=loc_code,
                        location_type=StorageLocation.LocationType.RACK,
                        slot_count=1,  # каждая "ячейка-бин" — один слот
                        slot_size_class=racks_cfg["slot_size"],
                        zone=racks_cfg["zone"],
                        aisle=aisle,
                        rack=rack_code,
                        shelf=shelf,
                        bin_=bin_code,
                    )
                    total_locations_created += int(created)
                    total_slots_created += slots

    # 4) PICK FACE
    # pick_cfg = CONFIG["pick_face"]
    # for aisle in pick_cfg["aisles"]:
    #     for r in range(1, pick_cfg["racks_per_aisle"] + 1):
    #         rack_code = f"R{r:02d}"
    #         for shelf in pick_cfg["shelves"]:
    #             for b in range(1, pick_cfg["bins_per_shelf"] + 1):
    #                 bin_code = f"B{b:02d}"
    #                 loc_code = f"{pick_cfg['zone']}-{aisle}-{rack_code}-{shelf}-{bin_code}"
    #                 created, slots = ensure_location_with_slots(
    #                     code=loc_code,
    #                     location_type=StorageLocation.LocationType.PICK_FACE,
    #                     slot_count=1,
    #                     slot_size_class=pick_cfg["slot_size"],
    #                     zone=pick_cfg["zone"],
    #                     aisle=aisle,
    #                     rack=rack_code,
    #                     shelf=shelf,
    #                     bin_=bin_code,
    #                 )
    #                 total_locations_created += int(created)
    #                 total_slots_created += slots

    # 5) OUTBOUND
    out_cfg = CONFIG["outbound"]
    for i in range(1, out_cfg["count"] + 1):
        code = f"OUT-{i:02d}"
        created, slots = ensure_location_with_slots(
            code=code,
            location_type=StorageLocation.LocationType.OUTBOUND,
            slot_count=out_cfg["slot_count"],
            slot_size_class=out_cfg["slot_size"],
            zone="OUT",
        )
        total_locations_created += int(created)
        total_slots_created += slots

    # 6) QC
    qc = CONFIG["qc"]
    for i in range(1, qc["count"] + 1):
        code = f"QC-{i:02d}"
        created, slots = ensure_location_with_slots(
            code=code,
            location_type=StorageLocation.LocationType.QC,
            slot_count=qc["slot_count"],
            slot_size_class=qc["slot_size"],
            zone="QC",
        )
        total_locations_created += int(created)
        total_slots_created += slots

    print(f"Готово. Создано локаций: {total_locations_created}, создано слотов: {total_slots_created}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        raise
