# core/management/commands/generate_virtual_warehouse.py
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction, connections
from django.utils.text import slugify

from core.models import StorageLocation  # поправь импорт под свой app

# --- Вспомогалки -------------------------------------------------------------

def get_location_type_enum():
    """
    Возвращает enum с типами: LocationType или Kind (как ты его назовёшь в модели).
    """
    return getattr(StorageLocation, 'LocationType', None) or getattr(StorageLocation, 'Kind', None)

def loc_type_value(enum, name: str):
    """
    Безопасно берёт значение из enum по имени (receiving, rack, outbound, staging, qc, pick).
    Разрешает писать как 'receiving'/'RECEIVING'.
    """
    key = name.strip().upper()
    if not enum or not hasattr(enum, key):
        raise CommandError(f"StorageLocation has no enum value '{key}'. Check your model's choices.")
    return getattr(enum, key)

def has_field(model, field_name: str) -> bool:
    try:
        model._meta.get_field(field_name)
        return True
    except Exception:
        return False

def build_code(zone, aisle, rack, shelf, bin_, fmt: str) -> str:
    """
    Собирает код из частей по формату. По умолчанию: Z{zone}-A{aisle:02}-R{rack:02}-S{shelf}-B{bin:02}
    """
    return fmt.format(zone=zone, aisle=aisle, rack=rack, shelf=shelf, bin=bin_).upper()

def chunked(iterable, size=5000):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

# --- Команда -----------------------------------------------------------------

class Command(BaseCommand):
    help = (
        "Generate a virtual warehouse: racks grid and service areas.\n"
        "Creates StorageLocation rows idempotently (skips duplicates)."
    )

    def add_arguments(self, parser):
        # Сетка стеллажей
        parser.add_argument('--zones', nargs='+', default=['Z1'], help="Список зон (например: Z1 Z2 Z3).")
        parser.add_argument('--aisles', type=int, default=2, help="Количество рядов (aisles) на зону.")
        parser.add_argument('--racks', type=int, default=3, help="Количество стоек (racks) на ряд.")
        parser.add_argument('--shelves', type=int, default=2, help="Полок (shelves) на стойку.")
        parser.add_argument('--bins', type=int, default=5, help="Ячеек (bins) на полку.")

        parser.add_argument('--code-format', default='Z{zone}-A{aisle:02}-R{rack:02}-S{shelf}-B{bin:02}',
                            help="Шаблон кода для стеллажей (используются zone/aisle/rack/shelf/bin).")

        parser.add_argument('--single-occupancy', action='store_true', default=True,
                            help="Ячейка для одного груза (по умолчанию True).")
        parser.add_argument('--multi-occupancy', action='store_true',
                            help="Если указан, ячейки будут много-местными (single_occupancy=False).")

        parser.add_argument('--max-weight', type=float, default=0.0, help="Лимит веса (0 = не ограничено).")
        parser.add_argument('--max-volume', type=float, default=0.0, help="Лимит объёма (0 = не ограничено).")

        # Служебные зоны
        parser.add_argument('--receiving', type=int, default=1, help="Сколько receiving-доков создать.")
        parser.add_argument('--outbound', type=int, default=1, help="Сколько outbound-доков создать.")
        parser.add_argument('--staging', type=int, default=0, help="Сколько staging-площадок создать.")
        parser.add_argument('--qc', type=int, default=0, help="Сколько QC-площадок создать.")
        parser.add_argument('--pick', type=int, default=0, help="Сколько pick-face зон создать.")

        # Произвольные префиксы кодов для служебных зон
        parser.add_argument('--receiving-prefix', default='RECV', help="Префикс кодов для receiving (RECV-01).")
        parser.add_argument('--outbound-prefix', default='OUT', help="Префикс кодов для outbound (OUT-01).")
        parser.add_argument('--staging-prefix', default='STG', help="Префикс кодов для staging (STG-01).")
        parser.add_argument('--qc-prefix', default='QC', help="Префикс кодов для QC (QC-01).")
        parser.add_argument('--pick-prefix', default='PICK', help="Префикс кодов для pick-face (PICK-01).")

        # Техника вставки
        parser.add_argument('--batch-size', type=int, default=5000, help="Размер батча для bulk_create.")
        parser.add_argument('--no-bulk', action='store_true', help="Вставлять через get_or_create (медленнее, но надёжнее).")

    @transaction.atomic
    def handle(self, *args, **opts):
        LT = get_location_type_enum()
        if not LT:
            raise CommandError("StorageLocation must have an inner enum LocationType or Kind with choices.")

        # Поддержка обоих имён поля: location_type или kind
        loc_type_field = 'location_type' if has_field(StorageLocation, 'location_type') else 'kind'

        single_occupancy = False if opts.get('multi_occupancy') else True if opts.get('single_occupancy') else True
        max_weight = opts['max_weight']
        max_volume = opts['max_volume']
        code_fmt   = opts['code_format']

        created = 0
        skipped = 0

        # --- 1) Служебные зоны ------------------------------------------------
        def service_items(prefix, count, type_name):
            if count <= 0:
                return []
            tval = loc_type_value(LT, type_name)
            items = []
            for i in range(1, count + 1):
                code = f"{prefix}-{i:02}".upper()
                base = {
                    'code': code,
                    'zone': '',
                    'aisle': '',
                    'rack': '',
                    'shelf': '',
                    'bin': '',
                    'single_occupancy': single_occupancy,
                    'max_weight_kg': max_weight,
                    'max_volume_m3': max_volume,
                }
                base[loc_type_field] = tval
                items.append(base)
            return items

        payload = []
        payload += service_items(opts['receiving_prefix'], opts['receiving'], 'RECEIVING')
        payload += service_items(opts['outbound_prefix'],  opts['outbound'],  'OUTBOUND')
        payload += service_items(opts['staging_prefix'],   opts['staging'],   'STAGING')
        payload += service_items(opts['qc_prefix'],        opts['qc'],        'QC')
        payload += service_items(opts['pick_prefix'],      opts['pick'],      'PICK_FACE')

        # --- 2) Сетка стеллажей ----------------------------------------------
        t_rack = loc_type_value(LT, 'RACK')
        zones = [z.upper() for z in opts['zones']]
        aisles = int(opts['aisles'])
        racks = int(opts['racks'])
        shelves = int(opts['shelves'])
        bins = int(opts['bins'])

        for z in zones:
            for a in range(1, aisles + 1):
                for r in range(1, racks + 1):
                    for s in range(1, shelves + 1):
                        for b in range(1, bins + 1):
                            code = build_code(z, a, r, s, b, code_fmt)
                            base = {
                                'code': code,
                                'zone': z,
                                'aisle': f"A{a:02}",
                                'rack':  f"R{r:02}",
                                'shelf': f"S{s}",
                                'bin':   f"B{b:02}",
                                'single_occupancy': single_occupancy,
                                'max_weight_kg': max_weight,
                                'max_volume_m3': max_volume,
                            }
                            base[loc_type_field] = t_rack
                            payload.append(base)

        # --- 3) Вставка -------------------------------------------------------
        if opts['no_bulk']:
            # медленный, но 100% переносимый путь
            for row in payload:
                obj, was_created = StorageLocation.objects.get_or_create(code=row['code'], defaults=row)
                if was_created:
                    created += 1
                else:
                    skipped += 1
        else:
            # быстрый путь (PostgreSQL/SQLite поддерживают ignore_conflicts для UNIQUE(code))
            objs = [StorageLocation(**row) for row in payload]
            for batch in chunked(objs, size=opts['batch_size']):
                StorageLocation.objects.bulk_create(batch, ignore_conflicts=True)
                # посчитать созданные строки достоверно сложно без upsert; примерно:
                created += len(batch)
            # оценим количество пропущенных по уникальному коду
            existing = StorageLocation.objects.filter(code__in=[r['code'] for r in payload]).count()
            skipped = existing - created if existing > created else 0
            if skipped < 0:
                skipped = 0

        self.stdout.write(self.style.SUCCESS(
            f"Done. Created ~{created} locations, skipped {skipped} duplicates."
        ))
