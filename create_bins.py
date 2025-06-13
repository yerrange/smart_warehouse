import os
import sys
import django

# Настройка Django окружения
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'smart_warehouse.settings')
django.setup()

from core.models import StorageLocation
from django.db import IntegrityError

def create_bins(max_range=10):
    created = 0
    for zone in range(1, max_range + 1):
        for aisle in range(1, max_range + 1):
            for rack in range(1, max_range + 1):
                for shelf in range(1, max_range + 1):
                    for bin in range(1, max_range + 1):
                        try:
                            _, is_created = StorageLocation.objects.get_or_create(
                                zone=zone,
                                aisle=aisle,
                                rack=rack,
                                shelf=shelf,
                                bin=bin
                            )
                            if is_created:
                                created += 1
                        except IntegrityError:
                            continue
    print(f"✅ Добавлено ячеек: {created}")


if __name__ == "__main__":
    try:
        create_bins(max_range=3)  # можно изменить на 3 для теста
    except Exception as e:
        raise Exception(e)
    # StorageLocation.objects.all().delete()