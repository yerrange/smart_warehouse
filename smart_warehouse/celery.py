import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "smart_warehouse.settings")

app = Celery("smart_warehouse")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks(related_name="celery_tasks")

# Опционально: общий retry по умолчанию
@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")