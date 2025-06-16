try:
    x = 123 / 0
except Exception as e:
    raise Exception(e)


http://localhost:8000/api/docs/
daphne smart_warehouse.asgi:application
http://localhost:8000/tasks/live/
python manage.py runserver 0.0.0.0:8001
http://localhost:8000/api/docs/