try:
    x = 123 / 0
except Exception as e:
    raise Exception(e)


http://localhost:8000/api/docs/
daphne smart_warehouse.asgi:application
http://localhost:8000/tasks/live/
python manage.py runserver 0.0.0.0:8001
http://localhost:8000/api/docs/
python manage.py graph_models core --dot -o schema.dot
dot -Tpng schema.dot -o schema.png


Принять груз
{
  "name": "Принять груз",
  "description": "Принять груз",
  "cargo_code": "C-AN1SI3",
  "task_type": "RECEIVE_TO_INBOUND",
  "payload": { "to_slot_code": "IN-01-#1"}
}


Разместить груз в зоне длительного хранения
{
  "name": "Переместить груз в зону длительного хранения",
  "description": "Переместить груз в зону длительного хранения",
  "cargo_code": "C-AN1SI3",
  "task_type": "PUTAWAY_TO_RACK",
  "payload": { "to_slot_code": "Z1-A01-R01-S1-B01-#1"}
}


Переместить груз между ячейками
{
  "name": "Переместить груз в зону отгрузки",
  "description": "Переместить груз в зону отгрузки",
  "cargo_code": "C-AN1SI3",
  "task_type": "MOVE_BETWEEN_SLOTS",
  "payload": { "to_slot_code": "OUT-01-#1"}
}

Завершить отгрузку
{
  "name": "Завершить отгрузку",
  "description": "Завершить отгрузку",
  "cargo_code": "C-AN1SI3",
  "task_type": "DISPATCH_CARGO"
}


Коротко: в админке лучше создавать справочники и расписание, а всё, что меняет поток операций (перемещения/события/назначения задач), — через API/сервисы.

Что можно создавать «без боли»

SKU — каталог номенклатуры (code, name, UoM).

Qualification — разряды/допуски сотрудников.

Employee — карточки сотрудников (код, ФИО, привязка квалификаций).

TaskPool — пулы задач (настройки, дефолтный приоритет).

Shift — смены (даты/время). Запуск/закрытие смен — отдельными действиями, но саму запись создавать в админке ок.

StorageLocation — зоны/адреса (INBOUND/RACK/OUTBOUND), но слоты лучше генерить скриптом. В админке — править метаданные (zone/aisle/rack…), не трогая логику слотов.

Что лучше только через API/сервисы

Task — создание только через API (попадает в пул, дальше автоназначение).
В админке: запретить add, assigned_to — read-only.

Cargo — если создаёте пачками → скрипт/API. В админке можно разрешить только статус created без слота; все движения — не руками.

CargoEvent — не руками: их создают доменные операции arrive/store/move/dispatch, чтобы инварианты и история были корректны.

TaskAssignmentLog — только системой (сигнал/сервис при назначении).

LocationSlot — не руками: создаём/сверяем через функцию ensure_location_with_slots(...), чтобы не нарушить нумерацию и связи.

EmployeeShiftStats — это through-модель статистики, её создаёт система (при добавлении сотрудника в смену/старте задач). Редактировать вручную не нужно.