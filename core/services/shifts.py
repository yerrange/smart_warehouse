# services/shifts.py
from datetime import date, datetime
from core.models import Shift, Employee, EmployeeShiftStats
from django.db import transaction
from django.utils.timezone import now

def create_shift(shift_date: date) -> Shift:
    shift, created = Shift.objects.get_or_create(date=shift_date)
    return shift


@transaction.atomic
def add_employees_to_shift(shift: Shift, employee_codes: list[str]) -> None:
    """
    Добавляет сотрудников в смену по employee_code и создаёт для них статистику.
    """
    employees = Employee.objects.filter(employee_code__in=employee_codes, is_active=True)
    shift.employees.add(*employees)

    for employee in employees:
        EmployeeShiftStats.objects.get_or_create(employee=employee, shift=shift)


def create_shift_with_employees(shift_date: date, employee_codes: list[str]) -> Shift:
    shift = create_shift(shift_date)
    add_employees_to_shift(shift, employee_codes)
    return shift


def get_active_shift(for_date: date = None) -> Shift | None:
    from datetime import date as dt
    target_date = for_date or dt.today()
    return Shift.objects.filter(date=target_date, is_active=True).first()


def close_shift(shift: Shift) -> None:
    shift.is_active = False
    shift.save()


def remove_employee_from_shift(shift: Shift, employee_code: str) -> bool:
    """
    Удаляет сотрудника из смены, если она ещё не началась.
    Возвращает True, если удаление прошло успешно.
    """
    if not shift.is_active:
        return False

    if shift.start_time and datetime.combine(shift.date, shift.start_time) <= now():
        return False  # смена уже началась

    try:
        employee = shift.employees.get(employee_code=employee_code)
    except Employee.DoesNotExist:
        return False

    # Удаляем сотрудника из смены
    shift.employees.remove(employee)

    # Удаляем его статистику, если есть
    EmployeeShiftStats.objects.filter(employee=employee, shift=shift).delete()

    return True