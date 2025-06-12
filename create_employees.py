import sys
import os
import django

# Указываем Django-проект
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'smart_warehouse.settings')
django.setup()
from core.models import Employee

# Employee.objects.all().delete()

try:
    Employee.objects.get_or_create(first_name="Иван", last_name="Иванов", employee_code="E001")
    Employee.objects.get_or_create(first_name="Пётр", last_name="Петров", employee_code="E002")
    Employee.objects.get_or_create(first_name="Алексей", last_name="Сидоров", employee_code="E003")
    Employee.objects.get_or_create(first_name="Дмитрий", last_name="Орлов", employee_code="E004")
    Employee.objects.get_or_create(first_name="Сергей", last_name="Кузнецов", employee_code="E005")
    Employee.objects.get_or_create(first_name="Максим", last_name="Зайцев", employee_code="E006")
    Employee.objects.get_or_create(first_name="Егор", last_name="Новиков", employee_code="E007")
    Employee.objects.get_or_create(first_name="Никита", last_name="Морозов", employee_code="E008")
    Employee.objects.get_or_create(first_name="Михаил", last_name="Воробьёв", employee_code="E009")
    Employee.objects.get_or_create(first_name="Андрей", last_name="Смирнов", employee_code="E010")
except Exception as e:
    raise Exception(e)