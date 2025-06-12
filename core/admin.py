from django.contrib import admin
from django import forms
from django.core.exceptions import ValidationError
import re

from core.models import (
    Employee,
    Qualification,
    Shift,
    EmployeeShiftStats,
    Task,
    TaskAssignmentLog,
    Cargo,
    CargoEvent,
    StorageLocation
)


# === Кастомная форма для Employee ===
class EmployeeForm(forms.ModelForm):
    class Meta:
        model = Employee
        fields = ['first_name', 'last_name', 'employee_code', 'qualifications', 'is_active']
        widgets = {
            'qualifications': forms.CheckboxSelectMultiple
        }

    def clean_employee_code(self):
        code = self.cleaned_data['employee_code']
        if not re.fullmatch(r"E\d{3}", code):
            raise ValidationError("Код сотрудника должен быть в формате E###, например E001.")
        return code


@admin.register(Employee)
class EmployeeAdmin(admin.ModelAdmin):
    form = EmployeeForm
    list_display = ("employee_code", "first_name", "last_name", "is_active")
    search_fields = ("employee_code", "first_name", "last_name")
    list_filter = ("is_active", "qualifications")
    filter_horizontal = ("qualifications",)


@admin.register(Qualification)
class QualificationAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")


@admin.register(Shift)
class ShiftAdmin(admin.ModelAdmin):
    list_display = ("date", "start_time", "end_time", "is_active")
    list_filter = ("is_active", "date")
    filter_horizontal = ("employees",)


@admin.register(EmployeeShiftStats)
class EmployeeShiftStatsAdmin(admin.ModelAdmin):
    list_display = ("employee", "shift", "task_count", "shift_score", "is_busy")
    list_filter = ("shift", "is_busy")
    search_fields = ("employee__first_name", "employee__last_name", "employee__employee_code")


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ("id", "description", "status", "shift", "assigned_to", "difficulty", "urgent")
    list_filter = ("status", "urgent", "shift")
    search_fields = ("description",)
    raw_id_fields = ("assigned_to", "cargo")
    autocomplete_fields = ("required_qualifications",)


@admin.register(TaskAssignmentLog)
class TaskAssignmentLogAdmin(admin.ModelAdmin):
    list_display = ("task", "employee", "timestamp")
    list_filter = ("timestamp",)
    search_fields = ("task__description", "employee__employee_code")


@admin.register(Cargo)
class CargoAdmin(admin.ModelAdmin):
    list_display = ("cargo_code", "name", "category", "current_status", "location")
    list_filter = ("category", "current_status", "is_dangerous", "requires_cold_storage", "fragile")
    search_fields = ("cargo_code", "name")
    autocomplete_fields = ("location",)


@admin.register(CargoEvent)
class CargoEventAdmin(admin.ModelAdmin):
    list_display = ("cargo", "event_type", "timestamp", "triggered_by")
    list_filter = ("event_type", "timestamp")
    search_fields = ("cargo__name", "cargo__cargo_code")


@admin.register(StorageLocation)
class StorageLocationAdmin(admin.ModelAdmin):
    list_display = ("zone", "aisle", "rack", "shelf", "bin")
    search_fields = ("zone", "aisle", "rack", "shelf", "bin")
    ordering = ("zone", "aisle", "rack", "shelf", "bin")
