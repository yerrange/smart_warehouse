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
    TaskPool,
    Cargo,
    CargoEvent,
    StorageLocation,
    LocationSlot,
)

# --- Вспомогательные inline'ы ---


class TaskInline(admin.TabularInline):
    model = Task
    extra = 0
    fields = ("name", "status", "assigned_to", "shift", )
    readonly_fields = ()
    show_change_link = True


class LocationSlotInline(admin.TabularInline):
    model = LocationSlot
    extra = 0
    fields = ("index", "code", "size_class")
    show_change_link = True


class CargoEventInline(admin.TabularInline):
    model = CargoEvent
    extra = 0
    fields = ("timestamp", "event_type", "from_slot", "to_slot", "quantity", "employee", "note")
    readonly_fields = ("timestamp",)
    show_change_link = False


class EmployeeShiftStatsInline(admin.TabularInline):
    model = EmployeeShiftStats
    extra = 0
    autocomplete_fields = ("employee",)
    fields = ("employee", "is_busy", "task_count", "shift_score", "last_task_at")
    readonly_fields = ("is_busy", "task_count", "shift_score", "last_task_at")


# === Employee ===

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


# === Qualification ===

@admin.register(Qualification)
class QualificationAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")


# === Shift & EmployeeShiftStats ===

@admin.register(Shift)
class ShiftAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "date",
        "start_time",
        "end_time",
        "actual_start_time",
        "actual_end_time",
        "is_active"
    )
    list_filter = ("is_active", "date")
    readonly_fields = ("actual_start_time", "actual_end_time")
    inlines = [EmployeeShiftStatsInline]


@admin.register(EmployeeShiftStats)
class EmployeeShiftStatsAdmin(admin.ModelAdmin):
    list_display = ("employee", "shift", "shift__date", "task_count", "shift_score", "is_busy")
    list_filter = ("shift", "is_busy")
    search_fields = ("shift__date", "employee__first_name", "employee__last_name", "employee__employee_code")


# === StorageLocation & LocationSlot ===

@admin.register(StorageLocation)
class StorageLocationAdmin(admin.ModelAdmin):
    list_display = ("code", "location_type", "zone", "aisle", "rack", "shelf", "bin", "slot_count", "slot_size_class")
    list_filter = ("location_type", "slot_size_class", "zone")
    search_fields = ("code", "zone", "aisle", "rack", "shelf", "bin")
    ordering = ("zone", "aisle", "rack", "shelf", "bin", "code")
    inlines = [LocationSlotInline]


@admin.register(LocationSlot)
class LocationSlotAdmin(admin.ModelAdmin):
    list_display = ("code", "location", "index", "size_class", "occupied", "cargo_display")
    list_filter = ("size_class", "location__location_type", "location__zone")
    search_fields = ("code", "location__code")
    ordering = ("location__id", "index")

    def occupied(self, obj):
        from core.models import Cargo
        return Cargo.objects.filter(current_slot=obj).exists()
    occupied.boolean = True

    def cargo_display(self, obj):
        try:
            return obj.cargo.cargo_code
        except Cargo.DoesNotExist:
            return "—"
    cargo_display.short_description = "Cargo"


# === Cargo & CargoEvent ===

@admin.register(Cargo)
class CargoAdmin(admin.ModelAdmin):
    list_display = ("cargo_code", "sku", "name", "container_type", "status", "slot_code", "location_code")
    list_filter = ("status", "container_type")
    search_fields = ("cargo_code", "sku", "name")
    raw_id_fields = ("current_slot",)
    inlines = [CargoEventInline]

    def slot_code(self, obj):
        return obj.current_slot.code if obj.current_slot_id else "—"
    slot_code.short_description = "Slot"

    def location_code(self, obj):
        return obj.current_slot.location.code if obj.current_slot_id else "—"
    location_code.short_description = "Location"


@admin.register(CargoEvent)
class CargoEventAdmin(admin.ModelAdmin):
    list_display = ("cargo", "event_type", "timestamp", "from_slot", "to_slot", "quantity", "employee")
    list_filter = ("event_type", "timestamp")
    search_fields = ("cargo__cargo_code", "cargo__name")


# === Task / TaskAssignmentLog / TaskPool ===

@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "status", "priority", "shift", "assigned_to", "difficulty")
    list_filter = ("status", "shift", "task_pool")
    search_fields = ("name", "description")
    raw_id_fields = ("assigned_to", "cargo")
    autocomplete_fields = ("required_qualifications",)

    def get_changeform_initial_data(self, request):
        initial = super().get_changeform_initial_data(request)
        try:
            pool = TaskPool.objects.get(name="Общий пул")
            initial["task_pool"] = pool.id
        except TaskPool.DoesNotExist:
            pass
        return initial


@admin.register(TaskAssignmentLog)
class TaskAssignmentLogAdmin(admin.ModelAdmin):
    list_display = ("task", "employee", "timestamp")
    list_filter = ("timestamp",)
    search_fields = ("task__name", "task__description", "employee__employee_code")


@admin.register(TaskPool)
class TaskPoolAdmin(admin.ModelAdmin):
    list_display = ("name", "is_active", "auto_assign_enabled", "default_priority")
    search_fields = ("name",)
    list_filter = ("is_active", "auto_assign_enabled")
    inlines = [TaskInline]
