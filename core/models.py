from django.db import models

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator


# === Квалификации сотрудников ===
class Qualification(models.Model):
    code = models.CharField(max_length=20, unique=True)
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)

    def __str__(self):
        return f"{self.name} ({self.code})"


# === Сотрудники ===
class Employee(models.Model):
    first_name = models.CharField(max_length=50)
    last_name = models.CharField(max_length=50)
    employee_code = models.CharField(max_length=20, unique=True)
    qualifications = models.ManyToManyField(Qualification, blank=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.last_name} {self.first_name} ({self.employee_code})"


# === Смены ===
class Shift(models.Model):
    date = models.DateField()
    start_time = models.TimeField(null=True, blank=True)
    end_time = models.TimeField(null=True, blank=True)
    employees = models.ManyToManyField(Employee, related_name='shifts')
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"Смена {self.date}"


# === Динамическая статистика по сменам ===
class EmployeeShiftStats(models.Model):
    employee = models.ForeignKey(Employee, on_delete=models.CASCADE, related_name='shift_stats')
    shift = models.ForeignKey(Shift, on_delete=models.CASCADE, related_name='employee_stats')
    task_count = models.IntegerField(default=0)
    shift_score = models.IntegerField(default=0)
    is_busy = models.BooleanField(default=False)

    class Meta:
        unique_together = ('employee', 'shift')

    def __str__(self):
        return f"{self.employee} @ {self.shift}"


# === Ячейки хранения ===
class StorageLocation(models.Model):
    zone = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(5)])
    aisle = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(5)])
    rack = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(5)])
    shelf = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(5)])
    bin = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(5)])
    is_occupied = models.BooleanField(default=False)

    class Meta:
        unique_together = ('zone', 'aisle', 'rack', 'shelf', 'bin')
        verbose_name = "Ячейка хранения"
        verbose_name_plural = "Ячейки хранения"

    def __str__(self):
        return f"З{self.zone}-П{self.aisle}-С{self.rack}-Пл{self.shelf}-Я{self.bin}"


# === Грузы ===
class Cargo(models.Model):
    name = models.CharField(max_length=200)
    cargo_code = models.CharField(max_length=50, unique=True)
    weight_kg = models.FloatField()
    volume_m3 = models.FloatField()
    packages_count = models.IntegerField(default=1)

    is_dangerous = models.BooleanField(default=False)
    requires_cold_storage = models.BooleanField(default=False)
    fragile = models.BooleanField(default=False)

    category = models.CharField(
        max_length=50,
        choices=[
            ('standard', 'Стандартный'),
            ('food', 'Пищевой'),
            ('electronic', 'Электроника'),
            ('chemical', 'Химия'),
            ('medical', 'Медицинский'),
            ('other', 'Другое'),
        ],
        default='standard'
    )

    origin = models.CharField(max_length=100, blank=True)

    current_status = models.CharField(
        max_length=30,
        choices=[
            ('created', 'Создан'),
            ('arrived', 'Поступил на склад'),
            ('stored', 'Размещён'),
            ('processing', 'Обрабатывается'),
            ('dispatched', 'Отгружен'),
        ],
        default='created'
    )

    location = models.OneToOneField(
        StorageLocation,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='cargo'
    )

    def __str__(self):
        return f"{self.cargo_code} – {self.name}"


# === История работы с грузом ===
class CargoEvent(models.Model):
    cargo = models.ForeignKey(Cargo, on_delete=models.CASCADE, related_name='events')
    event_type = models.CharField(
        max_length=30,
        choices=[
            ('arrived', 'Поступление'),
            ('stored', 'Размещение на хранение'),
            ('moved', 'Перемещение внутри склада'),
            ('processing', 'Обработка/упаковка'),
            ('dispatched', 'Отгрузка'),
            ('inspection', 'Контроль/проверка'),
            ('manual_note', 'Ручная отметка'),
        ]
    )
    timestamp = models.DateTimeField(auto_now_add=True)
    location = models.CharField(max_length=100, blank=True)
    triggered_by = models.ForeignKey(Employee, null=True, blank=True, on_delete=models.SET_NULL, related_name='cargo_events')
    note = models.TextField(blank=True)

    def __str__(self):
        return f"{self.cargo} — {self.get_event_type_display()} @ {self.timestamp}"


# === Задачи ===
class Task(models.Model):
    description = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    shift = models.ForeignKey(Shift, on_delete=models.CASCADE)
    cargo = models.ForeignKey(Cargo, null=True, blank=True, on_delete=models.SET_NULL)

    required_qualifications = models.ManyToManyField(Qualification, blank=True)
    assigned_to = models.ForeignKey(Employee, null=True, blank=True, on_delete=models.SET_NULL)

    status = models.CharField(
        max_length=20,
        choices=[
            ('pending', 'Ожидает назначения'),
            ('in_progress', 'Выполняется'),
            ('completed', 'Завершена'),
            ('cancelled', 'Отменена')
        ],
        default='pending'
    )

    difficulty = models.PositiveSmallIntegerField(default=1)
    urgent = models.BooleanField(default=False)

    def __str__(self):
        return f"Задача {self.id} ({self.get_status_display()})"


# === История назначения задач ===
class TaskAssignmentLog(models.Model):
    task = models.ForeignKey(Task, on_delete=models.CASCADE, related_name='assignment_history')
    employee = models.ForeignKey(Employee, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    note = models.TextField(blank=True)

    def __str__(self):
        return f"{self.task} → {self.employee} @ {self.timestamp}"

