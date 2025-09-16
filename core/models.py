from django.db import models

from django.db import models, transaction
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _
from django.db.models import Q, F
from django.utils.timezone import now


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
    qualifications = models.ManyToManyField('Qualification', blank=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.last_name} {self.first_name} ({self.employee_code})"


# === Смены ===
class Shift(models.Model):
    name = models.CharField(max_length=120, blank=True)          # например: "Дневная смена 12.09"
    date = models.DateField(db_index=True)
    is_active = models.BooleanField(default=False, db_index=True)

    start_time = models.DateTimeField(null=True, blank=True)
    end_time = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    employees = models.ManyToManyField('Employee', through='EmployeeShiftStats',related_name='shifts')

    class Meta:
        indexes = [
            models.Index(fields=['date', 'is_active']),
            models.Index(fields=['-start_time']),
        ]
        constraints = [
            models.CheckConstraint(
                check=Q(end_time__isnull=True) | Q(start_time__isnull=False),
                name='shift_end_requires_start'
            ),
            models.CheckConstraint(
                check=Q(end_time__isnull=True) | Q(end_time__gte=F('start_time')),
                name='shift_end_after_start'
            ),
        ]
        ordering = ['-date', '-start_time', 'id']

    def __str__(self):
        label = self.name or f"Смена {self.date}"
        state = "активна" if self.is_active else "закрыта"
        return f"{label} ({state})"

    # Доменные действия с инвариантами
    def can_start(self) -> bool:
        return not self.is_active and self.start_time is None

    def can_close(self) -> bool:
        return self.is_active

    def start(self) -> None:
        if not self.can_start():
            raise ValueError("Нельзя стартовать эту смену (уже активна или была запущена).")
        self.is_active = True
        self.start_time = now()
        self.save(update_fields=['is_active', 'start_time', 'updated_at'])

    def close(self) -> None:
        if not self.can_close():
            raise ValueError("Нельзя закрыть неактивную смену.")
        self.is_active = False
        self.end_time = now()
        self.save(update_fields=['is_active', 'end_time', 'updated_at'])


# === Динамическая статистика по сменам ===
class EmployeeShiftStats(models.Model):
    employee = models.ForeignKey('Employee', on_delete=models.CASCADE, related_name='shift_stats', db_index=True)
    shift = models.ForeignKey('Shift', on_delete=models.CASCADE, related_name='employee_stats', db_index=True)
    
    is_busy = models.BooleanField(default=False)
    task_count = models.IntegerField(default=0)
    shift_score = models.IntegerField(default=0)
    last_task_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = (('employee', 'shift'),)
        indexes = [
            models.Index(fields=['shift', 'is_busy']),
            models.Index(fields=['employee', 'shift']),
            models.Index(fields=['-shift_score', 'task_count']),
        ]
        ordering = ['-shift_score', 'task_count', 'employee_id']

    def __str__(self):
        return f"{self.employee} @ {self.shift}: {'занят' if self.is_busy else 'свободен'}, " \
               f"задач={self.task_count}, очки={self.shift_score}"


# === Ячейки хранения ===
class StorageLocation(models.Model):
    class LocationType(models.TextChoices):
        RECEIVING = 'receiving', _('Receiving dock')     # зона приемки
        STAGING   = 'staging',   _('Staging area')       # буфер
        RACK      = 'rack',      _('Rack/bin')           # стеллаж/ячейка
        PICK_FACE = 'pick',      _('Pick face')          # отборочная зона
        OUTBOUND  = 'outbound',  _('Outbound dock')      # отгрузка
        QC        = 'qc',        _('Quality control')    # контроль качества

    code = models.CharField(max_length=64, unique=True)  # Человеческий/сканируемый код ячейки, например Z1-A02-R03-S1-B05
    location_type = models.CharField(max_length=16, choices=LocationType.choices, default=LocationType.RACK)

    # Примитивная адресация (полезна для генератора ячеек и фильтров)
    zone  = models.CharField(max_length=16, blank=True)
    aisle = models.CharField(max_length=16, blank=True)
    rack  = models.CharField(max_length=16, blank=True)
    shelf = models.CharField(max_length=16, blank=True)
    bin   = models.CharField(max_length=16, blank=True)

    # Простая «ёмкость» (не перегружаем формулами) — пока справочно
    max_weight_kg = models.FloatField(default=0)         # 0 = не ограничено
    max_volume_m3 = models.FloatField(default=0)

    single_occupancy = models.BooleanField(default=True, help_text="True = ячейка рассчитана на один груз")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['zone','aisle','rack','shelf','bin','code']
        indexes = [
            models.Index(fields=['kind']),
            models.Index(fields=['zone','aisle','rack']),
        ]

    def __str__(self):
        return self.code


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

    # location = models.OneToOneField(
    #     StorageLocation,
    #     null=True,
    #     blank=True,
    #     on_delete=models.SET_NULL,
    #     related_name='cargo'
    # )
    location = models.CharField(
        max_length=100,
        blank=True,
        help_text="Произвольное описание местоположения груза (например, зона A3, полка 5)"
    )

    def __str__(self):
        return f"{self.cargo_code} – {self.name}"


# === История работы с грузом ===
# class CargoEvent(models.Model):
#     cargo = models.ForeignKey(Cargo, on_delete=models.CASCADE, related_name='events')
#     event_type = models.CharField(
#         max_length=30,
#         choices=[
#             ('arrived', 'Поступление'),
#             ('stored', 'Размещение на хранение'),
#             ('moved', 'Перемещение внутри склада'),
#             ('processing', 'Обработка/упаковка'),
#             ('dispatched', 'Отгрузка'),
#             ('inspection', 'Контроль/проверка'),
#             ('manual_note', 'Ручная отметка'),
#         ]
#     )
#     timestamp = models.DateTimeField(auto_now_add=True)
#     location = models.CharField(max_length=100, blank=True)
#     triggered_by = models.ForeignKey(Employee, null=True, blank=True, on_delete=models.SET_NULL, related_name='cargo_events')
#     note = models.TextField(blank=True)

#     def __str__(self):
#         return f"{self.cargo} — {self.get_event_type_display()} @ {self.timestamp}"


# === Пул для задач ===
class TaskPool(models.Model):
    name = models.CharField(max_length=120)
    is_active = models.BooleanField(default=True, db_index=True)
    auto_assign_enabled = models.BooleanField(default=True)
    default_priority = models.PositiveSmallIntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-is_active', 'name', 'id']

    def __str__(self):
        return f"Пул {self.name} ({'вкл' if self.is_active else 'выкл'})"


# === Задачи ===
class Task(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", _("Pending")
        IN_PROGRESS = "in_progress", _("In progress")
        PAUSED = "paused", _("Paused")
        COMPLETED = "completed", _("Completed")
        CANCELLED = "cancelled", _("Cancelled")
        FAILED = "failed", _("Failed")

    name = models.CharField(max_length=120)
    description = models.TextField()

    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    priority = models.PositiveSmallIntegerField(default=0, help_text="0..n, выше — важнее")

    difficulty = models.PositiveSmallIntegerField(default=1)
    estimated_minutes = models.PositiveIntegerField(default=0)
    actual_minutes = models.PositiveIntegerField(default=0)

    due_at = models.DateTimeField(null=True, blank=True)
    assigned_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    shift = models.ForeignKey(
        'Shift',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="tasks",
        db_index=True
    )
    task_pool = models.ForeignKey(
        'TaskPool',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="tasks",
        db_index=True
    )

    assigned_to = models.ForeignKey('Employee', null=True, blank=True, on_delete=models.SET_NULL)
    required_qualifications = models.ManyToManyField('Qualification', blank=True)

    cargo = models.ForeignKey('Cargo', null=True, blank=True, on_delete=models.SET_NULL)
    
    external_ref = models.CharField(max_length=64, null=True, blank=True)
    source = models.CharField(max_length=16, choices=[('manual','manual'),('auto','auto')], default='auto')

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Задача {self.id} ({self.get_status_display()})"

    class Meta:
        indexes = [
            models.Index(fields=['shift', 'status']),
            models.Index(fields=['status', 'priority', 'due_at']),
            models.Index(fields=['assigned_to', 'status']),
        ]
        constraints = [
            models.CheckConstraint(check=Q(difficulty__gte=1) & Q(difficulty__lte=5), name='task_difficulty_1_5'),
            models.CheckConstraint(check=Q(due_at__isnull=True) | Q(due_at__gte=models.F('created_at')), name='task_due_after_created'),
            models.UniqueConstraint(fields=['assigned_to'], condition=Q(status__in=['in_progress','paused']),
                                    name='unique_active_task_per_employee'),
        ]
        ordering = ('-priority', 'due_at', 'id')


# === История назначения задач ===
class TaskAssignmentLog(models.Model):
    task = models.ForeignKey('Task', on_delete=models.CASCADE, related_name='assignment_history')
    employee = models.ForeignKey('Employee', on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    note = models.TextField(blank=True)

    def __str__(self):
        return f"{self.task} → {self.employee} @ {self.timestamp}"


