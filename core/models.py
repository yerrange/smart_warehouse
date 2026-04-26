from django.db import models, transaction
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _
from django.db.models import Q, F
from django.utils.timezone import now


# ===== Начало блока "Сотрудники" =====


# === Сотрудники ===
class Employee(models.Model):
    first_name = models.CharField(max_length=50)
    last_name = models.CharField(max_length=50)
    employee_code = models.CharField(max_length=20, unique=True)

    qualifications = models.ManyToManyField(
        'Qualification',
        through='EmployeeQualification',
        related_name='employees',
        blank=True
    )

    is_active = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['employee_code']),  # дублирует unique, но ок
            models.Index(fields=['last_name', 'first_name']),
        ]

    def __str__(self):
        return f"{self.last_name} {self.first_name} ({self.employee_code})"


# === Квалификации сотрудников ===
class Qualification(models.Model):
    code = models.CharField(max_length=20, unique=True)
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.code})"


# === Связь сотрудника с квалификацией ===
class EmployeeQualification(models.Model):
    employee = models.ForeignKey(
        'Employee',
        on_delete=models.CASCADE,
        related_name='employee_qualifications',
        db_index=True
    )
    qualification = models.ForeignKey(
        'Qualification',
        on_delete=models.CASCADE,
        related_name='employee_qualifications',
        db_index=True
    )

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        verbose_name = 'Квалификация сотрудника'
        verbose_name_plural = 'Квалификации сотрудников'
        constraints = [
            models.UniqueConstraint(
                fields=['employee', 'qualification'],
                name='uniq_employee_qualification'
            ),
        ]
        indexes = [
            models.Index(fields=['employee', 'qualification']),
            models.Index(fields=['qualification']),
        ]

    def __str__(self):
        return f"{self.employee.employee_code} | {self.qualification.code}"


# ===== Конец блока "Сотрудники" =====





# ===== Начало блока "Смены" =====


# === Смены ===
class Shift(models.Model):
    name = models.CharField(max_length=120, blank=True) # например: "Дневная смена 12.09"
    date = models.DateField(db_index=True)
    is_active = models.BooleanField(default=False, db_index=True)

    # Плановое время
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()

    # Фактическое время
    actual_start_time = models.DateTimeField(null=True, blank=True)
    actual_end_time = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    employees = models.ManyToManyField(
        'Employee',
        through='EmployeeShiftStats',
        related_name='shifts',
        blank=True
    )

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
        return not self.is_active and self.actual_start_time is None

    def can_close(self) -> bool:
        return self.is_active

    def start(self) -> None:
        if not self.can_start():
            raise ValueError("Нельзя стартовать эту смену (уже активна или была запущена).")
        self.is_active = True
        self.actual_start_time = now()
        self.save(update_fields=['is_active', 'actual_start_time', 'updated_at'])

    def close(self) -> None:
        if not self.can_close():
            raise ValueError("Нельзя закрыть неактивную смену.")
        self.is_active = False
        self.actual_end_time = now()
        self.save(update_fields=['is_active', 'actual_end_time', 'updated_at'])


# === Динамическая статистика по сменам ===
class EmployeeShiftStats(models.Model):
    employee = models.ForeignKey(
        'Employee',
        on_delete=models.CASCADE,
        related_name='shift_stats',
        db_index=True
    )
    shift = models.ForeignKey(
        'Shift',
        on_delete=models.CASCADE,
        related_name='employee_stats',
        db_index=True
    )

    is_busy = models.BooleanField(default=False)
    task_assigned_count = models.IntegerField(default=0)
    task_completed_count = models.IntegerField(default=0)
    shift_score = models.IntegerField(default=0)
    last_task_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = (('employee', 'shift'),)
        indexes = [
            models.Index(fields=['shift', 'is_busy']),
            models.Index(fields=['employee', 'shift']),
            models.Index(fields=['-shift_score', 'task_completed_count']),
        ]
        ordering = ['-shift_score', 'task_completed_count', 'employee_id']

    def __str__(self):
        return f"{self.employee} @ {self.shift}: {'занят' if self.is_busy else 'свободен'}, " \
               f"задач={self.task_completed_count}, очки={self.shift_score}"


# ===== Конец блока "Смены" =====





# ===== Начало блока "Задачи" =====


# === Задачи ===
class Task(models.Model):

    class TaskType(models.TextChoices):
        RECEIVE_TO_INBOUND = "RECEIVE_TO_INBOUND", _("Receive to inbound")   # → cargo.arrive
        PUTAWAY_TO_RACK = "PUTAWAY_TO_RACK", _("Putaway to rack")            # → cargo.store
        MOVE_BETWEEN_SLOTS = "MOVE_BETWEEN_SLOTS", _("Move between slots")   # → cargo.move
        DISPATCH_CARGO = "DISPATCH_CARGO", _("Dispatch cargo")               # → cargo.dispatch
        GENERAL = "GENERAL", _("GENERAL TASK")

    class Status(models.TextChoices):
        PENDING = "pending", _("Pending")
        IN_PROGRESS = "in_progress", _("In progress")
        PAUSED = "paused", _("Paused")
        COMPLETED = "completed", _("Completed")
        CANCELLED = "cancelled", _("Cancelled")
        FAILED = "failed", _("Failed")

    name = models.CharField(max_length=120)
    description = models.TextField()

    task_type = models.CharField(
        max_length=40,
        choices=TaskType.choices,
        db_index=True,
    )
    payload = models.JSONField(default=dict, blank=True)

    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING
    )
    priority = models.PositiveSmallIntegerField(
        default=0,
        help_text="0..n, выше — важнее"
    )

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

    assigned_to = models.ForeignKey(
        'Employee',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    required_qualifications = models.ManyToManyField(
        'Qualification',
        blank=True
    )

    cargo = models.ForeignKey(
        'Cargo',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    external_ref = models.CharField(max_length=64, null=True, blank=True)
    source = models.CharField(
        max_length=16,
        choices=[('manual', 'manual'), ('auto', 'auto')],
        default='auto'
    )

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
            models.CheckConstraint(
                check=Q(difficulty__gte=1) & Q(difficulty__lte=5),
                name='task_difficulty_1_5'
            ),
            models.CheckConstraint(
                check=Q(due_at__isnull=True) | Q(due_at__gte=models.F('created_at')),
                name='task_due_after_created'
            ),
            models.UniqueConstraint(
                fields=['assigned_to'],
                condition=Q(status__in=['in_progress', 'paused']),
                name='unique_active_task_per_employee'
            ),
        ]
        ordering = ('-priority', 'due_at', 'id')


# === История назначения задач ===
class TaskAssignmentLog(models.Model):
    task = models.ForeignKey(
        'Task',
        on_delete=models.CASCADE,
        related_name='assignment_history'
    )
    employee = models.ForeignKey('Employee', on_delete=models.CASCADE)
    timestamp = models.DateTimeField(db_index=True)
    note = models.TextField(blank=True)

    class Meta:
        ordering = ['-timestamp', 'id']
        indexes = [
            models.Index(fields=['task', 'timestamp']),
            models.Index(fields=['employee', 'timestamp']),
        ]

    def __str__(self):
        return f"{self.task} → {self.employee} @ {self.timestamp:%Y-%m-%d %H:%M}"


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


# =====  Конец блока "Задачи" =====





# ===== Начало блока "Профили сотрудников" =====


# === Базовый профиль сотрудника по типу задач ===
class EmployeeTaskProfile(models.Model):
    class SourceKind(models.TextChoices):
        SYNTHETIC = "synthetic", "Synthetic"
        REAL = "real", "Real"
        MIXED = "mixed", "Mixed"

    employee = models.ForeignKey(
        'Employee',
        on_delete=models.CASCADE,
        related_name='task_profiles',
        verbose_name='Сотрудник',
    )

    task_type = models.CharField(
        max_length=40,
        choices=Task.TaskType.choices,
        db_index=True,
        verbose_name='Тип задачи',
    )

    performance_factor = models.FloatField(
        default=1.0,
        validators=[MinValueValidator(0.1)],
        verbose_name='Коэффициент производительности',
        help_text='Меньше 1.0 – сотрудник обычно быстрее среднего по этому типу задач, больше 1.0 – медленнее.',
    )

    sigma = models.FloatField(
        default=0.10,
        validators=[MinValueValidator(0.0)],
        verbose_name='Разброс',
        help_text='Насколько стабильно сотрудник выполняет этот тип задач.',
    )

    sample_count = models.PositiveIntegerField(
        default=0,
        verbose_name='Число наблюдений',
        help_text='Сколько выполненных задач лежит в основе профиля.',
    )

    mean_minutes = models.FloatField(
        null=True,
        blank=True,
        verbose_name='Среднее фактическое время',
        help_text='Служебное поле для аналитики и пересчёта профиля.',
    )

    source_kind = models.CharField(
        max_length=16,
        choices=SourceKind.choices,
        default=SourceKind.SYNTHETIC,
        verbose_name='Источник профиля',
    )

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Профиль сотрудника по типу задач'
        verbose_name_plural = 'Профили сотрудников по типам задач'
        constraints = [
            models.UniqueConstraint(
                fields=['employee', 'task_type'],
                name='uniq_employee_task_profile',
            ),
        ]
        indexes = [
            models.Index(fields=['employee', 'task_type']),
            models.Index(fields=['task_type']),
            models.Index(fields=['source_kind']),
        ]

    def __str__(self):
        return f"{self.employee.employee_code} | {self.task_type}"


# === Поправка к профилю сотрудника по конкретной квалификации ===
class EmployeeTaskQualificationModifier(models.Model):
    class SourceKind(models.TextChoices):
        SYNTHETIC = "synthetic", "Synthetic"
        REAL = "real", "Real"
        MIXED = "mixed", "Mixed"

    profile = models.ForeignKey(
        'EmployeeTaskProfile',
        on_delete=models.CASCADE,
        related_name='qualification_modifiers',
        verbose_name='Базовый профиль',
    )

    employee_qualification = models.ForeignKey(
        'EmployeeQualification',
        on_delete=models.CASCADE,
        related_name='task_modifiers',
        verbose_name='Квалификация сотрудника',
    )

    factor = models.FloatField(
        default=1.0,
        validators=[MinValueValidator(0.1)],
        verbose_name='Поправочный коэффициент',
        help_text='Дополнительный множитель, если задача этого типа требует данную квалификацию.',
    )

    sigma_bonus = models.FloatField(
        default=0.0,
        validators=[MinValueValidator(0.0)],
        verbose_name='Добавка к разбросу',
        help_text='Насколько наличие этой квалификации влияет на вариативность времени.',
    )

    sample_count = models.PositiveIntegerField(
        default=0,
        verbose_name='Число наблюдений',
        help_text='Сколько наблюдений лежит в основе этой поправки.',
    )

    source_kind = models.CharField(
        max_length=16,
        choices=SourceKind.choices,
        default=SourceKind.SYNTHETIC,
        verbose_name='Источник поправки',
    )

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Поправка профиля по квалификации'
        verbose_name_plural = 'Поправки профилей по квалификациям'
        constraints = [
            models.UniqueConstraint(
                fields=['profile', 'employee_qualification'],
                name='uniq_profile_employee_qualification_modifier',
            ),
        ]
        indexes = [
            models.Index(fields=['employee_qualification']),
            models.Index(fields=['source_kind']),
        ]

    def clean(self):
        super().clean()
        if self.profile_id and self.employee_qualification_id:
            if self.profile.employee_id != self.employee_qualification.employee_id:
                raise ValidationError(
                    'Профиль и employee_qualification должны относиться к одному и тому же сотруднику.'
                )

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    @property
    def qualification(self):
        return self.employee_qualification.qualification

    def __str__(self):
        return f"{self.profile} + {self.employee_qualification.qualification.code}"


# ===== Конец блока "Профили сотрудников" =====





# ===== Начало блока "Грузы"


# === Грузы ===
class Cargo(models.Model):
    # ассортимент/идентификация
    sku = models.ForeignKey(
        "SKU",
        on_delete=models.PROTECT,
        related_name="cargos"
    )
    sku_name_snapshot = models.CharField(max_length=255, blank=True)
    cargo_code = models.CharField(max_length=50, unique=True)

    class Container(models.TextChoices):
        PALLET = 'pallet', 'Паллет'
        CRATE = 'crate', 'Ящик'
        DRUM = 'drum', 'Бочка'
        BOX = 'box', 'Коробка'
        TOTE = 'tote', 'Сумка'

    container_type = models.CharField(
        max_length=16,
        choices=Container.choices,
        default=Container.PALLET
    )

    units = models.PositiveIntegerField(default=1)
    weight_kg = models.FloatField(default=0)
    volume_m3 = models.FloatField(default=0)

    class Status(models.TextChoices):
        CREATED = 'created', 'Создан'
        ARRIVED = 'arrived', 'Поступил'
        STORED = 'stored', 'Размещён'
        DISPATCHED = 'dispatched', 'Отгружен'

    class HandlingState(models.TextChoices):
        IDLE = 'idle', 'Ожидает'
        PROCESSING = 'processing', 'Обрабатывается'

    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.CREATED,
        db_index=True
    )

    handling_state = models.CharField(
        max_length=16,
        choices=HandlingState.choices,
        default=HandlingState.IDLE,
        db_index=True,
    )

    current_slot = models.OneToOneField(
        'LocationSlot', null=True, blank=True, on_delete=models.SET_NULL,
        related_name='cargo', db_index=True
    )

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['status','current_slot']),
            models.Index(fields=['sku']),
        ]
        constraints = [
            # Отгруженный груз не должен занимать слот
            models.CheckConstraint(
                check=~Q(status='dispatched') | Q(current_slot__isnull=True),
                name='cargo_dispatched_without_slot'
            ),
            models.CheckConstraint(
                name="cargo_created_requires_no_slot",
                check=Q(status="created", current_slot__isnull=True) | ~Q(status="created"),
            ),
            models.CheckConstraint(
                name="cargo_dispatched_handling_is_idle",
                check=Q(status="dispatched", handling_state="idle") | ~Q(status="dispatched"),
            ),
        ]

    # Удобный shortcut: получить локацию через слот
    @property
    def current_location(self):
        return self.current_slot.location if self.current_slot_id else None

    @property
    def is_processing(self) -> bool:
        return self.handling_state != self.HandlingState.IDLE

    def __str__(self):
        where = self.current_slot.code if self.current_slot_id else '—'
        return f"{self.cargo_code}"


# === История работы с грузом ===
class CargoEvent(models.Model):
    class EventType(models.TextChoices):
        CREATED = 'created', 'Создание'
        ARRIVED = 'arrived', 'Поступление'
        STORED = 'stored', 'Размещение'
        MOVED = 'moved', 'Перемещение'
        PICKED = 'picked', 'Отбор'
        DISPATCHED = 'dispatched', 'Отгрузка'
        QC = 'qc', 'Контроль'
        NOTE = 'note', 'Заметка'

    cargo = models.ForeignKey(
        'Cargo',
        on_delete=models.CASCADE,
        related_name='events',
        db_index=True
    )
    event_type = models.CharField(max_length=16, choices=EventType.choices)
    timestamp = models.DateTimeField(db_index=True)

    from_slot = models.ForeignKey(
        'LocationSlot',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='events_from'
    )
    to_slot = models.ForeignKey(
        'LocationSlot',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='events_to'
    )

    employee = models.ForeignKey(
        'Employee',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    quantity = models.PositiveIntegerField(default=0)
    note = models.TextField(blank=True)

    class Meta:
        ordering = ['-timestamp', 'id']
        indexes = [
            models.Index(fields=['event_type', 'timestamp']),
            models.Index(fields=['cargo', 'timestamp']),
        ]

    def __str__(self):
        return f"[{self.timestamp:%Y-%m-%d %H:%M}] {self.cargo.cargo_code}: {self.get_event_type_display()}"


# === Ячейки хранения ===
class StorageLocation(models.Model):
    class LocationType(models.TextChoices):
        INBOUND = 'inbound', 'Зона приёмки'
        STAGING = 'staging', 'Зона временного хранения'
        RACK = 'rack', 'Зона длительного хранения'
        PICK_FACE = 'pick', 'Отборочная зона'
        OUTBOUND = 'outbound', 'Зона отгрузки'
        QC = 'qc', 'Зона контроля качества'

    code = models.CharField(max_length=64, unique=True)  # Человеческий/сканируемый код ячейки, например Z1-A02-R03-S1-B05
    location_type = models.CharField(
        max_length=16,
        choices=LocationType.choices,
        default=LocationType.RACK
    )

    zone = models.CharField(max_length=16, blank=True)
    aisle = models.CharField(max_length=16, blank=True)
    rack = models.CharField(max_length=16, blank=True)
    shelf = models.CharField(max_length=16, blank=True)
    bin = models.CharField(max_length=16, blank=True)

    class SlotSize(models.TextChoices):
        PALLET = 'pallet', _('Pallet-size')
        CRATE = 'crate', _('Crate-size')
        DRUM = 'drum', _('Drum-size')
        BOX = 'box', _('Box-size')
        TOTE = 'tote', _('Tote-size')

    slot_count = models.PositiveSmallIntegerField(default=1)
    slot_size_class = models.CharField(
        max_length=16,
        choices=SlotSize.choices,
        default=SlotSize.PALLET
    )

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['zone', 'aisle', 'rack', 'shelf', 'bin', 'code']
        indexes = [
            models.Index(fields=['location_type']),
            models.Index(fields=['zone', 'aisle', 'rack']),
        ]

    def __str__(self):
        return self.code


# === Слоты ячеек ===
class LocationSlot(models.Model):
    location = models.ForeignKey(
        'StorageLocation',
        on_delete=models.CASCADE,
        related_name='slots',
        db_index=True
    )
    index = models.PositiveSmallIntegerField(help_text="Порядковый номер слота внутри локации (1..slot_count)")
    code = models.CharField(max_length=80, unique=True)  # например: "{location.code}-#1"

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = (('location', 'index'),)
        ordering = ['location_id', 'index']
        indexes = [
            models.Index(fields=['location', 'index']),
        ]

    def __str__(self):
        return self.code


# === SKU (Stock Keeping Unit) - позиции товаров на складе ===
class SKU(models.Model):
    code = models.CharField(max_length=40, unique=True, db_index=True)
    name = models.CharField(max_length=255)
    unit_of_measurement = models.CharField(max_length=16, default="pcs")  # единица измерения
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):  # удобнее в админке
        return f"{self.code} — {self.name}"


# ===== Конец блока "Грузы" =====
