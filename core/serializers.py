from rest_framework import serializers
from core.models import (
    Shift,
    Employee,
    Task,
    TaskAssignmentLog,
    Qualification,
    TaskPool,
    StorageLocation,
    LocationSlot,
    Cargo,
    CargoEvent,
)
from django.db import transaction
from datetime import datetime
from core.services import cargo as cargo_service

# === Работники и смены ===

class QualificationShortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Qualification
        fields = ["code", "name"]


class EmployeeShortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Employee
        fields = ["employee_code", "first_name", "last_name"]


class EmployeeSerializer(serializers.ModelSerializer):
    qualifications = QualificationShortSerializer(many=True, read_only=True)

    class Meta:
        model = Employee
        fields = ["id", "employee_code", "first_name", "last_name", "qualifications", "is_active"]


class ShiftSerializer(serializers.ModelSerializer):
    # через through=EmployeeShiftStats связь остаётся доступной; читаем только
    employees = EmployeeShortSerializer(many=True, read_only=True)

    class Meta:
        model = Shift
        fields = ["id", "name", "date", "start_time", "end_time", "is_active", "employees"]


class ShiftCreateSerializer(serializers.Serializer):
    name = serializers.CharField(required=False)
    date = serializers.DateField()
    start_time = serializers.DateTimeField()
    end_time = serializers.DateTimeField()
    employee_codes = serializers.ListField(child=serializers.CharField(), allow_empty=False)

    def validate_employee_codes(self, codes):
        employees = Employee.objects.filter(employee_code__in=codes, is_active=True)
        if employees.count() != len(set(codes)):
            raise serializers.ValidationError("Некоторые employee_code не найдены или неактивны.")
        return codes

    def validate(self, time):
        start_time = time.get('start_time')
        end_time = time.get('end_time')

        if end_time <= start_time:
            raise serializers.ValidationError({
                "end_time": "Время окончания должно быть позже времени начала."
            })
        return time


class ShiftEmployeeUpdateSerializer(serializers.Serializer):
    employee_code = serializers.CharField()


# === Задачи и лог назначения ===

class CargoShortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cargo
        fields = ["cargo_code", "name"]


class TaskReadSerializer(serializers.ModelSerializer):
    required_qualifications = QualificationShortSerializer(many=True, read_only=True)
    assigned_to = EmployeeShortSerializer(read_only=True)
    cargo = CargoShortSerializer(read_only=True)

    class Meta:
        model = Task
        fields = [
            "id",
            "name",
            "description",
            "status",
            "priority",
            "shift",
            "created_at",
            "required_qualifications",
            "assigned_to",
            "difficulty",
            "cargo",
            "task_pool",
            "source"
        ]


class TaskCreateSerializer(serializers.ModelSerializer):
    required_qualification_codes = serializers.ListField(
        child=serializers.CharField(), write_only=True, required=False
    )
    assigned_employee_code = serializers.CharField(required=False, write_only=True)
    cargo_code = serializers.CharField(required=False, allow_null=True, write_only=True)

    class Meta:
        model = Task
        fields = [
            "name",
            "description",
            "shift",
            "difficulty",
            "priority",
            "required_qualification_codes",
            "assigned_employee_code",
            "cargo_code",
            "task_type",
            "payload"
        ]

    def validate(self, data):
        shift = data.get("shift")
        if not shift or not getattr(shift, "is_active", False):
            raise serializers.ValidationError("Смена не найдена или неактивна.")

        task_type = data.get("task_type")
        payload = data.get("payload") or {}

        SLOT_IS_REQUIRED = {
            Task.TaskType.RECEIVE_TO_INBOUND,
            Task.TaskType.PUTAWAY_TO_RACK,
            Task.TaskType.MOVE_BETWEEN_SLOTS,
        }
        if task_type in SLOT_IS_REQUIRED and not payload.get("to_slot_code"):
            raise serializers.ValidationError({"payload": "Поле 'to_slot_code' обязательно для этого типа задачи."})

        return data

    def create(self, validated_data):
        qualification_codes = validated_data.pop("required_qualification_codes", [])
        employee_code = validated_data.pop("assigned_employee_code", None)
        cargo_code = validated_data.pop("cargo_code", None)

        if not validated_data.get("task_pool"):
            pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
            validated_data["task_pool"] = pool

        if cargo_code:
            try:
                cargo = Cargo.objects.get(cargo_code=cargo_code)
            except Cargo.DoesNotExist:
                raise serializers.ValidationError({"cargo_code": "Груз не найден"})
            validated_data["cargo"] = cargo

        task = Task.objects.create(**validated_data)

        if qualification_codes:
            qualifications = Qualification.objects.filter(code__in=qualification_codes)
            task.required_qualifications.set(qualifications)

        if employee_code:
            try:
                employee = Employee.objects.get(employee_code=employee_code)
            except Employee.DoesNotExist:
                raise serializers.ValidationError({"assigned_employee_code": "Сотрудник не найден"})
            task.assigned_to = employee
            task.save(update_fields=["assigned_to"])

        return task


class TaskAssignmentLogSerializer(serializers.ModelSerializer):
    employee = EmployeeShortSerializer()
    task = serializers.StringRelatedField()

    class Meta:
        model = TaskAssignmentLog
        fields = ["id", "task", "employee", "timestamp", "note"]


class TaskPoolSerializer(serializers.ModelSerializer):
    class Meta:
        model = TaskPool
        fields = ["id", "name"]


# === Склад: локации, слоты, грузы, события ===

class StorageLocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = StorageLocation
        fields = [
            "id",
            "code",
            "location_type",
            "zone",
            "aisle",
            "rack",
            "shelf",
            "bin",
            "slot_count",
            "slot_size_class",
        ]


class LocationSlotShortSerializer(serializers.ModelSerializer):
    location_code = serializers.CharField(source="location.code", read_only=True)

    class Meta:
        model = LocationSlot
        fields = ["code", "location_code", "index", "size_class"]


class CargoReadSerializer(serializers.ModelSerializer):
    current_slot = LocationSlotShortSerializer(read_only=True)

    class Meta:
        model = Cargo
        fields = [
            "id",
            "cargo_code",
            "sku",
            "name",
            "container_type",
            "units",
            "weight_kg",
            "volume_m3",
            "status",
            "current_slot",
            "created_at",
            "updated_at",
        ]


class CargoCreateSerializer(serializers.ModelSerializer):
    # можно сразу положить в слот по его коду
    current_slot_code = serializers.CharField(write_only=True, required=False, allow_blank=True)

    class Meta:
        model = Cargo
        fields = [
            "cargo_code",
            "sku",
            "name",
            "container_type",
            "units",
            "weight_kg",
            "volume_m3",
        ]

    @transaction.atomic
    def create(self, validated_data):
        cargo = Cargo.objects.create(
            cargo_code=validated_data["cargo_code"],
            sku=validated_data["sku"],
            name=validated_data["name"],
            container_type=validated_data["container_type"],
            units=validated_data["units"],
            status=Cargo.Status.CREATED,
            handling_state=Cargo.HandlingState.IDLE,
        )

        CargoEvent.objects.create(
            cargo=cargo,
            event_type="created",
            from_slot=None,
            to_slot=None,
            quantity=cargo.units or 0,
            note="Груз создан",
        )
        return cargo


class CargoArriveSerializer(serializers.Serializer):
    to_slot_code = serializers.CharField()
    employee_code = serializers.CharField(required=False, allow_blank=True)

    def save(self, *, cargo):
        return cargo_service.arrive(
            cargo.cargo_code,
            self.validated_data["to_slot_code"],
            self.validated_data.get("employee_code")
        )


class CargoStoreSerializer(serializers.Serializer):
    to_slot_code = serializers.CharField()
    employee_code = serializers.CharField(required=False, allow_blank=True)

    def save(self, *, cargo):
        return cargo_service.store(
            cargo.cargo_code,
            self.validated_data["to_slot_code"],
            self.validated_data.get("employee_code")
        )


class CargoMoveSerializer(serializers.Serializer):
    to_slot_code = serializers.CharField()
    employee_code = serializers.CharField(required=False, allow_blank=True)

    def save(self, *, cargo):
        return cargo_service.move(
            cargo.cargo_code,
            self.validated_data["to_slot_code"],
            self.validated_data.get("employee_code")
        )


class CargoDispatchSerializer(serializers.Serializer):
    employee_code = serializers.CharField(required=False, allow_blank=True)
    note = serializers.CharField(required=False, allow_blank=True)

    def save(self, *, cargo):
        return cargo_service.dispatch(
            cargo.cargo_code,
            self.validated_data.get("employee_code"),
            self.validated_data.get("note")
        )


class CargoEventSerializer(serializers.ModelSerializer):
    cargo_code = serializers.CharField(source="cargo.cargo_code", read_only=True)
    from_slot_code = serializers.CharField(source="from_slot.code", read_only=True)
    to_slot_code = serializers.CharField(source="to_slot.code", read_only=True)
    employee = EmployeeShortSerializer(read_only=True)

    class Meta:
        model = CargoEvent
        fields = [
            "id",
            "cargo_code",
            "event_type",
            "timestamp",
            "from_slot_code",
            "to_slot_code",
            "quantity",
            "employee",
            "note",
        ]
        read_only_fields = fields  # события создаём доменными методами/сервисами
