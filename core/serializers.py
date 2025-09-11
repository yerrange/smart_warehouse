from core.models import (
    Shift,
    Employee,
    Task,
    TaskAssignmentLog,
    Qualification,
    Cargo,
    TaskPool
)
from rest_framework import serializers


# === Сериализаторы для блока "работники и смены" ===

class EmployeeShortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Employee
        fields = ['employee_code', 'first_name', 'last_name']


class ShiftSerializer(serializers.ModelSerializer):
    employees = EmployeeShortSerializer(many=True, read_only=True)

    class Meta:
        model = Shift
        fields = ['id', 'date', 'start_time', 'end_time', 'is_active', 'employees']


class ShiftCreateSerializer(serializers.Serializer):
    date = serializers.DateField()
    employee_codes = serializers.ListField(
        child=serializers.CharField(), allow_empty=False
    )

    def validate_employee_codes(self, codes):
        from core.models import Employee
        employees = Employee.objects.filter(employee_code__in=codes, is_active=True)
        if employees.count() != len(set(codes)):
            raise serializers.ValidationError("Некоторые employee_code не найдены или неактивны.")
        return codes


class ShiftEmployeeUpdateSerializer(serializers.Serializer):
    employee_code = serializers.CharField()


# === Сериализаторы для блока "Задачи и история их назначений" ===
class QualificationShortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Qualification
        fields = ['code', 'name']


class CargoShortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cargo
        fields = ['cargo_code', 'name']


class TaskReadSerializer(serializers.ModelSerializer):
    required_qualifications = QualificationShortSerializer(many=True, read_only=True)
    assigned_to = EmployeeShortSerializer(read_only=True)
    cargo = CargoShortSerializer(read_only=True)

    class Meta:
        model = Task
        fields = [
            'id',
            'name',
            'description',
            'status',
            'shift',
            'created_at',
            'required_qualifications',
            'assigned_to',
            'difficulty',
            'urgent',
            'cargo',
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
            'name', 'description', 'shift', 'difficulty', 'urgent',
            'required_qualification_codes', 'assigned_employee_code', 'cargo_code'
        ]

    def validate(self, data):
        # Проверка существования shift
        shift = data.get('shift')
        if not shift or not shift.is_active:
            raise serializers.ValidationError("Смена не найдена или неактивна.")

        return data

    def create(self, validated_data):
        from core.models import Qualification, Employee, Cargo

        qualification_codes = validated_data.pop('required_qualification_codes', [])
        employee_code = validated_data.pop('assigned_employee_code', None)
        cargo_code = validated_data.pop('cargo_code', None)
        pool, _ = TaskPool.objects.get_or_create(name="Общий пул")
        validated_data["task_pool"] = pool

        task = Task.objects.create(**validated_data)

        if qualification_codes:
            qualifications = Qualification.objects.filter(code__in=qualification_codes)
            task.required_qualifications.set(qualifications)

        if employee_code:
            try:
                employee = Employee.objects.get(employee_code=employee_code)
                task.assigned_to = employee
                task.save()
            except Employee.DoesNotExist:
                raise serializers.ValidationError({"assigned_employee_code": "Сотрудник не найден"})

        if cargo_code:
            try:
                cargo = Cargo.objects.get(cargo_code=cargo_code)
                task.cargo = cargo
                task.save()
            except Cargo.DoesNotExist:
                raise serializers.ValidationError({"cargo_code": "Груз не найден"})

        print(f"[POOL] Задача '{task.name}' добавлена в пул задач.")


        from channels.layers import get_channel_layer
        from asgiref.sync import async_to_sync
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            "task_updates",
            {
                "type": "task_created",
                "message": {
                    "id": task.id,
                    "name": task.name,
                    "status": task.status,
                    "difficulty": task.difficulty,
                }
            }
        )   

        return task


class TaskAssignmentLogSerializer(serializers.ModelSerializer):
    employee = EmployeeShortSerializer()
    task = serializers.StringRelatedField()

    class Meta:
        model = TaskAssignmentLog
        fields = ['id', 'task', 'employee', 'timestamp', 'note']


class TaskPoolSerializer(serializers.ModelSerializer):
    class Meta:
        model = TaskPool
        fields = ['id', 'name']


# === Сериализаторы для блока "Грузы и их перемещение" ===
# from core.models import Cargo, StorageLocation, CargoEvent


# class StorageLocationSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = StorageLocation
#         fields = ['id', 'zone', 'aisle', 'rack', 'shelf', 'bin']

#     def validate(self, data):
#         if StorageLocation.objects.filter(
#             zone=data['zone'],
#             aisle=data['aisle'],
#             rack=data['rack'],
#             shelf=data['shelf'],
#             bin=data['bin']
#         ).exists():
#             raise serializers.ValidationError(
#                 "Ячейка с таким расположением уже существует."
#             )
#         return data


class CargoSerializer(serializers.ModelSerializer):
    # location = StorageLocationSerializer(read_only=True)

    class Meta:
        model = Cargo
        fields = [
            'id', 'cargo_code', 'name', 'weight_kg', 'volume_m3', 'packages_count',
            'is_dangerous', 'requires_cold_storage', 'fragile', 'category',
            'origin', 'current_status', 'location'
        ]


class CargoCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cargo
        fields = [
            'cargo_code', 'name', 'weight_kg', 'volume_m3', 'packages_count',
            'is_dangerous', 'requires_cold_storage', 'fragile', 'category',
            'origin'
        ]


# class CargoEventSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = CargoEvent
#         fields = ['id', 'event_type', 'timestamp', 'location', 'note']


class EmployeeSerializer(serializers.ModelSerializer):
    qualifications = QualificationShortSerializer(many=True, read_only=True)
    class Meta:
        model = Employee
        fields = ['id', 'employee_code', 'first_name', 'last_name', 'qualifications', 'is_active']