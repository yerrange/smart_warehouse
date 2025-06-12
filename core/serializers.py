from rest_framework import serializers
from core.models import Shift, Employee


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