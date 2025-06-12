# core/views.py

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response

from core.models import Shift
from core.serializers import (
    ShiftSerializer,
    ShiftCreateSerializer,
    ShiftEmployeeUpdateSerializer
)
from core.services.shifts import (
    create_shift_with_employees,
    get_active_shift,
    close_shift,
    remove_employee_from_shift
)


class ShiftViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Shift.objects.all()
    serializer_class = ShiftSerializer

    @action(detail=False, methods=['post'], url_path='create_with_employees')
    def create_with_employees(self, request):
        serializer = ShiftCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        shift = create_shift_with_employees(
            shift_date=serializer.validated_data['date'],
            employee_codes=serializer.validated_data['employee_codes']
        )
        return Response(ShiftSerializer(shift).data, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=['get'], url_path='active')
    def active(self, request):
        shift = get_active_shift()
        if not shift:
            return Response({"detail": "Нет активной смены."}, status=404)
        return Response(ShiftSerializer(shift).data)

    @action(detail=True, methods=['post'], url_path='close')
    def close(self, request, pk=None):
        shift = self.get_object()
        close_shift(shift)
        return Response({"detail": "Смена закрыта."})

    @action(detail=True, methods=['post'], url_path='remove_employee')
    def remove_employee(self, request, pk=None):
        shift = self.get_object()
        serializer = ShiftEmployeeUpdateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        success = remove_employee_from_shift(
            shift,
            employee_code=serializer.validated_data['employee_code']
        )

        if success:
            return Response({"detail": "Сотрудник удалён из смены."})
        return Response({"detail": "Нельзя удалить сотрудника (смена уже началась или код не найден)."}, status=400)
