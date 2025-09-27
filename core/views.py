# === core/views.py (thin controllers, only orchestration & HTTP) ===

from django.shortcuts import render
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError, NotFound

from core.models import (
    Shift,
    TaskPool,
    TaskAssignmentLog,
    Employee,
    Task,
    EmployeeShiftStats,
    Cargo,
)
from core.serializers import (
    # shifts & employees
    ShiftSerializer,
    ShiftCreateSerializer,
    ShiftEmployeeUpdateSerializer,
    EmployeeSerializer,
    # tasks
    TaskReadSerializer,
    TaskCreateSerializer,
    TaskAssignmentLogSerializer,
    TaskPoolSerializer,
    # cargo
    CargoReadSerializer,
    CargoCreateSerializer,
)

# Import service layer (existing files: core/shifts.py, core/tasks.py)
from core.services.shifts import (
    create_shift_with_employees,
    get_active_shift,
    start_shift as service_start_shift,
    close_shift as service_close_shift,
    assign_tasks_from_pool_to_shift,
)
from core.services.tasks import (
    assign_task_to_best_employee,
    complete_task,
    assign_task_manually,
)


class ShiftViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Shift.objects.all()
    serializer_class = ShiftSerializer

    def get_serializer_class(self):
        # В swagger для этого action показываем входной сериализатор
        if getattr(self, "action", None) == "create_with_employees":
            return ShiftCreateSerializer
        return super().get_serializer_class()

    @action(detail=False, methods=['post'], url_path='create_with_employees')
    def create_with_employees(self, request):
        serializer = ShiftCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        shift = create_shift_with_employees(
            name=serializer.validated_data.get('name') if serializer.validated_data.get('name') else '',
            date=serializer.validated_data['date'],
            start_time=serializer.validated_data['start_time'],
            end_time=serializer.validated_data['end_time'],
            employee_codes=serializer.validated_data['employee_codes']
        )
        return Response(ShiftSerializer(shift).data, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=['get'], url_path='active')
    def active(self, request):
        shift = get_active_shift()
        if not shift:
            return Response({"detail": "Нет активной смены."}, status=404)
        return Response(ShiftSerializer(shift).data)

    @action(detail=True, methods=["post"], url_path="start")
    def start_shift(self, request, pk=None):
        shift: Shift = self.get_object()
        try:
            assigned = service_start_shift(shift)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)
        return Response({"detail": f"Смена запущена. Назначено из пула: {assigned} задач."}, status=200)

    @action(detail=True, methods=["post"], url_path="end")
    def end_shift(self, request, pk=None):
        shift: Shift = self.get_object()
        try:
            returned_count = service_close_shift(shift)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)
        return Response({"detail": f"Смена закрыта. В пул задач возвращено: {returned_count}."}, status=200)


class TaskViewSet(viewsets.ModelViewSet):
    queryset = Task.objects.all().order_by('-created_at')
    serializer_class = TaskReadSerializer
    filterset_fields = ['status']

    def get_serializer_class(self):
        if self.action == 'create':
            return TaskCreateSerializer
        return TaskReadSerializer

    @action(detail=True, methods=["post"], url_path="assign")
    def assign_manually_action(self, request, pk=None):
        task: Task = self.get_object()
        employee_code = request.data.get("employee_code")
        if not employee_code:
            raise ValidationError({"employee_code": "Обязательное поле."})
        try:
            assign_task_manually(task, employee_code)
        except NotFound as nf:
            # пробрасываем DRF-исключение как 404
            raise nf
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)
        return Response({"detail": "Сотрудник назначен на задачу."}, status=200)

    @action(detail=True, methods=["post"], url_path="assign_auto")
    def assign_automatically(self, request, pk=None):
        task = self.get_object()
        employee = assign_task_to_best_employee(task, task.shift)
        if not employee:
            return Response({"detail": "Нет подходящего сотрудника для назначения."}, status=400)
        return Response({"detail": f"Задача назначена сотруднику {employee.employee_code}"}, status=200)

    @action(detail=True, methods=["get"], url_path="history")
    def assignment_history(self, request, pk=None):
        task = self.get_object()
        logs = task.assignment_history.all().order_by('-timestamp')
        serializer = TaskAssignmentLogSerializer(logs, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=["post"], url_path="complete")
    def complete(self, request, pk=None):
        task = self.get_object()
        if complete_task(task):
            return Response({"detail": "Задача завершена."})
        return Response({"detail": "Задачу нельзя завершить."}, status=400)


class TaskPoolViewSet(viewsets.ModelViewSet):
    queryset = TaskPool.objects.all()
    serializer_class = TaskPoolSerializer


class CargoViewSet(viewsets.ModelViewSet):
    queryset = Cargo.objects.all()
    serializer_class = CargoReadSerializer

    def get_serializer_class(self):
        if self.action in ("create",):
            return CargoCreateSerializer
        return CargoReadSerializer


class EmployeeViewSet(viewsets.ModelViewSet):
    queryset = Employee.objects.all()
    serializer_class = EmployeeSerializer


def live_tasks_view(request):
    return render(request, "core/live_tasks.html")
