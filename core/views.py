# core/views.py

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from core.services.shifts import assign_tasks_from_pool_to_shift

from core.models import Shift
from core.serializers import (
    ShiftSerializer,
    ShiftCreateSerializer,
    ShiftEmployeeUpdateSerializer,
    EmployeeSerializer
)
from core.services.shifts import (
    create_shift_with_employees,
    get_active_shift,
    close_shift,
    remove_employee_from_shift
)

# === view для смен ===
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

    @action(detail=True, methods=["post"], url_path="close")
    def close(self, request, pk=None):
        """Закрытие смены + возврат задач в пул"""
        try:
            shift = self.get_object()
        except Shift.DoesNotExist:
            raise NotFound("Смена не найдена.")

        if not shift.is_active:
            return Response({"detail": "Смена уже закрыта."}, status=400)

        shift.is_active = False
        shift.save()

        # Получаем/создаём пул задач
        task_pool, _ = TaskPool.objects.get_or_create(name="Общий пул")

        # Находим незавершённые задачи
        unfinished_tasks = shift.tasks.filter(status__in=["pending", "in_progress"])

        returned_count = 0
        for task in unfinished_tasks:
            previous_employee = task.assigned_to

            task.assigned_to = None
            task.status = "pending"
            task.shift = None
            task.task_pool = task_pool
            task.save()

            TaskAssignmentLog.objects.create(
                task=task,
                employee=previous_employee,
                note="Снята и возвращена в пул при завершении смены"
            )
            returned_count += 1


            from asgiref.sync import async_to_sync
            from channels.layers import get_channel_layer


            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                "task_updates",
                {
                    "type": "shift_closed",
                    "message": {"reason": "смена завершена"}
                }
            )

        from core.models import EmployeeShiftStats
        EmployeeShiftStats.objects.filter(shift=shift).update(is_busy=False)

        return Response(
            {"detail": f"Смена закрыта. В пул задач возвращено: {returned_count}."},
            status=200
        )

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
    
    @action(detail=True, methods=["post"], url_path="start")
    def start_shift(self, request, pk=None):
        """Активировать смену и назначить задачи из пула"""
        shift = self.get_object()

        if shift.is_active:
            return Response({"detail": "Смена уже активна."}, status=400)

        shift.is_active = True
        shift.save()

        # Назначаем задачи из пула при запуске смены
        assigned = assign_tasks_from_pool_to_shift(shift)

        return Response(
            {"detail": f"Смена запущена. Назначено из пула: {assigned} задач."},
            status=200
        )


from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError, NotFound

from core.models import Task, TaskAssignmentLog, Employee
from core.serializers import (
    TaskReadSerializer,
    TaskCreateSerializer,
    TaskAssignmentLogSerializer
)
from core.services.tasks import assign_task_to_best_employee, complete_task


# === view для задач ===
class TaskViewSet(viewsets.ModelViewSet):
    queryset = Task.objects.all().order_by('-created_at')
    serializer_class = TaskReadSerializer
    filterset_fields = ['status']

    def get_serializer_class(self):
        if self.action == 'create':
            return TaskCreateSerializer
        return TaskReadSerializer

    @action(detail=True, methods=["post"], url_path="assign")
    def assign_manually(self, request, pk=None):
        """Ручное назначение задачи сотруднику по коду"""
        task = self.get_object()
        employee_code = request.data.get("employee_code")
        if not employee_code:
            raise ValidationError({"employee_code": "Обязательное поле."})
        try:
            employee = Employee.objects.get(employee_code=employee_code)
        except Employee.DoesNotExist:
            raise NotFound("Сотрудник не найден")

        task.assigned_to = employee
        task.save()

        TaskAssignmentLog.objects.create(task=task, employee=employee, note="Ручное назначение через API")

        return Response({"detail": "Сотрудник назначен на задачу."}, status=200)

    @action(detail=True, methods=["post"], url_path="assign_auto")
    def assign_automatically(self, request, pk=None):
        """Назначить задачу автоматически через ИИ-эвристику"""
        task = self.get_object()
        employee = assign_task_to_best_employee(task, task.shift)

        if not employee:
            return Response(
                {"detail": "Нет подходящего сотрудника для назначения."},
                status=status.HTTP_400_BAD_REQUEST
            )

        return Response(
            {"detail": f"Задача назначена сотруднику {employee.employee_code}"},
            status=200
        )

    @action(detail=True, methods=["get"], url_path="history")
    def assignment_history(self, request, pk=None):
        """Получить историю назначений задачи"""
        task = self.get_object()
        logs = task.assignment_history.all().order_by('-timestamp')
        serializer = TaskAssignmentLogSerializer(logs, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=["post"], url_path="complete")
    def complete(self, request, pk=None):
        """Завершить задачу и освободить сотрудника"""
        task = self.get_object()

        if complete_task(task):
            return Response({"detail": "Задача завершена."})
        return Response({"detail": "Задачу нельзя завершить."}, status=400)
    

from core.models import TaskPool
from core.serializers import TaskPoolSerializer


class TaskPoolViewSet(viewsets.ModelViewSet):
    queryset = TaskPool.objects.all()
    serializer_class = TaskPoolSerializer


# === view для грузов и их перемещений ===
from core.models import Cargo, Employee
# CargoEvent, StorageLocation
from core.serializers import (
    CargoSerializer,
    CargoCreateSerializer,
)
    # CargoEventSerializer,
    # StorageLocationSerializer
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.exceptions import NotFound, ValidationError
from django.utils.timezone import now


class CargoViewSet(viewsets.ModelViewSet):
    queryset = Cargo.objects.all()
    serializer_class = CargoSerializer

    def get_serializer_class(self):
        if self.action == "create":
            return CargoCreateSerializer
        return CargoSerializer

    # @action(detail=True, methods=["post"], url_path="store")
    # def store(self, request, pk=None):
    #     """Разместить груз в свободной ячейке"""
    #     cargo = self.get_object()
    #     loc_id = request.data.get("location_id")

    #     if not loc_id:
    #         raise ValidationError({"location_id": "Обязательное поле."})

    #     try:
    #         location = StorageLocation.objects.get(id=loc_id)
    #     except StorageLocation.DoesNotExist:
    #         raise NotFound("Ячейка не найдена.")

    #     if location.is_occupied:
    #         return Response({"detail": "Эта ячейка уже занята."}, status=400)

    #     # Обновляем статус новой ячейки
    #     location.is_occupied = True
    #     location.save()

    #     # Освобождаем предыдущую ячейку, если есть
    #     if cargo.location and cargo.location != location:
    #         old_location = cargo.location
    #         old_location.is_occupied = False
    #         old_location.save()

    #     # Обновляем груз
    #     cargo.location = location
    #     cargo.current_status = "stored"
    #     cargo.save()

    #     CargoEvent.objects.create(
    #         cargo=cargo,
    #         event_type="stored",
    #         location=str(location),
    #         triggered_by=None,
    #         note="Груз размещён вручную"
    #     )

    #     return Response({"detail": "Груз успешно размещён."})

    # @action(detail=True, methods=["post"], url_path="move")
    # def move(self, request, pk=None):
    #     """Переместить груз в другую ячейку"""
    #     cargo = self.get_object()
    #     loc_id = request.data.get("location_id")

    #     if not loc_id:
    #         raise ValidationError({"location_id": "Обязательное поле."})

    #     try:
    #         new_location = StorageLocation.objects.get(id=loc_id)
    #     except StorageLocation.DoesNotExist:
    #         raise NotFound("Ячейка не найдена.")

    #     if new_location.is_occupied:
    #         return Response({"detail": "Ячейка уже занята другим грузом."}, status=400)

    #     # Обновляем статус новой ячейки
    #     new_location.is_occupied = True
    #     new_location.save()

    #     # Освобождаем старую ячейку
    #     if cargo.location and cargo.location != new_location:
    #         old_location = cargo.location
    #         old_location.is_occupied = False
    #         old_location.save()

    #     cargo.location = new_location
    #     cargo.current_status = "stored"
    #     cargo.save()

    #     CargoEvent.objects.create(
    #         cargo=cargo,
    #         event_type="moved",
    #         location=str(new_location),
    #         triggered_by=None,
    #         note="Груз перемещён вручную"
    #     )

    #     return Response({"detail": "Груз успешно перемещён."})

#     @action(detail=True, methods=["post"], url_path="remove_from_location")
#     def remove_from_location(self, request, pk=None):
#         """Снять груз с хранения и освободить ячейку"""
#         cargo = self.get_object()

#         if not cargo.location:
#             return Response({"detail": "Груз не размещён."}, status=400)

#         old_location = cargo.location
#         old_location.is_occupied = False
#         old_location.save()

#         cargo.location = None
#         cargo.current_status = "in_stock"
#         cargo.save()

#         CargoEvent.objects.create(
#             cargo=cargo,
#             event_type="removed",
#             location=str(old_location),
#             triggered_by=None,
#             note="Груз снят с хранения вручную"
#         )

#         return Response({"detail": "Груз снят с хранения."})

#     @action(detail=True, methods=["get"], url_path="events")
#     def events(self, request, pk=None):
#         """История всех операций с грузом"""
#         cargo = self.get_object()
#         events = cargo.events.all().order_by('-timestamp')
#         serializer = CargoEventSerializer(events, many=True)
#         return Response(serializer.data)


# class StorageLocationViewSet(viewsets.ModelViewSet):
#     queryset = StorageLocation.objects.all().order_by('zone', 'aisle', 'rack', 'shelf', 'bin')
#     serializer_class = StorageLocationSerializer

from django.shortcuts import render


class EmployeeViewSet(viewsets.ModelViewSet):
    queryset = Employee.objects.all()
    serializer_class = EmployeeSerializer



def live_tasks_view(request):
    return render(request, "core/live_tasks.html")