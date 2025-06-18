from django.urls import path, include
from rest_framework.routers import DefaultRouter
from core.views import ShiftViewSet, TaskViewSet, CargoViewSet, TaskPoolViewSet, EmployeeViewSet

router = DefaultRouter()
router.register(r'shifts', ShiftViewSet, basename='shift')
router.register(r'tasks', TaskViewSet, basename='task')
router.register(r'cargo', CargoViewSet, basename='cargo')
# router.register(r'storage-locations', StorageLocationViewSet, basename='storage-location')
router.register(r'task-pools', TaskPoolViewSet, basename='task-pool'),
router.register(r'employees', EmployeeViewSet, basename='employees')

urlpatterns = [
    path('', include(router.urls)),
]
