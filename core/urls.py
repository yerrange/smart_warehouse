from django.urls import path, include
from rest_framework.routers import DefaultRouter
from core.views import ShiftViewSet, TaskViewSet, CargoViewSet, StorageLocationViewSet

router = DefaultRouter()
router.register(r'shifts', ShiftViewSet, basename='shift')
router.register(r'tasks', TaskViewSet, basename='task')
router.register(r'cargo', CargoViewSet, basename='cargo')
router.register(r'storage-locations', StorageLocationViewSet, basename='storage-location')

urlpatterns = [
    path('', include(router.urls)),
]
