from django.urls import path, include
from rest_framework.routers import DefaultRouter
from core.views import ShiftViewSet

router = DefaultRouter()
router.register(r'shifts', ShiftViewSet, basename='shift')

urlpatterns = [
    path('', include(router.urls)),
]

