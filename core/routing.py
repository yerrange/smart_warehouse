from django.urls import re_path
from core.consumers import TaskNotificationConsumer

websocket_urlpatterns = [
    re_path(r"ws/tasks/$", TaskNotificationConsumer.as_asgi()),
]
