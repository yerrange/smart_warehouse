from channels.generic.websocket import AsyncWebsocketConsumer, JsonWebsocketConsumer
import json

class TaskNotificationConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("task_updates", self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("task_updates", self.channel_name)

    async def task_created(self, event):
        await self.send(text_data=json.dumps(event["message"]))

    async def task_assigned(self, event):
        print("📨 CONSUMER task_assigned triggered")
        await self.send(text_data=json.dumps(event["message"]))

    async def task_returned_to_pool(self, event):
        await self.send(text_data=json.dumps(event["message"]))

    async def task_started(self, event):
        await self.send(text_data=json.dumps(event["message"]))

    async def task_completed(self, event):
        await self.send(text_data=json.dumps(event["message"]))

    async def shift_closed(self, event):
        await self.send(text_data=json.dumps(event["message"]))