from __future__ import annotations

from app.models import InternalBroadcastMessage
from app.websockets.manager import ConnectionManager


class BroadcastService:
    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager

    async def broadcast(self, message: InternalBroadcastMessage) -> dict:
        recipient_count = await self.connection_manager.broadcast(message.model_dump(mode="json"))
        return {"status": "message broadcasted", "recipients": recipient_count}