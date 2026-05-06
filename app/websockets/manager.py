from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Set

from fastapi import WebSocket

from app.core.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ConnectionMetadata:
    connected_at: datetime
    last_ping: datetime
    message_count: int = 0


class ConnectionManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_metadata: Dict[str, ConnectionMetadata] = {}

    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None) -> str:
        await websocket.accept()

        if not client_id:
            client_id = str(uuid.uuid4())

        now = datetime.now(timezone.utc)
        self.active_connections[client_id] = websocket
        self.connection_metadata[client_id] = ConnectionMetadata(
            connected_at=now,
            last_ping=now,
        )

        logger.info(
            "Client %s connected. Total connections: %s",
            client_id,
            len(self.active_connections),
        )
        return client_id

    def disconnect(self, client_id: str) -> None:
        self.active_connections.pop(client_id, None)
        self.connection_metadata.pop(client_id, None)
        logger.info(
            "Client %s disconnected. Total connections: %s",
            client_id,
            len(self.active_connections),
        )

    def touch(self, client_id: str) -> None:
        metadata = self.connection_metadata.get(client_id)
        if metadata is not None:
            metadata.last_ping = datetime.now(timezone.utc)

    async def send_to_client(self, client_id: str, message: dict) -> bool:
        websocket = self.active_connections.get(client_id)
        if websocket is None:
            return False

        try:
            await websocket.send_text(json.dumps(message, default=str))
            self.connection_metadata[client_id].message_count += 1
            return True
        except Exception:
            logger.exception("Error sending message to client %s", client_id)
            self.disconnect(client_id)
            return False

    async def broadcast(self, message: dict, exclude_clients: Optional[Set[str]] = None) -> int:
        if not self.active_connections:
            return 0

        exclude_clients = exclude_clients or set()
        message_str = json.dumps(message, default=str)
        successful_sends = 0
        failed_clients = set()

        for client_id, websocket in list(self.active_connections.items()):
            if client_id in exclude_clients:
                continue

            try:
                await websocket.send_text(message_str)
                self.connection_metadata[client_id].message_count += 1
                successful_sends += 1
            except Exception:
                logger.exception("Error broadcasting to client %s", client_id)
                failed_clients.add(client_id)

        for client_id in failed_clients:
            self.disconnect(client_id)

        return successful_sends

    def get_connection_stats(self) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "total_connections": len(self.active_connections),
            "connections": {
                client_id: {
                    "connected_duration": (now - metadata.connected_at).total_seconds(),
                    "message_count": metadata.message_count,
                    "last_ping": metadata.last_ping.isoformat(),
                }
                for client_id, metadata in self.connection_metadata.items()
            },
        }

    async def cleanup_stale_connections(self) -> None:
        cutoff_time = datetime.now(timezone.utc).timestamp() - self.settings.connection_timeout
        stale_clients = []

        for client_id, metadata in list(self.connection_metadata.items()):
            if metadata.last_ping.timestamp() < cutoff_time:
                stale_clients.append(client_id)

        for client_id in stale_clients:
            logger.info("Removing stale connection: %s", client_id)
            self.disconnect(client_id)