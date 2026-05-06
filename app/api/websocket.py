from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from app.api.dependencies import get_websocket_connection_manager, get_websocket_security_service
from app.core.observability import trace_span
from app.core.security import SecurityService
from app.websockets.manager import ConnectionManager

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    connection_manager: ConnectionManager = Depends(get_websocket_connection_manager),
    security_service: SecurityService = Depends(get_websocket_security_service),
):
    client_id = None
    with trace_span("websocket.session", path=websocket.url.path):
        try:
            websocket.state.security_principal = security_service.authenticate_websocket(websocket)
            client_id = await connection_manager.connect(websocket)

            await connection_manager.send_to_client(
                client_id,
                {
                    "event_type": "connection_established",
                    "client_id": client_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )

            while True:
                try:
                    data = await asyncio.wait_for(
                        websocket.receive_text(),
                        timeout=connection_manager.settings.heartbeat_interval * 2,
                    )

                    if data == "ping":
                        await websocket.send_text("pong")
                        connection_manager.touch(client_id)
                    elif data.startswith("subscribe:"):
                        pass
                except asyncio.TimeoutError:
                    break

        except WebSocketDisconnect:
            pass
        except Exception:
            logger.exception("WebSocket error for client %s", client_id)
        finally:
            if client_id:
                connection_manager.disconnect(client_id, reason="websocket_closed")