from __future__ import annotations

from fastapi import Depends, Request, Security, WebSocket
from fastapi.security import APIKeyHeader

from app.core.security import SecurityPrincipal, SecurityService
from app.services.broadcast_service import BroadcastService
from app.services.notification_delivery_service import NotificationDeliveryService
from app.services.order_service import OrderService
from app.websockets.manager import ConnectionManager

user_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
internal_api_key_header = APIKeyHeader(name="X-Internal-Token", auto_error=False)


def get_order_service(request: Request) -> OrderService:
    return request.app.state.order_service


def get_broadcast_service(request: Request) -> BroadcastService:
    return request.app.state.broadcast_service


def get_notification_delivery_service(request: Request) -> NotificationDeliveryService:
    return request.app.state.notification_delivery_service


def get_connection_manager(request: Request) -> ConnectionManager:
    return request.app.state.connection_manager


def get_security_service(request: Request) -> SecurityService:
    return request.app.state.security_service


def get_websocket_connection_manager(websocket: WebSocket) -> ConnectionManager:
    return websocket.app.state.connection_manager


def get_websocket_security_service(websocket: WebSocket) -> SecurityService:
    return websocket.app.state.security_service


def require_authenticated_request(
    request: Request,
    api_key: str | None = Security(user_api_key_header),
    security_service: SecurityService = Depends(get_security_service),
) -> SecurityPrincipal:
    return security_service.authenticate_user_request(request, api_key)


def require_internal_request(
    request: Request,
    internal_api_key: str | None = Security(internal_api_key_header),
    security_service: SecurityService = Depends(get_security_service),
) -> SecurityPrincipal:
    return security_service.authenticate_internal_request(request, internal_api_key)