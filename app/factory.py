from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import internal_router, orders_router, stats_router, websocket_router
from app.core.logging_config import configure_logging
from app.core.security import SecurityService
from app.core.settings import Settings, load_settings
from app.db.connection import DatabasePool, ensure_notification_delivery_schema, ensure_orders_schema
from app.repositories.notification_event_repository import NotificationEventRepository
from app.repositories.order_repository import OrderRepository
from app.services.broadcast_service import BroadcastService
from app.services.notification_delivery_service import NotificationDeliveryService
from app.services.order_service import OrderNotFoundError, OrderService
from app.websockets.manager import ConnectionManager
from app.workers.cleanup import periodic_cleanup
from app.workers.notification_dispatcher import periodic_notification_dispatch


def create_app(settings: Settings | None = None) -> FastAPI:
    runtime_settings = settings or load_settings()
    configure_logging(runtime_settings.log_file)

    database_pool = DatabasePool(runtime_settings)
    connection_manager = ConnectionManager(runtime_settings)
    security_service = SecurityService(runtime_settings)
    order_repository = OrderRepository(database_pool)
    notification_event_repository = NotificationEventRepository(database_pool)
    order_service = OrderService(order_repository, connection_manager)
    broadcast_service = BroadcastService(connection_manager)
    notification_delivery_service = NotificationDeliveryService(
        notification_event_repository,
        connection_manager,
        runtime_settings,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        runtime_settings.validate()
        await database_pool.create_pool()
        await ensure_orders_schema(database_pool.get_pool())
        await ensure_notification_delivery_schema(database_pool.get_pool())
        cleanup_task = asyncio.create_task(
            periodic_cleanup(connection_manager, runtime_settings.cleanup_interval)
        )
        notification_dispatch_task = asyncio.create_task(
            periodic_notification_dispatch(
                notification_delivery_service,
                runtime_settings.notification_dispatch_interval,
            )
        )

        try:
            yield
        finally:
            cleanup_task.cancel()
            notification_dispatch_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
            try:
                await notification_dispatch_task
            except asyncio.CancelledError:
                pass
            await database_pool.close_pool()

    app = FastAPI(
        title="Real-time Order Notifications System",
        description="High-performance real-time order tracking with WebSocket notifications",
        version="2.0.0",
        lifespan=lifespan,
    )

    app.state.settings = runtime_settings
    app.state.database_pool = database_pool
    app.state.connection_manager = connection_manager
    app.state.security_service = security_service
    app.state.order_repository = order_repository
    app.state.notification_event_repository = notification_event_repository
    app.state.order_service = order_service
    app.state.broadcast_service = broadcast_service
    app.state.notification_delivery_service = notification_delivery_service

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(runtime_settings.cors_allow_origins),
        allow_credentials=False,
        allow_methods=["DELETE", "GET", "POST", "PUT"],
        allow_headers=["Content-Type", "X-API-Key", "X-Internal-Token"],
    )

    @app.exception_handler(OrderNotFoundError)
    async def order_not_found_handler(request: Request, exc: OrderNotFoundError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    app.include_router(orders_router)
    app.include_router(stats_router)
    app.include_router(internal_router)
    app.include_router(websocket_router)

    return app