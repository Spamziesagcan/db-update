from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import health_router, internal_router, orders_router, stats_router, websocket_router
from app.core.logging_config import configure_logging
from app.core.observability import generate_request_id, metrics, normalize_http_route, request_id_context, trace_id_context, trace_span
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
    metrics.reset()

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
        app.state.started_at = datetime.now(timezone.utc)
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
        app.state.cleanup_task = cleanup_task
        app.state.notification_dispatch_task = notification_dispatch_task

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
            app.state.cleanup_task = None
            app.state.notification_dispatch_task = None
            await database_pool.close_pool()

    app = FastAPI(
        title="Real-time Order Notifications System",
        description="High-performance real-time order tracking with WebSocket notifications",
        version="2.0.0",
        lifespan=lifespan,
    )

    app.state.started_at = None
    app.state.settings = runtime_settings
    app.state.database_pool = database_pool
    app.state.connection_manager = connection_manager
    app.state.security_service = security_service
    app.state.order_repository = order_repository
    app.state.notification_event_repository = notification_event_repository
    app.state.order_service = order_service
    app.state.broadcast_service = broadcast_service
    app.state.notification_delivery_service = notification_delivery_service
    app.state.cleanup_task = None
    app.state.notification_dispatch_task = None

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(runtime_settings.cors_allow_origins),
        allow_credentials=False,
        allow_methods=["DELETE", "GET", "POST", "PUT"],
        allow_headers=["Content-Type", "X-API-Key", "X-Internal-Token"],
    )

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or generate_request_id()
        request_token = request_id_context.set(request_id)
        trace_token = trace_id_context.set(request_id)
        request.state.request_id = request_id
        request.state.trace_id = request_id

        started_at = time.perf_counter()
        route_label = normalize_http_route(request.url.path, request.method)
        status_code = 500
        response = None

        try:
            with trace_span("http.request", method=request.method, route=route_label, request_id=request_id):
                response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_seconds = time.perf_counter() - started_at
            status_class = f"{status_code // 100}xx"
            metrics.increment_counter(
                "http_requests_total",
                method=request.method,
                route=route_label,
                status_class=status_class,
            )
            metrics.observe_duration(
                "http_request_duration_seconds",
                duration_seconds,
                method=request.method,
                route=route_label,
                status_class=status_class,
            )
            if status_code >= 400:
                metrics.increment_counter(
                    "http_errors_total",
                    method=request.method,
                    route=route_label,
                    status_class=status_class,
                )

            if response is not None:
                response.headers["X-Request-ID"] = request_id

            request_id_context.reset(request_token)
            trace_id_context.reset(trace_token)

    @app.exception_handler(OrderNotFoundError)
    async def order_not_found_handler(request: Request, exc: OrderNotFoundError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    app.include_router(orders_router)
    app.include_router(stats_router)
    app.include_router(health_router)
    app.include_router(internal_router)
    app.include_router(websocket_router)

    return app