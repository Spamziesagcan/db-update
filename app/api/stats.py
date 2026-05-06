from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.api.dependencies import (
    get_notification_delivery_service,
    get_order_service,
    require_authenticated_request,
)
from app.core.observability import metrics
from app.services.notification_delivery_service import NotificationDeliveryService
from app.services.order_service import OrderService

router = APIRouter(tags=["stats"], dependencies=[Depends(require_authenticated_request)])


@router.get("/api/stats", response_model=dict)
async def get_system_stats(
    request: Request,
    order_service: OrderService = Depends(get_order_service),
    notification_delivery_service: NotificationDeliveryService = Depends(get_notification_delivery_service),
):
    database_pool = request.app.state.database_pool
    if hasattr(database_pool, "record_pool_metrics"):
        database_pool.record_pool_metrics()

    stats = await order_service.get_system_stats()
    stats["notifications"] = await notification_delivery_service.get_delivery_stats()
    stats["observability"] = metrics.snapshot()
    return stats