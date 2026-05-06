from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends

from app.api.dependencies import get_order_service, require_authenticated_request
from app.models import OrderCreate, OrderResponse, OrderUpdate
from app.services.order_service import OrderService

router = APIRouter(tags=["orders"], dependencies=[Depends(require_authenticated_request)])


@router.post("/api/orders", response_model=dict)
async def create_order(order: OrderCreate, order_service: OrderService = Depends(get_order_service)):
    return await order_service.create_order(order)


@router.put("/api/orders/{order_id}", response_model=dict)
async def update_order(
    order_id: int,
    order: OrderUpdate,
    order_service: OrderService = Depends(get_order_service),
):
    return await order_service.update_order(order_id, order)


@router.delete("/api/orders/{order_id}", response_model=dict)
async def delete_order(order_id: int, order_service: OrderService = Depends(get_order_service)):
    return await order_service.delete_order(order_id)


@router.get("/api/orders", response_model=List[OrderResponse])
async def get_orders(
    limit: int = 50,
    offset: int = 0,
    order_service: OrderService = Depends(get_order_service),
):
    return await order_service.list_orders(limit, offset)