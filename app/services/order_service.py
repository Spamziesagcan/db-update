from __future__ import annotations

from typing import Any, Dict, List

from app.models import OrderCreate, OrderUpdate
from app.repositories.order_repository import OrderRepository
from app.websockets.manager import ConnectionManager


class OrderNotFoundError(Exception):
    def __init__(self, order_id: int):
        super().__init__(f"Order {order_id} not found")
        self.order_id = order_id


class OrderService:
    def __init__(self, repository: OrderRepository, connection_manager: ConnectionManager):
        self.repository = repository
        self.connection_manager = connection_manager

    async def create_order(self, order: OrderCreate) -> Dict[str, Any]:
        order_id = await self.repository.create_order(
            order.customer_name,
            order.product_name,
            order.status,
        )
        return {
            "success": True,
            "message": "Order created successfully",
            "order_id": order_id,
        }

    async def update_order(self, order_id: int, order: OrderUpdate) -> Dict[str, Any]:
        existing_order = await self.repository.get_order(order_id)
        if existing_order is None:
            raise OrderNotFoundError(order_id)

        await self.repository.update_order_status(order_id, order.status)
        return {
            "success": True,
            "message": "Order updated successfully",
            "order_id": order_id,
        }

    async def delete_order(self, order_id: int) -> Dict[str, Any]:
        existing_order = await self.repository.get_order(order_id)
        if existing_order is None:
            raise OrderNotFoundError(order_id)

        await self.repository.delete_order(order_id)
        return {
            "success": True,
            "message": "Order deleted successfully",
            "order_id": order_id,
        }

    async def list_orders(self, limit: int, offset: int) -> List[Dict[str, Any]]:
        return await self.repository.list_orders(limit, offset)

    async def get_system_stats(self) -> Dict[str, Any]:
        return {
            "connections": self.connection_manager.get_connection_stats(),
            "orders": await self.repository.count_orders_by_status(),
        }