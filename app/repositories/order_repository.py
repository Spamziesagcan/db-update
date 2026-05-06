from __future__ import annotations

from typing import Any, Dict, List, Optional

import aiomysql

from app.db.connection import DatabasePool


class OrderRepository:
    def __init__(self, database_pool: DatabasePool):
        self.database_pool = database_pool

    async def create_order(self, customer_name: str, product_name: str, status: str) -> int:
        pool = self.database_pool.get_pool()
        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    INSERT INTO orders (customer_name, product_name, status)
                    VALUES (%s, %s, %s)
                    """,
                    (customer_name, product_name, status),
                )
                return cursor.lastrowid

    async def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        pool = self.database_pool.get_pool()
        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
                return await cursor.fetchone()

    async def update_order_status(self, order_id: int, status: str) -> int:
        pool = self.database_pool.get_pool()
        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    UPDATE orders
                    SET status = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (status, order_id),
                )
                return cursor.rowcount

    async def delete_order(self, order_id: int) -> int:
        pool = self.database_pool.get_pool()
        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("DELETE FROM orders WHERE id = %s", (order_id,))
                return cursor.rowcount

    async def list_orders(self, limit: int, offset: int) -> List[Dict[str, Any]]:
        pool = self.database_pool.get_pool()
        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    SELECT * FROM orders
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
                rows = await cursor.fetchall()
                return list(rows)

    async def count_orders_by_status(self) -> Dict[str, int]:
        pool = self.database_pool.get_pool()
        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM orders
                    GROUP BY status
                    """
                )
                rows = await cursor.fetchall()
                return {row["status"]: row["count"] for row in rows}