from __future__ import annotations

import logging
from typing import Optional

import aiomysql
from aiomysql import Pool

from app.core.observability import metrics, trace_span
from app.core.settings import Settings

logger = logging.getLogger(__name__)


class DatabasePool:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: Optional[Pool] = None

    async def create_pool(self) -> Pool:
        with trace_span("db.pool.create", host=self.settings.db_host, database=self.settings.db_name):
            if self.pool is not None:
                return self.pool

            try:
                self.pool = await aiomysql.create_pool(
                    host=self.settings.db_host,
                    port=self.settings.db_port,
                    user=self.settings.db_user,
                    password=self.settings.db_password,
                    db=self.settings.db_name,
                    minsize=self.settings.db_pool_min_size,
                    maxsize=self.settings.db_pool_max_size,
                    autocommit=True,
                    charset="utf8mb4",
                )
                self.record_pool_metrics()
                logger.info(
                    "Database pool created: %s-%s connections",
                    self.settings.db_pool_min_size,
                    self.settings.db_pool_max_size,
                )
                return self.pool
            except Exception:
                metrics.increment_counter("db_connection_failures_total", stage="create_pool")
                logger.exception("Failed to create database pool")
                raise

    async def close_pool(self) -> None:
        if self.pool is None:
            self.record_pool_metrics()
            return

        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None
        self.record_pool_metrics()
        logger.info("Database pool closed")

    def get_pool(self) -> Pool:
        if self.pool is None:
            raise RuntimeError("Database pool not initialized")
        return self.pool

    def get_pool_stats(self) -> dict[str, int]:
        if self.pool is None:
            return {
                "size": 0,
                "free": 0,
                "used": 0,
                "min_size": self.settings.db_pool_min_size,
                "max_size": self.settings.db_pool_max_size,
            }

        size = int(getattr(self.pool, "size", 0) or 0)
        free = int(getattr(self.pool, "freesize", 0) or 0)
        used = max(size - free, 0)
        return {
            "size": size,
            "free": free,
            "used": used,
            "min_size": self.settings.db_pool_min_size,
            "max_size": self.settings.db_pool_max_size,
        }

    def record_pool_metrics(self) -> dict[str, int]:
        stats = self.get_pool_stats()
        metrics.set_gauge("db_pool_size", stats["size"])
        metrics.set_gauge("db_pool_free_connections", stats["free"])
        metrics.set_gauge("db_pool_used_connections", stats["used"])
        metrics.set_gauge("db_pool_min_size", stats["min_size"])
        metrics.set_gauge("db_pool_max_size", stats["max_size"])
        metrics.set_gauge("db_pool_connected", 1 if self.pool is not None else 0)
        return stats


async def ensure_orders_schema(pool: Pool) -> None:
    with trace_span("db.schema.ensure_orders"):
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orders (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        customer_name VARCHAR(255) NOT NULL,
                        product_name VARCHAR(255) NOT NULL,
                        status ENUM('pending', 'shipped', 'delivered') DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_status (status),
                        INDEX idx_updated_at (updated_at)
                    )
                    """
                )
                logger.info("Database connection successful and orders table verified")


async def ensure_notification_delivery_schema(pool: Pool) -> None:
    with trace_span("db.schema.ensure_notification_delivery"):
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS notification_events (
                        event_id CHAR(64) PRIMARY KEY,
                        schema_version INT NOT NULL,
                        event_type VARCHAR(64) NOT NULL,
                        action VARCHAR(16) NOT NULL,
                        order_id INT NOT NULL,
                        source_topic VARCHAR(255) NOT NULL,
                        source_partition INT NOT NULL,
                        source_offset BIGINT NOT NULL,
                        source_connector VARCHAR(255) DEFAULT NULL,
                        source_database VARCHAR(255) DEFAULT NULL,
                        source_table VARCHAR(255) DEFAULT NULL,
                        source_file VARCHAR(255) DEFAULT NULL,
                        source_pos BIGINT DEFAULT NULL,
                        source_row INT DEFAULT NULL,
                        event_timestamp DATETIME(6) NOT NULL,
                        payload_json LONGTEXT NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'pending',
                        attempts INT NOT NULL DEFAULT 0,
                        last_error TEXT DEFAULT NULL,
                        available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                        locked_at DATETIME(6) DEFAULT NULL,
                        delivered_at DATETIME(6) DEFAULT NULL,
                        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                        INDEX idx_notification_events_status_available (status, available_at, created_at),
                        INDEX idx_notification_events_processing (status, locked_at)
                    )
                    """
                )
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS notification_dead_letters (
                        dead_letter_id CHAR(64) PRIMARY KEY,
                        event_id CHAR(64) DEFAULT NULL,
                        schema_version INT DEFAULT NULL,
                        event_type VARCHAR(64) DEFAULT NULL,
                        action VARCHAR(16) DEFAULT NULL,
                        stage VARCHAR(32) NOT NULL,
                        reason VARCHAR(128) NOT NULL,
                        error_message TEXT NOT NULL,
                        raw_payload LONGTEXT NOT NULL,
                        source_topic VARCHAR(255) DEFAULT NULL,
                        source_partition INT DEFAULT NULL,
                        source_offset BIGINT DEFAULT NULL,
                        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                        INDEX idx_notification_dead_letters_event_id (event_id),
                        INDEX idx_notification_dead_letters_created_at (created_at)
                    )
                    """
                )
                logger.info("Notification delivery tables verified")