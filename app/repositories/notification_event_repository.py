from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiomysql

from app.core.settings import Settings
from app.db.connection import DatabasePool
from app.models import InternalBroadcastMessage
from app.services.notification_event_factory import build_dead_letter_id

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class QueuedNotificationEvent:
    event_id: str
    schema_version: int
    event_type: str
    action: str
    source_topic: str
    source_partition: int
    source_offset: int
    payload_json: str
    attempts: int
    last_error: str | None = None


class NotificationEventRepository:
    def __init__(self, database_pool: DatabasePool):
        self.database_pool = database_pool

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.utcnow()

    @staticmethod
    def _to_db_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _serialize_payload(message: InternalBroadcastMessage) -> str:
        return message.model_dump_json()

    async def enqueue_event(self, message: InternalBroadcastMessage) -> bool:
        pool = self.database_pool.get_pool()
        event_timestamp = self._to_db_datetime(message.timestamp)
        payload_json = self._serialize_payload(message)

        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT IGNORE INTO notification_events (
                        event_id,
                        schema_version,
                        event_type,
                        action,
                        order_id,
                        source_topic,
                        source_partition,
                        source_offset,
                        source_connector,
                        source_database,
                        source_table,
                        source_file,
                        source_pos,
                        source_row,
                        event_timestamp,
                        payload_json,
                        status,
                        attempts,
                        available_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, 'pending', 0, %s
                    )
                    """,
                    (
                        message.event_id,
                        message.schema_version,
                        message.event_type,
                        message.action,
                        message.order_id,
                        message.source.topic,
                        message.source.partition,
                        message.source.offset,
                        message.source.connector,
                        message.source.database,
                        message.source.table,
                        message.source.binlog_file,
                        message.source.binlog_position,
                        message.source.binlog_row,
                        event_timestamp,
                        payload_json,
                        self._utc_now(),
                    ),
                )
                inserted = cursor.rowcount == 1

        if inserted:
            logger.info("Queued notification event %s", message.event_id)
        else:
            logger.info("Duplicate notification event %s ignored", message.event_id)

        return inserted

    async def claim_due_events(
        self,
        batch_size: int,
        processing_timeout_seconds: int,
    ) -> list[QueuedNotificationEvent]:
        pool = self.database_pool.get_pool()
        now = self._utc_now()
        stale_cutoff = now - timedelta(seconds=processing_timeout_seconds)

        async with pool.acquire() as connection:
            await connection.begin()
            try:
                async with connection.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(
                        """
                        SELECT
                            event_id,
                            schema_version,
                            event_type,
                            action,
                            source_topic,
                            source_partition,
                            source_offset,
                            payload_json,
                            attempts,
                            last_error
                        FROM notification_events
                        WHERE (
                            status IN ('pending', 'failed')
                            AND available_at <= %s
                        ) OR (
                            status = 'processing'
                            AND locked_at IS NOT NULL
                            AND locked_at < %s
                        )
                        ORDER BY created_at ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (now, stale_cutoff, batch_size),
                    )
                    rows = await cursor.fetchall()

                    for row in rows:
                        await cursor.execute(
                            """
                            UPDATE notification_events
                            SET status = 'processing',
                                locked_at = %s,
                                updated_at = %s
                            WHERE event_id = %s
                            """,
                            (now, now, row["event_id"]),
                        )

                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

        return [
            QueuedNotificationEvent(
                event_id=row["event_id"],
                schema_version=int(row["schema_version"]),
                event_type=row["event_type"],
                action=row["action"],
                source_topic=row["source_topic"],
                source_partition=int(row["source_partition"]),
                source_offset=int(row["source_offset"]),
                payload_json=row["payload_json"],
                attempts=int(row["attempts"]) + 1,
                last_error=row.get("last_error"),
            )
            for row in rows
        ]

    async def mark_delivered(self, event_id: str) -> None:
        pool = self.database_pool.get_pool()
        now = self._utc_now()

        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE notification_events
                    SET status = 'delivered',
                        delivered_at = %s,
                        locked_at = NULL,
                        last_error = NULL,
                        updated_at = %s
                    WHERE event_id = %s
                    """,
                    (now, now, event_id),
                )

    async def mark_failed(
        self,
        event_id: str,
        error_message: str,
        retry_delay_seconds: int,
    ) -> None:
        pool = self.database_pool.get_pool()
        now = self._utc_now()
        available_at = now + timedelta(seconds=retry_delay_seconds)

        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE notification_events
                    SET status = 'failed',
                        last_error = %s,
                        available_at = %s,
                        locked_at = NULL,
                        updated_at = %s
                    WHERE event_id = %s
                    """,
                    (error_message, available_at, now, event_id),
                )

    async def mark_dead_letter(
        self,
        *,
        event_id: str,
        payload_json: str,
        stage: str,
        reason: str,
        error_message: str,
        schema_version: int | None = None,
        event_type: str | None = None,
        action: str | None = None,
        source_topic: str | None = None,
        source_partition: int | None = None,
        source_offset: int | None = None,
    ) -> None:
        pool = self.database_pool.get_pool()
        now = self._utc_now()
        dead_letter_id = build_dead_letter_id(
            event_id=event_id,
            stage=stage,
            reason=reason,
            raw_payload=payload_json,
            topic=source_topic,
            partition=source_partition,
            offset=source_offset,
        )

        async with pool.acquire() as connection:
            await connection.begin()
            try:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        """
                        UPDATE notification_events
                        SET status = 'dead_letter',
                            last_error = %s,
                            locked_at = NULL,
                            updated_at = %s
                        WHERE event_id = %s
                        """,
                        (error_message, now, event_id),
                    )
                    await self._insert_dead_letter(
                        cursor,
                        dead_letter_id=dead_letter_id,
                        event_id=event_id,
                        schema_version=schema_version,
                        event_type=event_type,
                        action=action,
                        stage=stage,
                        reason=reason,
                        error_message=error_message,
                        raw_payload=payload_json,
                        source_topic=source_topic,
                        source_partition=source_partition,
                        source_offset=source_offset,
                    )

                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def record_dead_letter(
        self,
        *,
        event_id: str | None,
        payload_json: str,
        stage: str,
        reason: str,
        error_message: str,
        schema_version: int | None = None,
        event_type: str | None = None,
        action: str | None = None,
        source_topic: str | None = None,
        source_partition: int | None = None,
        source_offset: int | None = None,
    ) -> None:
        pool = self.database_pool.get_pool()
        dead_letter_id = build_dead_letter_id(
            event_id=event_id,
            stage=stage,
            reason=reason,
            raw_payload=payload_json,
            topic=source_topic,
            partition=source_partition,
            offset=source_offset,
        )

        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await self._insert_dead_letter(
                    cursor,
                    dead_letter_id=dead_letter_id,
                    event_id=event_id,
                    schema_version=schema_version,
                    event_type=event_type,
                    action=action,
                    stage=stage,
                    reason=reason,
                    error_message=error_message,
                    raw_payload=payload_json,
                    source_topic=source_topic,
                    source_partition=source_partition,
                    source_offset=source_offset,
                )

    async def get_delivery_stats(self) -> dict[str, Any]:
        pool = self.database_pool.get_pool()

        async with pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM notification_events
                    GROUP BY status
                    """
                )
                status_rows = await cursor.fetchall()

                await cursor.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM notification_dead_letters
                    """
                )
                dead_letter_row = await cursor.fetchone()

        return {
            "status_counts": {row["status"]: int(row["count"]) for row in status_rows},
            "dead_letter_count": int(dead_letter_row["count"]) if dead_letter_row else 0,
        }

    async def _insert_dead_letter(
        self,
        cursor: aiomysql.Cursor,
        *,
        dead_letter_id: str,
        event_id: str | None,
        schema_version: int | None,
        event_type: str | None,
        action: str | None,
        stage: str,
        reason: str,
        error_message: str,
        raw_payload: str,
        source_topic: str | None,
        source_partition: int | None,
        source_offset: int | None,
    ) -> None:
        await cursor.execute(
            """
            INSERT IGNORE INTO notification_dead_letters (
                dead_letter_id,
                event_id,
                schema_version,
                event_type,
                action,
                stage,
                reason,
                error_message,
                raw_payload,
                source_topic,
                source_partition,
                source_offset
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            """,
            (
                dead_letter_id,
                event_id,
                schema_version,
                event_type,
                action,
                stage,
                reason,
                error_message,
                raw_payload,
                source_topic,
                source_partition,
                source_offset,
            ),
        )
