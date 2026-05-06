from __future__ import annotations

import logging

from pydantic import ValidationError

from app.core.settings import Settings
from app.models import InternalBroadcastMessage
from app.repositories.notification_event_repository import (
    NotificationEventRepository,
    QueuedNotificationEvent,
)
from app.websockets.manager import ConnectionManager

logger = logging.getLogger(__name__)


class NotificationDeliveryService:
    def __init__(
        self,
        repository: NotificationEventRepository,
        connection_manager: ConnectionManager,
        settings: Settings,
    ) -> None:
        self.repository = repository
        self.connection_manager = connection_manager
        self.settings = settings

    async def dispatch_pending_events(self) -> int:
        delivered_events = 0

        while True:
            queued_events = await self.repository.claim_due_events(
                self.settings.notification_batch_size,
                self.settings.notification_processing_timeout_seconds,
            )
            if not queued_events:
                break

            for queued_event in queued_events:
                if await self._deliver_event(queued_event):
                    delivered_events += 1

        return delivered_events

    async def get_delivery_stats(self) -> dict[str, object]:
        return await self.repository.get_delivery_stats()

    async def _deliver_event(self, queued_event: QueuedNotificationEvent) -> bool:
        try:
            message = InternalBroadcastMessage.model_validate_json(queued_event.payload_json)
        except ValidationError as exc:
            logger.warning(
                "Queued notification %s failed validation and was quarantined",
                queued_event.event_id,
            )
            await self.repository.mark_dead_letter(
                event_id=queued_event.event_id,
                payload_json=queued_event.payload_json,
                stage="queue_validation",
                reason="invalid_notification_payload",
                error_message=str(exc),
                schema_version=queued_event.schema_version,
                event_type=queued_event.event_type,
                action=queued_event.action,
                source_topic=queued_event.source_topic,
                source_partition=queued_event.source_partition,
                source_offset=queued_event.source_offset,
            )
            return False

        try:
            recipient_count = await self.connection_manager.broadcast(message.model_dump(mode="json"))
        except Exception as exc:
            error_message = f"{exc.__class__.__name__}: {exc}"
            if queued_event.attempts >= self.settings.notification_max_attempts:
                logger.error(
                    "Notification %s exceeded retry budget and was quarantined: %s",
                    queued_event.event_id,
                    error_message,
                )
                await self.repository.mark_dead_letter(
                    event_id=queued_event.event_id,
                    payload_json=queued_event.payload_json,
                    stage="delivery",
                    reason="delivery_failed",
                    error_message=error_message,
                    schema_version=queued_event.schema_version,
                    event_type=queued_event.event_type,
                    action=queued_event.action,
                    source_topic=queued_event.source_topic,
                    source_partition=queued_event.source_partition,
                    source_offset=queued_event.source_offset,
                )
            else:
                logger.warning(
                    "Notification %s delivery failed; retrying in %s seconds: %s",
                    queued_event.event_id,
                    self.settings.notification_retry_delay_seconds,
                    error_message,
                )
                await self.repository.mark_failed(
                    queued_event.event_id,
                    error_message,
                    self.settings.notification_retry_delay_seconds,
                )
            return False

        await self.repository.mark_delivered(queued_event.event_id)
        logger.info(
            "Delivered notification %s to %s websocket clients",
            queued_event.event_id,
            recipient_count,
        )
        return True
