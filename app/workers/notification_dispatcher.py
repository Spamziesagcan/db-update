from __future__ import annotations

import asyncio
import logging
import time

from app.core.observability import metrics
from app.services.notification_delivery_service import NotificationDeliveryService

logger = logging.getLogger(__name__)


async def periodic_notification_dispatch(
    notification_delivery_service: NotificationDeliveryService,
    interval_seconds: float,
) -> None:
    while True:
        try:
            delivered = await notification_delivery_service.dispatch_pending_events()
            metrics.set_gauge("notification_dispatch_last_success_unix_seconds", time.time())
            if delivered == 0:
                await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
        except Exception:
            metrics.increment_counter("background_task_failures_total", task="notification_dispatch")
            logger.exception("Notification dispatch loop error")
            await asyncio.sleep(interval_seconds)
