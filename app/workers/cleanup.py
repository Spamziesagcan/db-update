from __future__ import annotations

import asyncio
import logging
import time

from app.core.observability import metrics
from app.websockets.manager import ConnectionManager

logger = logging.getLogger(__name__)


async def periodic_cleanup(connection_manager: ConnectionManager, interval_seconds: int) -> None:
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            await connection_manager.cleanup_stale_connections()
        except asyncio.CancelledError:
            break
        except Exception:
            metrics.increment_counter("background_task_failures_total", task="cleanup")
            logger.exception("Periodic cleanup error")
        else:
            metrics.set_gauge("background_cleanup_last_success_unix_seconds", time.time())