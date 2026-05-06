from __future__ import annotations

import asyncio
import logging

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
            logger.exception("Periodic cleanup error")