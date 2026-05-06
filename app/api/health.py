from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from app.core.observability import metrics, trace_span

router = APIRouter(tags=["health"])


def _uptime_seconds(request: Request) -> float | None:
    started_at = getattr(request.app.state, "started_at", None)
    if started_at is None:
        return None

    if isinstance(started_at, datetime):
        return round((datetime.now(timezone.utc) - started_at).total_seconds(), 3)

    return None


def _background_task_summary(request: Request) -> dict[str, dict[str, object]]:
    tasks = {
        "cleanup_task": getattr(request.app.state, "cleanup_task", None),
        "notification_dispatch_task": getattr(request.app.state, "notification_dispatch_task", None),
    }

    summary: dict[str, dict[str, object]] = {}
    for task_name, task in tasks.items():
        summary[task_name] = {
            "running": bool(task) and not task.done(),
            "cancelled": bool(task) and task.cancelled(),
        }

    return summary


async def _ping_database(request: Request) -> dict[str, object]:
    database_pool = request.app.state.database_pool
    pool = database_pool.get_pool()

    async def _execute_ping() -> None:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
                await cursor.fetchone()

    await asyncio.wait_for(_execute_ping(), timeout=2.0)
    pool_stats = database_pool.record_pool_metrics()
    return {"status": "ok", "pool": pool_stats}


@router.get("/health/live")
async def live_health(request: Request) -> JSONResponse:
    with trace_span("health.live"):
        payload = {
            "status": "ok",
            "uptime_seconds": _uptime_seconds(request),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return JSONResponse(status_code=status.HTTP_200_OK, content=payload)


@router.get("/health/ready")
async def ready_health(request: Request) -> JSONResponse:
    issues: dict[str, str] = {}
    database_state: dict[str, object] | None = None

    with trace_span("health.ready"):
        try:
            database_state = await _ping_database(request)
        except Exception as exc:
            issues["database"] = f"{exc.__class__.__name__}: {exc}"

        background_tasks = _background_task_summary(request)
        for task_name, task_state in background_tasks.items():
            if not task_state["running"]:
                issues[task_name] = "background task is not running"

        connection_manager = request.app.state.connection_manager
        connection_stats = connection_manager.get_connection_stats()
        active_connections = connection_stats.get("total_connections", 0)
        metrics.set_gauge("websocket_active_connections", float(active_connections))

        payload = {
            "status": "ready" if not issues else "not_ready",
            "uptime_seconds": _uptime_seconds(request),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": database_state,
            "websockets": {
                "active_connections": active_connections,
            },
            "background_tasks": background_tasks,
            "issues": issues,
        }

        return JSONResponse(
            status_code=status.HTTP_200_OK if not issues else status.HTTP_503_SERVICE_UNAVAILABLE,
            content=payload,
        )