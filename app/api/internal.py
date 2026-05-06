from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.dependencies import get_broadcast_service, require_internal_request
from app.models import InternalBroadcastMessage
from app.services.broadcast_service import BroadcastService

router = APIRouter(tags=["internal"], dependencies=[Depends(require_internal_request)])


@router.post("/internal/broadcast")
async def internal_broadcast(
    message: InternalBroadcastMessage,
    broadcast_service: BroadcastService = Depends(get_broadcast_service),
):
    return await broadcast_service.broadcast(message)