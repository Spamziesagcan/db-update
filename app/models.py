from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

OrderStatus = Literal["pending", "shipped", "delivered"]


class OrderCreate(BaseModel):
    customer_name: str = Field(..., min_length=1, max_length=255)
    product_name: str = Field(..., min_length=1, max_length=255)
    status: OrderStatus = "pending"


class OrderUpdate(BaseModel):
    status: OrderStatus


class OrderResponse(BaseModel):
    id: int
    customer_name: str
    product_name: str
    status: str
    created_at: datetime
    updated_at: datetime


class EventSourceMetadata(BaseModel):
    connector: Optional[str] = None
    topic: str
    partition: int
    offset: int
    database: Optional[str] = None
    table: Optional[str] = None
    binlog_file: Optional[str] = None
    binlog_position: Optional[int] = None
    binlog_row: Optional[int] = None
    snapshot: Optional[str | bool] = None
    source_ts_ms: Optional[datetime] = None

    model_config = ConfigDict(extra="forbid")


class InternalBroadcastMessage(BaseModel):
    schema_version: int = Field(default=1, ge=1)
    event_id: str = Field(..., min_length=16, max_length=128)
    event_type: Literal["order_change"]
    action: Literal["INSERT", "UPDATE", "DELETE"]
    order_id: int
    timestamp: datetime
    source: EventSourceMetadata
    old_data: Optional[Dict[str, Any]] = None
    new_data: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")