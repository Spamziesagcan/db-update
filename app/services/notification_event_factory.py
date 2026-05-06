from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping

from app.models import EventSourceMetadata, InternalBroadcastMessage

ACTION_MAP = {"c": "INSERT", "u": "UPDATE", "d": "DELETE"}


class NotificationEventParseError(ValueError):
    pass


def _coerce_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise NotificationEventParseError(f"{field_name} must be an integer")

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise NotificationEventParseError(f"{field_name} must be an integer") from exc


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None

    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError) as exc:
        raise NotificationEventParseError("ts_ms must be a valid epoch millisecond value") from exc


def _coerce_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise NotificationEventParseError(f"{field_name} must be a JSON object")

    return value


def _copy_mapping(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None

    return dict(value)


def build_event_id(
    *,
    source: Mapping[str, Any],
    topic: str,
    action: str,
    order_id: int,
    timestamp_ms: Any,
) -> str:
    canonical = {
        "connector": source.get("name"),
        "database": source.get("db"),
        "table": source.get("table"),
        "file": source.get("file"),
        "position": source.get("pos"),
        "row": source.get("row"),
        "snapshot": source.get("snapshot"),
        "topic": topic,
        "action": action,
        "order_id": order_id,
        "timestamp_ms": timestamp_ms,
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_dead_letter_id(
    *,
    event_id: str | None,
    stage: str,
    reason: str,
    raw_payload: str,
    topic: str | None,
    partition: int | None,
    offset: int | None,
) -> str:
    canonical = {
        "event_id": event_id,
        "stage": stage,
        "reason": reason,
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "payload_hash": hashlib.sha256(raw_payload.encode("utf-8")).hexdigest(),
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_notification_message(
    raw_event: Mapping[str, Any],
    *,
    topic: str,
    partition: int,
    offset: int,
    received_at: datetime,
) -> InternalBroadcastMessage:
    payload = _coerce_mapping(raw_event.get("payload"), "payload")

    action = ACTION_MAP.get(payload.get("op"))
    if action is None:
        raise NotificationEventParseError(f"Unsupported Debezium operation: {payload.get('op')!r}")

    before = payload.get("before")
    after = payload.get("after")
    order_source = after if isinstance(after, Mapping) else before
    if not isinstance(order_source, Mapping):
        raise NotificationEventParseError("Debezium event does not contain row data")

    order_id = _coerce_int(order_source.get("id"), "order_id")
    source = _coerce_mapping(payload.get("source"), "source")
    event_timestamp = _coerce_timestamp(payload.get("ts_ms")) or received_at

    return InternalBroadcastMessage(
        schema_version=1,
        event_id=build_event_id(
            source=source,
            topic=topic,
            action=action,
            order_id=order_id,
            timestamp_ms=payload.get("ts_ms"),
        ),
        event_type="order_change",
        action=action,
        order_id=order_id,
        timestamp=event_timestamp,
        source=EventSourceMetadata(
            connector=source.get("name"),
            topic=topic,
            partition=partition,
            offset=offset,
            database=source.get("db"),
            table=source.get("table"),
            binlog_file=source.get("file"),
            binlog_position=source.get("pos"),
            binlog_row=source.get("row"),
            snapshot=source.get("snapshot"),
            source_ts_ms=_coerce_timestamp(source.get("ts_ms")),
        ),
        old_data=_copy_mapping(before if isinstance(before, Mapping) else None) if action in {"UPDATE", "DELETE"} else None,
        new_data=_copy_mapping(after if isinstance(after, Mapping) else None) if action in {"INSERT", "UPDATE"} else None,
        metadata={
            "received_at": received_at.isoformat(),
            "kafka_topic": topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "source_name": source.get("name"),
            "source_snapshot": source.get("snapshot"),
        },
    )
