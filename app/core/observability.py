from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator


request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)
trace_id_context: ContextVar[str | None] = ContextVar("trace_id", default=None)
span_name_context: ContextVar[str | None] = ContextVar("span_name", default=None)

_STANDARD_LOG_RECORD_FIELDS = set(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
) | {"message", "request_id", "trace_id", "span_name", "environment", "service"}


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, set):
        return sorted(value)

    return str(value)


def generate_request_id() -> str:
    return uuid.uuid4().hex


def normalize_http_route(path: str, method: str | None = None) -> str:
    if path.startswith("/api/orders/") and path.count("/") == 3:
        return "/api/orders/{order_id}"

    return path


def _normalize_labels(labels: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((key, _label_value(value)) for key, value in labels.items()))


def _label_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"

    if value is None:
        return "null"

    return str(value)


@dataclass(slots=True)
class TimerStats:
    count: int = 0
    total: float = 0.0
    minimum: float | None = None
    maximum: float | None = None

    def observe(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

    def snapshot(self) -> dict[str, float | int | None]:
        average = self.total / self.count if self.count else None
        return {
            "count": self.count,
            "total": round(self.total, 6),
            "average": round(average, 6) if average is not None else None,
            "minimum": round(self.minimum, 6) if self.minimum is not None else None,
            "maximum": round(self.maximum, 6) if self.maximum is not None else None,
        }


class ObservabilityMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._timers: dict[tuple[str, tuple[tuple[str, str], ...]], TimerStats] = defaultdict(TimerStats)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()

    def increment_counter(self, name: str, amount: float = 1.0, **labels: Any) -> None:
        key = (name, _normalize_labels(labels))
        with self._lock:
            self._counters[key] += amount

    def set_gauge(self, name: str, value: float, **labels: Any) -> None:
        key = (name, _normalize_labels(labels))
        with self._lock:
            self._gauges[key] = value

    def observe_duration(self, name: str, value: float, **labels: Any) -> None:
        key = (name, _normalize_labels(labels))
        with self._lock:
            self._timers[key].observe(value)

    def snapshot(self) -> dict[str, list[dict[str, Any]]]:
        with self._lock:
            counters = [
                {
                    "name": name,
                    "labels": {label_key: label_value for label_key, label_value in labels},
                    "value": round(value, 6),
                }
                for (name, labels), value in sorted(self._counters.items())
            ]
            gauges = [
                {
                    "name": name,
                    "labels": {label_key: label_value for label_key, label_value in labels},
                    "value": round(value, 6),
                }
                for (name, labels), value in sorted(self._gauges.items())
            ]
            timers = [
                {
                    "name": name,
                    "labels": {label_key: label_value for label_key, label_value in labels},
                    **timer.snapshot(),
                }
                for (name, labels), timer in sorted(self._timers.items())
            ]

        return {"counters": counters, "gauges": gauges, "timers": timers}


metrics = ObservabilityMetrics()


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_context.get()
        record.trace_id = trace_id_context.get()
        record.span_name = span_name_context.get()
        record.environment = os.getenv("APP_ENV", "development")
        record.service = os.getenv("SERVICE_NAME", "updatis")
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", None),
            "trace_id": getattr(record, "trace_id", None),
            "span_name": getattr(record, "span_name", None),
            "environment": getattr(record, "environment", None),
            "service": getattr(record, "service", None),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        extras = self._extract_extra_fields(record)
        if extras:
            payload.update(extras)

        return json.dumps(payload, default=_json_default, ensure_ascii=True)

    def _extract_extra_fields(self, record: logging.LogRecord) -> dict[str, Any]:
        extras: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key in _STANDARD_LOG_RECORD_FIELDS:
                continue

            extras[key] = value

        return extras


@contextmanager
def trace_span(name: str, **fields: Any) -> Iterator[str]:
    created_trace_id = False
    trace_id = trace_id_context.get()
    trace_token = None
    if trace_id is None:
        trace_id = generate_request_id()
        trace_token = trace_id_context.set(trace_id)
        created_trace_id = True

    span_token = span_name_context.set(name)
    started_at = time.perf_counter()
    logger = logging.getLogger("app.observability")

    try:
        yield trace_id
    except Exception:
        duration_ms = round((time.perf_counter() - started_at) * 1000.0, 3)
        logger.exception(
            "span_failed",
            extra={"span": name, "duration_ms": duration_ms, **fields},
        )
        raise
    else:
        duration_ms = round((time.perf_counter() - started_at) * 1000.0, 3)
        logger.info(
            "span_completed",
            extra={"span": name, "duration_ms": duration_ms, **fields},
        )
    finally:
        span_name_context.reset(span_token)
        if created_trace_id and trace_token is not None:
            trace_id_context.reset(trace_token)
