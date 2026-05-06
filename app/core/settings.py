from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv


def _parse_csv(value: str | None, default: Tuple[str, ...]) -> Tuple[str, ...]:
    if value is None:
        return default

    entries = tuple(part.strip() for part in value.split(",") if part.strip())
    return entries


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    environment: str
    db_host: str
    db_user: str
    db_password: str
    db_name: str
    db_port: int
    db_pool_min_size: int
    db_pool_max_size: int
    poll_interval: float
    max_batch_size: int
    heartbeat_interval: int
    connection_timeout: int
    cleanup_interval: int
    kafka_topic: str
    kafka_brokers: str
    notification_dispatch_interval: float
    notification_batch_size: int
    notification_retry_delay_seconds: int
    notification_processing_timeout_seconds: int
    notification_max_attempts: int
    user_api_keys: Tuple[str, ...]
    internal_api_keys: Tuple[str, ...]
    http_rate_limit_requests: int
    http_rate_limit_window_seconds: int
    websocket_connection_limit: int
    websocket_connection_window_seconds: int
    cors_allow_origins: Tuple[str, ...]
    require_https: bool
    log_file: str

    def validate(self) -> None:
        if not self.db_password:
            raise RuntimeError(
                "DB_PASSWORD is not set. Load it from the environment or a secret manager before starting the application."
            )

        if not self.user_api_keys:
            raise RuntimeError(
                "API_KEYS is not set. Provide at least one user-facing API key through the environment or a secret manager."
            )

        if not self.internal_api_keys:
            raise RuntimeError(
                "INTERNAL_API_KEYS is not set. Provide at least one internal service token through the environment or a secret manager."
            )

        if set(self.user_api_keys).intersection(self.internal_api_keys):
            raise RuntimeError("API_KEYS and INTERNAL_API_KEYS must not overlap.")

        if self.http_rate_limit_requests <= 0:
            raise RuntimeError("HTTP_RATE_LIMIT_REQUESTS must be greater than zero.")

        if self.http_rate_limit_window_seconds <= 0:
            raise RuntimeError("HTTP_RATE_LIMIT_WINDOW_SECONDS must be greater than zero.")

        if self.websocket_connection_limit <= 0:
            raise RuntimeError("WEBSOCKET_CONNECTION_LIMIT must be greater than zero.")

        if self.websocket_connection_window_seconds <= 0:
            raise RuntimeError("WEBSOCKET_CONNECTION_WINDOW_SECONDS must be greater than zero.")

        if not self.cors_allow_origins:
            raise RuntimeError("CORS_ALLOW_ORIGINS must contain at least one allowed origin.")

        invalid_origins = []
        insecure_origins = []
        local_origins = []

        for origin in self.cors_allow_origins:
            parsed_origin = urlparse(origin)
            hostname = parsed_origin.hostname or ""

            if parsed_origin.scheme not in {"http", "https"} or not parsed_origin.netloc:
                invalid_origins.append(origin)
                continue

            if self.environment in {"staging", "production"} and parsed_origin.scheme != "https":
                insecure_origins.append(origin)

            if self.environment in {"staging", "production"} and hostname in {
                "localhost",
                "127.0.0.1",
                "::1",
            }:
                local_origins.append(origin)

        if invalid_origins:
            raise RuntimeError(
                "CORS_ALLOW_ORIGINS must contain explicit http or https origins only."
            )

        if self.environment in {"staging", "production"} and not self.require_https:
            raise RuntimeError("REQUIRE_HTTPS must be true in staging and production.")

        if self.environment in {"staging", "production"} and insecure_origins:
            raise RuntimeError(
                "CORS_ALLOW_ORIGINS must use https origins in staging and production."
            )

        if self.environment in {"staging", "production"} and local_origins:
            raise RuntimeError(
                "CORS_ALLOW_ORIGINS cannot include localhost or loopback hosts in staging and production."
            )


def load_settings() -> Settings:
    load_dotenv()

    return Settings(
        environment=os.getenv("APP_ENV", "development").strip().lower(),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_user=os.getenv("DB_USER", "root"),
        db_password=os.getenv("DB_PASSWORD", ""),
        db_name=os.getenv("DB_NAME", "realtime_orders"),
        db_port=int(os.getenv("DB_PORT", "3306")),
        db_pool_min_size=int(os.getenv("DB_POOL_MIN_SIZE", "5")),
        db_pool_max_size=int(os.getenv("DB_POOL_MAX_SIZE", "20")),
        poll_interval=float(os.getenv("POLL_INTERVAL", "0.1")),
        max_batch_size=int(os.getenv("MAX_BATCH_SIZE", "100")),
        heartbeat_interval=int(os.getenv("HEARTBEAT_INTERVAL", "30")),
        connection_timeout=int(os.getenv("CONNECTION_TIMEOUT", "300")),
        cleanup_interval=int(os.getenv("CLEANUP_INTERVAL", "300")),
        kafka_topic=os.getenv("KAFKA_TOPIC", "dbserver.realtime_orders.orders"),
        kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        notification_dispatch_interval=float(os.getenv("NOTIFICATION_DISPATCH_INTERVAL", "1.0")),
        notification_batch_size=int(os.getenv("NOTIFICATION_BATCH_SIZE", "100")),
        notification_retry_delay_seconds=int(os.getenv("NOTIFICATION_RETRY_DELAY_SECONDS", "5")),
        notification_processing_timeout_seconds=int(
            os.getenv("NOTIFICATION_PROCESSING_TIMEOUT_SECONDS", "60")
        ),
        notification_max_attempts=int(os.getenv("NOTIFICATION_MAX_ATTEMPTS", "10")),
        user_api_keys=_parse_csv(os.getenv("API_KEYS"), tuple()),
        internal_api_keys=_parse_csv(os.getenv("INTERNAL_API_KEYS"), tuple()),
        http_rate_limit_requests=int(os.getenv("HTTP_RATE_LIMIT_REQUESTS", "120")),
        http_rate_limit_window_seconds=int(os.getenv("HTTP_RATE_LIMIT_WINDOW_SECONDS", "60")),
        websocket_connection_limit=int(os.getenv("WEBSOCKET_CONNECTION_LIMIT", "20")),
        websocket_connection_window_seconds=int(
            os.getenv("WEBSOCKET_CONNECTION_WINDOW_SECONDS", "60")
        ),
        cors_allow_origins=_parse_csv(os.getenv("CORS_ALLOW_ORIGINS"), tuple()),
        require_https=_parse_bool(os.getenv("REQUIRE_HTTPS"), False),
        log_file=os.getenv("LOG_FILE", "server.log"),
    )