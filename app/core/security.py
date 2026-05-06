from __future__ import annotations

import hashlib
import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Deque, Mapping

from fastapi import HTTPException, Request, WebSocket, WebSocketException, status

from app.core.settings import Settings


@dataclass(frozen=True, slots=True)
class SecurityPrincipal:
    kind: str
    credential_fingerprint: str
    transport: str


class FixedWindowRateLimiter:
    def __init__(self) -> None:
        self._buckets: dict[str, Deque[float]] = {}
        self._lock = Lock()

    def allow(self, key: str, limit: int, window_seconds: int) -> bool:
        now = time.monotonic()
        cutoff = now - window_seconds

        with self._lock:
            bucket = self._buckets.setdefault(key, deque())
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= limit:
                return False

            bucket.append(now)
            return True


def _fingerprint_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


class SecurityService:
    def __init__(self, settings: Settings, rate_limiter: FixedWindowRateLimiter | None = None) -> None:
        self.settings = settings
        self.rate_limiter = rate_limiter or FixedWindowRateLimiter()

    def _client_identifier(self, client_host: str | None) -> str:
        return client_host or "unknown"

    def _request_scheme(self, headers: Mapping[str, str], scheme: str) -> str:
        forwarded_proto = headers.get("x-forwarded-proto")
        if forwarded_proto:
            return forwarded_proto.split(",")[0].strip().lower()

        return scheme.lower()

    def _enforce_https(self, headers: Mapping[str, str], scheme: str, context: str) -> None:
        if not self.settings.require_https:
            return

        if self._request_scheme(headers, scheme) not in {"https", "wss"}:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"{context} requires HTTPS or WSS",
            )

    def _enforce_rate_limit(
        self,
        *,
        scope: str,
        client_identifier: str,
        limit: int,
        window_seconds: int,
    ) -> None:
        if not self.rate_limiter.allow(
            f"{scope}:{client_identifier}",
            limit,
            window_seconds,
        ):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests",
            )

    def _authenticate_token(
        self,
        *,
        token: str | None,
        allowed_tokens: tuple[str, ...],
        kind: str,
        header_name: str,
        transport: str,
    ) -> SecurityPrincipal:
        resolved_token = token.strip() if token else None
        if not resolved_token:
            if transport == "websocket":
                raise WebSocketException(
                    code=status.WS_1008_POLICY_VIOLATION,
                    reason=f"Missing {header_name}",
                )

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Missing {header_name}",
            )

        if resolved_token not in allowed_tokens:
            if transport == "websocket":
                raise WebSocketException(
                    code=status.WS_1008_POLICY_VIOLATION,
                    reason=f"Invalid {header_name}",
                )

            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Invalid {header_name}",
            )

        return SecurityPrincipal(
            kind=kind,
            credential_fingerprint=_fingerprint_token(resolved_token),
            transport=transport,
        )

    def authenticate_user_request(self, request: Request, api_key: str | None) -> SecurityPrincipal:
        client_identifier = self._client_identifier(request.client.host if request.client else None)
        self._enforce_rate_limit(
            scope=f"user:{request.url.path}",
            client_identifier=client_identifier,
            limit=self.settings.http_rate_limit_requests,
            window_seconds=self.settings.http_rate_limit_window_seconds,
        )
        self._enforce_https(request.headers, request.url.scheme, "This API")
        return self._authenticate_token(
            token=api_key,
            allowed_tokens=self.settings.user_api_keys,
            kind="user",
            header_name="X-API-Key",
            transport="http",
        )

    def authenticate_internal_request(
        self,
        request: Request,
        internal_api_key: str | None,
    ) -> SecurityPrincipal:
        client_identifier = self._client_identifier(request.client.host if request.client else None)
        self._enforce_rate_limit(
            scope=f"internal:{request.url.path}",
            client_identifier=client_identifier,
            limit=self.settings.http_rate_limit_requests,
            window_seconds=self.settings.http_rate_limit_window_seconds,
        )
        self._enforce_https(request.headers, request.url.scheme, "This internal endpoint")
        return self._authenticate_token(
            token=internal_api_key,
            allowed_tokens=self.settings.internal_api_keys,
            kind="internal",
            header_name="X-Internal-Token",
            transport="http",
        )

    def authenticate_websocket(self, websocket: WebSocket) -> SecurityPrincipal:
        client_identifier = self._client_identifier(websocket.client.host if websocket.client else None)
        self._enforce_rate_limit(
            scope=f"websocket:{websocket.url.path}",
            client_identifier=client_identifier,
            limit=self.settings.websocket_connection_limit,
            window_seconds=self.settings.websocket_connection_window_seconds,
        )
        self._enforce_https(websocket.headers, websocket.url.scheme, "This websocket endpoint")
        return self._authenticate_token(
            token=websocket.query_params.get("api_key") or websocket.headers.get("x-api-key"),
            allowed_tokens=self.settings.user_api_keys,
            kind="user",
            header_name="api_key query parameter or X-API-Key header",
            transport="websocket",
        )