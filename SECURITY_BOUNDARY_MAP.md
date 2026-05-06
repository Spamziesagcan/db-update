# Security Boundary Map

## Public And Authenticated Surfaces

- `GET /api/orders`, `POST /api/orders`, `PUT /api/orders/{id}`, and `DELETE /api/orders/{id}` are authenticated user-facing endpoints.
- `GET /api/stats` is authenticated because it exposes live system information.
- `GET /health/live` and `GET /health/ready` are unauthenticated orchestration endpoints and should be protected by network policy or ingress rules.
- `WS /ws` is authenticated for connected clients.
- `POST /internal/broadcast` is internal-only and is not meant to be called by browsers or anonymous clients.

## Enforcement

- User-facing HTTP requests must send `X-API-Key`.
- Internal service-to-service requests must send `X-Internal-Token`.
- WebSocket clients must present the API key as `api_key` in the query string or as `X-API-Key` when the client can set headers.
- CORS is allowlisted through `CORS_ALLOW_ORIGINS`; wildcard origins are rejected.
- HTTP requests and WebSocket connection attempts are rate limited per client address and route scope.
- `REQUIRE_HTTPS=true` is mandatory in staging and production.

## Authorization Model

- There is no tenant, role, or workspace model in the current backend, so no additional object-level authorization rules are applied yet.
- Separate public and internal API keys provide the current authorization boundary.

## Secret Handling Plan

- Store `DB_PASSWORD`, `API_KEYS`, and `INTERNAL_API_KEYS` in a secret manager or injected environment variables.
- Keep `.env` only for local development.
- Register Debezium with `debezium-connector.json` unchanged; it resolves `DB_PASSWORD` from the environment instead of embedding the password in source control.
- Do not log the raw secret values.