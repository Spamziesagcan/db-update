# Real-Time Order Notification System

FastAPI, Kafka, Debezium, MySQL, and WebSockets for real-time order updates.

## Architecture

[MySQL DB] -> [Debezium Connector] -> [Kafka] -> [Notification Worker] -> [MySQL Notification Queue] -> [FastAPI Dispatcher] -> [WebSocket Manager] -> [Connected Clients]

The worker ingests CDC events from Kafka, normalizes them, and stores them durably in MySQL. The FastAPI app drains that queue and fans notifications out to connected websocket clients.

## Security Model

- `GET /api/orders`, `GET /api/stats`, and the order mutation routes require `X-API-Key`.
- `POST /internal/broadcast` requires `X-Internal-Token` and is internal-only.
- `WS /ws` accepts the user API key as `api_key` in the query string or `X-API-Key` when the client can set headers.
- CORS is an explicit allowlist. Wildcards are rejected.
- `REQUIRE_HTTPS=true` is required in staging and production.

See [SECURITY_BOUNDARY_MAP.md](SECURITY_BOUNDARY_MAP.md) for the route-by-route boundary map.

## Quick Start

1. Clone the repository.
2. Copy `.env.example` to `.env` and replace the placeholder values.
3. Start the infrastructure services with `docker-compose up -d`.
4. Register `debezium-connector.json` with Debezium.
5. Install Python dependencies with `pip install -r requirements.txt`.
6. Start the backend with `uvicorn main:app --reload --port 8000`.
7. Start the ingest worker with `python consumer.py`.

The quickstart and the MySQL-specific setup are documented in [QUICKSTART.md](QUICKSTART.md) and [MYSQL_SETUP.md](MYSQL_SETUP.md).

## Environment Variables

- `DB_PASSWORD`
- `API_KEYS`
- `INTERNAL_API_KEYS`
- `CORS_ALLOW_ORIGINS`
- `APP_ENV`
- `REQUIRE_HTTPS`
- `HTTP_RATE_LIMIT_REQUESTS`
- `HTTP_RATE_LIMIT_WINDOW_SECONDS`
- `WEBSOCKET_CONNECTION_LIMIT`
- `WEBSOCKET_CONNECTION_WINDOW_SECONDS`

Use the provided `.env.example` for local development and inject real secrets from a secret manager in staging or production.

## API Endpoints

| Method | Endpoint | Description |
| --- | --- | --- |
| GET | `/api/orders` | List orders, requires `X-API-Key` |
| POST | `/api/orders` | Create an order, requires `X-API-Key` |
| PUT | `/api/orders/{id}` | Update an order, requires `X-API-Key` |
| DELETE | `/api/orders/{id}` | Delete an order, requires `X-API-Key` |
| GET | `/api/stats` | Read system stats, requires `X-API-Key` |
| WS | `/ws` | Client websocket connection, requires API key |
| POST | `/internal/broadcast` | Internal broadcast, requires `X-Internal-Token` |

## Testing

- Open the dashboard and verify that websocket notifications arrive after creating or updating orders.
- Run `python client.py` to exercise the websocket client.
- Check `QUICKSTART.md` for the end-to-end local setup.
- Check `MYSQL_SETUP.md` if you are using an existing MySQL server.

## Notes

- No secrets should be committed to the repository.
- The Debezium connector resolves `DB_PASSWORD` from the environment.
- For deployment guidance, keep staging and production behind TLS terminators that set `X-Forwarded-Proto` correctly.
