# Production Readiness Plan

## Goal

Turn the current backend into a production-grade service that can be deployed safely, maintained by another engineer, and trusted by real users without manual heroics.

## What "10/10" Means

- No hardcoded secrets or environment drift.
- One clean and documented runtime path from database change to client notification.
- Clear module boundaries instead of one large script.
- Reliable delivery with retries, idempotency, and backpressure handling.
- Authentication, authorization, and rate limiting where needed.
- Health checks, metrics, logs, tracing, and alerting.
- Automated tests that cover the critical flows.
- Reproducible local, staging, and production deployments.

## Current Problems To Eliminate

- The same route is registered twice in the backend.
- The WebSocket heartbeat behavior is inconsistent between server and CLI client.
- A real database password is committed in the Debezium connector config.
- Startup configuration validation is happening in the wrong place and uses the wrong env-file name.
- The consumer sends events over HTTP to the backend, which is fragile for production scaling.
- There are no visible tests, no quality gates, and no production monitoring.

## Target Architecture

MySQL -> Debezium -> Kafka -> Notification Worker -> Durable Event Delivery Layer -> FastAPI WebSocket/API -> Clients

If you want the simplest production-safe version, keep the worker and API as separate processes in the same repo. If you want multiple backend instances later, add a shared broker such as Redis Pub/Sub or NATS between the worker and the websocket gateways.

## Phase 1: Fix The Current Backend Correctness Issues

- Remove the duplicate update route decorator and keep one canonical implementation.
- Standardize the ping/pong contract so the backend and clients speak the same heartbeat protocol.
- Replace startup-time `print` and `sys.exit` checks with structured settings validation.
- Stop failing on import just because one env var is missing; validate config during app startup with a clear error.
- Replace the hardcoded `.env` assumptions with a single documented environment contract.
- Remove the committed Debezium password and replace it with a template file plus secret injection at deploy time.
- Make internal broadcast inputs schema-validated instead of accepting arbitrary dictionaries.

Acceptance criteria:

- The backend starts cleanly from a fresh checkout with documented env vars.
- The websocket client stays connected with no heartbeat errors.
- No secrets remain in tracked configuration.
- The duplicate route is gone.

## Phase 2: Refactor Into A Real Application Structure

- Split the monolithic `main.py` into modules for config, database, schemas, API routes, websocket handling, and background workers.
- Introduce an app factory so startup and shutdown are explicit.
- Move all database access behind a repository/service layer.
- Separate transport concerns from business logic.
- Replace inline HTML and ad hoc helper code with maintainable components or remove them from the backend entirely if the frontend will live elsewhere.

Suggested structure:

- `app/core/settings.py`
- `app/core/logging.py`
- `app/db/connection.py`
- `app/models/`
- `app/api/routes/`
- `app/services/`
- `app/workers/`
- `app/websockets/`
- `tests/`

Acceptance criteria:

- No single file contains the whole application.
- Routes are thin and delegate to services.
- The app can be reasoned about by a new engineer in under an hour.

## Phase 3: Make Event Delivery Reliable

- Replace the consumer-to-HTTP broadcast hop with a durable internal delivery path.
- Add event IDs, source metadata, schema versioning, and timestamps to every notification.
- Make event processing idempotent so retries do not create duplicate user-visible effects.
- Define retry policy, timeout policy, and dead-letter handling for failed events.
- Handle Kafka consumer restarts, rebalances, and duplicate deliveries explicitly.
- Decide whether notification state needs to be persisted for audit, replay, or recovery.

Recommended production pattern:

- Debezium writes CDC events to Kafka.
- A dedicated worker consumes Kafka and normalizes events.
- A shared delivery layer fans out to websocket servers or publishes to a shared bus.
- Websocket servers only deal with client connections and outbound messages.

Acceptance criteria:

- A backend restart does not lose live event delivery.
- The same CDC event can be retried safely.
- There is a clear dead-letter path for malformed or broken messages.

## Phase 4: Add Security And Access Control

- Replace permissive CORS with an explicit allowlist.
- Require authentication for any real user-facing API endpoints.
- Protect internal endpoints so only trusted services can call them.
- Add authorization rules if the product has tenant, role, or workspace boundaries.
- Add rate limiting for websocket connection attempts and API requests.
- Ensure secrets are stored in a real secret manager in production.
- Run everything over TLS in staging and production.

Acceptance criteria:

- Anonymous users cannot access sensitive operations.
- Internal event endpoints cannot be called from outside the service network.
- Secrets never appear in source control or logs.

## Phase 5: Add Observability And Operational Controls

- Use structured JSON logging with request IDs and correlation IDs.
- Export metrics for websocket connections, event lag, event fanout success, and API errors.
- Add distributed tracing across the API, worker, and database boundaries.
- Create readiness and liveness endpoints for orchestration platforms.
- Add alerts for Kafka lag, consumer failures, websocket disconnect spikes, and database connection pool exhaustion.
- Write runbooks for restarts, connector recovery, and incident triage.

Acceptance criteria:

- A production issue can be diagnosed from logs and metrics without guessing.
- An on-call engineer knows what to check first when delivery breaks.

## Phase 6: Build A Real Test Suite

- Add unit tests for event transformation, validation, and business rules.
- Add integration tests for MySQL, Debezium event mapping, Kafka consumption, and websocket broadcast.
- Add contract tests for the payload shape that clients receive.
- Add load tests for websocket connection counts and bursty event traffic.
- Add failure-mode tests for consumer restarts, malformed payloads, and database outages.

Suggested tools:

- `pytest`
- `pytest-asyncio`
- `testcontainers`
- `ruff`
- `mypy`
- `pre-commit`

Acceptance criteria:

- CI fails when the critical behavior regresses.
- The main happy path and the main failure paths are covered.

## Phase 7: Harden Deployment And Release Management

- Create separate dev, staging, and production configurations.
- Build versioned Docker images for each service.
- Add health checks and startup ordering to container orchestration.
- Add schema migrations instead of relying on startup DDL.
- Document rollback steps and database recovery steps.
- Use release tagging and changelogs so production versions are traceable.
- Make local startup as close as possible to staging startup.

Acceptance criteria:

- A new deployment can be rolled forward and rolled back predictably.
- Staging behaves like production, not like a different application.

## Suggested Build Order

1. Fix correctness bugs and secret handling.
2. Split the monolith into a real module structure.
3. Replace the fragile consumer-to-HTTP path with durable delivery.
4. Add auth, rate limiting, and strict config management.
5. Add metrics, tracing, logs, and alerts.
6. Add automated tests and CI gates.
7. Harden deployment, migrations, and rollback procedures.

## Definition Of Done

This project is production-ready when all of the following are true:

- A clean checkout can be deployed with documented environment variables only.
- No secrets live in the repo.
- One event path is clearly defined and reliable.
- The backend survives restarts and transient failures without manual repair.
- The team can observe, test, and troubleshoot the system quickly.
- The codebase is modular enough that a new contributor can extend it safely.

## Final Standard

If you can deploy it, monitor it, secure it, test it, and recover it without panic, then it is a 10/10 backend.
