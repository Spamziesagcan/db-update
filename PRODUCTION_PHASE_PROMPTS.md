# Production Phase Prompts

This document turns each phase from the production readiness plan into a detailed, copy-paste prompt that can be used with an AI coding agent or a human engineer.

Use one phase at a time. Do not widen scope until the current phase is complete, validated, and reviewed.

## Phase 1 Prompt: Fix The Current Backend Correctness Issues

You are working in the backend repository for a real-time order notification system built with FastAPI, Kafka, Debezium, MySQL, and WebSockets. Your task is to complete Phase 1 only: eliminate the current correctness issues without refactoring the entire application.

First inspect the existing backend entry points and configuration files, especially the FastAPI app, Kafka consumer, CLI client, Debezium connector config, Docker Compose file, and environment setup docs. Identify the shortest path to fixing the following problems:

- A route is registered twice and must be reduced to one canonical implementation.
- WebSocket heartbeat behavior between the server and client must use one consistent ping/pong contract.
- A real database password is committed in the Debezium config and must be removed from source control.
- Startup configuration validation is happening at import time and points to the wrong env-file name.
- The internal broadcast endpoint should only accept validated data, not arbitrary dictionaries.

Apply only the minimal changes necessary to make the backend correct and safe. Preserve existing API behavior unless a bug requires a breaking fix. Do not redesign the architecture yet. Do not touch unrelated frontend code.

Deliverables:

- Remove the duplicate route registration.
- Fix the heartbeat protocol so client and server stay connected reliably.
- Replace hardcoded secret handling with documented environment-based configuration.
- Move configuration validation into a clear startup path.
- Add schema validation for internal broadcast payloads.
- Validate the changes with a focused backend check and report any residual risks.

Success criteria:

- The backend starts from a clean checkout using documented environment variables.
- The websocket client no longer logs heartbeat-related errors.
- No secrets remain in tracked configuration.
- The duplicate route is gone.

## Phase 2 Prompt: Refactor Into A Real Application Structure

You are now responsible for turning the monolithic backend into a maintainable Python application. Focus only on code organization and separation of concerns. Do not add new product features yet.

Start by identifying which responsibilities currently live inside one file and split them into clear modules. Separate configuration, database access, data models, API routes, websocket handling, and background workers. Introduce an app factory or equivalent startup pattern so the application lifecycle is explicit and testable.

Refactor the code so that:

- Routes stay thin and delegate business logic to service functions.
- Database access is isolated behind a repository or data-access layer.
- WebSocket connection management lives in its own module.
- Background work, if any, is separated from request handlers.
- Shared configuration and logging are centralized.

Do not over-engineer the folder structure. Prefer a small but meaningful module split that a new engineer can understand quickly. If a piece of inline HTML or ad hoc helper code is not part of the backend’s real responsibility, remove it or move it out of the backend service.

Deliverables:

- A proposed module layout.
- Code moved into separate files with clear responsibilities.
- Updated imports and startup wiring.
- Confirmation that the app still runs after the refactor.

Success criteria:

- No single file contains the whole backend.
- Business logic is no longer mixed with transport code.
- The structure is understandable without reading every line.

## Phase 3 Prompt: Make Event Delivery Reliable

You are improving the event delivery path for the notification system. Your goal is to make the flow from database change to user-visible notification durable, retry-safe, and resilient to restarts.

Analyze the current data path from Debezium through Kafka into the backend and identify weak points. Replace the fragile consumer-to-HTTP broadcast hop with a delivery design that can survive restarts, handle duplicate messages safely, and make failures observable. If the architecture needs a worker process, event bus, queue, or brokered fanout layer, introduce the simplest version that is still production-safe.

Design the event payloads so they include:

- A stable event identifier.
- A source identifier or topic metadata.
- A versioned schema.
- A timestamp.
- Enough metadata to support retries and debugging.

Treat idempotency as a first-class requirement. A retried event must not create duplicate user-visible state or double notifications. Define what happens when a message is malformed, when Kafka replays a record, when the worker restarts, and when the delivery layer is temporarily unavailable.

Deliverables:

- A durable event delivery design.
- Code changes that implement the design.
- Idempotent or retry-safe event handling.
- A dead-letter or quarantine strategy for broken messages.
- Validation of restart and replay behavior.

Success criteria:

- A backend restart does not lose in-flight event delivery.
- Duplicate CDC events are handled safely.
- Failures have a documented and observable path.

## Phase 4 Prompt: Add Security And Access Control

You are responsible for hardening the backend so it can be exposed to real users and trusted internal services. Focus on authentication, authorization, secret handling, and network boundaries.

Replace any permissive defaults with explicit security choices. Audit every API endpoint and internal hook to determine whether it should be public, authenticated, or internal-only. Add protection for internal broadcast or service-to-service endpoints so they cannot be called from the open internet.

Implement practical production controls, including:

- Explicit CORS allowlists.
- Authentication for user-facing operations.
- Authorization if there are tenants, roles, or workspace boundaries.
- Rate limiting for connection attempts and API abuse.
- Secret management that works in production, not just locally.
- TLS expectations for staging and production.

Deliverables:

- A security boundary map for the backend.
- Authentication and authorization changes where required.
- Protection for internal endpoints.
- A plan for storing and injecting secrets safely.
- Verification that insecure defaults are removed.

Success criteria:

- Anonymous users cannot access protected operations.
- Internal-only endpoints are not reachable from outside trusted services.
- Secrets do not appear in source control, logs, or docs.

## Phase 5 Prompt: Add Observability And Operational Controls

You are improving the service so an operator can diagnose problems quickly in production. Focus on observability, alerting, and operational runbooks.

Add structured logging, metrics, and tracing across the backend’s critical paths. Every log line should be useful for debugging a production incident. Every important dependency or boundary should emit enough data to detect lag, saturation, or failure before users notice it.

Prioritize the following telemetry:

- WebSocket connection counts and disconnect rates.
- Kafka consumer lag and message processing errors.
- Event fanout success and failure counts.
- API error rates and latency.
- Database pool usage and connection failures.

Add readiness and liveness endpoints suitable for orchestration systems. Create a small set of operator runbooks for common incidents such as Kafka lag, connector failure, consumer crashes, database outage, or websocket spike events.

Deliverables:

- Structured logging with request or correlation IDs.
- Useful service metrics.
- Tracing or span instrumentation on critical paths.
- Health endpoints for orchestration.
- Short operational runbooks.

Success criteria:

- A production incident can be diagnosed without guessing.
- The on-call person knows what to check first.

## Phase 6 Prompt: Build A Real Test Suite

You are making the backend safe to change by adding automated tests around the most important behavior. Focus on tests that protect the production path, not on testing trivial code.

Build a layered test strategy:

- Unit tests for transformation, validation, and business rules.
- Integration tests for MySQL access, Kafka event handling, and websocket broadcast.
- Contract tests for the payload shape delivered to clients.
- Failure-mode tests for malformed payloads, restarts, and dependency outages.
- Load or stress tests for websocket fanout and bursty event traffic.

Use the smallest effective test stack. Mock only what is necessary. Prefer real infrastructure in integration tests when it materially increases confidence. Add CI gates so regressions are caught before deployment.

Deliverables:

- A test plan that matches the backend architecture.
- Automated tests for critical flows.
- Tooling and CI configuration for linting and type checks if needed.
- Clear instructions for running the test suite locally.

Success criteria:

- The happy path is covered.
- The main failure paths are covered.
- CI catches the kinds of bugs that matter in production.

## Phase 7 Prompt: Harden Deployment And Release Management

You are responsible for making releases predictable and reversible. The goal is to make local, staging, and production environments consistent enough that a deployment is not a guessing game.

Turn the current setup into a repeatable deployment story. Define configuration for dev, staging, and production. Add health checks, startup ordering, and versioned images. Replace startup DDL with explicit schema migrations. Document rollback and recovery steps so an operator can reverse a bad release without improvising.

Make sure the deployment process covers:

- Versioned container images.
- Environment-specific configuration.
- Database migrations.
- Health probes and startup checks.
- Release tagging and traceability.
- Rollback and recovery instructions.

Deliverables:

- A deployment plan for dev, staging, and production.
- Docker or orchestration updates as needed.
- Migration strategy and migration tooling.
- Clear rollback documentation.
- Release/versioning conventions.

Success criteria:

- A release can be rolled forward and rolled back safely.
- Staging behaves like production.
- A new deployment is predictable rather than fragile.

## How To Use These Prompts

1. Pick one phase.
2. Feed only that phase prompt to the coding agent or engineer.
3. Finish implementation and validation for that phase before moving on.
4. Repeat in order.

The phases are intentionally sequenced: fix correctness first, then structure, then reliability, then security, then observability, then tests, then deployment.