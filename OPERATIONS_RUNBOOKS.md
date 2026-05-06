# Operations Runbooks

Short checks for the most common production incidents.

## Kafka Lag

Symptoms:

- `/api/stats` shows growing `kafka_consumer_lag_messages`.
- `python consumer.py` logs retry loops or database reconnects.

First checks:

1. Confirm Kafka brokers are reachable and the `dbserver.realtime_orders.orders` topic is advancing.
2. Check Debezium connector status and restart the connector if it stopped producing.
3. Review the consumer logs for `kafka_message_processing_total` outcomes marked `retry`, `malformed`, or `unexpected_error`.
4. Verify MySQL is healthy, since the worker persists events before acknowledging offsets.

## Connector Failure

Symptoms:

- No new CDC events land in the queue table.
- Debezium reports connector errors or the source database stops streaming binlog changes.

First checks:

1. Inspect the Debezium connector status and logs.
2. Verify `DB_PASSWORD`, replication privileges, and MySQL binary logging configuration.
3. Confirm the MySQL host and port are reachable from the connector runtime.
4. Recreate the connector from `debezium-connector.json` if the configuration drifted.

## Consumer Crashes

Symptoms:

- `python consumer.py` exits or restarts repeatedly.
- The `kafka_consumer_running` gauge drops to `0`.

First checks:

1. Read the last worker log lines for database reconnect failures or malformed payload handling.
2. Confirm the database is reachable and the notification tables exist.
3. Look for poison messages that are repeatedly landing in dead-letter storage.
4. Restart the worker once the dependency issue is fixed.

## Database Outage

Symptoms:

- `/health/ready` returns `503` with a database issue.
- API requests fail or hang while acquiring connections.

First checks:

1. Verify MySQL is up and accepting connections.
2. Check `db_connection_failures_total` and `db_pool_used_connections` in `/api/stats`.
3. Confirm credentials and network policy changes were not rolled out recently.
4. Bring the database back before restarting the backend so the queue and broadcast workers can recover cleanly.

## WebSocket Spike

Symptoms:

- `websocket_active_connections` rises quickly.
- Disconnect counts or send failures increase.

First checks:

1. Check whether clients are reconnecting in a loop because of a bad deployment or heartbeat mismatch.
2. Review `websocket_disconnect_total`, `websocket_broadcast_failures_total`, and `websocket_send_failures_total` in `/api/stats`.
3. Confirm the load balancer and proxy timeout settings still match the heartbeat interval.
4. If needed, raise the websocket connection limit temporarily while you fix the client-side storm.