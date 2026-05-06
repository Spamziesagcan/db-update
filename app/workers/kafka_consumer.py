from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import pymysql
from dotenv import load_dotenv
from kafka import KafkaConsumer

from app.core.logging_config import configure_logging
from app.core.observability import metrics, trace_span
from app.core.settings import Settings, load_settings
from app.services.notification_event_factory import (
    NotificationEventParseError,
    build_dead_letter_id,
    build_notification_message,
)

load_dotenv()

logger = logging.getLogger(__name__)


def _connect_to_database(settings: Settings) -> pymysql.Connection:
    with trace_span("kafka.consumer.database_connect", database=settings.db_name):
        try:
            return pymysql.connect(
                host=settings.db_host,
                user=settings.db_user,
                password=settings.db_password,
                database=settings.db_name,
                port=settings.db_port,
                autocommit=True,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.Cursor,
            )
        except Exception:
            metrics.increment_counter("db_connection_failures_total", stage="consumer_connect")
            raise


def _store_notification_event(connection: pymysql.Connection, raw_payload: dict[str, object], *, topic: str, partition: int, offset: int, received_at: datetime) -> bool:
    message = build_notification_message(
        raw_payload,
        topic=topic,
        partition=partition,
        offset=offset,
        received_at=received_at,
    )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT IGNORE INTO notification_events (
                event_id,
                schema_version,
                event_type,
                action,
                order_id,
                source_topic,
                source_partition,
                source_offset,
                source_connector,
                source_database,
                source_table,
                source_file,
                source_pos,
                source_row,
                event_timestamp,
                payload_json,
                status,
                attempts,
                available_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, 'pending', 0, %s
            )
            """,
            (
                message.event_id,
                message.schema_version,
                message.event_type,
                message.action,
                message.order_id,
                message.source.topic,
                message.source.partition,
                message.source.offset,
                message.source.connector,
                message.source.database,
                message.source.table,
                message.source.binlog_file,
                message.source.binlog_position,
                message.source.binlog_row,
                message.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                message.model_dump_json(),
                datetime.utcnow(),
            ),
        )

        inserted = cursor.rowcount == 1

    if inserted:
        metrics.increment_counter("kafka_events_ingested_total", outcome="queued")
        logger.info("Queued notification event %s", message.event_id)
    else:
        metrics.increment_counter("kafka_events_ingested_total", outcome="duplicate")
        logger.info("Duplicate notification event %s ignored", message.event_id)
    return inserted


def _record_dead_letter(
    connection: pymysql.Connection,
    *,
    event_id: str | None,
    raw_payload: str,
    stage: str,
    reason: str,
    error_message: str,
    schema_version: int | None = None,
    event_type: str | None = None,
    action: str | None = None,
    topic: str | None = None,
    partition: int | None = None,
    offset: int | None = None,
) -> None:
    dead_letter_id = build_dead_letter_id(
        event_id=event_id,
        stage=stage,
        reason=reason,
        raw_payload=raw_payload,
        topic=topic,
        partition=partition,
        offset=offset,
    )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT IGNORE INTO notification_dead_letters (
                dead_letter_id,
                event_id,
                schema_version,
                event_type,
                action,
                stage,
                reason,
                error_message,
                raw_payload,
                source_topic,
                source_partition,
                source_offset
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            """,
            (
                dead_letter_id,
                event_id,
                schema_version,
                event_type,
                action,
                stage,
                reason,
                error_message,
                raw_payload,
                topic,
                partition,
                offset,
            ),
        )
    metrics.increment_counter("kafka_dead_letters_total", stage=stage, reason=reason)


def _record_consumer_lag(consumer: KafkaConsumer) -> None:
    partitions = list(consumer.assignment())
    if not partitions:
        return

    try:
        end_offsets = consumer.end_offsets(partitions)
        total_lag = 0
        for partition in partitions:
            current_position = consumer.position(partition)
            lag = max(end_offsets.get(partition, current_position) - current_position, 0)
            total_lag += lag
            metrics.set_gauge(
                "kafka_consumer_partition_lag_messages",
                float(lag),
                topic=partition.topic,
                partition=partition.partition,
            )

        metrics.set_gauge("kafka_consumer_lag_messages", float(total_lag))
    except Exception:
        metrics.increment_counter("kafka_consumer_lag_sampling_failures_total")
        logger.exception("Unable to sample Kafka consumer lag")


def start_consumer(settings: Settings | None = None) -> None:
    runtime_settings = settings or load_settings()
    configure_logging(runtime_settings.log_file)
    metrics.set_gauge("kafka_consumer_running", 1)

    logger.info(
        "Connecting to Kafka brokers at %s on topic %s...",
        runtime_settings.kafka_brokers,
        runtime_settings.kafka_topic,
    )

    consumer = KafkaConsumer(
        runtime_settings.kafka_topic,
        bootstrap_servers=runtime_settings.kafka_brokers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="notification_event_ingestor",
        value_deserializer=None,
        max_poll_records=runtime_settings.max_batch_size,
    )

    logger.info("Kafka consumer connected successfully. Waiting for messages...")
    database = None

    while database is None:
        try:
            database = _connect_to_database(runtime_settings)
        except pymysql.MySQLError:
            logger.exception("Unable to connect to the database; retrying")
            time.sleep(runtime_settings.notification_retry_delay_seconds)

    try:
        for message in consumer:
            with trace_span(
                "kafka.consumer.message",
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            ):
                raw_value = message.value
                if raw_value is None:
                    metrics.increment_counter("kafka_message_processing_total", outcome="tombstone")
                    metrics.increment_counter("kafka_events_ingested_total", outcome="tombstone")
                    logger.debug("Skipping tombstone at %s-%s-%s", message.topic, message.partition, message.offset)
                    consumer.commit()
                    _record_consumer_lag(consumer)
                    continue

                raw_payload_text = raw_value.decode("utf-8", errors="replace")
                received_at = datetime.now(timezone.utc)

                try:
                    decoded_payload = json.loads(raw_payload_text)
                    if not isinstance(decoded_payload, dict):
                        raise NotificationEventParseError("Kafka value must decode to a JSON object")

                    _store_notification_event(
                        database,
                        decoded_payload,
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        received_at=received_at,
                    )
                    consumer.commit()
                    metrics.increment_counter("kafka_message_processing_total", outcome="success")
                    _record_consumer_lag(consumer)
                except NotificationEventParseError as exc:
                    metrics.increment_counter("kafka_message_processing_total", outcome="malformed")
                    logger.warning(
                        "Malformed Debezium event at %s-%s-%s: %s",
                        message.topic,
                        message.partition,
                        message.offset,
                        exc,
                    )
                    try:
                        _record_dead_letter(
                            database,
                            event_id=None,
                            raw_payload=raw_payload_text,
                            stage="ingest",
                            reason="malformed_debezium_event",
                            error_message=str(exc),
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset,
                        )
                        consumer.commit()
                        _record_consumer_lag(consumer)
                    except pymysql.MySQLError:
                        metrics.increment_counter("kafka_message_processing_total", outcome="dead_letter_retry")
                        logger.exception("Failed to record malformed event; will retry")
                        try:
                            database.close()
                        except Exception:
                            logger.exception("Error closing database connection after dead-letter failure")
                        database = None
                        while database is None:
                            try:
                                database = _connect_to_database(runtime_settings)
                            except pymysql.MySQLError:
                                logger.exception("Unable to reconnect to the database after dead-letter failure")
                                time.sleep(runtime_settings.notification_retry_delay_seconds)
                        time.sleep(runtime_settings.notification_retry_delay_seconds)
                except (pymysql.MySQLError, OSError, json.JSONDecodeError, UnicodeDecodeError):
                    metrics.increment_counter("kafka_message_processing_total", outcome="retry")
                    logger.exception(
                        "Error persisting Kafka event at %s-%s-%s; will retry after backoff",
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    try:
                        if database is not None:
                            database.close()
                    except Exception:
                        logger.exception("Error closing database connection after failure")
                    database = None
                    while database is None:
                        try:
                            database = _connect_to_database(runtime_settings)
                        except pymysql.MySQLError:
                            logger.exception("Unable to reconnect to the database; retrying")
                            time.sleep(runtime_settings.notification_retry_delay_seconds)
                    time.sleep(runtime_settings.notification_retry_delay_seconds)
                except Exception:
                    metrics.increment_counter("kafka_message_processing_total", outcome="unexpected_error")
                    logger.exception(
                        "Unexpected error processing Kafka event at %s-%s-%s",
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    time.sleep(runtime_settings.notification_retry_delay_seconds)
    finally:
        metrics.set_gauge("kafka_consumer_running", 0)
        try:
            if database is not None:
                database.close()
        except Exception:
            logger.exception("Error closing database connection")
        consumer.close()