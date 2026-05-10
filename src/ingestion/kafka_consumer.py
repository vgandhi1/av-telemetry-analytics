"""Kafka consumer: deserializes and validates telemetry events from all topics."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from .connectors.schemas import TelemetryBase, TOPIC_TO_SCHEMA

logger = structlog.get_logger(__name__)


class TelemetryConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        extra_config: dict | None = None,
    ) -> None:
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
            **(extra_config or {}),
        }
        self._consumer = Consumer(config)
        self._consumer.subscribe(topics)
        self._running = False
        self._lock = threading.Lock()
        self._stats = {"received": 0, "parse_errors": 0, "committed": 0}

    def _parse_message(self, msg: Message) -> TelemetryBase | None:
        topic = msg.topic()
        schema_cls = TOPIC_TO_SCHEMA.get(topic)
        if schema_cls is None:
            logger.warning("unknown_topic", topic=topic)
            return None
        try:
            data = json.loads(msg.value().decode("utf-8"))
            return schema_cls.model_validate(data)
        except Exception as exc:
            self._stats["parse_errors"] += 1
            logger.error(
                "parse_error",
                topic=topic,
                offset=msg.offset(),
                error=str(exc),
            )
            return None

    def consume(
        self,
        on_event: Callable[[TelemetryBase], None],
        on_error: Callable[[Exception], None] | None = None,
        poll_timeout: float = 1.0,
        batch_size: int = 500,
    ) -> None:
        """Block and call on_event for each valid message. Commits offsets per batch."""
        self._running = True
        batch_count = 0
        logger.info("consumer_started", topics=self._consumer.assignment())

        try:
            while self._running:
                msg = self._consumer.poll(timeout=poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    err = KafkaException(msg.error())
                    logger.error("kafka_error", error=str(err))
                    if on_error:
                        on_error(err)
                    continue

                self._stats["received"] += 1
                event = self._parse_message(msg)
                if event is not None:
                    try:
                        on_event(event)
                    except Exception as exc:
                        logger.error("handler_error", error=str(exc))
                        if on_error:
                            on_error(exc)

                batch_count += 1
                if batch_count >= batch_size:
                    self._consumer.commit(asynchronous=False)
                    self._stats["committed"] += batch_count
                    batch_count = 0
        finally:
            if batch_count > 0:
                self._consumer.commit(asynchronous=False)
            self._consumer.close()
            logger.info("consumer_stopped", stats=self._stats)

    def stop(self) -> None:
        self._running = False

    def iter_events(self, poll_timeout: float = 1.0) -> Iterator[TelemetryBase]:
        """Generator interface for consuming events one at a time."""
        self._running = True
        try:
            while self._running:
                msg = self._consumer.poll(timeout=poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                self._stats["received"] += 1
                event = self._parse_message(msg)
                if event is not None:
                    yield event
                    self._consumer.commit(asynchronous=False)
        finally:
            self._consumer.close()

    @property
    def stats(self) -> dict[str, Any]:
        return dict(self._stats)
