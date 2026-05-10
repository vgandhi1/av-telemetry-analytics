"""Kafka producer: serializes telemetry events and publishes to topic-per-sensor."""

from __future__ import annotations

import threading

import structlog
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .connectors.schemas import TelemetryBase, SensorType

logger = structlog.get_logger(__name__)

SENSOR_TOPIC_MAP: dict[SensorType, str] = {
    SensorType.GPS: "av.telemetry.gps",
    SensorType.IMU: "av.telemetry.imu",
    SensorType.CAN_BUS: "av.telemetry.can_bus",
    SensorType.LIDAR: "av.telemetry.lidar",
    SensorType.CAMERA: "av.telemetry.camera",
    SensorType.SYSTEM_HEALTH: "av.telemetry.system",
}


class TelemetryProducer:
    def __init__(
        self, bootstrap_servers: str, extra_config: dict | None = None
    ) -> None:
        config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.size": 65536,
            **(extra_config or {}),
        }
        self._producer = Producer(config)
        self._lock = threading.Lock()
        self._sent = 0
        self._errors = 0

    def _delivery_callback(self, err, msg) -> None:
        if err:
            self._errors += 1
            logger.error("delivery_failed", topic=msg.topic(), error=str(err))
        else:
            self._sent += 1

    def publish(self, event: TelemetryBase) -> None:
        topic = SENSOR_TOPIC_MAP[event.sensor_type]
        with self._lock:
            self._producer.produce(
                topic=topic,
                key=event.kafka_key(),
                value=event.to_kafka_value(),
                on_delivery=self._delivery_callback,
            )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("flush_incomplete", remaining=remaining)

    @property
    def stats(self) -> dict:
        return {"sent": self._sent, "errors": self._errors}

    def ensure_topics(self, bootstrap_servers: str, topic_configs: dict) -> None:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        new_topics = [
            NewTopic(
                name,
                num_partitions=cfg.get("partitions", 6),
                replication_factor=cfg.get("replication_factor", 1),
            )
            for name, cfg in topic_configs.items()
        ]
        futures = admin.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("topic_created", topic=topic)
            except KafkaException as e:
                if "already exists" in str(e).lower():
                    logger.debug("topic_exists", topic=topic)
                else:
                    logger.error("topic_create_failed", topic=topic, error=str(e))
