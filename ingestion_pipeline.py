"""
Entry point for the AV telemetry ingestion pipeline.

Two modes:
  --mode produce   Spin up synthetic vehicle connectors and publish to Kafka.
  --mode consume   Consume from Kafka, validate, and hand off to downstream storage.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import signal
import sys
import time
from pathlib import Path

import structlog
import yaml
from dotenv import load_dotenv

load_dotenv()

# Ensure src/ is on the path when run directly
sys.path.insert(0, str(Path(__file__).parent))

from src.ingestion.connectors.vehicle_connector import VehicleConnector  # noqa: E402
from src.ingestion.kafka_producer import TelemetryProducer  # noqa: E402
from src.ingestion.kafka_consumer import TelemetryConsumer  # noqa: E402
from src.ingestion.connectors.schemas import TelemetryBase  # noqa: E402

logger = structlog.get_logger(__name__)


def load_config(path: str = "config/app_config.yaml") -> dict:
    with open(path) as f:
        raw = yaml.safe_load(f)

    def _expand(obj):
        if isinstance(obj, dict):
            return {k: _expand(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_expand(i) for i in obj]
        if isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            parts = obj[2:-1].split(":-", 1)
            return os.environ.get(parts[0], parts[1] if len(parts) > 1 else "")
        return obj

    return _expand(raw)


# ---------------------------------------------------------------------------
# Producer mode
# ---------------------------------------------------------------------------


def run_producer(config: dict, vehicle_ids: list[str], hz: float = 10.0) -> None:
    bootstrap = config["kafka"]["bootstrap_servers"]
    producer = TelemetryProducer(bootstrap_servers=bootstrap)

    kafka_cfg = yaml.safe_load(open("config/kafka_config.yaml"))
    producer.ensure_topics(bootstrap, kafka_cfg.get("topics", {}))

    stop_event = False

    def _sigint(_sig, _frame):
        nonlocal stop_event
        stop_event = True
        logger.info("shutdown_requested")

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    def _stream_vehicle(vid: str) -> None:
        connector = VehicleConnector(vehicle_id=vid)
        logger.info("vehicle_stream_started", vehicle_id=vid)
        for event in connector.stream(hz=hz):
            if stop_event:
                break
            producer.publish(event)
        logger.info("vehicle_stream_stopped", vehicle_id=vid, stats=producer.stats)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(vehicle_ids)) as pool:
        futures = [pool.submit(_stream_vehicle, vid) for vid in vehicle_ids]
        while not stop_event:
            time.sleep(5)
            logger.info("producer_stats", stats=producer.stats)
        for f in futures:
            f.cancel()

    producer.flush()
    logger.info("producer_done", stats=producer.stats)


# ---------------------------------------------------------------------------
# Consumer mode
# ---------------------------------------------------------------------------


def _build_event_handler(config: dict):
    """Returns an event handler that validates and dispatches to storage."""
    from src.storage.parquet_writer import ParquetWriter

    writer = ParquetWriter(
        local_dir=config["storage"]["local_parquet_dir"],
        s3_bucket=config["storage"].get("s3_bucket"),
        s3_prefix=config["storage"].get("s3_prefix", "telemetry/v1"),
        batch_size=config["storage"].get("write_batch_size", 5000),
    )

    def handle(event: TelemetryBase) -> None:
        writer.add(event)

    return handle, writer


def run_consumer(config: dict) -> None:
    kafka_cfg = config["kafka"]
    all_topics = list(kafka_cfg["topics"].values())
    all_topics = [t for t in all_topics if t != kafka_cfg["topics"].get("alerts")]

    handler, writer = _build_event_handler(config)

    consumer = TelemetryConsumer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        group_id=kafka_cfg["consumer_group"],
        topics=all_topics,
    )

    def _shutdown(_sig, _frame):
        logger.info("shutdown_requested")
        consumer.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("consumer_started", topics=all_topics)
    consumer.consume(
        on_event=handler,
        batch_size=kafka_cfg.get("batch_size", 1000),
    )
    writer.flush()
    logger.info("consumer_done", stats=consumer.stats)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    import logging

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
    )

    parser = argparse.ArgumentParser(description="AV Telemetry Ingestion Pipeline")
    parser.add_argument(
        "--mode",
        choices=["produce", "consume"],
        default="produce",
        help="Run as producer (synthetic data) or consumer (storage sink)",
    )
    parser.add_argument(
        "--vehicles",
        nargs="+",
        default=["ZX-001", "ZX-002", "ZX-003"],
        help="Vehicle IDs to simulate (producer mode)",
    )
    parser.add_argument(
        "--hz", type=float, default=10.0, help="Events per second per vehicle"
    )
    parser.add_argument(
        "--config", default="config/app_config.yaml", help="Config file path"
    )
    args = parser.parse_args()

    config = load_config(args.config)

    if args.mode == "produce":
        run_producer(config, vehicle_ids=args.vehicles, hz=args.hz)
    else:
        run_consumer(config)


if __name__ == "__main__":
    main()
