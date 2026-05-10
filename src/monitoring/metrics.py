"""
Prometheus metrics and in-memory ring-buffer for real-time dashboard.

Metrics are exposed on /metrics (Prometheus scrape endpoint via prometheus_client).
The ring-buffer is read directly by the Streamlit dashboard for low-latency plots.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

EVENTS_RECEIVED = Counter(
    "av_telemetry_events_received_total",
    "Total telemetry events received by the ingestion pipeline",
    ["vehicle_id", "sensor_type"],
)

EVENTS_PROCESSED = Counter(
    "av_telemetry_events_processed_total",
    "Total events successfully processed by Spark",
    ["sensor_type"],
)

ANOMALIES_DETECTED = Counter(
    "av_anomalies_detected_total",
    "Total anomalies detected by the ML pipeline",
    ["vehicle_id"],
)

ACTIVE_VEHICLES = Gauge(
    "av_active_vehicles",
    "Number of vehicles reporting telemetry in the last 5 minutes",
)

PROCESSING_LAG = Histogram(
    "av_processing_lag_seconds",
    "Latency between event timestamp and processing time",
    ["sensor_type"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)

KAFKA_CONSUMER_LAG = Gauge(
    "av_kafka_consumer_lag",
    "Current Kafka consumer lag per topic partition",
    ["topic", "partition"],
)

PARQUET_WRITE_DURATION = Histogram(
    "av_parquet_write_duration_seconds",
    "Time taken to write and upload a Parquet batch",
    buckets=[0.1, 0.5, 1, 5, 10, 30],
)


# ---------------------------------------------------------------------------
# In-memory ring buffer for dashboard
# ---------------------------------------------------------------------------

@dataclass
class TelemetrySnapshot:
    ts: datetime
    vehicle_id: str
    sensor_type: str
    speed_ms: float = 0.0
    accel_magnitude: float = 0.0
    engine_temp: float = 0.0
    anomaly_score: float | None = None
    is_anomaly: bool = False
    lat: float | None = None
    lon: float | None = None


class MetricsRingBuffer:
    """Thread-safe fixed-size ring buffer of recent telemetry snapshots."""

    def __init__(self, maxlen: int = 10_000) -> None:
        self._buf: deque[TelemetrySnapshot] = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._alert_buf: deque[dict] = deque(maxlen=500)

    def push(self, snapshot: TelemetrySnapshot) -> None:
        with self._lock:
            self._buf.append(snapshot)

    def push_alert(self, alert: dict) -> None:
        with self._lock:
            self._alert_buf.append({**alert, "ts": datetime.now(timezone.utc)})

    def recent_snapshots(self, limit: int = 1000) -> list[TelemetrySnapshot]:
        with self._lock:
            return list(self._buf)[-limit:]

    def recent_alerts(self, limit: int = 50) -> list[dict]:
        with self._lock:
            return list(self._alert_buf)[-limit:]

    def active_vehicle_ids(self, last_seconds: int = 300) -> set[str]:
        cutoff = time.time() - last_seconds
        with self._lock:
            return {
                s.vehicle_id
                for s in self._buf
                if s.ts.timestamp() > cutoff
            }


# Singleton instance shared across modules
_buffer = MetricsRingBuffer()


def get_buffer() -> MetricsRingBuffer:
    return _buffer


def start_metrics_server(port: int = 9090) -> None:
    start_http_server(port)
