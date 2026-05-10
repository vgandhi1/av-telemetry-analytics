"""
Batched Parquet writer: buffers telemetry events, flushes to local staging,
then uploads to S3 and optionally syncs to DuckDB.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from src.ingestion.connectors.schemas import TelemetryBase, SensorType

logger = structlog.get_logger(__name__)

# Map sensor type → PyArrow schema for each Parquet file
_BASE_FIELDS = [
    pa.field("event_id", pa.string()),
    pa.field("vehicle_id", pa.string()),
    pa.field("sensor_type", pa.string()),
    pa.field("timestamp", pa.timestamp("us", tz="UTC")),
    pa.field("sequence_number", pa.int64()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
]


class ParquetWriter:
    """
    Thread-safe batching writer.
    Buffer events per (sensor_type, vehicle_id, date) partition.
    Flush when batch_size is reached or flush() is called explicitly.
    """

    def __init__(
        self,
        local_dir: str,
        s3_bucket: str | None = None,
        s3_prefix: str = "telemetry/v1/raw",
        batch_size: int = 5000,
    ) -> None:
        self._local_dir = Path(local_dir)
        self._local_dir.mkdir(parents=True, exist_ok=True)
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._batch_size = batch_size

        self._buffers: dict[str, list[dict]] = defaultdict(list)
        self._lock = threading.Lock()
        self._written_files: list[str] = []

        self._s3: Any = None
        if s3_bucket:
            from src.storage.s3_client import S3Client
            self._s3 = S3Client(bucket=s3_bucket)

    def _partition_key(self, event: TelemetryBase) -> str:
        ts = event.timestamp
        return (
            f"{event.sensor_type.value}/"
            f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/"
            f"vehicle_id={event.vehicle_id}"
        )

    def add(self, event: TelemetryBase) -> None:
        key = self._partition_key(event)
        record = event.model_dump()
        record["sensor_type"] = event.sensor_type.value
        record["timestamp"] = event.timestamp
        record["ingested_at"] = event.ingested_at

        with self._lock:
            self._buffers[key].append(record)
            if len(self._buffers[key]) >= self._batch_size:
                self._flush_partition(key)

    def _flush_partition(self, key: str) -> None:
        """Must be called with self._lock held."""
        records = self._buffers.pop(key, [])
        if not records:
            return

        import pandas as pd
        df = pd.DataFrame(records)

        rel_path = f"{key}/{datetime.now(timezone.utc).strftime('%H%M%S%f')}.parquet"
        local_path = self._local_dir / rel_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(
            table,
            str(local_path),
            compression="snappy",
            row_group_size=100_000,
        )
        self._written_files.append(str(local_path))
        logger.info("parquet_written", path=str(local_path), rows=len(records))

        if self._s3 is not None:
            s3_key = f"{self._s3_prefix}/{rel_path}"
            try:
                self._s3.upload_file(str(local_path), s3_key)
                local_path.unlink()  # remove staging file after upload
            except Exception as exc:
                logger.error("s3_upload_failed", key=s3_key, error=str(exc))

    def flush(self) -> int:
        """Flush all pending buffers. Returns total records flushed."""
        total = 0
        with self._lock:
            for key in list(self._buffers.keys()):
                total += len(self._buffers[key])
                self._flush_partition(key)
        logger.info("parquet_flush_complete", records_flushed=total)
        return total

    @property
    def pending_count(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._buffers.values())

    @property
    def written_files(self) -> list[str]:
        return list(self._written_files)
