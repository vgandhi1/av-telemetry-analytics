"""Tests for Parquet writer and DuckDB manager."""
import os
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.ingestion.connectors.schemas import GPSEvent
from src.storage.parquet_writer import ParquetWriter
from src.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture
def gps_event():
    return GPSEvent(
        vehicle_id="ZX-001",
        timestamp=datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc),
        latitude=37.386,
        longitude=-122.083,
        altitude_m=10.0,
        speed_ms=5.5,
        heading_deg=90.0,
        accuracy_m=1.2,
        num_satellites=12,
    )


class TestParquetWriter:
    def test_flush_creates_files(self, tmp_dir, gps_event):
        writer = ParquetWriter(local_dir=tmp_dir, batch_size=1)
        writer.add(gps_event)
        # batch_size=1 triggers immediate flush
        assert len(writer.written_files) == 1

    def test_manual_flush(self, tmp_dir, gps_event):
        writer = ParquetWriter(local_dir=tmp_dir, batch_size=100)
        for _ in range(5):
            writer.add(gps_event)
        assert writer.pending_count == 5
        flushed = writer.flush()
        assert flushed == 5
        assert writer.pending_count == 0

    def test_no_s3_upload_without_bucket(self, tmp_dir, gps_event):
        writer = ParquetWriter(local_dir=tmp_dir, s3_bucket=None, batch_size=1)
        writer.add(gps_event)
        assert len(writer.written_files) == 1
        assert os.path.exists(writer.written_files[0])


class TestDuckDBManager:
    def test_schema_creation(self, tmp_dir):
        db_path = os.path.join(tmp_dir, "test.duckdb")
        db = DuckDBManager(db_path=db_path)
        tables = db.query("SHOW TABLES")
        table_names = set(tables.iloc[:, 0].tolist())
        assert "gps_events" in table_names
        assert "can_bus_events" in table_names
        assert "anomaly_detections" in table_names
        db.close()

    def test_insert_and_query(self, tmp_dir):
        db_path = os.path.join(tmp_dir, "test.duckdb")
        db = DuckDBManager(db_path=db_path)

        df = pd.DataFrame([{
            "event_id": "evt-001",
            "vehicle_id": "ZX-001",
            "timestamp": pd.Timestamp("2024-01-01 12:00:00+00:00"),
            "latitude": 37.386,
            "longitude": -122.083,
            "altitude_m": 10.0,
            "speed_ms": 5.5,
            "speed_kmh": 19.8,
            "heading_deg": 90.0,
            "accuracy_m": 1.2,
            "num_satellites": 12,
            "is_moving": True,
            "processing_lag_ms": 0.0,
            "year": 2024,
            "month": 1,
            "day": 1,
            "hour": 12,
        }])

        db.execute("INSERT INTO gps_events SELECT * FROM df")
        result = db.query("SELECT COUNT(*) AS cnt FROM gps_events")
        assert result["cnt"].iloc[0] == 1
        db.close()
