"""
DuckDB manager: schema creation, bulk loading from Parquet,
and a query interface used by the dashboard and API.
"""

from __future__ import annotations

import threading
from pathlib import Path

import duckdb
import pandas as pd
import structlog

logger = structlog.get_logger(__name__)

DDL = {
    "gps_events": """
        CREATE TABLE IF NOT EXISTS gps_events (
            event_id      VARCHAR PRIMARY KEY,
            vehicle_id    VARCHAR NOT NULL,
            timestamp     TIMESTAMPTZ NOT NULL,
            latitude      DOUBLE,
            longitude     DOUBLE,
            altitude_m    DOUBLE,
            speed_ms      DOUBLE,
            speed_kmh     DOUBLE,
            heading_deg   DOUBLE,
            accuracy_m    DOUBLE,
            num_satellites INTEGER,
            is_moving     BOOLEAN,
            year          INTEGER,
            month         INTEGER,
            day           INTEGER,
            hour          INTEGER,
        )
    """,
    "imu_events": """
        CREATE TABLE IF NOT EXISTS imu_events (
            event_id          VARCHAR PRIMARY KEY,
            vehicle_id        VARCHAR NOT NULL,
            timestamp         TIMESTAMPTZ NOT NULL,
            accel_x           DOUBLE,
            accel_y           DOUBLE,
            accel_z           DOUBLE,
            accel_magnitude   DOUBLE,
            lateral_g         DOUBLE,
            longitudinal_g    DOUBLE,
            gyro_magnitude    DOUBLE,
            roll_deg          DOUBLE,
            pitch_deg         DOUBLE,
            yaw_deg           DOUBLE,
            year              INTEGER,
            month             INTEGER,
            day               INTEGER,
        )
    """,
    "can_bus_events": """
        CREATE TABLE IF NOT EXISTS can_bus_events (
            event_id              VARCHAR PRIMARY KEY,
            vehicle_id            VARCHAR NOT NULL,
            timestamp             TIMESTAMPTZ NOT NULL,
            speed_ms              DOUBLE,
            speed_kmh             DOUBLE,
            steering_angle_deg    DOUBLE,
            throttle_pct          DOUBLE,
            brake_pressure_pct    DOUBLE,
            gear                  VARCHAR,
            engine_rpm            DOUBLE,
            engine_temp_celsius   DOUBLE,
            oil_pressure_kpa      DOUBLE,
            battery_voltage       DOUBLE,
            odometer_km           DOUBLE,
            brake_wear_pct        DOUBLE,
            hard_braking          BOOLEAN,
            engine_overheating    BOOLEAN,
            low_battery           BOOLEAN,
            year                  INTEGER,
            month                 INTEGER,
            day                   INTEGER,
        )
    """,
    "anomaly_detections": """
        CREATE TABLE IF NOT EXISTS anomaly_detections (
            event_id       VARCHAR,
            vehicle_id     VARCHAR NOT NULL,
            timestamp      TIMESTAMPTZ NOT NULL,
            sensor_type    VARCHAR,
            anomaly_score  DOUBLE,
            features       JSON,
            detected_at    TIMESTAMPTZ DEFAULT NOW(),
        )
    """,
    "vehicle_summary_1min": """
        CREATE TABLE IF NOT EXISTS vehicle_summary_1min (
            window_start      TIMESTAMPTZ NOT NULL,
            window_end        TIMESTAMPTZ,
            vehicle_id        VARCHAR NOT NULL,
            avg_speed_ms      DOUBLE,
            max_speed_ms      DOUBLE,
            min_speed_ms      DOUBLE,
            speed_stddev      DOUBLE,
            event_count       BIGINT,
            PRIMARY KEY (window_start, vehicle_id)
        )
    """,
}


class DuckDBManager:
    def __init__(
        self, db_path: str, read_only: bool = False, memory_limit: str = "4GB"
    ) -> None:
        self._path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(db_path, read_only=read_only)
        self._conn.execute(f"SET memory_limit='{memory_limit}'")
        self._conn.execute("SET threads=4")
        self._lock = threading.Lock()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._lock:
            for table_name, ddl in DDL.items():
                self._conn.execute(ddl)
            # Enable S3 extension for querying remote Parquet
            try:
                self._conn.execute("INSTALL httpfs; LOAD httpfs;")
                self._conn.execute("INSTALL aws; LOAD aws;")
            except Exception:
                pass  # extensions may already be installed
        logger.info("duckdb_schema_ready", path=self._path)

    def load_parquet_dir(self, parquet_dir: str, table: str) -> int:
        """Bulk-insert all Parquet files from a directory into a table."""
        pattern = f"{parquet_dir}/**/*.parquet"
        with self._lock:
            result = self._conn.execute(
                f"INSERT INTO {table} SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)"
            )
        rows = result.fetchone()[0] if result else 0
        logger.info("parquet_loaded_to_duckdb", table=table, rows=rows)
        return rows

    def load_s3_parquet(self, s3_uri: str, table: str) -> int:
        """Load Parquet from S3 directly (requires httpfs extension)."""
        with self._lock:
            self._conn.execute("CALL load_aws_credentials()")
            result = self._conn.execute(
                f"INSERT INTO {table} SELECT * FROM read_parquet('{s3_uri}/**/*.parquet', hive_partitioning=true)"
            )
        rows = result.fetchone()[0] if result else 0
        logger.info("s3_parquet_loaded", s3_uri=s3_uri, table=table, rows=rows)
        return rows

    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        with self._lock:
            if params:
                return self._conn.execute(sql, params).df()
            return self._conn.execute(sql).df()

    def execute(self, sql: str, params: list | None = None) -> None:
        with self._lock:
            if params:
                self._conn.execute(sql, params)
            else:
                self._conn.execute(sql)

    def insert_df(self, table: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._conn.execute(f"INSERT INTO {table} SELECT * FROM df")

    # ------------------------------------------------------------------
    # Convenience query methods used by the dashboard / API
    # ------------------------------------------------------------------

    def active_vehicles(self, last_minutes: int = 5) -> pd.DataFrame:
        return self.query(
            f"""
            SELECT vehicle_id, MAX(timestamp) AS last_seen,
                   AVG(speed_ms) AS avg_speed_ms,
                   MAX(speed_ms) AS max_speed_ms
            FROM gps_events
            WHERE timestamp >= NOW() - INTERVAL '{last_minutes} minutes'
            GROUP BY vehicle_id
            ORDER BY last_seen DESC
        """
        )

    def recent_anomalies(self, limit: int = 50) -> pd.DataFrame:
        return self.query(
            f"""
            SELECT vehicle_id, timestamp, sensor_type, anomaly_score, features
            FROM anomaly_detections
            ORDER BY detected_at DESC
            LIMIT {limit}
        """
        )

    def speed_timeseries(self, vehicle_id: str, hours: int = 1) -> pd.DataFrame:
        return self.query(
            """
            SELECT timestamp, speed_ms, speed_kmh, heading_deg
            FROM gps_events
            WHERE vehicle_id = ? AND timestamp >= NOW() - INTERVAL ? HOUR
            ORDER BY timestamp
        """,
            [vehicle_id, hours],
        )

    def engine_health(self, hours: int = 6) -> pd.DataFrame:
        return self.query(
            f"""
            SELECT vehicle_id, timestamp,
                   engine_temp_celsius, oil_pressure_kpa,
                   battery_voltage, brake_wear_pct,
                   engine_overheating, low_battery
            FROM can_bus_events
            WHERE timestamp >= NOW() - INTERVAL '{hours} hours'
              AND (engine_overheating OR low_battery OR brake_wear_pct > 80)
            ORDER BY timestamp DESC
        """
        )

    def fleet_throughput_last_hour(self) -> pd.DataFrame:
        return self.query(
            """
            SELECT date_trunc('minute', timestamp) AS minute,
                   COUNT(*) AS event_count,
                   COUNT(DISTINCT vehicle_id) AS active_vehicles
            FROM gps_events
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
            GROUP BY 1
            ORDER BY 1
        """
        )

    def close(self) -> None:
        self._conn.close()
