"""PySpark transformations applied to raw telemetry streams."""

from __future__ import annotations


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Shared Kafka envelope schema
# ---------------------------------------------------------------------------

KAFKA_ENVELOPE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("sequence_number", IntegerType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

GPS_SCHEMA = StructType(
    KAFKA_ENVELOPE_SCHEMA.fields
    + [
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("altitude_m", DoubleType(), True),
        StructField("speed_ms", DoubleType(), False),
        StructField("heading_deg", DoubleType(), False),
        StructField("accuracy_m", DoubleType(), True),
        StructField("num_satellites", IntegerType(), True),
    ]
)

IMU_SCHEMA = StructType(
    KAFKA_ENVELOPE_SCHEMA.fields
    + [
        StructField("accel_x", DoubleType(), False),
        StructField("accel_y", DoubleType(), False),
        StructField("accel_z", DoubleType(), False),
        StructField("gyro_x", DoubleType(), False),
        StructField("gyro_y", DoubleType(), False),
        StructField("gyro_z", DoubleType(), False),
        StructField("roll_deg", DoubleType(), True),
        StructField("pitch_deg", DoubleType(), True),
        StructField("yaw_deg", DoubleType(), True),
        StructField("temperature_celsius", DoubleType(), True),
    ]
)

CAN_BUS_SCHEMA = StructType(
    KAFKA_ENVELOPE_SCHEMA.fields
    + [
        StructField("speed_ms", DoubleType(), False),
        StructField("steering_angle_deg", DoubleType(), False),
        StructField("throttle_pct", DoubleType(), True),
        StructField("brake_pressure_pct", DoubleType(), True),
        StructField("gear", StringType(), True),
        StructField("engine_rpm", DoubleType(), True),
        StructField("engine_temp_celsius", DoubleType(), True),
        StructField("oil_pressure_kpa", DoubleType(), True),
        StructField("battery_voltage", DoubleType(), True),
        StructField("odometer_km", DoubleType(), True),
        StructField("brake_wear_pct", DoubleType(), True),
        StructField("turn_signal", StringType(), True),
    ]
)

SCHEMA_MAP: dict[str, StructType] = {
    "av.telemetry.gps": GPS_SCHEMA,
    "av.telemetry.imu": IMU_SCHEMA,
    "av.telemetry.can_bus": CAN_BUS_SCHEMA,
}


def parse_kafka_stream(df: DataFrame, schema: StructType) -> DataFrame:
    """Deserialize JSON value bytes and enforce schema."""
    return (
        df.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
        )
        .select("topic", "partition", "offset", "data.*")
        .filter(F.col("vehicle_id").isNotNull())
    )


def add_time_partitions(df: DataFrame, ts_col: str = "timestamp") -> DataFrame:
    """Append year/month/day/hour partition columns derived from timestamp."""
    return (
        df.withColumn("year", F.year(ts_col))
        .withColumn("month", F.month(ts_col))
        .withColumn("day", F.dayofmonth(ts_col))
        .withColumn("hour", F.hour(ts_col))
    )


def enrich_gps(df: DataFrame) -> DataFrame:
    """Derive speed_kmh and movement status from GPS events."""
    return (
        df.withColumn("speed_kmh", F.round(F.col("speed_ms") * 3.6, 2))
        .withColumn("is_moving", F.col("speed_ms") > 0.5)
        .withColumn(
            "processing_lag_ms",
            (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("timestamp"))
            * 1000,
        )
    )


def enrich_imu(df: DataFrame) -> DataFrame:
    """Compute acceleration magnitude and lateral G-force."""
    return (
        df.withColumn(
            "accel_magnitude",
            F.round(
                F.sqrt(
                    F.col("accel_x") ** 2
                    + F.col("accel_y") ** 2
                    + F.col("accel_z") ** 2
                ),
                4,
            ),
        )
        .withColumn(
            "lateral_g",
            F.round(F.col("accel_y") / 9.81, 4),
        )
        .withColumn(
            "longitudinal_g",
            F.round(F.col("accel_x") / 9.81, 4),
        )
        .withColumn(
            "gyro_magnitude",
            F.round(
                F.sqrt(
                    F.col("gyro_x") ** 2 + F.col("gyro_y") ** 2 + F.col("gyro_z") ** 2
                ),
                5,
            ),
        )
    )


def enrich_can_bus(df: DataFrame) -> DataFrame:
    """Derive speed_kmh, hard-braking flag, and engine temp warning."""
    return (
        df.withColumn("speed_kmh", F.round(F.col("speed_ms") * 3.6, 2))
        .withColumn("hard_braking", F.col("brake_pressure_pct") > 70.0)
        .withColumn("engine_overheating", F.col("engine_temp_celsius") > 105.0)
        .withColumn("low_battery", F.col("battery_voltage") < 11.9)
        .withColumn("high_wear_brakes", F.col("brake_wear_pct") > 80.0)
    )


def deduplicate(
    df: DataFrame, key_col: str = "event_id", watermark_col: str = "timestamp"
) -> DataFrame:
    """Drop duplicate events within the watermark window."""
    return df.dropDuplicates([key_col])


def filter_invalid(df: DataFrame, sensor_type: str) -> DataFrame:
    """Drop records that fail basic sanity checks per sensor type."""
    if sensor_type == "gps":
        return df.filter(
            F.col("latitude").between(-90, 90)
            & F.col("longitude").between(-180, 180)
            & (F.col("speed_ms") >= 0)
        )
    if sensor_type == "imu":
        return df.filter(F.col("accel_magnitude") < 100)  # implausible if > 10g
    if sensor_type == "can_bus":
        return df.filter(
            F.col("engine_rpm").between(0, 8000)
            & F.col("engine_temp_celsius").between(-40, 200)
        )
    return df
