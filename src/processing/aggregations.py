"""Windowed aggregations over telemetry streams for real-time metrics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def vehicle_speed_summary(
    df: DataFrame, window_duration: str = "1 minute"
) -> DataFrame:
    """Rolling per-vehicle speed statistics over a tumbling window."""
    return (
        df.withWatermark("timestamp", "1 minute")
        .groupBy(
            F.window("timestamp", window_duration),
            "vehicle_id",
        )
        .agg(
            F.avg("speed_ms").alias("avg_speed_ms"),
            F.max("speed_ms").alias("max_speed_ms"),
            F.min("speed_ms").alias("min_speed_ms"),
            F.stddev("speed_ms").alias("speed_stddev"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "vehicle_id",
            F.round("avg_speed_ms", 3).alias("avg_speed_ms"),
            F.round("max_speed_ms", 3).alias("max_speed_ms"),
            F.round("min_speed_ms", 3).alias("min_speed_ms"),
            F.round("speed_stddev", 4).alias("speed_stddev"),
            "event_count",
        )
    )


def hard_braking_events(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """Count hard-braking events per vehicle per window."""
    return (
        df.withWatermark("timestamp", "2 minutes")
        .filter(F.col("hard_braking"))
        .groupBy(
            F.window("timestamp", window_duration),
            "vehicle_id",
        )
        .agg(
            F.count("*").alias("hard_brake_count"),
            F.max("brake_pressure_pct").alias("max_brake_pressure_pct"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "vehicle_id",
            "hard_brake_count",
            F.round("max_brake_pressure_pct", 2).alias("max_brake_pressure_pct"),
        )
    )


def engine_health_summary(
    df: DataFrame, window_duration: str = "5 minutes"
) -> DataFrame:
    """Per-vehicle engine health aggregation."""
    return (
        df.withWatermark("timestamp", "2 minutes")
        .groupBy(
            F.window("timestamp", window_duration),
            "vehicle_id",
        )
        .agg(
            F.avg("engine_temp_celsius").alias("avg_engine_temp"),
            F.max("engine_temp_celsius").alias("max_engine_temp"),
            F.avg("oil_pressure_kpa").alias("avg_oil_pressure"),
            F.min("battery_voltage").alias("min_battery_voltage"),
            F.sum(F.col("engine_overheating").cast("int")).alias("overheating_events"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "vehicle_id",
            F.round("avg_engine_temp", 2).alias("avg_engine_temp"),
            F.round("max_engine_temp", 2).alias("max_engine_temp"),
            F.round("avg_oil_pressure", 2).alias("avg_oil_pressure"),
            F.round("min_battery_voltage", 3).alias("min_battery_voltage"),
            "overheating_events",
        )
    )


def imu_vibration_summary(
    df: DataFrame, window_duration: str = "1 minute"
) -> DataFrame:
    """Aggregate IMU data to detect rough roads or suspension issues."""
    return (
        df.withWatermark("timestamp", "1 minute")
        .groupBy(
            F.window("timestamp", window_duration),
            "vehicle_id",
        )
        .agg(
            F.avg("accel_magnitude").alias("avg_accel_magnitude"),
            F.max("accel_magnitude").alias("max_accel_magnitude"),
            F.avg("gyro_magnitude").alias("avg_gyro_magnitude"),
            F.stddev("lateral_g").alias("lateral_g_stddev"),
            F.count("*").alias("sample_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "vehicle_id",
            F.round("avg_accel_magnitude", 4).alias("avg_accel_magnitude"),
            F.round("max_accel_magnitude", 4).alias("max_accel_magnitude"),
            F.round("avg_gyro_magnitude", 5).alias("avg_gyro_magnitude"),
            F.round("lateral_g_stddev", 4).alias("lateral_g_stddev"),
            "sample_count",
        )
    )


def fleet_throughput(df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """Overall fleet event throughput per window — useful for pipeline health monitoring."""
    return (
        df.withWatermark("timestamp", "1 minute")
        .groupBy(F.window("timestamp", window_duration), "sensor_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("vehicle_id").alias("active_vehicles"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "sensor_type",
            "event_count",
            "active_vehicles",
        )
    )
