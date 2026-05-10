"""
Entry point for Spark Structured Streaming pipeline.

Reads from Kafka, applies transformations + aggregations, scores ML models,
and sinks results to S3 Parquet and DuckDB.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import structlog
import yaml
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))

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


def build_spark_session(spark_cfg: dict):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for key, value in spark_cfg.get("session", {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, bootstrap_servers: str, topic: str, streaming_cfg: dict):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", streaming_cfg.get("startingOffsets", "earliest"))
        .option("failOnDataLoss", streaming_cfg.get("failOnDataLoss", "false"))
        .option(
            "maxOffsetsPerTrigger",
            str(streaming_cfg.get("maxOffsetsPerTrigger", 10_000)),
        )
        .load()
    )


def write_stream_to_parquet(
    df, output_path: str, checkpoint_path: str, partition_cols: list[str]
):
    return (
        df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy(*partition_cols)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


def write_stream_to_console(df, truncate: bool = False):
    return (
        df.writeStream.format("console")
        .option("truncate", str(truncate).lower())
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )


def run(config: dict, debug: bool = False) -> None:
    from pyspark.sql import functions as F
    from src.processing.transformations import (
        GPS_SCHEMA,
        IMU_SCHEMA,
        CAN_BUS_SCHEMA,
        parse_kafka_stream,
        add_time_partitions,
        enrich_gps,
        enrich_imu,
        enrich_can_bus,
        filter_invalid,
        deduplicate,
    )
    from src.processing.aggregations import (
        vehicle_speed_summary,
        hard_braking_events,
        engine_health_summary,
        imu_vibration_summary,
    )

    spark_cfg_file = yaml.safe_load(open("config/spark_config.yaml"))
    spark = build_spark_session(spark_cfg_file)
    kafka_cfg = config["kafka"]
    storage_cfg = config["storage"]
    spark_cfg = config["spark"]

    bootstrap = kafka_cfg["bootstrap_servers"]
    topics = kafka_cfg["topics"]
    checkpoint_base = spark_cfg["checkpoint_dir"]
    s3_bucket = storage_cfg["s3_bucket"]
    s3_prefix = storage_cfg["s3_prefix"]

    def s3_path(sensor: str) -> str:
        return f"s3a://{s3_bucket}/{s3_prefix}/processed/{sensor}"

    def checkpoint(name: str) -> str:
        return f"{checkpoint_base}/{name}"

    queries = []

    # ------------------------------------------------------------------
    # GPS stream
    # ------------------------------------------------------------------
    gps_raw = read_kafka_stream(
        spark, bootstrap, topics["gps"], spark_cfg_file.get("kafka_source", {})
    )
    gps = (
        parse_kafka_stream(gps_raw, GPS_SCHEMA)
        .transform(lambda df: filter_invalid(df, "gps"))
        .transform(deduplicate)
        .transform(enrich_gps)
        .transform(add_time_partitions)
    )

    queries.append(
        write_stream_to_parquet(
            gps,
            s3_path("gps"),
            checkpoint("gps"),
            storage_cfg["parquet_partition_cols"],
        )
    )

    speed_agg = vehicle_speed_summary(gps, "1 minute")
    if debug:
        queries.append(write_stream_to_console(speed_agg))
    else:
        queries.append(
            write_stream_to_parquet(
                speed_agg.transform(add_time_partitions),
                s3_path("gps_speed_agg"),
                checkpoint("gps_speed_agg"),
                ["year", "month", "day"],
            )
        )

    # ------------------------------------------------------------------
    # IMU stream
    # ------------------------------------------------------------------
    imu_raw = read_kafka_stream(
        spark, bootstrap, topics["imu"], spark_cfg_file.get("kafka_source", {})
    )
    imu = (
        parse_kafka_stream(imu_raw, IMU_SCHEMA)
        .transform(lambda df: filter_invalid(df, "imu"))
        .transform(deduplicate)
        .transform(enrich_imu)
        .transform(add_time_partitions)
    )

    queries.append(
        write_stream_to_parquet(
            imu,
            s3_path("imu"),
            checkpoint("imu"),
            storage_cfg["parquet_partition_cols"],
        )
    )

    vibration_agg = imu_vibration_summary(imu, "1 minute")
    queries.append(
        write_stream_to_parquet(
            vibration_agg.transform(add_time_partitions),
            s3_path("imu_vibration_agg"),
            checkpoint("imu_vibration_agg"),
            ["year", "month", "day"],
        )
    )

    # ------------------------------------------------------------------
    # CAN bus stream (richest source — drives most downstream analytics)
    # ------------------------------------------------------------------
    can_raw = read_kafka_stream(
        spark, bootstrap, topics["can_bus"], spark_cfg_file.get("kafka_source", {})
    )
    can = (
        parse_kafka_stream(can_raw, CAN_BUS_SCHEMA)
        .transform(lambda df: filter_invalid(df, "can_bus"))
        .transform(deduplicate)
        .transform(enrich_can_bus)
        .transform(add_time_partitions)
    )

    queries.append(
        write_stream_to_parquet(
            can,
            s3_path("can_bus"),
            checkpoint("can_bus"),
            storage_cfg["parquet_partition_cols"],
        )
    )

    # Anomaly scoring via ML UDF (only if model exists)
    anomaly_model_path = config["ml"]["anomaly_detection"]["model_path"]
    if Path(anomaly_model_path).exists():
        from src.processing.ml.anomaly_detector import (
            build_spark_udf as build_anomaly_udf,
        )

        score_udf, feature_cols = build_anomaly_udf(anomaly_model_path)
        can_scored = can.withColumn(
            "anomaly_score",
            score_udf(*[F.col(c) for c in feature_cols if c in can.columns]),
        ).withColumn(
            "is_anomaly",
            F.col("anomaly_score") < -0.1,
        )
        queries.append(
            write_stream_to_parquet(
                can_scored.filter(F.col("is_anomaly")),
                s3_path("anomalies"),
                checkpoint("anomalies"),
                ["year", "month", "day", "vehicle_id"],
            )
        )

    queries.append(
        write_stream_to_parquet(
            hard_braking_events(can, "5 minutes").transform(add_time_partitions),
            s3_path("hard_braking_agg"),
            checkpoint("hard_braking_agg"),
            ["year", "month", "day"],
        )
    )

    queries.append(
        write_stream_to_parquet(
            engine_health_summary(can, "5 minutes").transform(add_time_partitions),
            s3_path("engine_health_agg"),
            checkpoint("engine_health_agg"),
            ["year", "month", "day"],
        )
    )

    logger.info("streaming_queries_started", count=len(queries))

    # Wait for all queries to terminate (or until interrupted)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("shutdown_requested")
        for q in queries:
            q.stop()
        spark.stop()


def main() -> None:
    import logging

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
    )
    parser = argparse.ArgumentParser(description="AV Telemetry Stream Processor")
    parser.add_argument("--config", default="config/app_config.yaml")
    parser.add_argument(
        "--debug", action="store_true", help="Print aggregations to console"
    )
    args = parser.parse_args()

    config = load_config(args.config)
    run(config, debug=args.debug)


if __name__ == "__main__":
    main()
