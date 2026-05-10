"""
Isolation Forest-based anomaly detector for AV telemetry.

Used in two ways:
  1. Offline training on historical Parquet data.
  2. Online scoring via Spark UDF in the stream processing pipeline.
"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import structlog
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

logger = structlog.get_logger(__name__)

FEATURE_COLS = [
    "speed_ms",
    "accel_magnitude",
    "steering_angle_deg",
    "brake_pressure_pct",
    "engine_temp_celsius",
]


class TelemetryAnomalyDetector:
    """Wraps an sklearn IsolationForest + StandardScaler pipeline."""

    def __init__(self, contamination: float = 0.05, n_estimators: int = 100) -> None:
        self._pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "iforest",
                    IsolationForest(
                        n_estimators=n_estimators,
                        contamination=contamination,
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        )
        self._fitted = False

    def fit(self, df: pd.DataFrame) -> "TelemetryAnomalyDetector":
        X = self._extract_features(df)
        logger.info("anomaly_detector_training", samples=len(X))
        self._pipeline.fit(X)
        self._fitted = True
        logger.info("anomaly_detector_trained")
        return self

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Return -1 for anomaly, 1 for normal."""
        if not self._fitted:
            raise RuntimeError("Call fit() before predict()")
        X = self._extract_features(df)
        return self._pipeline.predict(X)

    def score_samples(self, df: pd.DataFrame) -> np.ndarray:
        """Return anomaly scores (lower = more anomalous)."""
        if not self._fitted:
            raise RuntimeError("Call fit() before score_samples()")
        X = self._extract_features(df)
        return self._pipeline.score_samples(X)

    def _extract_features(self, df: pd.DataFrame) -> np.ndarray:
        missing = [c for c in FEATURE_COLS if c not in df.columns]
        if missing:
            # Derive accel_magnitude if raw components are available
            if "accel_magnitude" in missing and {
                "accel_x",
                "accel_y",
                "accel_z",
            }.issubset(df.columns):
                df = df.copy()
                df["accel_magnitude"] = np.sqrt(
                    df["accel_x"] ** 2 + df["accel_y"] ** 2 + df["accel_z"] ** 2
                )
                missing.remove("accel_magnitude")
            if missing:
                raise ValueError(f"Missing required feature columns: {missing}")
        return df[FEATURE_COLS].fillna(0.0).values

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._pipeline, path)
        logger.info("anomaly_detector_saved", path=path)

    @classmethod
    def load(cls, path: str) -> "TelemetryAnomalyDetector":
        detector = cls()
        detector._pipeline = joblib.load(path)
        detector._fitted = True
        logger.info("anomaly_detector_loaded", path=path)
        return detector


def build_spark_udf(model_path: str):
    """Return a Spark pandas UDF for batch scoring in Structured Streaming."""
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DoubleType

    pipeline = joblib.load(model_path)

    @pandas_udf(DoubleType())
    def score_udf(*cols: pd.Series) -> pd.Series:
        df = pd.concat(cols, axis=1)
        df.columns = FEATURE_COLS
        scores = pipeline.score_samples(df.fillna(0.0).values)
        return pd.Series(scores)

    return score_udf, FEATURE_COLS


def train_from_parquet(
    parquet_dir: str, output_model_path: str, sample_frac: float = 0.1
) -> None:
    """Convenience function to train the anomaly detector from stored Parquet data."""
    import pyarrow.dataset as ds

    logger.info("loading_training_data", parquet_dir=parquet_dir)
    dataset = ds.dataset(parquet_dir, format="parquet")
    available_cols = [c for c in FEATURE_COLS if c in dataset.schema.names]
    table = dataset.to_table(columns=available_cols)
    df = table.to_pandas().dropna()

    if sample_frac < 1.0:
        df = df.sample(frac=sample_frac, random_state=42)

    logger.info("training_sample_size", rows=len(df))
    detector = TelemetryAnomalyDetector()
    detector.fit(df)
    detector.save(output_model_path)
