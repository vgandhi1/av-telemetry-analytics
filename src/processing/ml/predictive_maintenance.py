"""
Gradient-boosted classifier for predicting maintenance needs
within a configurable time horizon (default 24 hours).

Features are derived from CAN-bus and system health telemetry.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import structlog
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score

logger = structlog.get_logger(__name__)

FEATURE_COLS = [
    "engine_temp_celsius",
    "oil_pressure_kpa",
    "battery_voltage",
    "odometer_km",
    "brake_wear_pct",
    "avg_speed_ms_1h",
    "hard_brake_count_1h",
    "engine_overheating_count_1h",
]

LABEL_COL = "needs_maintenance_24h"


class MaintenancePredictor:
    def __init__(
        self,
        n_estimators: int = 200,
        learning_rate: float = 0.05,
        max_depth: int = 4,
    ) -> None:
        self._pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("gbc", GradientBoostingClassifier(
                n_estimators=n_estimators,
                learning_rate=learning_rate,
                max_depth=max_depth,
                random_state=42,
            )),
        ])
        self._fitted = False

    def fit(self, df: pd.DataFrame) -> "MaintenancePredictor":
        X, y = self._prepare(df)
        logger.info("maintenance_predictor_training", samples=len(X), positive_rate=y.mean())
        scores = cross_val_score(self._pipeline, X, y, cv=5, scoring="roc_auc")
        logger.info("cv_roc_auc", mean=scores.mean(), std=scores.std())
        self._pipeline.fit(X, y)
        self._fitted = True
        return self

    def predict_proba(self, df: pd.DataFrame) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("Call fit() before predict_proba()")
        X = self._extract_features(df)
        return self._pipeline.predict_proba(X)[:, 1]

    def predict(self, df: pd.DataFrame, threshold: float = 0.5) -> np.ndarray:
        return (self.predict_proba(df) >= threshold).astype(int)

    def _prepare(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        missing_features = [c for c in FEATURE_COLS if c not in df.columns]
        if missing_features:
            raise ValueError(f"Missing training feature columns: {missing_features}")
        if LABEL_COL not in df.columns:
            raise ValueError(f"Missing label column: {LABEL_COL}")
        X = df[FEATURE_COLS].fillna(0.0).values
        y = df[LABEL_COL].values
        return X, y

    def _extract_features(self, df: pd.DataFrame) -> np.ndarray:
        available = [c for c in FEATURE_COLS if c in df.columns]
        result = pd.DataFrame(index=df.index)
        for col in FEATURE_COLS:
            result[col] = df[col] if col in df.columns else 0.0
        return result.fillna(0.0).values

    def feature_importance(self) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        gbc = self._pipeline.named_steps["gbc"]
        return pd.DataFrame({
            "feature": FEATURE_COLS,
            "importance": gbc.feature_importances_,
        }).sort_values("importance", ascending=False)

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._pipeline, path)
        logger.info("maintenance_predictor_saved", path=path)

    @classmethod
    def load(cls, path: str) -> "MaintenancePredictor":
        predictor = cls()
        predictor._pipeline = joblib.load(path)
        predictor._fitted = True
        logger.info("maintenance_predictor_loaded", path=path)
        return predictor


def build_spark_udf(model_path: str):
    """Return a Spark pandas UDF for streaming maintenance risk scoring."""
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DoubleType

    pipeline = joblib.load(model_path)

    @pandas_udf(DoubleType())
    def maintenance_risk_udf(*cols: pd.Series) -> pd.Series:
        df = pd.concat(cols, axis=1)
        df.columns = FEATURE_COLS
        proba = pipeline.predict_proba(df.fillna(0.0).values)[:, 1]
        return pd.Series(proba)

    return maintenance_risk_udf, FEATURE_COLS
