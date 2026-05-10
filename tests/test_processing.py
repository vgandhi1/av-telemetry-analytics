"""Tests for stream processing transformations and ML models."""
import numpy as np
import pandas as pd
import pytest

from src.processing.ml.anomaly_detector import TelemetryAnomalyDetector, FEATURE_COLS


class TestAnomalyDetector:
    @pytest.fixture
    def normal_data(self):
        np.random.seed(42)
        n = 500
        return pd.DataFrame({
            "speed_ms": np.random.uniform(0, 20, n),
            "accel_magnitude": np.random.uniform(9.5, 10.5, n),
            "steering_angle_deg": np.random.uniform(-30, 30, n),
            "brake_pressure_pct": np.random.uniform(0, 30, n),
            "engine_temp_celsius": np.random.uniform(80, 95, n),
        })

    @pytest.fixture
    def anomalous_data(self):
        return pd.DataFrame({
            "speed_ms": [100.0],            # implausibly high
            "accel_magnitude": [50.0],      # ~5g
            "steering_angle_deg": [400.0],  # beyond physical limits
            "brake_pressure_pct": [99.0],
            "engine_temp_celsius": [200.0], # overheating
        })

    def test_fit_predict(self, normal_data):
        detector = TelemetryAnomalyDetector(contamination=0.05)
        detector.fit(normal_data)
        preds = detector.predict(normal_data)
        assert set(preds).issubset({-1, 1})
        # Contamination=0.05: ~95% should be normal (1)
        assert (preds == 1).mean() > 0.90

    def test_anomalous_scores_lower(self, normal_data, anomalous_data):
        detector = TelemetryAnomalyDetector()
        detector.fit(normal_data)
        normal_score = detector.score_samples(normal_data).mean()
        anomaly_score = detector.score_samples(anomalous_data)[0]
        assert anomaly_score < normal_score

    def test_save_load(self, normal_data, tmp_path):
        model_path = str(tmp_path / "anomaly_detector.pkl")
        detector = TelemetryAnomalyDetector()
        detector.fit(normal_data)
        detector.save(model_path)

        loaded = TelemetryAnomalyDetector.load(model_path)
        original_preds = detector.predict(normal_data)
        loaded_preds = loaded.predict(normal_data)
        assert np.array_equal(original_preds, loaded_preds)

    def test_predict_before_fit_raises(self, normal_data):
        detector = TelemetryAnomalyDetector()
        with pytest.raises(RuntimeError, match="fit"):
            detector.predict(normal_data)

    def test_missing_columns_raises(self):
        detector = TelemetryAnomalyDetector()
        bad_df = pd.DataFrame({"speed_ms": [5.0]})
        with pytest.raises(ValueError, match="Missing"):
            bad_df_full = pd.DataFrame({c: [0.0] for c in FEATURE_COLS})
            detector.fit(bad_df_full)
            detector.predict(bad_df)
