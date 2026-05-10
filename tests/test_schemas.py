"""Tests for telemetry event schemas and vehicle connector."""
import pytest
from datetime import datetime, timezone

from src.ingestion.connectors.schemas import (
    GPSEvent,
    IMUEvent,
    CANBusEvent,
    SensorType,
)
from src.ingestion.connectors.vehicle_connector import VehicleConnector


@pytest.fixture
def ts():
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class TestGPSEvent:
    def test_valid(self, ts):
        e = GPSEvent(
            vehicle_id="ZX-001",
            timestamp=ts,
            latitude=37.386,
            longitude=-122.083,
            altitude_m=10.0,
            speed_ms=5.0,
            heading_deg=90.0,
            accuracy_m=1.2,
            num_satellites=12,
        )
        assert e.sensor_type == SensorType.GPS
        assert e.vehicle_id == "ZX-001"

    def test_invalid_vehicle_id(self, ts):
        with pytest.raises(ValueError, match="ZX-"):
            GPSEvent(
                vehicle_id="BAD-001",
                timestamp=ts,
                latitude=37.386,
                longitude=-122.083,
                altitude_m=0,
                speed_ms=0,
                heading_deg=0,
                accuracy_m=1,
                num_satellites=8,
            )

    def test_invalid_lat(self, ts):
        with pytest.raises(ValueError):
            GPSEvent(
                vehicle_id="ZX-001",
                timestamp=ts,
                latitude=91.0,
                longitude=0.0,
                altitude_m=0,
                speed_ms=0,
                heading_deg=0,
                accuracy_m=1,
                num_satellites=8,
            )

    def test_kafka_serialization(self, ts):
        e = GPSEvent(
            vehicle_id="ZX-002",
            timestamp=ts,
            latitude=10.0,
            longitude=20.0,
            altitude_m=5.0,
            speed_ms=3.0,
            heading_deg=45.0,
            accuracy_m=0.8,
            num_satellites=10,
        )
        assert e.kafka_key() == b"ZX-002"
        payload = e.to_kafka_value()
        assert b"ZX-002" in payload
        assert b"gps" in payload


class TestCANBusEvent:
    def test_hard_braking_flag(self, ts):
        e = CANBusEvent(
            vehicle_id="ZX-001",
            timestamp=ts,
            speed_ms=10.0,
            steering_angle_deg=0.0,
            throttle_pct=0.0,
            brake_pressure_pct=85.0,
            gear="D",
            engine_rpm=1500.0,
            engine_temp_celsius=90.0,
            oil_pressure_kpa=300.0,
            battery_voltage=12.3,
            odometer_km=10000.0,
            brake_wear_pct=40.0,
        )
        assert e.brake_pressure_pct == 85.0


class TestVehicleConnector:
    def test_stream_generates_events(self):
        connector = VehicleConnector("ZX-001", seed=42)
        events = []
        for event in connector.stream(hz=1000):
            events.append(event)
            if len(events) >= 20:
                break
        assert len(events) == 20

    def test_gps_event_shape(self):
        connector = VehicleConnector("ZX-001", seed=0)
        gps = connector.gps_event()
        assert gps.vehicle_id == "ZX-001"
        assert -90 <= gps.latitude <= 90
        assert -180 <= gps.longitude <= 180
        assert gps.speed_ms >= 0

    def test_sensor_type_variety(self):
        connector = VehicleConnector("ZX-001", seed=1)
        events = []
        for e in connector.stream(hz=1000):
            events.append(e)
            if len(events) >= 50:
                break
        types = {e.sensor_type for e in events}
        assert SensorType.GPS in types
        assert SensorType.IMU in types
        assert SensorType.CAN_BUS in types
