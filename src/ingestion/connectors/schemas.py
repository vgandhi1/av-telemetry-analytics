"""Pydantic schemas for all AV telemetry event types."""

from __future__ import annotations

import uuid
from enum import Enum
from typing import Optional
from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class SensorType(str, Enum):
    GPS = "gps"
    IMU = "imu"
    LIDAR = "lidar"
    CAMERA = "camera"
    CAN_BUS = "can_bus"
    SYSTEM_HEALTH = "system_health"


class TelemetryBase(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    vehicle_id: str
    sensor_type: SensorType
    timestamp: datetime
    sequence_number: int = 0
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("vehicle_id")
    @classmethod
    def validate_vehicle_id(cls, v: str) -> str:
        if not v.startswith("ZX-"):
            raise ValueError("vehicle_id must start with 'ZX-'")
        return v

    def kafka_key(self) -> bytes:
        return self.vehicle_id.encode("utf-8")

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class GPSEvent(TelemetryBase):
    sensor_type: SensorType = SensorType.GPS
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    altitude_m: float
    speed_ms: float = Field(..., ge=0.0)
    heading_deg: float = Field(..., ge=0.0, lt=360.0)
    accuracy_m: float = Field(..., ge=0.0)
    num_satellites: int = Field(..., ge=0)


class IMUEvent(TelemetryBase):
    sensor_type: SensorType = SensorType.IMU
    accel_x: float  # m/s²
    accel_y: float
    accel_z: float
    gyro_x: float  # rad/s
    gyro_y: float
    gyro_z: float
    roll_deg: float
    pitch_deg: float
    yaw_deg: float
    temperature_celsius: float


class CANBusEvent(TelemetryBase):
    sensor_type: SensorType = SensorType.CAN_BUS
    speed_ms: float = Field(..., ge=0.0)
    steering_angle_deg: float = Field(..., ge=-540.0, le=540.0)
    throttle_pct: float = Field(..., ge=0.0, le=100.0)
    brake_pressure_pct: float = Field(..., ge=0.0, le=100.0)
    gear: str
    engine_rpm: float = Field(..., ge=0.0)
    engine_temp_celsius: float
    oil_pressure_kpa: float = Field(..., ge=0.0)
    battery_voltage: float = Field(..., ge=0.0)
    odometer_km: float = Field(..., ge=0.0)
    brake_wear_pct: float = Field(..., ge=0.0, le=100.0)
    turn_signal: Optional[str] = None  # left | right | off


class LidarEvent(TelemetryBase):
    sensor_type: SensorType = SensorType.LIDAR
    scan_id: str
    point_count: int = Field(..., ge=0)
    range_min_m: float
    range_max_m: float
    field_of_view_deg: float
    scan_duration_ms: float
    obstacles_detected: int = 0
    closest_obstacle_m: Optional[float] = None


class CameraEvent(TelemetryBase):
    """Metadata only — raw frames go to S3 directly."""

    sensor_type: SensorType = SensorType.CAMERA
    camera_id: str
    frame_id: str
    resolution_width: int
    resolution_height: int
    fps: float
    exposure_ms: float
    s3_frame_uri: Optional[str] = None
    objects_detected: int = 0
    lanes_detected: bool = False


class SystemHealthEvent(TelemetryBase):
    sensor_type: SensorType = SensorType.SYSTEM_HEALTH
    cpu_usage_pct: float = Field(..., ge=0.0, le=100.0)
    memory_usage_pct: float = Field(..., ge=0.0, le=100.0)
    disk_usage_pct: float = Field(..., ge=0.0, le=100.0)
    gpu_usage_pct: Optional[float] = Field(None, ge=0.0, le=100.0)
    network_rx_mbps: float = Field(..., ge=0.0)
    network_tx_mbps: float = Field(..., ge=0.0)
    process_count: int
    uptime_seconds: float
    av_software_version: str
    error_count_last_minute: int = 0


TOPIC_TO_SCHEMA: dict[str, type[TelemetryBase]] = {
    "av.telemetry.gps": GPSEvent,
    "av.telemetry.imu": IMUEvent,
    "av.telemetry.can_bus": CANBusEvent,
    "av.telemetry.lidar": LidarEvent,
    "av.telemetry.camera": CameraEvent,
    "av.telemetry.system": SystemHealthEvent,
}
