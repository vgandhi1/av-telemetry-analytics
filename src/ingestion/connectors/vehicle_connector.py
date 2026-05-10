"""Simulated vehicle telemetry connector.

In production this would wrap a real vehicle data bus SDK (e.g., ROS2 bridge,
proprietary CAN-over-TCP adapter). Here it generates realistic synthetic data
for local development and integration testing.
"""
from __future__ import annotations

import math
import random
import time
from datetime import datetime, timezone
from typing import Iterator

from .schemas import (
    CANBusEvent,
    CameraEvent,
    GPSEvent,
    IMUEvent,
    LidarEvent,
    SystemHealthEvent,
    TelemetryBase,
)


class VehicleConnector:
    """Generates a continuous stream of telemetry events for a single vehicle."""

    def __init__(self, vehicle_id: str, seed: int | None = None) -> None:
        self.vehicle_id = vehicle_id
        self._rng = random.Random(seed)
        self._seq = 0
        # Simulated vehicle state
        self._lat = 37.3861 + self._rng.uniform(-0.05, 0.05)
        self._lon = -122.0839 + self._rng.uniform(-0.05, 0.05)
        self._speed_ms = 0.0
        self._heading_deg = self._rng.uniform(0, 360)
        self._odometer_km = self._rng.uniform(0, 50000)
        self._engine_temp = 85.0

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _base_kwargs(self) -> dict:
        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": datetime.now(timezone.utc),
            "sequence_number": self._next_seq(),
        }

    def _update_state(self) -> None:
        """Advance simulated vehicle state one tick."""
        self._speed_ms = max(0.0, self._speed_ms + self._rng.uniform(-1.0, 1.5))
        self._speed_ms = min(self._speed_ms, 30.0)
        self._heading_deg = (self._heading_deg + self._rng.uniform(-5, 5)) % 360
        delta = self._speed_ms * 0.1 / 111_000
        self._lat += delta * math.cos(math.radians(self._heading_deg))
        self._lon += delta * math.sin(math.radians(self._heading_deg))
        self._odometer_km += self._speed_ms * 0.1 / 1000
        self._engine_temp = max(70.0, min(120.0, self._engine_temp + self._rng.uniform(-0.5, 0.5)))

    def gps_event(self) -> GPSEvent:
        return GPSEvent(
            **self._base_kwargs(),
            latitude=round(self._lat, 6),
            longitude=round(self._lon, 6),
            altitude_m=round(self._rng.uniform(0, 50), 2),
            speed_ms=round(self._speed_ms, 2),
            heading_deg=round(self._heading_deg, 2),
            accuracy_m=round(self._rng.uniform(0.5, 3.0), 2),
            num_satellites=self._rng.randint(8, 15),
        )

    def imu_event(self) -> IMUEvent:
        g = 9.81
        return IMUEvent(
            **self._base_kwargs(),
            accel_x=round(self._rng.gauss(0, 0.5), 4),
            accel_y=round(self._rng.gauss(0, 0.3), 4),
            accel_z=round(g + self._rng.gauss(0, 0.1), 4),
            gyro_x=round(self._rng.gauss(0, 0.02), 5),
            gyro_y=round(self._rng.gauss(0, 0.02), 5),
            gyro_z=round(self._rng.gauss(0, 0.05), 5),
            roll_deg=round(self._rng.gauss(0, 1), 3),
            pitch_deg=round(self._rng.gauss(0, 1), 3),
            yaw_deg=round(self._heading_deg, 3),
            temperature_celsius=round(self._rng.uniform(20, 40), 1),
        )

    def can_bus_event(self) -> CANBusEvent:
        return CANBusEvent(
            **self._base_kwargs(),
            speed_ms=round(self._speed_ms, 2),
            steering_angle_deg=round(self._rng.gauss(0, 15), 2),
            throttle_pct=round(max(0, min(100, self._rng.uniform(0, 40))), 2),
            brake_pressure_pct=round(max(0, min(100, self._rng.uniform(0, 20))), 2),
            gear=self._rng.choice(["D", "D", "D", "N", "P"]),
            engine_rpm=round(self._rng.uniform(800, 4000), 0),
            engine_temp_celsius=round(self._engine_temp, 1),
            oil_pressure_kpa=round(self._rng.uniform(200, 450), 1),
            battery_voltage=round(self._rng.uniform(11.8, 12.6), 2),
            odometer_km=round(self._odometer_km, 3),
            brake_wear_pct=round(self._rng.uniform(10, 90), 1),
            turn_signal=self._rng.choice(["off", "off", "off", "left", "right"]),
        )

    def lidar_event(self) -> LidarEvent:
        import uuid
        obstacles = self._rng.randint(0, 8)
        return LidarEvent(
            **self._base_kwargs(),
            scan_id=str(uuid.uuid4()),
            point_count=self._rng.randint(50_000, 130_000),
            range_min_m=round(self._rng.uniform(0.1, 1.0), 2),
            range_max_m=round(self._rng.uniform(80, 120), 1),
            field_of_view_deg=360.0,
            scan_duration_ms=round(self._rng.uniform(90, 110), 2),
            obstacles_detected=obstacles,
            closest_obstacle_m=round(self._rng.uniform(2, 30), 2) if obstacles > 0 else None,
        )

    def camera_event(self, camera_id: str = "front") -> CameraEvent:
        import uuid
        frame_id = str(uuid.uuid4())
        return CameraEvent(
            **self._base_kwargs(),
            camera_id=camera_id,
            frame_id=frame_id,
            resolution_width=1920,
            resolution_height=1080,
            fps=30.0,
            exposure_ms=round(self._rng.uniform(4, 33), 2),
            s3_frame_uri=f"s3://av-telemetry-data/frames/{self.vehicle_id}/{frame_id}.jpg",
            objects_detected=self._rng.randint(0, 20),
            lanes_detected=self._rng.random() > 0.1,
        )

    def system_health_event(self) -> SystemHealthEvent:
        return SystemHealthEvent(
            **self._base_kwargs(),
            cpu_usage_pct=round(self._rng.uniform(20, 80), 1),
            memory_usage_pct=round(self._rng.uniform(30, 70), 1),
            disk_usage_pct=round(self._rng.uniform(10, 60), 1),
            gpu_usage_pct=round(self._rng.uniform(40, 95), 1),
            network_rx_mbps=round(self._rng.uniform(1, 50), 2),
            network_tx_mbps=round(self._rng.uniform(0.5, 10), 2),
            process_count=self._rng.randint(80, 200),
            uptime_seconds=self._seq * 0.1,
            av_software_version="2.4.1",
            error_count_last_minute=self._rng.choices([0, 1, 2], weights=[90, 8, 2])[0],
        )

    def stream(self, hz: float = 10.0) -> Iterator[TelemetryBase]:
        """Yield mixed telemetry events at the given frequency."""
        interval = 1.0 / hz
        camera_ids = ["front", "rear", "left", "right"]
        tick = 0
        while True:
            self._update_state()
            yield self.gps_event()
            yield self.imu_event()
            yield self.can_bus_event()
            if tick % 3 == 0:
                yield self.lidar_event()
            if tick % 2 == 0:
                for cam in camera_ids:
                    yield self.camera_event(cam)
            if tick % 10 == 0:
                yield self.system_health_event()
            tick += 1
            time.sleep(interval)
