"""
Alert rule engine and notification dispatcher.

Rules are evaluated against incoming telemetry snapshots.
Notifications go to a Slack webhook (configurable) and the in-memory alert buffer.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

import httpx
import structlog

from .metrics import TelemetrySnapshot, get_buffer

logger = structlog.get_logger(__name__)


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    name: str
    severity: Severity
    condition: Callable[[TelemetrySnapshot], bool]
    message_fn: Callable[[TelemetrySnapshot], str]
    cooldown_seconds: float = 300.0
    _last_fired: dict[str, float] = field(default_factory=dict)

    def evaluate(self, snapshot: TelemetrySnapshot) -> str | None:
        """Return alert message string if rule fires, else None."""
        if not self.condition(snapshot):
            return None
        key = snapshot.vehicle_id
        now = time.monotonic()
        if now - self._last_fired.get(key, 0) < self.cooldown_seconds:
            return None
        self._last_fired[key] = now
        return self.message_fn(snapshot)


# ---------------------------------------------------------------------------
# Built-in rule definitions
# ---------------------------------------------------------------------------

DEFAULT_RULES: list[AlertRule] = [
    AlertRule(
        name="anomaly_detected",
        severity=Severity.HIGH,
        condition=lambda s: s.is_anomaly,
        message_fn=lambda s: (
            f"[ANOMALY] Vehicle {s.vehicle_id} | sensor={s.sensor_type} | "
            f"score={s.anomaly_score:.3f} | ts={s.ts.isoformat()}"
        ),
        cooldown_seconds=60,
    ),
    AlertRule(
        name="engine_overheat",
        severity=Severity.CRITICAL,
        condition=lambda s: s.engine_temp > 105.0,
        message_fn=lambda s: (
            f"[ENGINE OVERHEAT] Vehicle {s.vehicle_id} | "
            f"temp={s.engine_temp:.1f}°C | ts={s.ts.isoformat()}"
        ),
        cooldown_seconds=120,
    ),
    AlertRule(
        name="high_speed",
        severity=Severity.MEDIUM,
        condition=lambda s: s.speed_ms > 25.0,
        message_fn=lambda s: (
            f"[HIGH SPEED] Vehicle {s.vehicle_id} | "
            f"speed={s.speed_ms * 3.6:.1f} km/h | ts={s.ts.isoformat()}"
        ),
        cooldown_seconds=300,
    ),
    AlertRule(
        name="high_acceleration",
        severity=Severity.MEDIUM,
        condition=lambda s: s.accel_magnitude > 15.0,
        message_fn=lambda s: (
            f"[HIGH ACCEL] Vehicle {s.vehicle_id} | "
            f"accel={s.accel_magnitude:.2f} m/s² | ts={s.ts.isoformat()}"
        ),
        cooldown_seconds=60,
    ),
]


class AlertEngine:
    def __init__(
        self,
        rules: list[AlertRule] | None = None,
        webhook_url: str | None = None,
    ) -> None:
        self._rules = rules or DEFAULT_RULES
        self._webhook_url = webhook_url
        self._buffer = get_buffer()
        self._fired_count = 0

    def evaluate(self, snapshot: TelemetrySnapshot) -> list[dict]:
        fired = []
        for rule in self._rules:
            message = rule.evaluate(snapshot)
            if message is None:
                continue
            alert = {
                "rule": rule.name,
                "severity": rule.severity.value,
                "vehicle_id": snapshot.vehicle_id,
                "message": message,
            }
            fired.append(alert)
            self._buffer.push_alert(alert)
            self._fired_count += 1
            logger.warning("alert_fired", **alert)
            if self._webhook_url:
                self._send_slack(message, rule.severity)
        return fired

    def _send_slack(self, message: str, severity: Severity) -> None:
        emoji = {"low": "ℹ️", "medium": "⚠️", "high": "🚨", "critical": "🔥"}.get(
            severity.value, "⚠️"
        )
        payload = {"text": f"{emoji} {message}"}
        try:
            httpx.post(self._webhook_url, json=payload, timeout=5.0)
        except Exception as exc:
            logger.error("slack_webhook_failed", error=str(exc))

    @property
    def fired_count(self) -> int:
        return self._fired_count
