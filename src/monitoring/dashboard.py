"""
Real-time monitoring dashboard built with Streamlit.

Run:  streamlit run src/monitoring/dashboard.py --server.port 8080
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
load_dotenv()

from src.monitoring.metrics import get_buffer  # noqa: E402

st.set_page_config(
    page_title="AV Telemetry Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
<style>
.metric-card {
    background: #1e1e2e; border-radius: 10px;
    padding: 16px; margin: 4px; text-align: center;
}
.alert-critical { border-left: 4px solid #ff4b4b; padding: 8px 12px; }
.alert-high     { border-left: 4px solid #ffa500; padding: 8px 12px; }
.alert-medium   { border-left: 4px solid #ffd700; padding: 8px 12px; }
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# DuckDB connection (cached per session)
# ---------------------------------------------------------------------------


@st.cache_resource
def get_db():
    from src.storage.duckdb_manager import DuckDBManager

    db_path = os.environ.get("DUCKDB_PATH", "./data/telemetry.duckdb")
    return DuckDBManager(db_path=db_path, read_only=True)


def safe_query(sql: str, params=None) -> pd.DataFrame:
    try:
        return get_db().query(sql, params)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🚗 AV Telemetry")
    st.markdown("---")
    refresh_interval = st.slider("Auto-refresh (sec)", 5, 60, 10)
    time_window = st.selectbox(
        "Time window", ["5 min", "15 min", "1 hour", "6 hours"], index=1
    )
    window_minutes = {"5 min": 5, "15 min": 15, "1 hour": 60, "6 hours": 360}[
        time_window
    ]

    selected_vehicle = st.selectbox(
        "Focus vehicle",
        options=["All"] + [f"ZX-{i:03d}" for i in range(1, 11)],
    )
    st.markdown("---")
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")


# ---------------------------------------------------------------------------
# Header KPIs
# ---------------------------------------------------------------------------

st.title("AV Fleet Telemetry Dashboard")

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

active_df = safe_query(
    f"""
    SELECT COUNT(DISTINCT vehicle_id) AS cnt FROM gps_events
    WHERE timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
"""
)
active_count = int(active_df["cnt"].iloc[0]) if not active_df.empty else 0

anomaly_df = safe_query(
    f"""
    SELECT COUNT(*) AS cnt FROM anomaly_detections
    WHERE detected_at >= NOW() - INTERVAL '{window_minutes} minutes'
"""
)
anomaly_count = int(anomaly_df["cnt"].iloc[0]) if not anomaly_df.empty else 0

avg_speed_df = safe_query(
    f"""
    SELECT ROUND(AVG(speed_ms) * 3.6, 1) AS avg_kmh FROM gps_events
    WHERE timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
"""
)
avg_speed = float(avg_speed_df["avg_kmh"].iloc[0]) if not avg_speed_df.empty else 0.0

overheat_df = safe_query(
    f"""
    SELECT COUNT(DISTINCT vehicle_id) AS cnt FROM can_bus_events
    WHERE engine_overheating AND timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
"""
)
overheat_count = int(overheat_df["cnt"].iloc[0]) if not overheat_df.empty else 0

event_df = safe_query(
    f"""
    SELECT COUNT(*) AS cnt FROM gps_events
    WHERE timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
"""
)
event_count = int(event_df["cnt"].iloc[0]) if not event_df.empty else 0

kpi1.metric("Active Vehicles", active_count)
kpi2.metric("Anomalies Detected", anomaly_count, delta=None)
kpi3.metric("Avg Fleet Speed", f"{avg_speed} km/h")
kpi4.metric("Overheating Vehicles", overheat_count)
kpi5.metric("Events Ingested", f"{event_count:,}")

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 1: Vehicle Map + Speed Timeseries
# ---------------------------------------------------------------------------

col_map, col_speed = st.columns([1, 1])

with col_map:
    st.subheader("Vehicle Locations")
    loc_df = safe_query(
        f"""
        SELECT vehicle_id,
               LAST(latitude ORDER BY timestamp)  AS lat,
               LAST(longitude ORDER BY timestamp) AS lon,
               LAST(speed_ms ORDER BY timestamp) * 3.6  AS speed_kmh
        FROM gps_events
        WHERE timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
        GROUP BY vehicle_id
    """
    )
    if not loc_df.empty:
        fig_map = px.scatter_mapbox(
            loc_df,
            lat="lat",
            lon="lon",
            hover_name="vehicle_id",
            color="speed_kmh",
            size_max=12,
            zoom=12,
            color_continuous_scale="RdYlGn_r",
            mapbox_style="carto-positron",
            title="",
        )
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=350)
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("No vehicle location data in the selected window.")

with col_speed:
    st.subheader("Speed Timeseries")
    vehicle_filter = (
        "" if selected_vehicle == "All" else f"AND vehicle_id = '{selected_vehicle}'"
    )
    speed_ts = safe_query(
        f"""
        SELECT date_trunc('minute', timestamp) AS minute,
               vehicle_id,
               AVG(speed_ms) * 3.6 AS avg_speed_kmh
        FROM gps_events
        WHERE timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
        {vehicle_filter}
        GROUP BY 1, 2
        ORDER BY 1
    """
    )
    if not speed_ts.empty:
        fig_speed = px.line(
            speed_ts,
            x="minute",
            y="avg_speed_kmh",
            color="vehicle_id",
            title="",
            labels={"avg_speed_kmh": "Speed (km/h)", "minute": "Time"},
        )
        fig_speed.update_layout(height=350, showlegend=True)
        st.plotly_chart(fig_speed, use_container_width=True)
    else:
        st.info("No speed data in the selected window.")

# ---------------------------------------------------------------------------
# Row 2: Engine Health + Anomaly Timeline
# ---------------------------------------------------------------------------

col_engine, col_anomaly = st.columns([1, 1])

with col_engine:
    st.subheader("Engine Temperature")
    eng_df = safe_query(
        f"""
        SELECT date_trunc('minute', timestamp) AS minute,
               vehicle_id,
               AVG(engine_temp_celsius) AS avg_temp,
               MAX(engine_temp_celsius) AS max_temp
        FROM can_bus_events
        WHERE timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
        {vehicle_filter if 'vehicle_filter' in dir() else ''}
        GROUP BY 1, 2
        ORDER BY 1
    """
    )
    if not eng_df.empty:
        fig_eng = go.Figure()
        for vid in eng_df["vehicle_id"].unique():
            vdf = eng_df[eng_df["vehicle_id"] == vid]
            fig_eng.add_trace(
                go.Scatter(
                    x=vdf["minute"],
                    y=vdf["avg_temp"],
                    mode="lines",
                    name=vid,
                )
            )
        fig_eng.add_hline(
            y=105,
            line_dash="dash",
            line_color="red",
            annotation_text="Overheat threshold",
        )
        fig_eng.update_layout(
            height=300,
            yaxis_title="Temperature (°C)",
            xaxis_title="Time",
        )
        st.plotly_chart(fig_eng, use_container_width=True)
    else:
        st.info("No engine temperature data.")

with col_anomaly:
    st.subheader("Anomaly Score History")
    anom_df = safe_query(
        f"""
        SELECT timestamp, vehicle_id, sensor_type,
               anomaly_score, features
        FROM anomaly_detections
        WHERE detected_at >= NOW() - INTERVAL '{window_minutes} minutes'
        ORDER BY timestamp DESC
        LIMIT 200
    """
    )
    if not anom_df.empty:
        fig_anom = px.scatter(
            anom_df,
            x="timestamp",
            y="anomaly_score",
            color="vehicle_id",
            symbol="sensor_type",
            labels={"anomaly_score": "Anomaly Score", "timestamp": "Time"},
            title="",
        )
        fig_anom.add_hline(
            y=-0.1,
            line_dash="dash",
            line_color="orange",
            annotation_text="Anomaly threshold",
        )
        fig_anom.update_layout(height=300)
        st.plotly_chart(fig_anom, use_container_width=True)
    else:
        st.info("No anomalies detected in selected window.")

# ---------------------------------------------------------------------------
# Row 3: Hard Braking Events + Fleet Throughput
# ---------------------------------------------------------------------------

col_brake, col_throughput = st.columns([1, 1])

with col_brake:
    st.subheader("Hard Braking Events")
    brake_df = safe_query(
        f"""
        SELECT vehicle_id,
               COUNT(*) AS hard_brake_count,
               MAX(brake_pressure_pct) AS max_pressure
        FROM can_bus_events
        WHERE hard_braking
          AND timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
        GROUP BY vehicle_id
        ORDER BY hard_brake_count DESC
    """
    )
    if not brake_df.empty:
        fig_brake = px.bar(
            brake_df,
            x="vehicle_id",
            y="hard_brake_count",
            color="max_pressure",
            color_continuous_scale="Reds",
            labels={"hard_brake_count": "Count", "vehicle_id": "Vehicle"},
        )
        fig_brake.update_layout(height=280)
        st.plotly_chart(fig_brake, use_container_width=True)
    else:
        st.info("No hard braking events.")

with col_throughput:
    st.subheader("Event Throughput (per minute)")
    tp_df = safe_query(
        f"""
        SELECT date_trunc('minute', timestamp) AS minute,
               COUNT(*) AS events
        FROM gps_events
        WHERE timestamp >= NOW() - INTERVAL '{window_minutes} minutes'
        GROUP BY 1 ORDER BY 1
    """
    )
    if not tp_df.empty:
        fig_tp = px.area(
            tp_df,
            x="minute",
            y="events",
            labels={"events": "Events/min", "minute": "Time"},
        )
        fig_tp.update_layout(height=280)
        st.plotly_chart(fig_tp, use_container_width=True)
    else:
        st.info("No throughput data.")

# ---------------------------------------------------------------------------
# Recent Alerts Table
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Recent Alerts")

alerts = get_buffer().recent_alerts(limit=20)
if alerts:
    alert_df = pd.DataFrame(alerts)
    st.dataframe(
        alert_df.style.applymap(
            lambda v: (
                "color: red"
                if v == "critical"
                else "color: orange" if v == "high" else ""
            ),
            subset=["severity"] if "severity" in alert_df.columns else [],
        ),
        use_container_width=True,
    )
else:
    st.info("No alerts fired yet.")

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------

time.sleep(refresh_interval)
st.rerun()


def main() -> None:
    """Entry point when called as a module (setup.py console_scripts)."""
    import subprocess

    subprocess.run(
        [
            "streamlit",
            "run",
            __file__,
            "--server.port",
            os.environ.get("DASHBOARD_PORT", "8080"),
            "--server.address",
            "0.0.0.0",
        ]
    )


if __name__ == "__main__":
    main()
