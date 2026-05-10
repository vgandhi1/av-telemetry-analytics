# AV Telemetry Analytics Platform

> **Real-time data ingestion, stream processing, and analytics for autonomous vehicle fleets.**

A production-grade platform for ingesting, processing, storing, and visualizing petabyte-scale telemetry from autonomous vehicle sensors — built with Apache Kafka, Apache Spark Structured Streaming, DuckDB, Apache Parquet, and AWS.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring Dashboard](#monitoring-dashboard)
- [Machine Learning](#machine-learning)
- [Infrastructure (Terraform)](#infrastructure-terraform)
- [Configuration](#configuration)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Autonomous vehicles generate hundreds of megabytes of telemetry per second — GPS, IMU, LiDAR, camera metadata, CAN bus, and system health events — across an entire fleet. This platform provides:

- **Sub-second ingestion** of mixed sensor streams via Kafka topic-per-sensor partitioning
- **Real-time stream processing** with Spark Structured Streaming: transformations, windowed aggregations, and ML inference in a single pipeline
- **Hybrid storage**: Apache Parquet on S3 for long-term columnar storage, DuckDB for millisecond interactive queries
- **Anomaly detection** (Isolation Forest) and **predictive maintenance** (Gradient Boosting) scoring inline in the Spark pipeline
- **Live monitoring dashboard** (Streamlit + Plotly) with vehicle map, speed timeseries, engine health, and anomaly history
- **Rule-based alerting** with Slack notifications and per-vehicle cooldowns
- **Prometheus metrics** and CloudWatch integration for pipeline observability
- **One-command infrastructure** provisioning via Terraform (VPC, EMR, S3, DynamoDB)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AV Fleet (vehicles)                         │
│   GPS · IMU · LiDAR · Camera · CAN Bus · System Health             │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ 100k+ events/sec
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Apache Kafka (topic per sensor)                  │
│  av.telemetry.gps  av.telemetry.imu  av.telemetry.can_bus  ...     │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ Structured Streaming
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Apache Spark Structured Streaming (EMR)                │
│                                                                     │
│  parse JSON → validate → enrich → deduplicate → aggregate          │
│       │                                                             │
│       └──► ML Scoring (Isolation Forest UDF / GBM UDF)            │
└───────┬──────────────────────────┬──────────────────────────────────┘
        │                          │
        ▼                          ▼
┌───────────────┐        ┌─────────────────────────┐
│  S3 (Parquet) │        │  Alerts → Slack / SNS   │
│  Hive-part.   │        └─────────────────────────┘
│  Snappy comp. │
└───────┬───────┘
        │
        ▼
┌─────────────────────┐      ┌────────────────────────────────────────┐
│  DuckDB (local)     │◄─────│  Streamlit Dashboard  :8080            │
│  httpfs → S3 query  │      │  Vehicle map · Speed · Engine · Alerts │
└─────────────────────┘      └────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│         DynamoDB  (metadata · catalog · alert history)              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka (confluent-kafka), topic-per-sensor |
| Stream Processing | Apache Spark Structured Streaming 3.5 |
| ML | scikit-learn (Isolation Forest, Gradient Boosting), Spark pandas UDFs |
| Long-term Storage | Apache Parquet + S3 (Snappy, Hive partitioning) |
| Interactive Query | DuckDB 0.10 (httpfs for direct S3 reads) |
| Metadata / Catalog | AWS DynamoDB |
| Compute | AWS EMR 7.0 (auto-scaling task groups) |
| Monitoring | Streamlit + Plotly, Prometheus metrics |
| Alerting | Rule engine + Slack webhooks |
| Infrastructure | Terraform 1.7 (VPC, EMR, S3, DynamoDB modules) |
| CI/CD | GitHub Actions (lint, test, terraform validate, docker build) |
| Language | Python 3.11 |

---

## Project Structure

```
av-telemetry-analytics/
│
├── ingestion_pipeline.py        # Entry point: produce synthetic data OR consume → storage
├── stream_processing.py         # Entry point: Spark Structured Streaming pipeline
│
├── config/
│   ├── app_config.yaml          # Central config (env-var interpolation)
│   ├── kafka_config.yaml        # Producer/consumer settings + topic definitions
│   ├── spark_config.yaml        # SparkSession config + streaming settings
│   └── storage_config.yaml      # S3, Parquet, DuckDB, DynamoDB settings
│
├── src/
│   ├── ingestion/
│   │   ├── connectors/
│   │   │   ├── schemas.py       # Pydantic v2 event schemas (GPS, IMU, CAN bus, …)
│   │   │   └── vehicle_connector.py  # Synthetic vehicle simulator
│   │   ├── kafka_producer.py    # Idempotent Kafka producer, topic-per-sensor routing
│   │   └── kafka_consumer.py    # Batch-committing consumer with schema validation
│   │
│   ├── processing/
│   │   ├── transformations.py   # Spark schemas, enrichment, validity filters
│   │   ├── aggregations.py      # Windowed aggregations (speed, braking, engine, vibration)
│   │   └── ml/
│   │       ├── anomaly_detector.py       # Isolation Forest + Spark UDF
│   │       └── predictive_maintenance.py # Gradient Boosting + Spark UDF
│   │
│   ├── storage/
│   │   ├── parquet_writer.py    # Thread-safe batching → local staging → S3
│   │   ├── duckdb_manager.py    # Schema DDL, bulk Parquet load, query helpers
│   │   └── s3_client.py         # Upload, download, presign, multipart
│   │
│   └── monitoring/
│       ├── dashboard.py         # Streamlit real-time dashboard
│       ├── alerting.py          # Rule engine + Slack webhook dispatcher
│       └── metrics.py           # Prometheus metrics + in-memory ring buffer
│
├── terraform/
│   ├── main.tf                  # Root module: wires VPC, S3, DynamoDB, EMR + IAM
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
│       ├── vpc/                 # VPC, private subnets, NAT gateway, EMR security group
│       ├── s3/                  # Data bucket (versioned, lifecycle), logs bucket
│       ├── dynamodb/            # 4 tables: vehicle metadata, catalog, alerts, pipeline state
│       └── emr/                 # Spark cluster with auto-scaling task group
│
├── tests/
│   ├── test_schemas.py          # Schema validation, Kafka serialization, connector tests
│   ├── test_storage.py          # ParquetWriter, DuckDBManager integration tests
│   └── test_processing.py       # Anomaly detector fit/predict/save/load
│
├── .github/workflows/ci.yml     # Lint → test (with Kafka service) → Terraform validate → Docker
├── Dockerfile
├── requirements.txt
└── setup.py
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- Java 17 (for Spark — `brew install openjdk@17` on macOS)
- Docker (optional, for local Kafka)
- AWS CLI + credentials (for S3/DynamoDB/EMR)
- Terraform 1.7+ (for infra provisioning)

### 1. Clone and install

```bash
git clone https://github.com/vgandhi1/av-telemetry-analytics.git
cd av-telemetry-analytics
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your Kafka brokers, AWS credentials, S3 bucket, etc.
```

### 3. Start local Kafka (Docker)

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS="1@localhost:9093" \
  -e KAFKA_CFG_LISTENERS="PLAINTEXT://:9092,CONTROLLER://:9093" \
  -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://localhost:9092" \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnami/kafka:3.6
```

---

## Running the Pipeline

### Produce synthetic vehicle telemetry

```bash
# Stream 3 simulated vehicles at 10 events/sec each
python ingestion_pipeline.py --mode produce --vehicles ZX-001 ZX-002 ZX-003 --hz 10
```

### Consume and write to Parquet / DuckDB

```bash
python ingestion_pipeline.py --mode consume
```

### Run Spark stream processing

```bash
# Local mode (uses local[*] Spark master)
python stream_processing.py

# Debug mode — prints aggregations to console
python stream_processing.py --debug
```

### Launch the monitoring dashboard

```bash
streamlit run src/monitoring/dashboard.py --server.port 8080
# Open: http://localhost:8080
```

### Expose Prometheus metrics

```python
from src.monitoring.metrics import start_metrics_server
start_metrics_server(port=9090)
# Scrape: http://localhost:9090/metrics
```

---

## Monitoring Dashboard

The Streamlit dashboard at `http://localhost:8080` provides:

| Panel | Description |
|---|---|
| KPI tiles | Active vehicles, anomalies, avg fleet speed, overheating count, event throughput |
| Vehicle map | Real-time Mapbox scatter plot colored by speed |
| Speed timeseries | Per-vehicle rolling average speed (km/h) |
| Engine temperature | Per-vehicle temp with 105°C overheat threshold line |
| Anomaly score history | Scatter plot of ML anomaly scores per vehicle/sensor |
| Hard braking events | Bar chart ranked by vehicle |
| Event throughput | Area chart of events/minute ingested |
| Alert feed | Live table of fired alert rules |

Auto-refreshes every 5–60 seconds (configurable in the sidebar).

---

## Machine Learning

### Anomaly Detection

Uses **Isolation Forest** (sklearn) to detect out-of-distribution sensor readings inline in the Spark pipeline.

**Features**: `speed_ms`, `accel_magnitude`, `steering_angle_deg`, `brake_pressure_pct`, `engine_temp_celsius`

```bash
# Train from historical Parquet data
python -c "
from src.processing.ml.anomaly_detector import train_from_parquet
train_from_parquet('./data/parquet/can_bus', './models/anomaly_detector.pkl')
"
```

Once `./models/anomaly_detector.pkl` exists, `stream_processing.py` picks it up automatically.

### Predictive Maintenance

Uses **Gradient Boosting Classifier** to predict maintenance needs within a 24-hour horizon.

**Features**: engine temp, oil pressure, battery voltage, odometer, brake wear, rolling speed/braking aggregates

```python
from src.processing.ml.predictive_maintenance import MaintenancePredictor
predictor = MaintenancePredictor()
predictor.fit(training_df)   # training_df must include 'needs_maintenance_24h' label
predictor.save('./models/maintenance_predictor.pkl')
print(predictor.feature_importance())
```

---

## Infrastructure (Terraform)

```bash
cd terraform

# Initialize (configure backend bucket/key first in main.tf)
terraform init

# Plan for dev environment
terraform plan -var="environment=dev"

# Apply
terraform apply -var="environment=dev"
```

### Provisioned resources

| Module | Resources |
|---|---|
| `vpc` | VPC, 3 private subnets across AZs, NAT gateway, EMR security group |
| `s3` | `av-telemetry-data-{env}` (versioned, AES-256, lifecycle tiers), `av-telemetry-logs-{env}` |
| `dynamodb` | `vehicle-metadata`, `data-catalog` (GSI), `alert-history` (TTL), `pipeline-state` |
| `emr` | Spark 3.5 cluster on EMR 7.0, auto-scaling task group (1–10 nodes), AQE enabled |

### Environment variables for Terraform

```bash
export TF_VAR_environment=prod
export TF_VAR_emr_core_instance_count=5
export TF_VAR_alert_email=oncall@yourorg.com
```

---

## Configuration

All config lives in `config/app_config.yaml` with `${ENV_VAR:-default}` interpolation. Override any value via environment variables — no code changes needed.

Key settings:

| Config path | Description | Default |
|---|---|---|
| `kafka.bootstrap_servers` | Kafka broker list | `localhost:9092` |
| `kafka.consumer_group` | Consumer group ID | `av-analytics-consumer` |
| `spark.master` | Spark master URL | `local[*]` |
| `spark.checkpoint_dir` | Streaming checkpoint path | `/tmp/spark-checkpoints` |
| `storage.s3_bucket` | S3 data bucket | `av-telemetry-data` |
| `storage.duckdb_path` | DuckDB database file | `./data/telemetry.duckdb` |
| `monitoring.dashboard_port` | Streamlit port | `8080` |
| `ml.anomaly_detection.contamination` | IForest contamination | `0.05` |

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage report
pytest tests/ --cov=src --cov-report=term-missing

# Individual test modules
pytest tests/test_schemas.py      # Schema + connector tests
pytest tests/test_storage.py      # Parquet + DuckDB tests
pytest tests/test_processing.py   # ML model tests
```

Tests use `tmp_path` fixtures — no external services required (Kafka/AWS are mocked via `moto`).

---

## CI/CD

GitHub Actions runs on every push to `main`/`develop` and on all pull requests:

| Job | What it does |
|---|---|
| `lint` | ruff, black --check, mypy |
| `test` | pytest with live Kafka service container, coverage upload |
| `terraform` | `validate` + `fmt -check` across all modules |
| `docker` | Build image (push disabled by default — add registry config to enable) |

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Install pre-commit hooks: `pre-commit install`
4. Make your changes and add tests
5. Run `pytest` and `ruff check` locally
6. Open a pull request against `main`

Please follow the existing code style — no comments explaining *what* the code does, only *why* when non-obvious.

---

## License

[MIT License](LICENSE)
