````markdown
# Log Anomaly Pipeline

A Kafka-native, Docker-orchestrated anomaly detection system for live service logs.

This system ingests structured log events from Kafka, persists them idempotently in PostgreSQL, extracts behavioral features in real time, scores them with `IsolationForest`, detects concept drift, and emits de-duplicated alerts for anomalous service behavior.

---

## Architecture

```mermaid
flowchart LR
    A[Producer] -->|raw-logs| B[Kafka]
    B --> C[Ingestion Consumer]
    C --> D[(PostgreSQL)]
    D --> E[Feature Extractor + Isolation Forest]
    E --> D
    D --> F[Alert Service]
````

***

## System Overview

The pipeline operates as a streaming ML system:

1.  **Producer**
    Generates structured log events and publishes them to Kafka.

2.  **Kafka**
    Buffers and decouples log production from ingestion and downstream analysis.

3.  **Ingestion**
    Kafka-native consumer that:
    *   reads events from Kafka
    *   automatically provisions and synchronizes database schema
    *   performs idempotent writes using `log_id`

4.  **Feature Extraction**
    Aggregates logs into 1-minute behavioral windows per service.

5.  **Anomaly Detection**
    Uses `IsolationForest` to score feature windows and detect outliers.

6.  **Concept Drift Monitoring**
    Tracks rolling anomaly scores and retrains the model automatically when drift is detected.

7.  **Alerting**
    Polls newly detected anomalies and marks them as alerted to prevent duplicate notifications.

***

## Why this Architecture

### Kafka for Backpressure and Decoupling

Log traffic is inherently bursty. Kafka absorbs production spikes without forcing producers to wait on:

*   database write latency
*   feature extraction latency
*   model scoring latency
*   alert dispatch latency

This provides:

*   backpressure tolerance under uneven load
*   operational decoupling between ingestion and analysis
*   replayability for pipeline recovery or retraining

Kafka forms the boundary between event generation and durable ingestion.

***

### Idempotent Storage via `log_id`

Every produced log event includes a globally unique `log_id` (UUID).

The ingestion layer enforces uniqueness at the database level using this identifier.

This ensures:

*   safe ingestion under at-least-once delivery
*   correctness under retry or replay
*   duplicate-safe persistence

Distributed systems naturally produce duplicates. Idempotency is required for correctness, not optimization.

***

### Automatic Schema Provisioning

The ingestion service automatically provisions and synchronizes the PostgreSQL schema on startup.

Managed tables:

*   `logs`
*   `features`
*   `anomalies`

No manual SQL execution or migration step is required for runtime initialization.

***

### Unsupervised ML for Cold Start

Real operational incidents are rarely labeled. `IsolationForest` is used because it:

*   requires no labeled anomaly examples
*   adapts to unknown failure modes
*   learns the geometry of normal service behavior directly from observed traffic

This allows anomaly detection to begin immediately in environments with no historical incident dataset.

***

### Concept Drift for Model Decay

Service behavior changes over time:

*   traffic profiles evolve
*   latency distributions shift
*   infrastructure changes
*   scaling patterns differ

What was anomalous becomes normal.

The system monitors rolling anomaly scores as a drift signal. If recent scores trend downward beyond a defined threshold, the model automatically retrains on recent feature history.

This provides:

*   adaptation to evolving system behavior
*   prevention of false-positive accumulation
*   long-term alert quality without manual intervention

***

## Producer

Publishes structured log events to Kafka topic `raw-logs`.

Each event includes:

```json
{
  "log_id": "f0f3b8c4-8c0b-4e1f-bf40-5d49b4b4b21a",
  "timestamp": "2026-03-23T16:15:42.513248+00:00",
  "service_name": "payment-service",
  "log_level": "ERROR",
  "message": "Processed in 3124.44ms",
  "response_time_ms": 3124.44,
  "ip": "192.168.1.19"
}
```

Responsibilities:

*   generate synthetic service traffic
*   simulate normal and degraded behavior
*   assign globally unique identity per event
*   publish asynchronously to Kafka

***

## Ingestion

Kafka-native consumer responsible for:

*   consuming from `raw-logs`
*   provisioning database schema
*   validating payloads
*   idempotent insertion into PostgreSQL using `log_id`
*   committing Kafka offsets after successful handling

This layer guarantees durable ingestion under replay and retry conditions.

***

## Database Model

### `logs`

Stores raw immutable events.

Fields:

*   `id`
*   `log_id`
*   `timestamp`
*   `service_name`
*   `log_level`
*   `message`
*   `response_time_ms`
*   `ip`
*   `created_at`

***

### `features`

Stores per-service behavioral summaries.

Fields:

*   `id`
*   `window_start`
*   `window_end`
*   `service_name`
*   `error_count`
*   `warn_count`
*   `request_count`
*   `avg_response_time`
*   `unique_ip_count`
*   `created_at`

***

### `anomalies`

Stores model decisions and alert lifecycle state.

Fields:

*   `id`
*   `detected_at`
*   `service_name`
*   `anomaly_score`
*   `is_anomaly`
*   `alert_sent`

***

## Feature Extraction

Logs are aggregated into **1-minute service windows**.

For each service:

*   `error_count`
*   `warn_count`
*   `request_count`
*   `avg_response_time`
*   `unique_ip_count`

These five features form a compact behavioral fingerprint of service activity.

***

## ML Engine

Uses:

    IsolationForest

The model:

*   trains on recent feature windows
*   scores new windows on arrival
*   assigns anomaly scores
*   flags statistical outliers

No labeled anomaly dataset is required.

***

## Concept Drift Detection

The system tracks a rolling mean of anomaly scores.

If the rolling mean drops below a defined threshold:

*   the operating distribution has shifted
*   the model baseline is stale

The pipeline retrains automatically on recent feature history.

This enables:

*   self-healing model behavior
*   adaptation to evolving traffic patterns
*   sustained alert relevance

***

## Alert Service

State-aware polling loop that:

*   reads unalerted anomalies
*   emits alert payloads
*   marks anomalies as alerted

This ensures:

*   deterministic duplicate prevention
*   replay-safe notification lifecycle
*   durable alert tracking

Alerting is treated as tracked state rather than an ephemeral side effect.

***

## Repository Layout

```text
.
├── alert/
│   └── main.py
├── features/
│   └── extractor.py
├── ingestion/
│   └── consumer.py
├── producer/
│   └── main.py
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
└── README.md
```

***

## Tooling

### uv

Used for:

*   ultra-fast dependency resolution
*   deterministic environment creation
*   Python workflow acceleration across containers

***

### Docker Compose

Used for:

*   full system orchestration
*   reproducible runtime topology
*   service dependency coordination
*   one-command startup

***

## Runtime Stack

*   Python
*   uv
*   Apache Kafka
*   PostgreSQL
*   SQLAlchemy
*   pandas
*   NumPy
*   scikit-learn
*   Docker Compose

***

## Quick Start

Start the full pipeline:

```bash
docker compose up --build
```

Follow logs:

```bash
docker compose logs -f producer
docker compose logs -f ingestion
docker compose logs -f extractor
docker compose logs -f alert-service
```

```
```
