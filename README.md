# Mal Unified Payment Data Platform

A production-grade data pipeline that unifies payment events from three product squads (Cards, Transfers, Bill Payments) into a single canonical model. Built with **dlt** for ingestion/loading, **Apache Airflow** for orchestration, **Pydantic v2** for schema validation, and **DuckDB** as the local analytical warehouse.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Product Squads (Sources)                         │
├──────────────────┬──────────────────────┬───────────────────────────────┤
│  Cards Squad     │  Transfers Squad     │  Bill Payments Squad          │
│  ISO 8601 dates  │  Unix epoch ms       │  DD/MM/YYYY dates             │
│  approved/       │  COMPLETED/FAILED/   │  success/failed/              │
│  declined/       │  PROCESSING          │  in_progress                  │
│  pending         │                      │                               │
└────────┬─────────┴──────────┬───────────┴──────────────┬────────────────┘
         │                    │                          │
         ▼                    ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Airflow DAG (TaskFlow API)                            │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐   │
│  │ Extract   │─▶│ Validate  │─▶│ Load      │─▶│ Run Analytics     │   │
│  │ (3 CSVs)  │  │ (Pydantic)│  │ (dlt +    │  │ (5 SQL queries    │   │
│  └───────────┘  └─────┬─────┘  │  DuckDB)  │  │  against DuckDB)  │   │
│                       │        └───────────┘  └───────────────────┘   │
│                       ▼                                               │
│                Dead Letter Queue (errors.json)                        │
└─────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Output Layer                                    │
│  ┌──────────────┐  ┌──────────────────┐  ┌─────────────────────────┐  │
│  │ DuckDB       │  │ Parquet Files    │  │ analytics_report.json   │  │
│  │ (warehouse)  │  │ (portable)       │  │ (query results)         │  │
│  └──────────────┘  └────────┬─────────┘  └─────────────────────────┘  │
└─────────────────────────────┼───────────────────────────────────────────┘
                              ▼
               ┌──────────────────────────┐
               │   Streamlit Cloud        │
               │   (Interactive Dashboard)│
               │   reads Parquet directly │
               └──────────────────────────┘
```

## Project Structure

```
mal-cross-product-data-platform/
├── README.md
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
├── data/
│   └── raw/
│       ├── cards_events.csv          # Cards Squad format (50 events)
│       ├── transfers_events.csv      # Transfers Squad format (50 events)
│       └── bill_payments_events.csv  # Bill Payments Squad format (50 events)
├── app.py                          # Streamlit dashboard (deploys to Streamlit Cloud)
├── dags/
│   └── payment_pipeline_dag.py       # Airflow DAG (TaskFlow API, 4 tasks)
├── src/
│   ├── __init__.py
│   ├── schema.py                     # Pydantic v2 models (v1 + v2)
│   ├── sources.py                    # dlt sources + per-squad transformers
│   ├── pipeline.py                   # Standalone runner (no Airflow needed)
│   └── contracts.py                  # Schema versioning & migration
├── sql/
│   └── queries.sql                   # 5 downstream analytical queries
└── tests/
    └── test_pipeline.py              # Unit tests
```

## Quick Start (Docker -- Recommended)

### Prerequisites

- Docker and Docker Compose

### 1. Start everything

```bash
docker compose up --build -d
```

This builds the image, initializes Airflow, and starts the webserver + scheduler:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |

### 2. Trigger the pipeline

From the Airflow UI, find `mal_payment_pipeline` and click **Trigger DAG**. The DAG runs 4 tasks:

1. **extract_squad_payments** -- reads and transforms 3 CSVs
2. **validate_canonical_schema** -- Pydantic v2 validation with dead-letter routing
3. **load_to_duckdb_and_parquet** -- dlt loads to DuckDB, exports Parquet
4. **run_analytics_queries** -- executes 5 SQL queries, writes report

Or from the command line:

```bash
docker compose exec airflow-webserver airflow dags trigger mal_payment_pipeline
```

### 3. Run the standalone pipeline (no Airflow)

```bash
docker compose exec airflow-webserver python -m src.pipeline
```

### 4. Run tests

```bash
docker compose exec airflow-webserver python -m pytest tests/ -v
```

### 5. Stop everything

```bash
docker compose down -v
```

---

## Deployed Demo (Streamlit Cloud)

The interactive dashboard is deployed on **Streamlit Cloud** and reads the Parquet file directly from the repo -- no database or server needed.

### Deploy your own

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Select the repo and set `app.py` as the main file
4. Deploy -- the dashboard reads `data/output/payment_events.parquet` on startup

### Run the dashboard locally

```bash
streamlit run app.py
```

The dashboard shows:
- **KPI metrics**: total events, completed, failed, pending
- **Payment volume by type**: bar chart of card / transfer / bill payment totals
- **Status distribution** and **failure rate by source system**
- **Cross-product customers**: customers active in 2+ product squads
- **SQL query runner**: ad-hoc queries against the unified model

---

## Quick Start (Local -- No Docker)

### Prerequisites

- Python 3.10+
- pip

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the pipeline (standalone mode)

```bash
python -m src.pipeline
```

This ingests all 150 events from 3 CSVs, validates them, loads into DuckDB, and exports Parquet.

### 3. Run with Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow standalone
```

Open `http://localhost:8080`, find `mal_payment_pipeline`, and trigger it manually.

### 4. Run SQL queries

```bash
duckdb mal_payments.duckdb < sql/queries.sql
```

### 5. Run tests

```bash
pytest tests/ -v
```

## Canonical Schema (v2)

All three squad formats are normalized into a single `PaymentEvent` model:

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | UUID | Unique identifier (generated) |
| `source_system` | enum | `cards`, `transfers`, `bill_payments` |
| `source_event_id` | string | Original ID from source system |
| `customer_id` | string | Normalized customer identifier |
| `counterparty_id` | string? | Receiver / merchant / biller code |
| `counterparty_name` | string? | Human-readable counterparty name |
| `amount` | decimal | Transaction amount (non-negative) |
| `currency` | string | ISO 4217 (e.g., SAR, USD) |
| `event_timestamp` | datetime | UTC-normalized timestamp |
| `status` | enum | `completed`, `failed`, `pending` |
| `payment_type` | enum | `card_transaction`, `transfer`, `bill_payment` |
| `payment_method` | string? | Card type / transfer type / bill category |
| `metadata` | dict | Source-specific fields (MCC code, biller code, etc.) |
| `schema_version` | int | Contract version (currently 2) |

## Schema Versioning

The pipeline demonstrates a v1 → v2 migration:

- **v1**: Flat schema without counterparty or metadata fields
- **v2**: Adds `counterparty_id`, `counterparty_name`, and `metadata` dict
- `contracts.py` provides `migrate_v1_to_v2()` for backward-compatible upgrades
- dlt tracks schema evolution automatically in its metadata tables

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **dlt for ingestion** | Schema inference, evolution tracking, and DuckDB loading out of the box |
| **Airflow TaskFlow API** | Industry-standard orchestrator; `@task` decorators keep DAGs Pythonic |
| **Dual run mode** | Airflow DAG for production, standalone runner for dev/CI |
| **Docker Compose** | One command to start Airflow; zero local setup for reviewers |
| **Pydantic v2 validation** | Type safety, clear errors, JSON schema export for data contracts |
| **DuckDB + Parquet** | Zero-infrastructure analytics; same SQL patterns work on Snowflake/BigQuery |
| **counterparty abstraction** | Unifies merchant / receiver / biller without schema explosion |
| **Dead-letter pattern** | Invalid records captured separately; pipeline never blocks on bad data |
| **In-DAG analytics** | SQL queries run as a pipeline task; results logged in Airflow and saved to JSON |

## Production Architecture

In production at Mal, this local setup maps to:

| Local | Production |
|-------|-----------|
| CSV files | S3 event streams / Kafka topics |
| `airflow standalone` | Managed Airflow (MWAA / Cloud Composer) |
| DuckDB | Snowflake / BigQuery / Redshift |
| `dlt` destination swap | Same code, `destination="bigquery"` |
| Streamlit Cloud | Internal BI portal / Looker |
| `analytics_report.json` | Scheduled alerts + Metabase dashboards |
| `errors.json` | Dead-letter S3 bucket + alerting |
