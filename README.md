# Financial Data Pipeline

[![CI](https://github.com/webdevanki/financial-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/webdevanki/financial-pipeline/actions/workflows/ci.yml)

A production-style daily financial pipeline built with **Apache Airflow**, **PySpark**, **DuckDB**, and **PostgreSQL**, following the **medallion architecture** (Bronze → Silver → Gold).

Inspired by real-world fintech data engineering patterns (Allegro Pay scale).

---

## Architecture

```
┌──────────────┐   Bronze    ┌──────────────┐   Silver    ┌──────────────┐
│  Data Ingest │────────────▶│  Validation  │────────────▶│  PostgreSQL  │
│  (generator) │             │  + DQ Metrics│             │  raw_trans.  │
└──────────────┘             └──────────────┘             └──────┬───────┘
                                                                  │  Gold
                                                     ┌────────────┴────────────┐
                                              ┌──────▼──────┐          ┌───────▼──────┐
                                              │  PySpark /  │          │   Anomaly    │
                                              │  aggregate  │          │  Detection   │
                                              │    Gold     │          │  (z-score)   │
                                              └──────┬──────┘          └──────────────┘
                                                     │
                                              ┌──────▼──────┐
                                              │   DuckDB    │  ← in-memory OLAP
                                              │  Analytics  │    (revenue share,
                                              │             │     avg ticket)
                                              └──────┬──────┘
                                                     │
                                              ┌──────▼──────┐
                                              │   Notify    │  ← daily KPI report
                                              │  (+ alert)  │    + on-call escalation
                                              └─────────────┘
```

## Tech Stack

| Technology | Version | Role |
|---|---|---|
| Apache Airflow | 2.9.1 | Pipeline orchestration, scheduling, retries |
| Apache Spark / PySpark | 3.5 | Large-scale aggregations, window functions |
| DuckDB | ≥ 0.10 | In-memory OLAP — revenue share, trend analysis |
| PostgreSQL | 15 | DWH storage (Bronze / Silver / Gold tables) |
| Docker Compose | — | Local containerised environment |
| GitHub Actions | — | CI: lint + DAG import test + unit tests |

## Pipeline DAG

```
ingest_bronze
      │
validate_silver          ← business-rule checks, reject-rate SLA (< 10 %)
      │
data_quality_metrics     ← DQ KPIs (reject rate, nulls, avg/max amount)
      │
load_to_postgres         ← idempotent INSERT … ON CONFLICT DO NOTHING
      │
      ├─── aggregate_gold     ← category-level summary (PySpark in production)
      │          │
      │    duckdb_analytics   ← in-memory OLAP: revenue share, avg ticket
      │          │
      │    save_summary       ← upsert into daily_summary (Gold)
      │
      └─── detect_anomalies   ← SQL window: z-score > 2.5σ per category
                 │
            notify             ← daily report + PagerDuty escalation
```

**Schedule:** `0 6 * * *` UTC | **SLA:** 2 h | **Retries:** 2 × 5 min

## Quick Start

### Requirements

- Docker + Docker Compose
- 4 GB RAM available for Docker

### Run

```bash
git clone https://github.com/webdevanki/financial-pipeline
cd financial-pipeline

# Start the full stack (Airflow + Postgres + Spark)
docker compose up -d

# Wait ~2 minutes for Airflow to initialise, then check status
docker compose ps
```

### Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:8081 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |

### Trigger the DAG

```bash
# Via Airflow UI: DAGs → financial_pipeline → Trigger DAG

# Or via CLI:
docker compose exec airflow-scheduler \
    airflow dags trigger financial_pipeline
```

### Run the PySpark job standalone

```bash
# Inside the Spark container (sample data, no Postgres needed)
docker compose exec spark-master \
    spark-submit /opt/spark_jobs/aggregate_transactions.py --mode sample

# Or locally after: pip install pyspark
python aggregate_transactions.py --mode sample --date 2024-01-15
```

## Schema (Medallion DDL)

See [sql/schema.sql](sql/schema.sql) for the full DDL.

| Layer | Table | Description |
|---|---|---|
| Bronze | `raw_transactions` | Raw ingest, append-only, indexed by date/category |
| Silver | `anomalies` | Quarantined rows flagged by z-score detection |
| Gold | `daily_summary` | Aggregated KPIs per category, BI-ready |
| Audit | `dq_run_log` | Data-quality metrics per pipeline run |

## Tests & CI

```bash
# Install dev dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v
```

GitHub Actions runs on every push and PR:
1. **Lint** — `ruff check` on all Python files
2. **DAG smoke-test** — verifies the DAG imports without errors
3. **Unit tests** — schedule, SLA, retry config, task presence

## Project Structure

```
financial-pipeline/
├── dags/
│   └── financial_pipeline.py      # Airflow DAG (TaskFlow API, medallion layers)
├── aggregate_transactions.py       # PySpark job: aggregations + z-score anomaly detection
├── sql/
│   └── schema.sql                 # Bronze / Silver / Gold DDL
├── tests/
│   ├── conftest.py
│   └── test_pipeline.py
├── .github/
│   └── workflows/
│       └── ci.yml                 # Lint + smoke-test + unit tests
├── docker-compose.yml             # Airflow + Postgres + Spark
├── requirements.txt
└── README.md
```

## Key Engineering Concepts

**Medallion architecture** — Bronze (raw) → Silver (cleansed) → Gold (aggregated) separates concerns and enables independent reprocessing of any layer.

**Idempotency** — every load step uses `ON CONFLICT DO NOTHING` or `DO UPDATE SET`, making re-runs safe without data duplication.

**Statistical anomaly detection** — z-score computed via SQL window functions (`AVG` / `STDDEV` + `PARTITION BY category`) — one database pass instead of a Python loop.

**DuckDB for in-process OLAP** — Gold data is pulled into DuckDB (`:memory:`) to run columnar window queries (revenue share, avg-ticket ranking) without extra infrastructure. Mirrors how analysts iterate fast on a data mesh.

**Data quality gate** — `validate_silver` enforces a 10 % reject-rate SLA; `data_quality_metrics` emits per-run KPIs that can feed Grafana or a DQ platform.

**Observability** — `on_failure_callback` and `sla` on every task; `notify` escalates to on-call when anomaly count exceeds the `anomaly_alert_threshold` Airflow Variable.

**XCom discipline** — only small summary dicts travel through XCom; bulk data lives in Postgres.

## Sample Queries

```sql
-- Daily revenue share per category
SELECT
    summary_date,
    category,
    total_amount,
    ROUND(total_amount / SUM(total_amount) OVER (PARTITION BY summary_date), 4) AS share
FROM daily_summary
ORDER BY summary_date DESC, total_amount DESC;

-- Top anomalies in the last 7 days
SELECT tx_id, user_id, amount, reason, detected_at
FROM anomalies
WHERE detected_at >= NOW() - INTERVAL '7 days'
ORDER BY amount DESC
LIMIT 20;

-- Data-quality trend
SELECT run_date, total_count, reject_count, reject_rate
FROM dq_run_log
ORDER BY run_date DESC;
```
