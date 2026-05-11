"""
DAG: financial_pipeline
========================
Daily financial pipeline following the medallion architecture:

  Bronze  – ingest_bronze       : generate / ingest raw transactions
  Silver  – validate_silver     : cleansing + data-quality metrics
  Gold    – aggregate_gold      : category summaries (PySpark in production)
            detect_anomalies    : statistical anomaly flagging (z-score, SQL window)
            duckdb_analytics    : in-memory OLAP with DuckDB
  Publish – save_summary        : idempotent upsert into Gold DWH table
            notify              : daily KPI report + on-call escalation

Schedule : daily 06:00 UTC
SLA      : 2 h
Retries  : 2 × 5 min
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import date, timedelta

import duckdb
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


# ── Constants ──────────────────────────────────────────────────────────────────

POSTGRES_CONN_ID = "postgres_default"

CATEGORIES = ["food", "transport", "entertainment", "shopping", "utilities", "healthcare"]
MERCHANTS: dict[str, list[str]] = {
    "food":          ["Biedronka", "Lidl", "Żabka", "McDonald's", "Bolt Food"],
    "transport":     ["PKP", "Uber", "Bolt", "MPK Warszawa", "Orlen"],
    "entertainment": ["Netflix", "Spotify", "Steam", "Empik", "Cinema City"],
    "shopping":      ["Allegro", "Zalando", "H&M", "Ikea", "MediaMarkt"],
    "utilities":     ["PGE", "Orange", "T-Mobile", "Canal+", "Veolia"],
    "healthcare":    ["LuxMed", "Medicover", "Apteka", "Dentist", "Gym"],
}


def _on_failure_callback(context: dict) -> None:
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    # In production: post to Slack webhook / PagerDuty / OpsGenie
    print(f"[ALERT] {dag_id}.{task_id} failed in run {run_id} – would page on-call here")


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="financial_pipeline",
    description="Daily financial pipeline: Bronze → Silver → Gold (Airflow + PySpark + DuckDB)",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["fintech", "medallion", "pyspark", "duckdb", "allegro-pay-style"],
    default_args={
        "owner":               "data-team",
        "retries":             2,
        "retry_delay":         timedelta(minutes=5),
        "sla":                 timedelta(hours=2),
        "on_failure_callback": _on_failure_callback,
        "email_on_failure":    False,
    },
)
def financial_pipeline() -> None:

    # ── BRONZE: ingest ─────────────────────────────────────────────────────────

    @task()
    def ingest_bronze(n: int = 500) -> dict:
        """Simulate raw transaction ingest (Bronze layer – no business logic applied)."""
        today = date.today().isoformat()
        transactions = []

        for _ in range(n):
            category   = random.choice(CATEGORIES)
            is_anomaly = random.random() < 0.02
            amount     = round(random.uniform(5_000, 50_000), 2) if is_anomaly \
                         else round(random.uniform(1, 1_500), 2)

            transactions.append({
                "tx_id":    str(uuid.uuid4()),
                "user_id":  random.randint(1, 1_000),
                "amount":   amount,
                "currency": "PLN",
                "merchant": random.choice(MERCHANTS[category]),
                "category": category,
                "tx_date":  today,
            })

        print(f"[BRONZE] Ingested {len(transactions)} transactions for {today}")
        return {"transactions": transactions, "date": today}

    # ── SILVER: validate & cleanse ─────────────────────────────────────────────

    @task()
    def validate_silver(payload: dict) -> dict:
        """
        Cleanse Bronze data and enforce business rules (Silver layer).
        Rows failing validation are quarantined, not dropped silently.
        Raises if reject rate exceeds 10 % SLA.
        """
        transactions = payload["transactions"]
        clean, rejected = [], []

        for tx in transactions:
            issues = []
            if tx["amount"] <= 0:
                issues.append(f"non-positive amount ({tx['amount']})")
            if not tx.get("tx_id"):
                issues.append("missing tx_id")
            if tx["category"] not in CATEGORIES:
                issues.append(f"unknown category ({tx['category']})")

            (rejected if issues else clean).append(tx)

        reject_rate = len(rejected) / len(transactions) if transactions else 0.0
        print(f"[SILVER] Passed {len(clean)} | Rejected {len(rejected)} ({reject_rate:.1%})")

        if reject_rate > 0.10:
            raise ValueError(f"Reject rate {reject_rate:.1%} exceeds 10 % SLA – aborting")

        return {**payload, "transactions": clean, "rejected": rejected}

    @task()
    def data_quality_metrics(payload: dict) -> dict:
        """
        Emit DQ KPIs for observability.
        In production these feed Grafana dashboards or a DQ platform (e.g. Monte Carlo).
        """
        txs     = payload["transactions"]
        amounts = [tx["amount"] for tx in txs]

        dq = {
            "run_date":      payload["date"],
            "total_count":   len(txs),
            "reject_count":  len(payload.get("rejected", [])),
            "reject_rate":   round(len(payload.get("rejected", [])) / max(len(txs), 1), 4),
            "null_merchants": sum(1 for tx in txs if not tx.get("merchant")),
            "avg_amount":    round(sum(amounts) / len(amounts), 2) if amounts else 0.0,
            "max_amount":    max(amounts) if amounts else 0.0,
        }

        print(f"[DQ] {json.dumps(dq)}")
        return {**payload, "dq": dq}

    # ── Load ────────────────────────────────────────────────────────────────────

    @task()
    def load_to_postgres(payload: dict) -> str:
        """
        Load Silver records into raw_transactions via Airflow PostgresHook.
        Uses ON CONFLICT DO NOTHING for idempotent re-runs.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        txs  = payload["transactions"]

        inserted = 0
        with hook.get_conn() as conn, conn.cursor() as cur:
            for tx in txs:
                cur.execute("""
                    INSERT INTO raw_transactions
                        (tx_id, user_id, amount, currency, merchant, category, tx_date)
                    VALUES
                        (%(tx_id)s, %(user_id)s, %(amount)s, %(currency)s,
                         %(merchant)s, %(category)s, %(tx_date)s)
                    ON CONFLICT (tx_id) DO NOTHING
                """, tx)
                inserted += cur.rowcount

        print(f"[LOAD] {inserted}/{len(txs)} rows inserted into raw_transactions")
        return payload["date"]

    # ── GOLD: aggregate ────────────────────────────────────────────────────────

    @task()
    def aggregate_gold(run_date: str) -> dict:
        """
        Category-level summary (Gold layer).
        In production this is a SparkSubmitOperator calling spark_jobs/aggregate_transactions.py.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records("""
            SELECT
                category,
                SUM(amount)    AS total_amount,
                COUNT(*)       AS tx_count,
                AVG(amount)    AS avg_amount,
                MAX(amount)    AS max_amount
            FROM raw_transactions
            WHERE tx_date = %s
            GROUP BY category
            ORDER BY total_amount DESC
        """, parameters=(run_date,))

        # Explicit float/int cast: psycopg2 returns Decimal, which is not JSON-serialisable
        summary = [
            {
                "category":     r[0],
                "total_amount": float(r[1]),
                "tx_count":     int(r[2]),
                "avg_amount":   float(r[3]),
                "max_amount":   float(r[4]),
            }
            for r in rows
        ]

        print(f"[GOLD] Aggregated {len(summary)} categories for {run_date}")
        return {"summary": summary, "date": run_date}

    @task()
    def detect_anomalies(run_date: str) -> int:
        """
        Statistical anomaly detection: z-score > 2.5 per category.
        Uses SQL window functions to compute per-category mean/stddev in one pass,
        avoiding a Python-side loop over potentially millions of rows.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        anomaly_rows = hook.get_records("""
            WITH stats AS (
                SELECT
                    category,
                    AVG(amount)    AS cat_avg,
                    STDDEV(amount) AS cat_std
                FROM raw_transactions
                WHERE tx_date = %s
                GROUP BY category
            )
            SELECT
                r.tx_id,
                r.user_id,
                r.amount,
                r.category,
                s.cat_avg,
                ROUND(
                    ((r.amount - s.cat_avg) / NULLIF(s.cat_std, 0))::numeric, 2
                ) AS z_score
            FROM raw_transactions r
            JOIN stats s ON r.category = s.category
            WHERE r.tx_date = %s
              AND (r.amount - s.cat_avg) / NULLIF(s.cat_std, 0) > 2.5
        """, parameters=(run_date, run_date))

        with hook.get_conn() as conn, conn.cursor() as cur:
            for tx_id, user_id, amount, category, cat_avg, z_score in anomaly_rows:
                reason = (
                    f"[{category}] z-score={z_score} | {float(amount):.2f} PLN "
                    f"vs category avg {float(cat_avg):.2f} PLN"
                )
                cur.execute("""
                    INSERT INTO anomalies (tx_id, user_id, amount, reason)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (tx_id, user_id, float(amount), reason))

        print(f"[ANOMALY] Flagged {len(anomaly_rows)} anomalies for {run_date}")
        return len(anomaly_rows)

    @task()
    def duckdb_analytics(payload: dict) -> dict:
        """
        In-memory OLAP with DuckDB: revenue share and avg-ticket analysis over Gold data.
        DuckDB enables fast columnar analytics directly in Python without extra infrastructure –
        ideal for data mesh workloads and analyst self-service.
        """
        summary  = payload["summary"]
        run_date = payload["date"]

        con = duckdb.connect(":memory:")
        con.execute("""
            CREATE TABLE gold (
                category     TEXT,
                total_amount DOUBLE,
                tx_count     INTEGER,
                avg_amount   DOUBLE,
                max_amount   DOUBLE
            )
        """)
        con.executemany(
            "INSERT INTO gold VALUES (?, ?, ?, ?, ?)",
            [
                (r["category"], r["total_amount"], r["tx_count"],
                 r["avg_amount"], r["max_amount"])
                for r in summary
            ],
        )

        rows = con.execute("""
            SELECT
                category,
                total_amount,
                tx_count,
                ROUND(total_amount / SUM(total_amount) OVER (), 4) AS revenue_share,
                ROUND(avg_amount, 2)                               AS avg_tx_value
            FROM gold
            ORDER BY total_amount DESC
        """).fetchall()
        con.close()

        insights = [
            {
                "category":      r[0],
                "total_amount":  r[1],
                "tx_count":      r[2],
                "revenue_share": r[3],
                "avg_tx_value":  r[4],
            }
            for r in rows
        ]

        print(f"[DUCKDB] Revenue share for {run_date}:")
        for r in insights:
            print(f"  {r['category']:15s} {float(r['revenue_share']):>6.1%} | "
                  f"{float(r['total_amount']):>10.2f} PLN | "
                  f"avg ticket: {float(r['avg_tx_value']):.2f} PLN")

        return {**payload, "insights": insights}

    @task()
    def save_summary(payload: dict) -> None:
        """Idempotent upsert of Gold summaries into daily_summary for BI consumption."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with hook.get_conn() as conn, conn.cursor() as cur:
            for row in payload["summary"]:
                cur.execute("""
                    INSERT INTO daily_summary
                        (summary_date, category, total_amount, tx_count, avg_amount, max_amount)
                    VALUES (%(date)s, %(category)s, %(total_amount)s, %(tx_count)s,
                            %(avg_amount)s, %(max_amount)s)
                    ON CONFLICT (summary_date, category) DO UPDATE SET
                        total_amount = EXCLUDED.total_amount,
                        tx_count     = EXCLUDED.tx_count,
                        avg_amount   = EXCLUDED.avg_amount,
                        max_amount   = EXCLUDED.max_amount,
                        processed_at = NOW()
                """, {**row, "date": payload["date"]})

        print(f"[SAVE] Upserted {len(payload['summary'])} rows into daily_summary")

    @task()
    def notify(payload: dict, anomaly_count: int) -> None:
        """
        Daily KPI report to stdout.
        In production: Slack webhook. Escalates to PagerDuty if anomaly_count
        exceeds the Airflow Variable 'anomaly_alert_threshold' (default 10).
        """
        alert_threshold = int(Variable.get("anomaly_alert_threshold", default_var="10"))
        insights = payload.get("insights", payload["summary"])

        print("=" * 62)
        print(f"  DAILY REPORT – {payload['date']}")
        print("=" * 62)

        for r in insights:
            share = float(r.get("revenue_share", 0.0))
            print(f"  {r['category']:15s} | {float(r['total_amount']):>10.2f} PLN | "
                  f"tx: {int(r['tx_count']):>4d} | share: {share:>5.1%}")

        print("-" * 62)
        status = "OK" if anomaly_count <= alert_threshold else "ALERT"
        print(f"  Anomalies: {anomaly_count} [{status}]")
        if status == "ALERT":
            print(f"  >> Threshold {alert_threshold} exceeded – would trigger PagerDuty")
        print("=" * 62)

    # ── Pipeline wiring ────────────────────────────────────────────────────────

    bronze      = ingest_bronze()
    silver      = validate_silver(bronze)
    with_dq     = data_quality_metrics(silver)
    run_date    = load_to_postgres(with_dq)
    gold        = aggregate_gold(run_date)
    anomaly_cnt = detect_anomalies(run_date)
    enriched    = duckdb_analytics(gold)
    save_summary(enriched)
    notify(enriched, anomaly_cnt)


dag_instance = financial_pipeline()