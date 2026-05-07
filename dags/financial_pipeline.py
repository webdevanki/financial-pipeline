"""
DAG: financial_pipeline
========================
Orkiestruje dzienny pipeline finansowy:
  1. generate_data   – generuje losowe transakcje (symulacja źródła danych)
  2. validate_data   – sprawdza czy dane są kompletne i poprawne
  3. run_spark_job   – uruchamia PySpark do agregacji i wykrycia anomalii
  4. load_summary    – zapisuje wyniki do Postgresa
  5. notify          – loguje raport końcowy

Uruchomienie: codziennie o 6:00, lub ręcznie z UI Airflow.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import date, datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago


# ── Konfiguracja ──────────────────────────────────────────────────────────────

POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

CATEGORIES = ["food", "transport", "entertainment", "shopping", "utilities", "healthcare"]
MERCHANTS = {
    "food":          ["Biedronka", "Lidl", "Żabka", "McDonald's", "Bolt Food"],
    "transport":     ["PKP", "Uber", "Bolt", "MPK Warszawa", "Orlen"],
    "entertainment": ["Netflix", "Spotify", "Steam", "Empik", "Cinema City"],
    "shopping":      ["Allegro", "Zalando", "H&M", "Ikea", "MediaMarkt"],
    "utilities":     ["PGE", "Orange", "T-Mobile", "Canal+", "Veolia"],
    "healthcare":    ["LuxMed", "Medicover", "Apteka", "Dentist", "Gym"],
}


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="financial_pipeline",
    description="Dzienny pipeline finansowy: ingest → validate → spark → load",
    schedule_interval="0 6 * * *",        # codziennie o 6:00
    start_date=days_ago(1),
    catchup=False,
    tags=["fintech", "pyspark", "allegro-pay-style"],
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
)
def financial_pipeline():

    @task()
    def generate_data(n: int = 500) -> dict:
        """
        Krok 1: Generuje n losowych transakcji finansowych.
        Zwraca słownik z listą transakcji i datą
        """
        today = date.today().isoformat()
        transactions = []

        for _ in range(n):
            category = random.choice(CATEGORIES)

            # 2% transakcji to celowe anomalie (bardzo duże kwoty)
            is_anomaly = random.random() < 0.02
            amount = round(random.uniform(5000, 50000), 2) if is_anomaly \
                     else round(random.uniform(1, 1500), 2)

            transactions.append({
                "tx_id":    str(uuid.uuid4()),
                "user_id":  random.randint(1, 1000),
                "amount":   amount,
                "currency": "PLN",
                "merchant": random.choice(MERCHANTS[category]),
                "category": category,
                "tx_date":  today,
            })

        print(f"✓ Wygenerowano {len(transactions)} transakcji na dzień {today}")

        return {"transactions": transactions, "date": today, "count": len(transactions)}

    @task()
    def validate_data(payload: dict) -> dict:
        """
        Krok 2: Walidacja danych przed dalszym przetwarzaniem.
        Sprawdzamy kompletność, typy, zakresy wartości.
        """
        transactions = payload["transactions"]
        errors = []

        for tx in transactions:
            if tx["amount"] <= 0:
                errors.append(f"Ujemna kwota: {tx['tx_id']}")
            if not tx["tx_id"]:
                errors.append("Brak tx_id")
            if tx["category"] not in CATEGORIES:
                errors.append(f"Nieznana kategoria: {tx['category']}")

        if errors:
            raise ValueError(f"Walidacja nie przeszła! Błędy: {errors[:5]}")

        print(f"✓ Walidacja OK – {len(transactions)} transakcji poprawnych")
        return payload  # przekazujemy dalej bez zmian

    @task()
    def load_raw_to_postgres(payload: dict) -> str:
        """
        Krok 3: Ładowanie surowych danych do Postgresa (tabela raw_transactions).
        """
        transactions = payload["transactions"]
        run_date = payload["date"]

        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        inserted = 0
        for tx in transactions:
            cur.execute("""
                INSERT INTO raw_transactions
                    (tx_id, user_id, amount, currency, merchant, category, tx_date)
                VALUES
                    (%(tx_id)s, %(user_id)s, %(amount)s, %(currency)s,
                     %(merchant)s, %(category)s, %(tx_date)s)
                ON CONFLICT (tx_id) DO NOTHING
            """, tx)
            if cur.rowcount:
                inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        print(f"✓ Załadowano {inserted}/{len(transactions)} transakcji do Postgresa")
        return run_date

    @task()
    def aggregate_in_python(run_date: str) -> dict:
        """
        Krok 4: Agregacja w Pythonie (w prawdziwym projekcie → PySpark job).
        Grupujemy transakcje po kategorii i liczymy statystyki.
        """
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        cur.execute("""
            SELECT
                category,
                SUM(amount)   AS total_amount,
                COUNT(*)      AS tx_count,
                AVG(amount)   AS avg_amount,
                MAX(amount)   AS max_amount
            FROM raw_transactions
            WHERE tx_date = %s
            GROUP BY category
            ORDER BY total_amount DESC
        """, (run_date,))

        rows = cur.fetchall()
        columns = ["category", "total_amount", "tx_count", "avg_amount", "max_amount"]
        summary = [dict(zip(columns, row)) for row in rows]

        cur.close()
        conn.close()

        print(f"✓ Agregacja: {len(summary)} kategorii dla daty {run_date}")
        return {"summary": summary, "date": run_date}

    @task()
    def detect_anomalies(run_date: str) -> int:
        """
        Krok 5: Wykrycie anomalii – transakcje > 3x średnia dla kategorii.
        Zapisuje anomalie do tabeli anomalies.
        """
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        # Oblicz średnią per kategoria
        cur.execute("""
            WITH category_avg AS (
                SELECT category, AVG(amount) as avg_amt
                FROM raw_transactions
                WHERE tx_date = %s
                GROUP BY category
            )
            SELECT r.tx_id, r.user_id, r.amount, r.category, ca.avg_amt
            FROM raw_transactions r
            JOIN category_avg ca ON r.category = ca.category
            WHERE r.tx_date = %s
              AND r.amount > ca.avg_amt * 3
        """, (run_date, run_date))

        anomalies = cur.fetchall()
        anomaly_count = 0

        for tx_id, user_id, amount, category, avg_amt in anomalies:
            reason = f"Kwota {amount:.2f} PLN > 3x średnia {avg_amt:.2f} PLN w kategorii {category}"
            cur.execute("""
                INSERT INTO anomalies (tx_id, user_id, amount, reason)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (tx_id, user_id, amount, reason))
            anomaly_count += 1

        conn.commit()
        cur.close()
        conn.close()

        print(f"⚠️  Wykryto {anomaly_count} anomalii na dzień {run_date}")
        return anomaly_count

    @task()
    def save_summary(payload: dict) -> None:
        """
        Krok 6: Zapis agregacji do tabeli daily_summary.
        """
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        for row in payload["summary"]:
            cur.execute("""
                INSERT INTO daily_summary
                    (summary_date, category, total_amount, tx_count, avg_amount, max_amount)
                VALUES
                    (%(date)s, %(category)s, %(total_amount)s, %(tx_count)s,
                     %(avg_amount)s, %(max_amount)s)
                ON CONFLICT (summary_date, category)
                DO UPDATE SET
                    total_amount = EXCLUDED.total_amount,
                    tx_count     = EXCLUDED.tx_count,
                    avg_amount   = EXCLUDED.avg_amount,
                    max_amount   = EXCLUDED.max_amount,
                    processed_at = NOW()
            """, {**row, "date": payload["date"]})

        conn.commit()
        cur.close()
        conn.close()
        print(f"✓ Zapisano summary dla {len(payload['summary'])} kategorii")

    @task()
    def notify(payload: dict, anomaly_count: int) -> None:
        """
        Krok 7: Raport końcowy. W produkcji → Slack / email / PagerDuty.
        """
        print("=" * 50)
        print(f"📊 RAPORT DZIENNY – {payload['date']}")
        print("=" * 50)
        for row in payload["summary"]:
            print(f"  {row['category']:15s} | "
                  f"suma: {float(row['total_amount']):>10.2f} PLN | "
                  f"transakcji: {row['tx_count']:>4d} | "
                  f"avg: {float(row['avg_amount']):>8.2f} PLN")
        print("-" * 50)
        print(f"⚠️  Anomalie wykryte: {anomaly_count}")
        print("=" * 50)


    raw         = generate_data()
    validated   = validate_data(raw)
    run_date    = load_raw_to_postgres(validated)
    agg_payload = aggregate_in_python(run_date)
    anomalies   = detect_anomalies(run_date)
    save_summary(agg_payload)
    notify(agg_payload, anomalies)


dag_instance = financial_pipeline()
