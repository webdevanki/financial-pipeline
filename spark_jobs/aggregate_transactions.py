"""
spark_jobs/aggregate_transactions.py
=====================================
PySpark job do agregacji transakcji finansowych.

Uruchomienie lokalne (do nauki):
    python aggregate_transactions.py --date 2024-01-15

Uruchomienie przez Spark:
    spark-submit --master spark://spark-master:7077 aggregate_transactions.py

Przez Airflow → SparkSubmitOperator lub BashOperator z spark-submit.
"""

import argparse
import random
import uuid
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, FloatType, IntegerType, StringType, StructField, StructType,
)
from pyspark.sql.window import Window


# ── Schema danych wejściowych ──────────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType([
    StructField("tx_id",    StringType(),  nullable=False),
    StructField("user_id",  IntegerType(), nullable=False),
    StructField("amount",   FloatType(),   nullable=False),  
    StructField("currency", StringType(),  nullable=True),
    StructField("merchant", StringType(),  nullable=True),
    StructField("category", StringType(),  nullable=True),
    StructField("tx_date",  DateType(),    nullable=False),
])


def create_spark_session(app_name: str = "FinancialPipeline") -> SparkSession:
    """
    SparkSession = punkt wejścia do PySpark.
    Jeden SparkSession per aplikacja.
    .getOrCreate() = bezpieczne zwraca istniejącą sesję jeśli jest.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_from_postgres(spark: SparkSession, run_date: str):
    """
    Odczyt danych z Postgresa przez JDBC.
    """
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    properties = {
        "user":     "airflow",
        "password": "airflow",
        "driver":   "org.postgresql.Driver",
    }

    df = (
        spark.read
        .jdbc(url=jdbc_url, table="raw_transactions", properties=properties)
        .filter(F.col("tx_date") == run_date)
    )

    print(f"Wczytano {df.count()} rekordów dla daty {run_date}")
    return df


def read_sample_data(spark: SparkSession, run_date: str):
    """
    Wersja do nauki — generuje dane in-memory bez potrzeby Postgresa.
    """
    categories = ["food", "transport", "entertainment", "shopping", "utilities"]

    run_date_obj = datetime.strptime(run_date, "%Y-%m-%d").date()

    data = []
    for _ in range(1000):
        cat = random.choice(categories)
        amount = round(random.uniform(1, 2000), 2)
        data.append((
            str(uuid.uuid4()),
            random.randint(1, 500),
            amount,
            "PLN",
            "Merchant_" + cat,
            cat,
            run_date_obj,
        ))

    df = spark.createDataFrame(data, schema=TRANSACTION_SCHEMA)
    print(f"Wygenerowano {df.count()} przykładowych rekordów")
    return df


# ── Transformacje ──────────────────────────────────────────────────────────────

def aggregate_by_category(df):

    return (
        df.groupBy("category")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("tx_count"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.min("amount").alias("min_amount"),
        )
        .orderBy(F.desc("total_amount"))
    )


def detect_anomalies(df):
    """
    Window functions — obliczenia w obrębie grupy bez redukowania wierszy.
    Z-score > 2.5 = anomalia statystyczna.
    """
    window = Window.partitionBy("category")

    df_stats = (
        df
        .withColumn("cat_avg",    F.avg("amount").over(window))
        .withColumn("cat_stddev", F.stddev("amount").over(window))
        .withColumn("z_score",
            (F.col("amount") - F.col("cat_avg")) / F.col("cat_stddev")
        )
    )

    anomalies = (
        df_stats
        .filter(F.col("z_score") > 2.5)
        .select(
            "tx_id", "user_id", "amount", "category",
            F.round("cat_avg", 2).alias("category_avg"),
            F.round("z_score", 2).alias("z_score"),
        )
    )

    return anomalies


def user_spending_rank(df):
    """
    Ranking użytkowników po wydatkach.
    dense_rank nie pomija numerów po remisach.
    """
    window = Window.orderBy(F.desc("total_spent"))

    return (
        df.groupBy("user_id")
        .agg(F.sum("amount").alias("total_spent"))
        .withColumn("rank", F.dense_rank().over(window))
        .filter(F.col("rank") <= 10)
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Financial transactions aggregation")
    parser.add_argument("--date", default=date.today().isoformat(), help="Data YYYY-MM-DD")
    parser.add_argument("--mode", choices=["sample", "postgres"], default="sample")
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print(f"PySpark Financial Pipeline – data: {args.date}")
    print(f"{'='*50}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Wczytaj dane
    if args.mode == "postgres":
        df = read_from_postgres(spark, args.date)
    else:
        df = read_sample_data(spark, args.date)

    # 2. Podgląd danych
    print("\n── Schemat danych ──")
    df.printSchema()

    print("\n── Przykładowe rekordy ──")
    df.show(5, truncate=False)

    # 3. Agregacja
    print("\n── Agregacja per kategoria ──")
    summary = aggregate_by_category(df)
    summary.show(truncate=False)

    # 4. Anomalie
    print("\n── Wykryte anomalie (z-score > 2.5) ──")
    anomalies = detect_anomalies(df)
    anomalies.show(truncate=False)
    print(f"Liczba anomalii: {anomalies.count()}")

    # 5. Top użytkownicy
    print("\n── Top 10 użytkowników po wydatkach ──")
    top_users = user_spending_rank(df)
    top_users.show(truncate=False)

    # 6. Zapis wyników do Parquet
    output_path = f"/opt/data/processed/summary_{args.date}"
    summary.write.mode("overwrite").parquet(output_path)
    print(f"\n✓ Wyniki zapisane do: {output_path}")

    spark.stop()
    print("\n✓ Job zakończony sukcesem")


if __name__ == "__main__":
    main()