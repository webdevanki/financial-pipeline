# 💳 Financial Data Pipeline

> Dzienny pipeline danych finansowych zbudowany z Apache Airflow, PySpark i PostgreSQL.  
> Projekt demonstracyjny w stylu Allegro Pay – orkiestracja, transformacje i wykrywanie anomalii.

---

## Architektura

```
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐
│  Data Source │───▶│   Airflow    │───▶│    PySpark      │───▶│  PostgreSQL  │
│  (generator) │    │  Scheduler   │    │  Aggregation +  │    │  daily_      │
│              │    │  + DAG       │    │  Anomaly Det.   │    │  summary     │
└─────────────┘    └──────────────┘    └─────────────────┘    └──────────────┘
```

## Stack technologiczny

| Technologia | Wersja | Zastosowanie |
|---|---|---|
| Apache Airflow | 2.9.1 | Orkiestracja pipeline'u (DAGi, scheduling) |
| Apache Spark | 3.5 | Przetwarzanie dużych zbiorów danych |
| PySpark | 3.5.1 | Python API do Sparka |
| PostgreSQL | 15 | Data warehouse (raw + aggregated) |
| Docker Compose | 3.8 | Konteneryzacja środowiska |

## Pipeline – kroki

```
generate_data
     │
validate_data          ← walidacja kompletności i typów
     │
load_raw_to_postgres   ← INSERT z idempotencją (ON CONFLICT DO NOTHING)
     │
     ├── aggregate_in_python  ← agregacja per kategoria (→ daily_summary)
     │         │
     │    save_summary
     │
     └── detect_anomalies     ← reguła 2-sigma (z-score > 2.5)
               │
            notify             ← raport dzienny (Slack-ready)
```

## Uruchomienie lokalne

### Wymagania
- Docker + Docker Compose
- min. 4 GB RAM dla Dockera

### Start

```bash
git clone https://github.com/TWOJ_USERNAME/financial-pipeline
cd financial-pipeline

# Uruchom cały stack
docker compose up -d

# Poczekaj ~2 minuty na inicjalizację Airflow
# Sprawdź status
docker compose ps
```

### Dostęp do UI

| Serwis | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:8081 | – |
| PostgreSQL | localhost:5432 | airflow / airflow |

### Uruchomienie DAGa

```bash
# Przez Airflow UI: zaloguj się → DAGs → financial_pipeline → Trigger DAG

# Lub przez CLI:
docker compose exec airflow-scheduler airflow dags trigger financial_pipeline
```

### PySpark job lokalnie

```bash
# Wejdź do kontenera Spark i uruchom job z przykładowymi danymi
docker compose exec spark-master spark-submit \
    /opt/spark_jobs/aggregate_transactions.py --mode sample

# Lub z Pythonem lokalnie (po: pip install pyspark)
python spark_jobs/aggregate_transactions.py --mode sample --date 2024-01-15
```

## Struktura projektu

```
financial-pipeline/
├── dags/
│   └── financial_pipeline.py   # Główny DAG z TaskFlow API
├── spark_jobs/
│   └── aggregate_transactions.py  # PySpark: agregacje + anomalie
├── config/
│   └── init_db.sql             # Schemat bazy danych
├── data/
│   ├── raw/                    # Surowe dane (CSV, JSON)
│   └── processed/              # Wyniki Sparka (Parquet)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Kluczowe koncepty

**Idempotentność** – każdy task może być uruchomiony wielokrotnie bez duplikowania danych (`ON CONFLICT DO NOTHING`, `write.mode("overwrite")`).

**XCom** – mechanizm Airflow do przekazywania małych danych między taskami. Duże dane zawsze przez bazę lub storage.

**Lazy evaluation** – PySpark buduje plan wykonania (DAG operacji) i uruchamia go dopiero przy `show()`/`write()`. Pozwala na optymalizację całego planu.

**Window functions** – agregacje bez utraty wierszy. Używane do wykrywania anomalii metodą z-score.

## Wyniki

Po uruchomieniu pipeline'u w bazie dostępne są:

```sql
-- Dzienna agregacja per kategoria
SELECT * FROM daily_summary ORDER BY summary_date DESC, total_amount DESC;

-- Wykryte anomalie
SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 20;

-- Raw transactions
SELECT category, COUNT(*), AVG(amount) FROM raw_transactions GROUP BY 1;
```
