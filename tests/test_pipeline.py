"""Smoke and unit tests for the financial_pipeline DAG."""
from datetime import timedelta


def test_dag_loads_without_error():
    from dags.financial_pipeline import dag_instance
    assert dag_instance is not None


def test_dag_id():
    from dags.financial_pipeline import dag_instance
    assert dag_instance.dag_id == "financial_pipeline"


def test_expected_tasks_present():
    from dags.financial_pipeline import dag_instance

    task_ids = {t.task_id for t in dag_instance.tasks}
    required = {
        "ingest_bronze",
        "validate_silver",
        "data_quality_metrics",
        "load_to_postgres",
        "aggregate_gold",
        "detect_anomalies",
        "duckdb_analytics",
        "save_summary",
        "notify",
    }
    assert required.issubset(task_ids), f"Missing tasks: {required - task_ids}"


def test_retries_sla_and_callback():
    from dags.financial_pipeline import dag_instance, _on_failure_callback

    defaults = dag_instance.default_args
    assert defaults["retries"] == 2
    assert defaults["retry_delay"] == timedelta(minutes=5)
    assert defaults["sla"] == timedelta(hours=2)
    assert defaults["on_failure_callback"] is _on_failure_callback


def test_schedule_is_daily_6am_utc():
    from dags.financial_pipeline import dag_instance
    assert dag_instance.schedule_interval == "0 6 * * *"


def test_catchup_disabled():
    from dags.financial_pipeline import dag_instance
    assert dag_instance.catchup is False


def test_tags_present():
    from dags.financial_pipeline import dag_instance
    assert "medallion" in dag_instance.tags
    assert "duckdb" in dag_instance.tags
