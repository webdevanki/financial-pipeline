import os

os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "true")
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "sqlite:////tmp/airflow_test.db",
)
