"""Airflow 3 DAG: monthly NYC TLC green-taxi batch -> PySpark -> quality gate -> KPI refresh.
TLC publishes with a ~2-month lag, so the run for logical month M processes month M-2."""
import os, pathlib, sys
import pendulum
from airflow.sdk import dag, task

ROOT = pathlib.Path(os.environ.get("TLC_HOME", pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT))

@dag(dag_id="tlc_green_monthly", schedule="@monthly", start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
     catchup=False, max_active_runs=1, tags=["tlc", "pyspark"],
     default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)})
def tlc_green_monthly():

    @task
    def target_month(logical_date=None) -> str:
        return logical_date.subtract(months=2).format("YYYY-MM")

    @task
    def download(month: str) -> dict:
        os.chdir(ROOT)
        from tlc.ingest import fetch_month, fetch_zones
        fetch_zones()
        return fetch_month(month)

    @task
    def spark_transform(entry: dict) -> dict:
        os.chdir(ROOT)
        from tlc.spark_job import spark_session, transform
        spark = spark_session("tlc_green_monthly")
        try:
            out = f"data/processed/{entry['month']}"
            counts = transform(spark, {entry["month"]: entry["file"]}, "data/raw/taxi_zone_lookup.csv", out)
            return {**counts, "out_dir": out}
        finally:
            spark.stop()

    @task
    def quality_gate(result: dict) -> dict:
        os.chdir(ROOT)
        from tlc.quality import gate
        return gate(result["out_dir"], result)

    quality_gate(spark_transform(download(target_month())))

tlc_green_monthly()
