import pathlib, pytest
airflow = pytest.importorskip("airflow")

def test_dag_imports_and_shape():
    from airflow.models.dagbag import DagBag
    bag = DagBag(dag_folder=str(pathlib.Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert bag.import_errors == {}
    dag = bag.get_dag("tlc_green_monthly")
    assert [t.task_id for t in dag.topological_sort()] == ["target_month", "download", "spark_transform", "quality_gate"]
    assert dag.default_args["retries"] == 2
