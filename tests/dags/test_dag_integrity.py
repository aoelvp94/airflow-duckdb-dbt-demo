from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors, dag_bag.import_errors


def test_expected_dags_present():
    dag_bag = DagBag(include_examples=False)
    for dag_id in ("00_generate_data", "10_pipeline_pandas", "15_pipeline_pandas_pyarrow", "20_pipeline_duckdb_dbt", "30_compare_results"):
        assert dag_id in dag_bag.dags
