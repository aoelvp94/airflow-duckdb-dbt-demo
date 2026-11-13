from airflow.sdk import dag, task
from pendulum import datetime

BASE = "/usr/local/airflow/include"


@dag(
    dag_id="30_compare_results",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "validation"],
    doc_md=(
        "Correr DESPUÉS de `20_pipeline_duckdb_dbt` y de al menos uno de los pipelines de pandas "
        "(`10` y/o `15`): verifica que den resultados idénticos a DuckDB/dbt."
    ),
)
def compare_results():
    @task
    def compare() -> dict:
        from include.compare import compare as fn
        return fn(
            {
                "pandas": f"{BASE}/output/pandas_monthly_revenue.parquet",
                "pandas_pyarrow": f"{BASE}/output/pandas_pyarrow_monthly_revenue.parquet",
            },
            f"{BASE}/warehouse/ecommerce.duckdb",
        )

    compare()


compare_results()
