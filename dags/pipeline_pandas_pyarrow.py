from airflow.sdk import dag, task
from pendulum import datetime

BASE = "/usr/local/airflow/include"


@dag(
    dag_id="15_pipeline_pandas_pyarrow",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "pandas", "pyarrow"],
    doc_md=(
        "Pipeline **pandas optimizado sobre PyArrow**: pushdown de columnas y filtros en la lectura, "
        "join en Arrow y `dtype_backend='pyarrow'`. Mismo resultado que `10_pipeline_pandas`."
    ),
)
def pipeline_pandas_pyarrow():
    @task
    def extract_filter_join() -> str:
        from include.pipelines.pandas_pyarrow_pipeline import extract_filter_join as fn
        return fn(f"{BASE}/data", f"{BASE}/tmp")

    @task
    def transform_aggregate(enriched_path: str) -> str:
        from include.pipelines.pandas_pyarrow_pipeline import transform_aggregate as fn
        return fn(enriched_path, f"{BASE}/output")

    @task
    def quality_check(result_path: str) -> int:
        from include.pipelines.pandas_pyarrow_pipeline import quality_check as fn
        return fn(result_path)

    quality_check(transform_aggregate(extract_filter_join()))


pipeline_pandas_pyarrow()
