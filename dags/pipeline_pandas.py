from airflow.sdk import dag, task
from pendulum import datetime

BASE = "/usr/local/airflow/include"


@dag(
    dag_id="10_pipeline_pandas",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "pandas"],
    doc_md="Pipeline **pandas**: extract+join -> transform+aggregate -> check. Pasa datos por disco.",
)
def pipeline_pandas():
    @task
    def extract_join() -> str:
        from include.pipelines.pandas_pipeline import extract_join as fn
        return fn(f"{BASE}/data", f"{BASE}/tmp")

    @task
    def transform_aggregate(enriched_path: str) -> str:
        from include.pipelines.pandas_pipeline import transform_aggregate as fn
        return fn(enriched_path, f"{BASE}/output")

    @task
    def quality_check(result_path: str) -> int:
        from include.pipelines.pandas_pipeline import quality_check as fn
        return fn(result_path)

    quality_check(transform_aggregate(extract_join()))


pipeline_pandas()
