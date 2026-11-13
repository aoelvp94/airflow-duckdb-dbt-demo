from airflow.sdk import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import InvocationMode
from pendulum import datetime

DBT_PROJECT = "/usr/local/airflow/dbt/ecommerce"

profile_config = ProfileConfig(
    profile_name="ecommerce",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT}/profiles.yml",  # DuckDB: sin Airflow connection
)


@dag(
    dag_id="20_pipeline_duckdb_dbt",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "duckdb", "dbt-v2"],
    doc_md="Pipeline **DuckDB + dbt v2 (dbt-oss)** orquestado por Cosmos: un task por modelo y por test.",
)
def pipeline_duckdb_dbt():
    DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(DBT_PROJECT, install_dbt_deps=False),  # sin packages.yml
        profile_config=profile_config,
        # dbt v2 es un binario: lo invocamos como subproceso (`dbt` en el PATH del entorno).
        execution_config=ExecutionConfig(invocation_mode=InvocationMode.SUBPROCESS),
        operator_args={"pool": "duckdb_single_writer"},  # un solo escritor sobre el .duckdb
    )


pipeline_duckdb_dbt()
