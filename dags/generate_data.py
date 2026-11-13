from airflow.sdk import dag, task
from pendulum import datetime

DATA_DIR = "/usr/local/airflow/include/data"


@dag(
    dag_id="00_generate_data",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={"rows": 5_000_000},  # cambialo al disparar el DAG: 1M, 10M, 50M...
    tags=["demo", "setup"],
    doc_md="Genera `orders.parquet` y `customers.parquet`. Correr UNA vez antes de los pipelines.",
)
def generate_data():
    @task
    def generate(params=None) -> dict:
        from include.generate_data import generate as gen
        return gen(rows=int(params["rows"]), data_dir=DATA_DIR)

    generate()


generate_data()
