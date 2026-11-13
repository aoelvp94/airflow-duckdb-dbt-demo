"""Pipeline en pandas, partido en pasos como se suele hacer en Airflow.

Ojo con lo que enseña: cada paso relee/reescribe el dataset COMPLETO a disco, porque las
tareas no comparten memoria (y pasar un DataFrame grande por XCom sería peor).
"""
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def extract_join(data_dir: str, tmp_dir: str) -> str:
    orders = pd.read_parquet(f"{data_dir}/orders.parquet")      # TODAS las columnas y filas
    customers = pd.read_parquet(f"{data_dir}/customers.parquet")
    df = orders.merge(customers, on="customer_id")              # join antes de filtrar
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    path = f"{tmp_dir}/orders_enriched.parquet"
    df.to_parquet(path, index=False)                            # intermedio gigante a disco
    return path


def transform_aggregate(enriched_path: str, out_dir: str) -> str:
    df = pd.read_parquet(enriched_path)                         # se vuelve a cargar todo
    df = df[(df["status"] == "completed") & (df["order_ts"] >= "2023-01-01")]
    df["net"] = df["quantity"] * df["unit_price"] * (1 - df["discount"])
    df["month"] = df["order_ts"].dt.strftime("%Y-%m")
    out = (
        df.groupby(["country", "month"])
        .agg(orders=("order_id", "count"), revenue=("net", "sum"), avg_ticket=("net", "mean"))
        .reset_index()
        .sort_values(["country", "month"])
    )
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    path = f"{out_dir}/pandas_monthly_revenue.parquet"
    out.to_parquet(path, index=False)
    return path


def quality_check(result_path: str) -> int:
    """Chequeo manual: en el mundo dbt esto son `tests:` declarativos en schema.yml."""
    table = pq.read_table(result_path)
    assert table.num_rows > 0, "resultado vacío"
    for col in ("country", "month", "orders", "revenue"):
        assert table[col].null_count == 0, f"nulos en {col}"
    assert min(table["revenue"].to_pylist()) > 0, "revenue <= 0"
    return table.num_rows
