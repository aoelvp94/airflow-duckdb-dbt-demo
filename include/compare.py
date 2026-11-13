"""Verifica que cada pipeline de pandas coincida con el resultado de DuckDB/dbt."""
import math
import os

import duckdb
import pyarrow.parquet as pq


def _rows(path: str) -> list[dict]:
    return sorted(pq.read_table(path).to_pylist(), key=lambda r: (r["country"], r["month"]))


def compare(pandas_outputs: dict[str, str], duckdb_file: str) -> dict[str, int]:
    """Compara cada salida existente en `pandas_outputs` {nombre: parquet} contra monthly_revenue."""
    present = {name: p for name, p in pandas_outputs.items() if os.path.exists(p)}
    if not present:
        raise FileNotFoundError(f"No hay salidas de pandas para comparar: {list(pandas_outputs.values())}")

    con = duckdb.connect(duckdb_file, read_only=True)
    ref = con.sql("select country, month, orders, revenue, avg_ticket "
                  "from monthly_revenue order by country, month").fetchall()

    for name, path in present.items():
        rows = _rows(path)
        assert len(rows) == len(ref), f"[{name}] filas distintas: {len(rows)} vs {len(ref)}"
        for r, (country, month, orders, revenue, avg_ticket) in zip(rows, ref):
            assert (r["country"], r["month"], r["orders"]) == (country, month, orders), (name, r, country, month)
            assert math.isclose(r["revenue"], revenue, rel_tol=1e-6), (name, r, revenue)
            assert math.isclose(r["avg_ticket"], avg_ticket, rel_tol=1e-6), (name, r, avg_ticket)
    return {name: len(ref) for name in present}
