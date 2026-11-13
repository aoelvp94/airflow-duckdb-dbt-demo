"""Datos sintéticos de e-commerce en Parquet, escritos por chunks (no carga todo en RAM)."""
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

COUNTRIES = ["AR", "BR", "CL", "CO", "MX", "PE", "UY", "ES", "FR", "DE"]
SEGMENTS = ["consumer", "smb", "enterprise"]
STATUSES = ["completed", "completed", "completed", "cancelled", "refunded"]


def generate(rows: int, data_dir: str, customers: int = 500_000, seed: int = 42,
             chunk: int = 2_000_000) -> dict:
    out = Path(data_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)

    pq.write_table(
        pa.table({
            "customer_id": np.arange(customers, dtype=np.int64),
            "country": rng.choice(COUNTRIES, customers),
            "segment": rng.choice(SEGMENTS, customers),
        }),
        out / "customers.parquet",
    )

    start = np.datetime64("2022-01-01T00:00:00")
    span = 3 * 365 * 24 * 3600
    final, tmp = out / "orders.parquet", out / "orders.tmp"
    writer, written = None, 0
    try:
        while written < rows:
            n = min(chunk, rows - written)
            table = pa.table({
                "order_id": np.arange(written, written + n, dtype=np.int64),
                "customer_id": rng.integers(0, customers, n, dtype=np.int64),
                "order_ts": start + rng.integers(0, span, n).astype("timedelta64[s]"),
                "quantity": rng.integers(1, 6, n, dtype=np.int32),
                "unit_price": np.round(rng.gamma(2.0, 25.0, n), 2),
                "discount": np.round(rng.choice([0, 0.05, 0.1, 0.2], n), 2),
                "status": rng.choice(STATUSES, n),
                # columnas que el pipeline NO usa (tablas anchas, como en la vida real)
                "notes": rng.choice(["", "gift", "express", "fragile", "call before"], n),
                "channel": rng.choice(["web", "app", "store", "partner"], n),
            })
            writer = writer or pq.ParquetWriter(tmp, table.schema)
            writer.write_table(table)
            written += n
    finally:
        if writer:
            writer.close()
    tmp.replace(final)  # escritura atómica: nunca queda un parquet a medias
    return {"rows": rows, "orders_mb": round(final.stat().st_size / 1e6)}
