-- DuckDB lee Parquet directo: sin pasos de "cargar a una tabla" previos.
select
    order_id,
    customer_id,
    order_ts,
    quantity,
    unit_price,
    discount,
    status
from read_parquet('{{ var("data_dir") }}/orders.parquet')
