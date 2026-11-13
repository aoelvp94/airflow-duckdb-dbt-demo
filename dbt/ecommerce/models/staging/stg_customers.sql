select
    customer_id,
    country,
    segment
from read_parquet('{{ var("data_dir") }}/customers.parquet')
