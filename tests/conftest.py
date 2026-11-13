"""Config para `astro dev pytest`.

Cosmos guarda el resultado de `dbt ls` en una Airflow Variable (caché). En los tests no hay
base de metadatos inicializada, así que esa escritura falla con "no such table: variable".
Apagamos el caché solo en los tests: el DAG igual corre `dbt ls`, que es lo que queremos validar.
"""
import os

os.environ["AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS"] = "False"
