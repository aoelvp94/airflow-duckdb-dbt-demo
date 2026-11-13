# Airflow 3.3 (Astro Runtime 3.3). Para Airflow 3 se especifica solo major.minor.
# dbt v2 (dbt-oss) llega por requirements.txt: es un binario autocontenido (Rust),
# asi que ya no hace falta un virtualenv aparte para evitar conflictos con Airflow.
FROM astrocrpublic.azurecr.io/runtime:3.3
