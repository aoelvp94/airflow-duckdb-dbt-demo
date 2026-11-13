# Guion de demo (25-30 min): pandas vs DuckDB + dbt orquestados con Airflow

Objetivo: mostrar el **mismo pipeline** de tres formas dentro de Airflow (Astro CLI) y que los
alumnos distingan dos cosas que suelen confundirse: **el motor** (pandas, PyArrow, DuckDB), que
decide tiempo y memoria, y **el orquestador/framework** (Airflow, dbt, Cosmos), que aporta
reintentos, tests, linaje y visibilidad.

## Antes de la clase (30 min, una sola vez)

Requisitos: Docker corriendo y [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).

```bash
astro dev start                 # la primera vez construye la imagen (varios minutos)
```
1. Abrí http://localhost:8080 (si pide login en el entorno local de Astro, probá `admin` / `admin`).
2. Disparar `00_generate_data` con `rows` = 5000000 (o lo que aguante tu máquina).
3. Disparar una vez `10_pipeline_pandas`, `15_pipeline_pandas_pyarrow` y `20_pipeline_duckdb_dbt`.
   El primer `dbt` baja el driver de DuckDB, así que necesita internet: mejor que eso pase ahora.
4. Disparar `30_compare_results` y confirmar que termina en verde.
5. `astro dev pytest` para validar que los DAGs importan bien.
6. Anotá los tiempos del ensayo (Gantt) por si la demo en vivo se demora.

Si algo falla, mirá la sección "Gotchas" del `README.md` (Cosmos + dbt v2 y los locks de DuckDB
son lo más probable).

## Paso a paso

### 1. Mostrar el proyecto (3 min)
Abrí en el editor y marcá una cosa de cada uno:
- `Dockerfile`: una sola línea. dbt v2 es un binario y no necesita virtualenv aparte.
- `requirements.txt`: `astronomer-cosmos[dbt-oss]`, `duckdb`, `pandas`, `pyarrow`.
- `dags/`: cuatro DAGs. `include/pipelines/`: el código pandas. `dbt/ecommerce/`: el proyecto dbt.
- `airflow_settings.yaml`: un pool de 1 slot. Dejá la pregunta abierta: *¿por qué?* (se contesta en el paso 6).

### 2. Levantar Airflow y generar datos (3 min)
```bash
astro dev start
```
Disparar `00_generate_data` con **Trigger DAG w/ config** y `rows` = 5000000.
Mostrá los archivos que aparecen en la carpeta del proyecto: `ls -lh include/data`.

### 3. Pipeline 1, pandas ingenuo (4 min)
Disparar `10_pipeline_pandas`. Mientras corre, abrí `include/pipelines/pandas_pipeline.py`.
- Mostrá en el **Graph** las tres tasks y en los **logs** cuánto tarda cada una.
- Mostrá el intermedio que se escribe a disco entre tasks: `ls -lh include/tmp`.
  Las tasks no comparten memoria, así que pandas pasa el dataset completo por disco.

### 4. Pipeline 2, pandas sobre PyArrow (3 min)
Disparar `15_pipeline_pandas_pyarrow` y compará con el anterior:
- Duración de cada task en el **Gantt**.
- Tamaño del intermedio: `ls -lh include/tmp` (más chico: leyó solo lo necesario).
- Lo que tuvimos que escribir a mano: columnas, filtro en la lectura, orden de los pasos.

### 5. Pipeline 3, DuckDB + dbt con Cosmos (5 min)
Disparar `20_pipeline_duckdb_dbt`.
- En el **Graph**, el grupo `dbt` se expande en una task por modelo y por test. Cosmos lo genera
  leyendo el proyecto dbt: ese es el linaje (`ref()`) convertido en dependencias de Airflow.
- Abrí `dbt/ecommerce/models/marts/monthly_revenue.sql`: la lógica es SQL declarativo.
- Mostrá `schema.yml`: los tests (`data_tests:`) son tasks propias. En pandas esto era un
  `assert` a mano en `quality_check`.
- Contá el matiz honesto: con pocos datos, una task por modelo suma arranques de dbt. DuckDB
  gana cuando el volumen crece, no por magia del orquestador.

### 6. Los límites de DuckDB (2 min)
Volvé a la pregunta del pool. DuckDB en modo archivo admite **un solo proceso escritor**: por
eso el pool `duckdb_single_writer` y `threads: 1` en el profile. Es un límite real de una
herramienta embebida frente a un warehouse en la nube (o MotherDuck / DuckLake).

### 7. Validar y medir (4 min)
- Disparar `30_compare_results`: verifica que pandas y DuckDB/dbt dieron resultados idénticos.
- Para la memoria, en otra terminal:
  ```bash
  ./scripts/sample_mem.sh
  ```
  y volvé a disparar los tres pipelines uno por uno. Cortá con Ctrl+C y mirá el pico de cada
  corrida (anotalo en `BENCHMARK.md`).

### 8. Demo de OOM (opcional, 4 min)
```bash
cp docker-compose.override.yml.example docker-compose.override.yml
astro dev restart
```
Con el scheduler limitado a 2 GB y suficientes filas, la task de pandas muere por falta de
memoria. Después disparar `20_pipeline_duckdb_dbt` sobre los mismos datos. Ojo: el kernel puede
matar procesos del contenedor del scheduler; si Airflow tarda en responder, esperá unos
segundos. Para volver atrás: borrar el override y `astro dev restart`.

### 9. Cierre (2 min)
| Pregunta | Respuesta |
|---|---|
| ¿Qué hace más rápido o más liviano al pipeline? | El **motor** (DuckDB / Arrow), no Airflow |
| ¿Qué aporta Airflow? | Reintentos, schedule, visibilidad, dependencias |
| ¿Qué aporta dbt + Cosmos? | Tests declarativos, linaje, una task por modelo |
| ¿Qué cuesta? | Más piezas, un escritor en DuckDB, Cosmos con dbt OSS todavía experimental |

## Si algo sale mal en vivo
- **El DAG de dbt no aparece o da error de parseo**: mirá el log del *dag-processor*
  (`astro dev logs`). Si es un lock de DuckDB, pasá Cosmos a `LoadMode.DBT_MANIFEST`.
- **`30_compare_results` falla por archivo inexistente**: faltó correr el pipeline pandas o el de dbt.
- **El OOM no reproduce**: subí `rows` en `00_generate_data` o bajá `mem_limit` en el override.

## Ejercicios para los alumnos
1. Agregá un modelo `top_customers` en `dbt/ecommerce/models/marts/` y mirá cómo aparece en el DAG.
2. Rompé un test (un `status` inválido en los datos) y observá qué falla y qué no.
3. Probá `ExecutionMode.WATCHER` de Cosmos: un solo `dbt build` observado por Airflow.
4. Prendé `static_analysis` en `marts` y mirá qué errores detecta dbt v2 antes de ejecutar.
