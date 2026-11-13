# Benchmark dentro de Airflow: cómo medirlo y qué significa

Este benchmark compara el **mismo pipeline** (filtro → join → revenue mensual por país) en tres
implementaciones orquestadas con Airflow. Para la comparación pura de motores, sin orquestador,
ver el repo `duckdb-vs-pandas`.

| DAG | Implementación |
|---|---|
| `10_pipeline_pandas` | pandas ingenuo: lee todo, join antes de filtrar, intermedio completo a disco |
| `15_pipeline_pandas_pyarrow` | pandas optimizado: pushdown de columnas y filtros, join en Arrow |
| `20_pipeline_duckdb_dbt` | DuckDB + dbt v2, una task por modelo y por test (Cosmos) |

## Qué se mide

| Métrica | Dónde sale | Nota |
|---|---|---|
| Duración del DAG run | UI: Grid o Gantt del run | Incluye el arranque de cada task |
| Duración por task | UI: Gantt | Muestra dónde está el tiempo |
| Pico de RAM | `./scripts/sample_mem.sh` | Muestrea `docker stats` del contenedor scheduler (las tasks corren ahí con LocalExecutor) |
| Tamaño del intermedio | `ls -lh include/tmp` | Solo aplica a los pipelines pandas |
| Resultado correcto | DAG `30_compare_results` | Verifica que los resultados sean idénticos |

## Protocolo

1. Misma máquina, mismos datos (`rows` fijo en `00_generate_data`) y mismas versiones para los tres DAGs.
2. Un ensayo previo de cada DAG para llenar caché de disco y descargar el driver de dbt.
3. Por cada DAG: correr `./scripts/sample_mem.sh` en otra terminal, disparar **un** DAG, esperar que
   termine y cortar el script con Ctrl+C. No correr dos DAGs a la vez.
4. Repetir 3 veces y quedarse con la mediana de la duración y el máximo de la RAM.
5. Disparar `30_compare_results` al final para confirmar que los tres dan lo mismo.

## Resultados

> Todavía no hay números publicados en este repo. Completá la tabla con tu máquina y tus datos.

Máquina: `____` (CPUs, RAM, Docker con `__` GB asignados) · `rows` = `____`
Versiones: Astro Runtime `3.3`, Cosmos `____`, dbt `____`, DuckDB `____`, pandas `____`, PyArrow `____`

| DAG | Duración del run (s) | Pico RAM scheduler (MB) | Intermedio en disco |
|---|---:|---:|---:|
| `10_pipeline_pandas` | | | |
| `15_pipeline_pandas_pyarrow` | | | |
| `20_pipeline_duckdb_dbt` | | | n/a |

## Cómo leer los resultados

- **Tiempo y RAM los decide el motor, no Airflow.** Airflow no acelera nada: ordena, reintenta
  y muestra. Si DuckDB gana, es por ejecución columnar, paralelismo y pushdown.
- **El overhead de las tasks cuenta.** Cosmos arranca una invocación de dbt por modelo y por test.
  Con pocos datos ese costo fijo puede hacer que DuckDB + dbt **tarde más** que pandas. La ventaja
  aparece al subir `rows`. Probalo con 1M, 5M y 20M y graficá el cruce.
- **El intermedio a disco es un costo oculto de pandas en Airflow.** Las tasks no comparten
  memoria, así que cada paso relee y reescribe. Pasar el DataFrame por XCom sería peor.
- **RAM del scheduler**: como las tasks corren dentro de ese contenedor, el pico incluye
  a Airflow mismo. Sirve para comparar entre DAGs, no como valor absoluto.

## Limitaciones

- Datos sintéticos y una sola query de agregación.
- LocalExecutor en un solo contenedor: no refleja un cluster con workers separados.
- `docker stats` muestrea cada ~1-2 s; picos más cortos que eso pueden no verse.
- dbt OSS con Cosmos es experimental: los resultados de ese DAG pueden cambiar con nuevas versiones.
