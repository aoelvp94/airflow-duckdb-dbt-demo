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

Medido el 13-nov-2026 con `astro dev start` (LocalExecutor), siguiendo el protocolo de arriba.

**Entorno**

- Host: Apple M4, 24 GB de RAM, macOS 26.5.2. Docker Desktop (engine 29.9.2) con **10 CPUs y
  7.7 GiB** para la VM.
- Astro CLI 1.46.0, Astro Runtime 3.3 (Airflow 3.3.2+astro.3, Python 3.14.10), Cosmos 1.16.0,
  dbt-oss 2.0.11, DuckDB 2.0.0, pandas 3.1.1, PyArrow 26.0.1.
- **Ojo con el ruido**: en la misma VM corría otro proyecto de Airflow con DAGs activos (entre
  15 % y 500 % de CPU antes de cada corrida). Por eso las corridas varían mucho y se reporta la
  mediana. Tomalo como orden de magnitud, no como número exacto.

**Duración del DAG run** (mediana de 3 corridas, en segundos; entre paréntesis, mín-máx).
"Pico RAM" es el máximo del contenedor scheduler en MiB (`./scripts/sample_mem.sh`) e **incluye
Airflow mismo**: el scheduler ocioso ya usa ~1.1 GiB.

| `rows` | `10_pipeline_pandas` | `15_pipeline_pandas_pyarrow` | `20_pipeline_duckdb_dbt` |
|---|---:|---:|---:|
| 1,000 | 8.4 s (3.3-11.7) · 1,272 MiB | 3.3 s (3.1-6.5) · 1,312 MiB | 11.8 s (9.7-17.5) · 1,333 MiB |
| 100,000 | 3.9 s (3.2-4.5) · 1,465 MiB | 3.7 s (3.3-4.8) · 1,575 MiB | 9.2 s (7.2-10.2) · 1,455 MiB |
| 1,000,000 (1 corrida) | 4.8 s · 1,292 MiB | 3.5 s · 1,234 MiB | no medido |
| 5,000,000 | 18.9 s (14.9-24.0) · 2,292 MiB | 8.4 s (6.7-15.1) · 1,255 MiB | 11.5 s (10.0-14.4) · 1,269 MiB |

**Intermedio en disco** (`include/tmp`): a 1M, 20.8 MB (pandas) vs 8.4 MB (PyArrow); a 5M,
99 MB vs 37 MB. DuckDB + dbt no escribe intermedios: `monthly_revenue` queda en
`include/warehouse/ecommerce.duckdb`.

**Tasks dbt a 5M** (mediana, s): `stg_customers.run` 2.06, `stg_orders.run` 1.10,
`stg_customers.test` 0.72, `stg_orders.test` 0.87, `monthly_revenue.run` 1.53,
`monthly_revenue.test` 0.82. Corren de a una (pool `duckdb_single_writer`, verificado).

`30_compare_results` terminó en verde a 1k, 100k y 5M filas, comparando **las dos** salidas de
pandas contra DuckDB/dbt (240 filas cada una a 5M).

**Qué muestran estos números**

- Con pocos datos **gana el que tiene menos overhead**: a 1k y 100k filas, DuckDB + dbt tarda
  más que pandas porque son 6 tasks (3 modelos + 3 tests), cada una con su arranque de dbt.
  El motor casi no trabaja.
- A 5M el cruce ya se ve: pandas ingenuo pasa a ser el más lento y el que más RAM usa
  (~1.2 GiB por encima del scheduler ocioso). DuckDB + dbt se queda casi igual que con 1k filas.
- El pico de RAM a pocas filas es Airflow, no el pipeline: por eso los tres DAGs dan parecido.

## Cómo leer los resultados

- **Tiempo y RAM los decide el motor, no Airflow.** Airflow no acelera nada: ordena, reintenta
  y muestra. Si DuckDB gana, es por ejecución columnar, paralelismo y pushdown.
- **El overhead de las tasks cuenta.** Cosmos arranca una invocación de dbt por modelo y por test.
  Con pocos datos ese costo fijo puede hacer que DuckDB + dbt **tarde más** que pandas. La ventaja
  aparece al subir `rows`. Probalo con 1k, 100k, 1M y 5M y mirá dónde se cruzan.
- **El intermedio a disco es un costo oculto de pandas en Airflow.** Las tasks no comparten
  memoria, así que cada paso relee y reescribe. Pasar el DataFrame por XCom sería peor.
- **RAM del scheduler**: como las tasks corren dentro de ese contenedor, el pico incluye
  a Airflow mismo. Sirve para comparar entre DAGs, no como valor absoluto.

## Limitaciones

- Datos sintéticos y una sola query de agregación.
- LocalExecutor en un solo contenedor: no refleja un cluster con workers separados.
- `docker stats` muestrea cada ~1-2 s; picos más cortos que eso pueden no verse.
- dbt OSS con Cosmos es experimental: los resultados de ese DAG pueden cambiar con nuevas versiones.
