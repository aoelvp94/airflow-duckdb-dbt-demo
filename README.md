# Airflow + Astro CLI: pandas vs DuckDB 2.0 + dbt v2

Mismo pipeline de negocio (filtro → join → revenue mensual por país) implementado de dos
formas y orquestado con **Airflow 3.3** corriendo local con **Astro CLI**.

> **¿Vas a dar la clase?** Seguí el guion paso a paso en **[DEMO.md](DEMO.md)** (25-30 min).
> Cómo medir y resultados: [BENCHMARK.md](BENCHMARK.md).

## Stack (versiones al 12-nov-2026)

| Pieza | Versión | Nota |
|---|---|---|
| Astro Runtime | `3.3` (Airflow 3.3) | Python 3.12–3.14 (default 3.14) |
| dbt | **v2** vía `dbt-oss` (Apache 2.0) | Motor Rust; el adaptador de DuckDB viene incluido (beta) |
| DuckDB (Python) | `>=2.0.0` (LTS) | Requiere Python >= 3.11 |
| Cosmos | `>=1.16.0` | Soporte de dbt OSS marcado como *experimental* |
| pandas | `>=3.0` (3.0.x) | Dtype `str` por defecto (respaldado por PyArrow) y Copy-on-Write |

> `pip install dbt` instala el build propietario de dbt v2; acá usamos `dbt-oss` para
> tener solo código Apache 2.0. Para el demo funcionan igual.

| DAG | Qué hace |
|---|---|
| `00_generate_data` | Genera `orders.parquet` y `customers.parquet` (parámetro `rows`) |
| `10_pipeline_pandas` | pandas **ingenuo**: `extract_join → transform_aggregate → quality_check` |
| `15_pipeline_pandas_pyarrow` | pandas **optimizado sobre PyArrow**: pushdown de columnas/filtros, join en Arrow |
| `20_pipeline_duckdb_dbt` | dbt v2 (DuckDB) orquestado con **Cosmos**: 1 task por modelo y por test |
| `30_compare_results` | Verifica que ambos pipelines dan resultados idénticos |

## Requisitos
Docker (o Podman) y [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) >= 1.34.

## Correrlo
```bash
astro dev start            # build + levanta Airflow; al final imprime la URL de la UI
```
Abrí la URL que imprime `astro dev start` (con Astro CLI 1.46 es
`http://airflow-duckdb-dbt-demo.localhost:6563`). En local no hay login: todos son admin.

1. Disparar `00_generate_data` (alcanza con `rows` = 1000, 100000 o 1000000).
2. Disparar `10_pipeline_pandas`, `15_pipeline_pandas_pyarrow` y `20_pipeline_duckdb_dbt`.
3. Disparar `30_compare_results` (compara cada pandas contra DuckDB/dbt).
4. Comparar en la UI: **Gantt** (duración por task) y `docker stats` (RAM en vivo).

Tests de integridad de DAGs: `astro dev pytest`

## Resultados (resumen)

Duración del DAG run, mediana de 3 corridas (Apple M4, Docker con 10 CPUs y 7.7 GiB, otro
proyecto de Airflow corriendo en paralelo: hay ruido). Detalle y método en
[BENCHMARK.md](BENCHMARK.md#resultados).

| `rows` | pandas (`10`) | pandas + PyArrow (`15`) | DuckDB + dbt (`20`) |
|---|---:|---:|---:|
| 1,000 | 8.4 s | 3.3 s | 11.8 s |
| 100,000 | 3.9 s | 3.7 s | 9.2 s |
| 5,000,000 | 18.9 s | 8.4 s | 11.5 s |

Con pocos datos manda el overhead de Airflow y de dbt (6 tasks), no el motor. Al subir `rows`,
pandas ingenuo crece y DuckDB + dbt casi no cambia.

## Demo de OOM (opcional)
```bash
cp docker-compose.override.yml.example docker-compose.override.yml
astro dev restart
```
Limita el contenedor `scheduler` (donde corren las tasks) a 2 GB. **Ojo: con datos chicos no
reproduce.** Probado con `rows` = 5M: `10_pipeline_pandas` terminó bien (más lento, por la
presión de memoria). Para mostrar el OOM en clase es más rápido y confiable el repo
`duckdb-vs-pandas` con `docker run -m 2g` y 10M filas: ahí `pandas_naive` muere y DuckDB sigue.
Para volver atrás: `rm docker-compose.override.yml && astro dev restart`.

## La escalera de optimización (el hilo de la clase)

| Paso | DAG | Qué se gana | Qué sigue siendo manual |
|---|---|---|---|
| 1. pandas ingenuo | `10` | Nada: lee todo, join antes de filtrar | Todo |
| 2. pandas + PyArrow | `15` | Solo columnas necesarias, filtro en la lectura, join multihilo en Arrow, intermedio chico | Elegir columnas, escribir el predicado, ordenar los pasos |
| 3. DuckDB + dbt | `20` | Lo anterior **sin escribirlo**, más tests, linaje y un task por modelo | Escribir la query SQL |

Pregunta para la clase: ¿cuánto del speedup del paso 2 al 3 viene del motor y cuánto del
orquestador? (Pista: el orquestador no acelera nada; aporta tests, linaje y reintentos.)

## Qué mostrar a los alumnos

| Tema | pandas | DuckDB + dbt |
|---|---|---|
| Datos entre tasks | Dataset completo a disco en cada paso (un DataFrame por XCom sería peor) | Cada modelo lee/escribe en el warehouse |
| Memoria | Todo el dataset en RAM, varias veces (`merge`, copias) | Motor columnar con pushdown y spill a disco |
| Lógica | Imperativa: el orden de tus líneas es el plan | Declarativa: el optimizador decide |
| Data quality | `assert` a mano | `tests:` en `schema.yml`, un task por test |
| Linaje | Implícito en el código | `ref()` genera el grafo y Cosmos lo vuelve tasks |
| Reintentos | Re-ejecutar el paso (costoso) | Re-ejecutar solo el modelo que falló |

## Gotchas didácticos (vale la pena contarlos)
- **DuckDB admite un solo escritor** sobre el archivo. Por eso las tasks dbt usan el pool
  `duckdb_single_writer` (1 slot, `airflow_settings.yaml`) y el profile tiene `threads: 1`.
- **dbt v2 no es un paquete Python** sino un binario autocontenido: no hay conflicto de
  dependencias con Airflow, así que desapareció el virtualenv de dbt del `Dockerfile`.
  Cosmos detecta dbt v2 mirando el entorno de Airflow, por eso `dbt-oss` va en `requirements.txt`
  (y no puede convivir con `dbt-core` 1.x en ese entorno).
- **dbt v2 es más estricto**: no acepta funcionalidad deprecada. Por eso el `schema.yml`
  usa `data_tests:` y `arguments:`. Si migrás un proyecto viejo, corré `uvx dbt-autofix`.
- **Adaptador incluido, driver descargado en el primer uso**: dbt v2 baja y cachea el driver
  de DuckDB la primera vez, así que el primer `dbt` necesita internet. El driver embebido no
  carga extensiones (`httpfs`, etc.); si las necesitás, hay que instalar el driver con `dbc`.
- **Static analysis apagado** (`+static_analysis: off`): es la gran novedad de v2 (valida el
  SQL antes de ejecutarlo) pero `read_parquet()` puede escapar a lo que entiende el analizador.
- **Versiones de archivo**: dbt v2 trae su propio DuckDB *pinneado*, que puede ser más viejo
  que el 2.0 de Python. DuckDB 2.0 lee archivos viejos, pero un `.duckdb` escrito por 2.0 **no**
  se puede abrir con versiones anteriores: no escribas desde Python en `ecommerce.duckdb`.
- **`astro dev pytest` y el caché de Cosmos**: al parsear el DAG, Cosmos corre `dbt ls` y guarda
  el resultado en una Airflow Variable. En los tests no hay base de metadatos y eso falla con
  `no such table: variable`. `tests/conftest.py` apaga ese caché solo para los tests
  (`AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=False`).
- **Puertos ocupados** (otro proyecto de Astro corriendo): `astro config set postgres.port 5434`
  y `astro config set api-server.port 8081` cambian los puertos de este proyecto. Eso edita
  `.astro/config.yaml`; no lo commitees si es solo para tu máquina.
- **`scripts/sample_mem.sh` con varios proyectos de Astro**: el filtro por defecto (`scheduler`)
  también agarra los schedulers de otros proyectos. Usá
  `FILTER='airflow-duckdb-dbt-demo.*scheduler' ./scripts/sample_mem.sh`.
- Si el parseo del DAG de dbt falla por un lock mientras corre otro task, pasá Cosmos a
  `LoadMode.DBT_MANIFEST` con un manifest generado por `dbt parse`.
- Setup para clase y datos medianos. Para producción: warehouse real (o MotherDuck/DuckLake)
  y revisar el estado de Cosmos + dbt OSS, que todavía es experimental.

## Nota sobre pandas 3
pandas 3.0 cambió dos cosas que afectan esta comparación: las columnas de texto usan un dtype
`str` dedicado (respaldado por PyArrow, mucho más liviano que `object`) y rige Copy-on-Write.
Esto **achica la brecha de memoria en strings** frente a pandas 2, pero no cambia lo
estructural: pandas sigue cargando todas las columnas y filas en RAM y ejecutando en un hilo.
Si tenés números de pandas 2 en la cabeza, volvé a medir.

## Ejercicios
1. Agregá un modelo `top_customers` en `marts/` y mirá cómo aparece en el DAG.
2. Rompé un test (por ejemplo, un `status` inválido en los datos) y observá qué falla y qué no.
3. Mirá `15_pipeline_pandas_pyarrow` y medí cuánto del trabajo de DuckDB ya lo hace PyArrow a mano.
   Reemplazá el `groupby` de pandas por `pyarrow.Table.group_by(...).aggregate(...)` (cero pandas):
   ¿cambia el tiempo o la RAM?
4. Pasá `monthly_revenue` a `incremental` y comparalo con el pandas.
5. Probá `ExecutionMode.WATCHER` de Cosmos: corre un solo `dbt build` y Airflow observa cada
   nodo. Con DuckDB (un solo escritor) encaja muy bien y elimina la necesidad del pool.
   (Con dbt OSS el productor debe correr como subproceso.)
6. Prendé `static_analysis` en `marts` y mirá qué errores detecta dbt v2 antes de ejecutar.

## Codespaces
El devcontainer trae Docker-in-Docker y el Astro CLI (recomendado 16 GB de RAM).
Dentro: `astro dev start` y abrí el puerto 8080.

## Licencia
MIT (ver `LICENSE`).

## Documentación

| Archivo | Para qué |
|---|---|
| [`DEMO.md`](DEMO.md) | Guion paso a paso de la demo con Astro CLI (25-30 min) |
| [`BENCHMARK.md`](BENCHMARK.md) | Cómo medir tiempo y RAM dentro de Airflow, qué significan y plantilla de resultados |
| `scripts/sample_mem.sh` | Muestrea la RAM del contenedor scheduler mientras corrés los DAGs |

La comparación pura de motores (sin orquestador) está en el repo `duckdb-vs-pandas`.
