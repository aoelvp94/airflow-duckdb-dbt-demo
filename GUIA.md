# Guía paso a paso: Airflow + pandas vs DuckDB + dbt

En esta guía vas a levantar **Airflow 3** en tu máquina con el **Astro CLI** y correr el mismo
pipeline (filtro → join → revenue mensual por país) de tres formas: pandas, pandas sobre PyArrow
y DuckDB + dbt. Después comparás duración y memoria, y verificás que los tres den lo mismo.

Todo corre dentro de contenedores Docker: no instalás Python, Airflow ni dbt en tu máquina.

| Paso | Tiempo aprox. |
|---|---|
| 1. Instalar Docker y el Astro CLI | 20-40 min (una sola vez) |
| 2. Chequear la instalación | 2 min |
| 3. Clonar y levantar Airflow | 5-15 min la primera vez |
| 4. Correr los DAGs | 10 min |
| 5. Ver y comparar resultados | 10 min |

> Los pasos 1 a 3 se hacen **una sola vez**: necesitan internet y la primera vez tardan.
> Si ya hiciste la guía del repo `duckdb-vs-pandas`, Docker ya lo tenés: saltá al Astro CLI.

---

## 1. Instalar Docker y el Astro CLI

### macOS

1. Instalá [Docker Desktop para Mac](https://docs.docker.com/desktop/setup/install/mac-install/)
   (*Apple Silicon* para chips M1/M2/M3/M4, *Intel* si no). Abrilo y esperá *Engine running*.
2. Memoria: **Settings → Resources**, **Memory en 4 GB como mínimo, mejor 8 GB**. Apply & restart.
3. Astro CLI, con [Homebrew](https://brew.sh):
   ```bash
   brew install astro
   ```

### Linux (Ubuntu, Debian, Fedora...)

1. Instalá [Docker Engine](https://docs.docker.com/engine/install/) para tu distribución.
2. Para usar `docker` sin `sudo` (después cerrá la sesión y volvé a entrar):
   ```bash
   sudo usermod -aG docker $USER
   ```
3. Astro CLI:
   ```bash
   curl -sSL https://install.astronomer.io | sudo bash -s
   ```
4. Memoria: Docker usa la RAM de tu máquina. Con 8 GB de RAM o más estás bien.

### Windows

En Windows se trabaja dentro de **WSL2** (un Linux que corre dentro de Windows). Los comandos de
esta guía son de bash y **no funcionan en PowerShell ni en cmd**.

1. Instalá WSL2 con Ubuntu. En **PowerShell como administrador**:
   ```powershell
   wsl --install
   ```
   Reiniciá cuando te lo pida y creá tu usuario de Ubuntu.
   Más info: [instalar WSL](https://learn.microsoft.com/windows/wsl/install).
2. Instalá [Docker Desktop para Windows](https://docs.docker.com/desktop/setup/install/windows-install/)
   con la opción **Use WSL 2 based engine**.
3. En Docker Desktop: **Settings → Resources → WSL integration** y activá tu distro (Ubuntu).
4. Memoria: con WSL2 la define WSL, no Docker Desktop. Creá `C:\Users\<tu-usuario>\.wslconfig` con:
   ```ini
   [wsl2]
   memory=8GB
   ```
   y en PowerShell corré `wsl --shutdown` para que lo tome.
5. **Desde acá, todo en la terminal de Ubuntu** (buscá "Ubuntu" en el menú Inicio). Instalá el
   Astro CLI **dentro de Ubuntu**, con el mismo comando que en Linux:
   ```bash
   curl -sSL https://install.astronomer.io | sudo bash -s
   ```
6. Cloná el repo dentro de Ubuntu (`~/...`), **no** en `/mnt/c/...`, que es mucho más lento.

Más info del Astro CLI: [instalación](https://www.astronomer.io/docs/astro/cli/install-cli).

---

## 2. Chequear la instalación

```bash
docker run --rm hello-world
astro version
```

El primero tiene que imprimir *Hello from Docker!* y el segundo la versión del Astro CLI
(1.34 o más nueva; esta guía se probó con 1.46). Memoria que ve Docker:

```bash
docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes de RAM'
```

Airflow solo ya usa algo más de 1 GB, y las tasks de pandas se suman encima: con 4 GB (unos
4000000000 bytes) alcanza para 1M filas; con 8 GB vas cómodo.

---

## 3. Clonar y levantar Airflow

```bash
git clone https://github.com/aoelvp94/airflow-duckdb-dbt-demo.git
cd airflow-duckdb-dbt-demo
astro dev start
```

La primera vez construye la imagen (Airflow, dbt, DuckDB, pandas): varios minutos. Al terminar
imprime algo así:

```
➤ Airflow UI: http://airflow-duckdb-dbt-demo.localhost:6563
```

Abrí **esa URL** en el navegador (la tuya puede ser distinta). En local no hay login.

Vas a ver cinco DAGs:

| DAG | Qué hace |
|---|---|
| `00_generate_data` | Genera los datos: `include/data/orders.parquet` y `customers.parquet` |
| `10_pipeline_pandas` | pandas "ingenuo": lee todo, join antes de filtrar |
| `15_pipeline_pandas_pyarrow` | pandas optimizado: lee solo lo necesario, filtra en la lectura |
| `20_pipeline_duckdb_dbt` | DuckDB + dbt (vía Cosmos): una task por modelo y por test |
| `30_compare_results` | Verifica que los tres pipelines den el mismo resultado |

---

## 4. Correr los DAGs

Corré **uno por vez** y esperá a que termine antes de disparar el siguiente: si dos corren a la
vez, los tiempos y la memoria se mezclan. Podés hacerlo desde la UI o desde la terminal.

### Opción A: desde la UI

1. Los DAGs arrancan **pausados**: activá el switch de cada uno.
2. En `00_generate_data`, **Trigger** y poné `rows` = `1000000` (1 millón alcanza; tarda segundos).
3. Cuando termine en verde, disparalos en orden: `10_pipeline_pandas`,
   `15_pipeline_pandas_pyarrow` y `20_pipeline_duckdb_dbt`.
4. Por último, `30_compare_results`. Si termina en verde, los tres pipelines dieron lo mismo.

### Opción B: desde la terminal

Despausar los cinco DAGs:

```bash
for d in 00_generate_data 10_pipeline_pandas 15_pipeline_pandas_pyarrow 20_pipeline_duckdb_dbt 30_compare_results; do astro dev run dags unpause $d; done
```

Generar 1 millón de filas y ver el estado:

```bash
astro dev run dags trigger 00_generate_data -c '{"rows": 1000000}'
astro dev run dags list-runs 00_generate_data
```

Repetí `list-runs` hasta que la columna `state` diga `success`. Después, de a uno y esperando
`success` cada vez:

```bash
astro dev run dags trigger 10_pipeline_pandas
astro dev run dags list-runs 10_pipeline_pandas
```

Lo mismo con `15_pipeline_pandas_pyarrow`, `20_pipeline_duckdb_dbt` y, al final,
`30_compare_results`.

> La primera vez que corre `20_pipeline_duckdb_dbt`, dbt baja el driver de DuckDB: la primera
> task tarda casi un minuto. Las siguientes, pocos segundos.

---

## 5. Ver y comparar resultados

### Duración

- **UI**: entrá a cada DAG run y abrí la vista **Gantt**: una barra por task.
- **Terminal**: `list-runs` muestra inicio y fin de cada corrida. Para el detalle por task, usá el
  `run_id` que te muestra `list-runs`:
  ```bash
  astro dev run tasks states-for-dag-run 20_pipeline_duckdb_dbt <run_id>
  ```

### Lo que pandas escribe a disco

Las tasks de Airflow no comparten memoria, así que los pipelines de pandas pasan los datos de una
task a la otra por disco:

```bash
ls -lh include/tmp
```

Con 1M filas: unos 20 MB para pandas y 8 MB para PyArrow. DuckDB + dbt no escribe intermedios:
el resultado queda en `include/warehouse/ecommerce.duckdb`.

### Memoria

En **otra terminal**, desde la carpeta del repo, arrancá el muestreo **antes** de disparar un DAG
y cortalo con **Ctrl+C** cuando termine:

```bash
FILTER='airflow-duckdb-dbt-demo.*scheduler' ./scripts/sample_mem.sh
```

Al cortar, imprime el pico de RAM del contenedor `scheduler` (las tasks corren ahí). Ese número
**incluye a Airflow**, que ya usa cerca de 1 GB: sirve para comparar entre DAGs, no como valor
absoluto.

### ¿Y quién ganó?

Con pocos datos, DuckDB + dbt puede tardar **más** que pandas: son 6 tasks (3 modelos y 3 tests)
y cada una arranca dbt. Al subir `rows`, pandas crece y DuckDB + dbt casi no cambia. Compará tus
números con los de [BENCHMARK.md](BENCHMARK.md#resultados).

---

## 6. Apagar y limpiar

```bash
astro dev stop
```

Apaga los contenedores y conserva todo (la próxima vez, `astro dev start` arranca rápido).
Para borrar los contenedores y la base de Airflow (historial de corridas incluido):

```bash
astro dev kill
```

Para borrar los datos generados (en Linux, si da `permission denied`, anteponé `sudo`):

```bash
rm -f include/data/*.parquet include/tmp/*.parquet include/output/*.parquet include/warehouse/*.duckdb
```

---

## Si algo falla

| Síntoma | Qué hacer |
|---|---|
| `astro dev start` dice `port is already allocated` | `astro dev stop` y de nuevo `astro dev start`: la segunda vez elige otros puertos. |
| `Cannot connect to the Docker daemon` | Docker no está corriendo: abrí Docker Desktop (Mac/Windows) o `sudo systemctl start docker` (Linux). |
| `astro: command not found` | Falta instalar el Astro CLI (sección 1). En Windows, instalalo dentro de Ubuntu. |
| `30_compare_results` falla por archivo inexistente | Falta correr alguno de los pipelines (`10`, `15` o `20`). |
| La primera task de dbt tarda mucho | Normal la primera vez: baja el driver de DuckDB. Necesita internet. |
| Una task de pandas falla con `exit -9` o `SIGKILL` | Se quedó sin memoria: subí la memoria de Docker (sección 1) o bajá `rows`. |
| En Windows, todo anda muy lento | Clonaste el repo en `/mnt/c/...`: clonalo dentro de Ubuntu (`cd ~`). |

Más detalle: [README.md](README.md) (gotchas de dbt v2 y Cosmos) y [BENCHMARK.md](BENCHMARK.md).
La comparación pura de motores, sin Airflow, está en el repo
[duckdb-vs-pandas](https://github.com/aoelvp94/duckdb-vs-pandas).
