#!/usr/bin/env bash
# Publica este repo en tu cuenta de GitHub. Requiere: git y gh (gh auth login).
set -euo pipefail
cd "$(dirname "$0")/.."

NAME="airflow-duckdb-dbt-demo"
DESC="Astro CLI + Airflow: pipelines con pandas, pandas sobre PyArrow y DuckDB + dbt v2 (Cosmos)"

[ -d .git ] || git init -b main
git add .
git commit -m "feat: Airflow 3.3 + Astro demo, pandas vs DuckDB 2.0 + dbt v2" || true
gh repo create "$NAME" --public --source=. --push --description "$DESC"
gh repo edit --add-topic airflow --add-topic astro --add-topic dbt --add-topic duckdb --add-topic cosmos
echo "Listo: $(gh repo view --json url -q .url)"
