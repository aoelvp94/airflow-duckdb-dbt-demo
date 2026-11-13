#!/usr/bin/env bash
# Muestrea la RAM de los contenedores de Airflow mientras corrés los DAGs de la demo.
# Con LocalExecutor las tasks corren dentro del contenedor "scheduler", así que ese es el
# que importa. Cortá con Ctrl+C para ver el pico.
#
# Uso:   ./scripts/sample_mem.sh [segundos_entre_muestras]
# Filtro de contenedores (regex): FILTER='scheduler|api-server' ./scripts/sample_mem.sh
set -euo pipefail

INTERVAL="${1:-1}"
FILTER="${FILTER:-scheduler}"
mkdir -p .bench
OUT=".bench/mem_$(date +%Y%m%d_%H%M%S).csv"
echo "hora,contenedor,mem_mb" > "$OUT"

report() {
  echo
  echo "Pico de RAM por contenedor (MB):"
  awk -F, 'NR>1 { if ($3+0 > m[$2]) m[$2]=$3+0 } END { for (c in m) printf "  %-55s %.0f\n", c, m[c] }' "$OUT"
  echo "CSV completo: $OUT"
}
trap 'report; exit 0' INT TERM

echo "Muestreando cada ${INTERVAL}s (filtro: ${FILTER}). Corré los DAGs y cortá con Ctrl+C."
while true; do
  docker stats --no-stream --format '{{.Name}},{{.MemUsage}}' \
    | grep -E "$FILTER" \
    | awk -F, -v ts="$(date +%H:%M:%S)" '{
        split($2, a, " / "); v = a[1]; n = v + 0
        if (v ~ /GiB/) n = n * 1024
        else if (v ~ /KiB|kB/) n = n / 1024
        else if (v ~ /MiB/) n = n
        else n = n / 1048576
        printf "%s,%s,%.1f\n", ts, $1, n }' >> "$OUT" || true
  sleep "$INTERVAL"
done
