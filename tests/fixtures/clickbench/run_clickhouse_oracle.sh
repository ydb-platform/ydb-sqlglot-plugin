#!/usr/bin/env bash
# One-off: load the small ClickBench-like dataset into ClickHouse and print query results.
# Uses official queries.sql (same as ClickBench clickhouse/queries.sql).
#
# Usage (from repo root):
#   docker compose -f tests/fixtures/clickbench/docker-compose.yml up -d
#   ./tests/fixtures/clickbench/run_clickhouse_oracle.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="$SCRIPT_DIR/docker-compose.yml"

if [[ ! -f "$COMPOSE" ]]; then
  echo "docker-compose.yml not found at $COMPOSE" >&2
  exit 1
fi

docker compose -f "$COMPOSE" up -d

echo "Waiting for ClickHouse..."
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE" exec -T clickhouse clickhouse-client -q "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

docker compose -f "$COMPOSE" exec -T clickhouse clickhouse-client --multiquery \
  <"$SCRIPT_DIR/create_clickhouse.sql"
docker compose -f "$COMPOSE" exec -T clickhouse clickhouse-client --multiquery \
  <"$SCRIPT_DIR/insert_clickhouse.sql"

n=1
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ "$line" =~ ^[[:space:]]*$ ]] && continue
  printf '\n======== Q%02d ========\n' "$n"
  # Do not inherit stdin from queries.sql (exec would drain the file after Q01).
  docker compose -f "$COMPOSE" exec -T clickhouse clickhouse-client \
    --format PrettyCompact --query "$line" </dev/null
  n=$((n + 1))
done <"$SCRIPT_DIR/queries.sql"

echo ""
echo "Done ($((n - 1)) queries)."
