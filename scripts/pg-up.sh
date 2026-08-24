#!/bin/bash
# Bring up a single-node PostgreSQL for this package's suite.
#
# The major version is not free to choose: the entry tables declare their
# payload column `bytea STORAGE PLAIN` to keep a TOAST relation from ever being
# created, and per-column STORAGE inside CREATE TABLE is a syntax error before
# PostgreSQL 16. The check below fails loudly rather than at CreateLogs time.
set -eu -o pipefail

NAME="${NAME:-pgnotch-pg}"
IMAGE="${IMAGE:-postgres:18-alpine}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-pgnotch}"
PG_PASSWORD="${PG_PASSWORD:-pgnotch}"
PG_DATABASE="${PG_DATABASE:-pgnotch}"

# Poll until the probe prints something containing `marker`, giving up after two
# minutes. The output is captured rather than piped into `grep -q`, which exits
# on the first match and SIGPIPEs the writer — a failure under `set -o pipefail`.
wait_for() {
  local what="$1" marker="$2"; shift 2
  local output

  for _ in $(seq 1 60); do
    output=$("$@" 2>&1 || true)
    case "$output" in *"$marker"*) return 0 ;; esac
    if [ "$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null)" != "true" ]; then
      echo "the container died while waiting for $what:"
      docker logs "$NAME" 2>&1 | tail -20
      exit 1
    fi
    sleep 2
  done

  echo "timed out waiting for $what; last output was:"
  printf '%s\n' "${output:-<none>}"
  docker logs "$NAME" 2>&1 | tail -20
  exit 1
}

echo "== starting $NAME ($IMAGE) =="
docker rm -f "$NAME" >/dev/null 2>&1 || true
# Nothing a test writes is meant to outlive the run, so the data directory is a
# tmpfs.
docker run -d --name "$NAME" \
  -p "$PG_PORT":5432 \
  -e POSTGRES_USER="$PG_USER" \
  -e POSTGRES_PASSWORD="$PG_PASSWORD" \
  -e POSTGRES_DB="$PG_DATABASE" \
  -e PGDATA=/pgdata \
  --tmpfs /pgdata:rw,size=2g \
  "$IMAGE" >/dev/null

echo "== waiting for the server to accept queries =="
# Ask the server, not the port: a published port that accepts a connection
# proves docker-proxy is up and nothing about PostgreSQL.
wait_for "the server" "1 row" \
  docker exec "$NAME" psql -U "$PG_USER" -d "$PG_DATABASE" -c "SELECT 1"

echo "== checking the major version admits per-column STORAGE =="
if ! docker exec "$NAME" psql -U "$PG_USER" -d "$PG_DATABASE" -q \
  -c "CREATE TABLE pgnotch_storage_probe (p bytea STORAGE PLAIN)" >/dev/null 2>&1; then
  echo "this server rejects 'bytea STORAGE PLAIN' in CREATE TABLE, so the entry"
  echo "tables cannot keep TOAST from being created. PostgreSQL 16 or newer is required."
  docker exec "$NAME" psql -U "$PG_USER" -d "$PG_DATABASE" -tAc "SELECT version()"
  exit 1
fi
# The probe also checks the point of the clause: no toast relation was created.
toast=$(docker exec "$NAME" psql -U "$PG_USER" -d "$PG_DATABASE" -tAc \
  "SELECT reltoastrelid FROM pg_class WHERE relname = 'pgnotch_storage_probe'")
docker exec "$NAME" psql -U "$PG_USER" -d "$PG_DATABASE" -q -c "DROP TABLE pgnotch_storage_probe"
if [ "$toast" != "0" ]; then
  echo "'STORAGE PLAIN' was accepted but a toast relation was created anyway ($toast)"
  exit 1
fi

echo
echo "PostgreSQL is up. Run the suite with:"
echo "  make test"
echo "or point it there yourself:"
echo "  PGNOTCH_DSN='postgres://$PG_USER:$PG_PASSWORD@localhost:$PG_PORT/$PG_DATABASE' go test ./..."
