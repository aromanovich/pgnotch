#!/bin/bash
# Remove the PostgreSQL container started by pg-up.sh. Its data directory is a
# tmpfs, so this discards every log written to that cluster.
set -eu -o pipefail

NAME="${NAME:-pgnotch-pg}"

if ! docker inspect "$NAME" >/dev/null 2>&1; then
  echo "$NAME is not running"
  exit 0
fi

# A failure here is real (daemon down, permissions) and should surface.
docker rm -f "$NAME" >/dev/null
echo "removed $NAME"
