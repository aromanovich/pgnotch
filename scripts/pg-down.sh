#!/bin/bash
# Remove the PostgreSQL container started by pg-up.sh. Its data directory is a
# tmpfs inside the container, so this discards every log written to that
# cluster — pg-up.sh has to be run again from scratch afterwards.
set -eu -o pipefail

NAME="${NAME:-pgnotch-pg}"

if ! docker inspect "$NAME" >/dev/null 2>&1; then
  echo "$NAME is not running"
  exit 0
fi

# Any failure here is a real one (daemon down, permissions) and should surface
# rather than be reported as "not running".
docker rm -f "$NAME" >/dev/null
echo "removed $NAME"
