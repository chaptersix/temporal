#!/usr/bin/env bash

set -euo pipefail
source "$(dirname -- "$0")/lib.sh"

"$SCRIPT_DIR/stop.sh"

rm -f \
  "$DB_FILE" \
  "$DB_FILE-shm" \
  "$DB_FILE-wal" \
  "$RUNTIME_DIR/fixture-dump.txt" \
  "$RUNTIME_DIR/fixture-tree.json" \
  "$RUNTIME_DIR/latest.json" \
  "$RUNTIME_DIR/latest.html"

echo "schedule-describe runtime state reset"
if [[ ${1:-} != "--no-start" ]]; then
  "$SCRIPT_DIR/start.sh"
fi
