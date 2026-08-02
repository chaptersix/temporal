#!/usr/bin/env bash

set -euo pipefail
source "$(dirname -- "$0")/lib.sh"

"$SCRIPT_DIR/start.sh"

if [[ ${1:-} == "--reseed" || ! -s "$RUNTIME_DIR/fixture-tree.json" ]]; then
  "$SCRIPT_DIR/seed.sh"
fi

"$SCRIPT_DIR/render.sh"
echo "server log: $LOG_FILE"
