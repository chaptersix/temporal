#!/usr/bin/env bash

set -euo pipefail
source "$(dirname -- "$0")/lib.sh"

require_command jq
require_server
make -C "$REPO_ROOT" tdbg

json="$RUNTIME_DIR/latest.json"
html="$RUNTIME_DIR/latest.html"
"$REPO_ROOT/tdbg" --address "$ADDRESS" --namespace default schedule describe \
  --schedule-id "$SCHEDULE_ID" --format json --output-filename "$json"
"$REPO_ROOT/tdbg" --address "$ADDRESS" --namespace default schedule describe \
  --schedule-id "$SCHEDULE_ID" --format html --output-filename "$html"

jq empty "$json"
grep -q '<!doctype html>' "$html"

echo "JSON report: $json"
echo "HTML report: $html"
