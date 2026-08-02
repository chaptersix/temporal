#!/usr/bin/env bash

set -euo pipefail
source "$(dirname -- "$0")/lib.sh"

pid=$(server_pid)
if [[ -z "$pid" ]]; then
  echo "schedule-describe server is not running"
  exit 0
fi

if ! is_harness_server; then
  command=$(server_command "$pid")
  if [[ -z "$command" ]]; then
    rm -f "$PID_FILE"
    echo "removed stale PID file"
    exit 0
  fi
  echo "refusing to stop pid $pid because it is not the schedule-describe server: $command" >&2
  exit 1
fi

kill -TERM "$pid"
deadline=$((SECONDS + 30))
while kill -0 "$pid" 2>/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "server did not stop within 30 seconds; pid $pid was not force-killed" >&2
    exit 1
  fi
  sleep 1
done

rm -f "$PID_FILE"
echo "schedule-describe server stopped"
