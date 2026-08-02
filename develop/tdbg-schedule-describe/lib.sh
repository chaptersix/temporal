#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
RUNTIME_DIR="$REPO_ROOT/.tmp/tdbg-schedule-describe"
PID_FILE="$RUNTIME_DIR/server.pid"
LOG_FILE="$RUNTIME_DIR/server.log"
CONFIG_FILE="$RUNTIME_DIR/server.yaml"
DB_FILE="$RUNTIME_DIR/temporal.db"
ADDRESS="localhost:17236"
SCHEDULE_ID="tdbg-chasm-internals"

server_pid() {
  if [[ -f "$PID_FILE" ]]; then
    tr -d '[:space:]' < "$PID_FILE"
  fi
}

server_command() {
  local pid=$1
  ps -p "$pid" -o command= 2>/dev/null || true
}

is_harness_server() {
  local pid command
  pid=$(server_pid)
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  command=$(server_command "$pid")
  [[ "$command" == *"$REPO_ROOT/temporal-server"* && "$command" == *"$CONFIG_FILE"* ]]
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

wait_for_server() {
  local deadline=$((SECONDS + 60))
  until temporal operator cluster health --address "$ADDRESS" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "server did not become healthy; see $LOG_FILE" >&2
      return 1
    fi
    if ! is_harness_server; then
      echo "server exited before becoming healthy; see $LOG_FILE" >&2
      return 1
    fi
    sleep 1
  done
}

require_server() {
  if ! is_harness_server || ! temporal operator cluster health --address "$ADDRESS" >/dev/null 2>&1; then
    echo "the schedule-describe development server is not running; run $SCRIPT_DIR/start.sh" >&2
    exit 1
  fi
}
