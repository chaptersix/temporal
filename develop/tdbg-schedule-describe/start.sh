#!/usr/bin/env bash

set -euo pipefail
source "$(dirname -- "$0")/lib.sh"

require_command temporal
mkdir -p "$RUNTIME_DIR"

ensure_default_namespace() {
  if ! temporal operator namespace describe --address "$ADDRESS" --namespace default >/dev/null 2>&1; then
    temporal operator namespace create --address "$ADDRESS" --namespace default --retention 24h >/dev/null
  fi
  local deadline=$((SECONDS + 30))
  until temporal operator namespace describe --address "$ADDRESS" --namespace default >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "default namespace did not become available; see $LOG_FILE" >&2
      return 1
    fi
    sleep 1
  done
}

if is_harness_server; then
  wait_for_server
  ensure_default_namespace
  echo "schedule-describe server already running at $ADDRESS"
  exit 0
fi

if [[ -f "$PID_FILE" ]]; then
  echo "removing stale PID file $PID_FILE"
  rm -f "$PID_FILE"
fi

make -C "$REPO_ROOT" temporal-server tdbg

sed \
  -e "s#databaseName: \"default\"#databaseName: \"$DB_FILE\"#g" \
  -e 's/port: 7936/port: 17936/' \
  -e 's#listenAddress: "127.0.0.1:8000"#listenAddress: "127.0.0.1:18000"#' \
  -e 's/grpcPort: 7233/grpcPort: 17236/' \
  -e 's/membershipPort: 6933/membershipPort: 16933/' \
  -e 's/httpPort: 7243/httpPort: 17246/' \
  -e 's/grpcPort: 7235/grpcPort: 17238/' \
  -e 's/membershipPort: 6935/membershipPort: 16935/' \
  -e 's/grpcPort: 7234/grpcPort: 17237/' \
  -e 's/membershipPort: 6934/membershipPort: 16934/' \
  -e 's/grpcPort: 7239/grpcPort: 17239/' \
  -e 's/membershipPort: 6939/membershipPort: 16939/' \
  -e 's#rpcAddress: "localhost:7233"#rpcAddress: "localhost:17236"#' \
  -e 's#httpAddress: "localhost:7243"#httpAddress: "localhost:17246"#' \
  -e "s#filepath: \"config/dynamicconfig/development-sql.yaml\"#filepath: \"$SCRIPT_DIR/dynamicconfig.yaml\"#" \
  "$REPO_ROOT/config/development-sqlite-file.yaml" > "$CONFIG_FILE"

nohup "$REPO_ROOT/temporal-server" --config-file "$CONFIG_FILE" start </dev/null > "$LOG_FILE" 2>&1 &
pid=$!
echo "$pid" > "$PID_FILE"

if ! wait_for_server; then
  exit 1
fi

ensure_default_namespace

echo "schedule-describe server running at $ADDRESS (pid $pid)"
echo "server log: $LOG_FILE"
