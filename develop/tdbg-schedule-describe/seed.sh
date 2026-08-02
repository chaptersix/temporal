#!/usr/bin/env bash

set -euo pipefail
source "$(dirname -- "$0")/lib.sh"

require_command jq
require_command temporal
require_server

temporal schedule delete --address "$ADDRESS" --namespace default --schedule-id "$SCHEDULE_ID" >/dev/null 2>&1 || true
fixture_run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
if backfill_start=$(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null); then
  backfill_end=$(date -u -v-1M +%Y-%m-%dT%H:%M:%SZ)
else
  backfill_start=$(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ)
  backfill_end=$(date -u -d '1 minute ago' +%Y-%m-%dT%H:%M:%SZ)
fi

temporal schedule create \
  --address "$ADDRESS" \
  --namespace default \
  --schedule-id "$SCHEDULE_ID" \
  --interval 1s \
  --paused \
  --notes 'CHASM internals UI fixture' \
  --overlap-policy BufferAll \
  --workflow-id "tdbg-scheduled-workflow-$fixture_run_id" \
  --task-queue tdbg-unserved-queue \
  --type TdbgScheduleFixtureWorkflow \
  --input '{"fixture":"schedule describe","count":42,"nested":{"enabled":true}}' \
  --schedule-memo 'purpose="tdbg schedule internals layout"' \
  --execution-timeout 24h \
  --run-timeout 1h \
  --task-timeout 30s \
  --output none

temporal schedule backfill \
  --address "$ADDRESS" \
  --namespace default \
  --schedule-id "$SCHEDULE_ID" \
  --start-time "$backfill_start" \
  --end-time "$backfill_end" \
  --overlap-policy BufferAll >/dev/null

dump="$RUNTIME_DIR/fixture-dump.txt"
tree="$RUNTIME_DIR/fixture-tree.json"
deadline=$((SECONDS + 45))
while true; do
  "$REPO_ROOT/tdbg" --address "$ADDRESS" --namespace default execution describe \
    --business-id "$SCHEDULE_ID" --archetype scheduler.scheduler > "$dump"
  awk '/^CHASM Tree Nodes:/{capture=1; next} /^Current branch token:/{capture=0} capture' "$dump" > "$tree"

  if jq -e '
    ([to_entries[] | select(.value.componentFQN == "scheduler.backfiller")] | length) > 0 and
    ([to_entries[] | select(.value.componentFQN == "scheduler.backfiller") | (.value.decodedData.attempt | tonumber)] | any(. > 0)) and
    ([.Invoker.decodedData.bufferedStarts[].attempt] | any(. == "-1")) and
    ([.Invoker.decodedData.bufferedStarts[].attempt] | any(. == "1")) and
    ([to_entries[] | select(.value.componentFQN == "scheduler.eventlog") | .key] | any(. == "EventLog")) and
    ([to_entries[] | select(.value.componentFQN == "scheduler.eventlog") | .key] | any(. == "Generator$EventLog")) and
    ([to_entries[] | select(.value.componentFQN == "scheduler.eventlog") | .key] | any(. == "Invoker$EventLog")) and
    ([to_entries[] | select(.value.componentFQN == "scheduler.eventlog") | .key] | any(startswith("Backfillers$")))
  ' "$tree" >/dev/null; then
    break
  fi

  if (( SECONDS >= deadline )); then
    echo "fixture did not reach the expected state; inspect $dump and $LOG_FILE" >&2
    exit 1
  fi
  sleep 1
done

buffer_count=$(jq '.Invoker.decodedData.bufferedStarts | length' "$tree")
backfiller_count=$(jq '[to_entries[] | select(.value.componentFQN == "scheduler.backfiller")] | length' "$tree")
event_count=$(jq '[to_entries[] | select(.value.componentFQN == "scheduler.eventlog") | .value.decodedData.events[]] | length' "$tree")
echo "fixture ready: $buffer_count buffered starts, $backfiller_count backfiller(s), $event_count event(s)"
echo "fixture tree: $tree"
