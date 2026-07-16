#!/usr/bin/env bash

set -euo pipefail

readonly package=./chasm/lib/scheduler
readonly tags="${CHASM_SCHEDULER_PROPERTY_TAGS:?}"
readonly checks="${CHASM_SCHEDULER_PROPERTY_CHECKS:?}"
readonly steps="${CHASM_SCHEDULER_PROPERTY_STEPS:?}"
readonly shrink_time="${CHASM_SCHEDULER_PROPERTY_SHRINKTIME:?}"
readonly timeout="${CHASM_SCHEDULER_PROPERTY_TIMEOUT:?}"
readonly shard_count="${CHASM_SCHEDULER_PROPERTY_SHARDS:?}"

if [[ ! "$shard_count" =~ ^[1-9][0-9]*$ ]]; then
    echo "CHASM_SCHEDULER_PROPERTY_SHARDS must be a positive integer" >&2
    exit 2
fi

test_list="$(CGO_ENABLED="${CGO_ENABLED:-}" go test -tags "$tags" "$package" -list '^Test.*Model$')"
tests=()
while IFS= read -r test_name; do
    if [[ "$test_name" =~ ^Test.*Model$ ]]; then
        tests[${#tests[@]}]="$test_name"
    fi
done <<< "$test_list"

if [[ ${#tests[@]} -eq 0 ]]; then
    echo "no scheduler property models found" >&2
    exit 2
fi

patterns=()
for ((i = 0; i < ${#tests[@]}; i++)); do
    shard=$((i % shard_count))
    if [[ -n "${patterns[$shard]-}" ]]; then
        patterns[shard]="${patterns[shard]}|${tests[i]}"
    else
        patterns[shard]="${tests[i]}"
    fi
done

pids=()
trap 'if [[ ${#pids[@]} -gt 0 ]]; then kill "${pids[@]}" 2>/dev/null || true; fi; exit 130' INT TERM

for ((shard = 0; shard < shard_count; shard++)); do
    if [[ -z "${patterns[$shard]-}" ]]; then
        continue
    fi

    echo "scheduler property shard $((shard + 1))/$shard_count: ${patterns[$shard]}"
    CGO_ENABLED="${CGO_ENABLED:-}" go test -tags "$tags" "$package" \
        -count=1 \
        -run="^(${patterns[$shard]})$" \
        -timeout="$timeout" \
        -rapid.checks="$checks" \
        -rapid.steps="$steps" \
        -rapid.shrinktime="$shrink_time" \
        "$@" &
    pids[${#pids[@]}]=$!
done

status=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        status=1
    fi
done

trap - INT TERM
exit "$status"
