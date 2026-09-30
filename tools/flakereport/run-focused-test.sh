#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 ./package TestNameRegex" >&2
  exit 2
fi

package=$1
test_name=$2

if [[ ! "$package" =~ ^\./[A-Za-z0-9_./-]+$ ]] || [[ "$package" == *".."* ]]; then
  echo "invalid package: $package" >&2
  exit 2
fi
if [[ -z "$test_name" ]] || [[ ${#test_name} -gt 512 ]] || [[ "$test_name" == *$'\n'* ]]; then
  echo "invalid test regex" >&2
  exit 2
fi
if [[ -z "${FLAKEREPORT_TEST_MARKER:-}" ]] || ! mkdir "$FLAKEREPORT_TEST_MARKER" 2>/dev/null; then
  echo "focused test already ran or execution marker is unavailable" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"

go test -tags test_dep "$package" -run "$test_name" -count=1 -timeout=10m
