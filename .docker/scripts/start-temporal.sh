#!/bin/sh

set -eu

: "${SERVICES:=}"

flags=""
if [ -n "${SERVICES}" ]; then
    # Convert colon (or comma, for backward compatibility) separated string (i.e. "history:matching")
    # to valid flag list (i.e. "--service=history --service=matching").
    SERVICES=$(echo "${SERVICES}" | tr ':' ',')
    SERVICES=$(echo "${SERVICES}" | tr ',' ' ')
    for i in $SERVICES; do
        flags="${flags} --service=${i}"
    done
fi

# shellcheck disable=SC2086
exec temporal-server --env docker start ${flags}
