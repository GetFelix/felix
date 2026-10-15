#!/usr/bin/env bash
# Create the benchmark streams on the session's NATS server.
#
#   SESSION=v060-a ./streams.sh [count] [file|memory]     (default 12 file)
#
# count R1 file streams, bench<i> on subject bench.<i>, with <i> zero-padded to
# the width of count, which is the subject suffix `nats bench --multisubject
# --multisubjectmax <count>` publishes to. A publisher therefore rotates over
# every stream the way a Felix publisher rotates over its routing keys. Fast
# batch publish is allowed on each stream.
#
# nats-cells.sh calls the same agent command after each reset; this script is
# for setting the streams up by hand.
set -euo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

n="${1:-12}"; storage="${2:-file}"
# Memory streams keep NATS_MEM_RETAIN records in total, like Felix's in-memory
# ring (nats-lib.sh).
nats_agent_on "${NATS_VM}" "nats-agent streams ${n} ${storage} ${NATS_MEM_RETAIN}" | grep -E '^streams=' | sed 's/^/   /'
