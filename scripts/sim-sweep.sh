#!/usr/bin/env bash
# Run a range of simulator seeds, a few at a time, one process per seed.
#
#   scripts/sim-sweep.sh <first seed> <count> [out dir]
#
# Each seed runs the `replay` test of crates/testing/felix-sim/tests/seeds.rs
# with FELIX_SIM_SEED set, so its outcome lands in <out>/seed-<n>.log. A seed
# that fails runs again up to FELIX_SIM_RETRIES more times (default 2): a run
# is not bit-for-bit repeatable, and "failed 1 of 3" and "failed 3 of 3" point
# at different kinds of bug. Every failing seed is listed in <out>/failed.txt
# with how often it failed and the command that replays it. Logs of seeds that
# passed are deleted. Exits 1 if any seed failed.
#
# FELIX_SIM_JOBS sets how many seeds run at once (default 2). A seed holds one
# core most of the time; running more than the machine has makes the stall
# watchdog report a busy machine as a stuck run. FELIX_SIM_BIN names a test
# binary built already; without it the script builds one.
set -euo pipefail

first=${1:?first seed}
count=${2:?seed count}
out=${3:-target/sim-sweep}
jobs=${FELIX_SIM_JOBS:-2}
retries=${FELIX_SIM_RETRIES:-2}

for value in "$first" "$count" "$jobs" "$retries"; do
  [[ "$value" =~ ^[0-9]{1,9}$ ]] || { echo "not a number: $value" >&2; exit 2; }
done

bin=${FELIX_SIM_BIN:-}
if [ -z "$bin" ]; then
  bin=$(cargo test -p felix-sim --test seeds --no-run --message-format=json |
    jq -r 'select(.profile.test == true and .target.name == "seeds") | .executable' |
    tail -1)
fi
[ -x "$bin" ] || { echo "no seeds test binary: $bin" >&2; exit 2; }
# `--exact replay` matching nothing would pass every seed without running one.
"$bin" --list --exact replay 2>/dev/null | grep -q '^replay: test$' ||
  { echo "$bin has no test named replay" >&2; exit 2; }

mkdir -p "$out"
: > "$out/failed.txt"

# Runs one seed; prints "<seed> <failures> <runs>" when it failed at least once.
run_one() {
  local seed=$1 failures=0 runs=0
  local log="$out/seed-$seed.log"
  : > "$log"
  while [ "$runs" -le "$retries" ]; do
    runs=$((runs + 1))
    echo "=== run $runs of seed $seed" >> "$log"
    if ! FELIX_SIM_SEED=$seed "$bin" replay --exact --nocapture >> "$log" 2>&1; then
      failures=$((failures + 1))
    elif [ "$failures" -eq 0 ]; then
      # Passed the first time: nothing to keep.
      rm -f "$log"
      return 0
    fi
  done
  echo "$seed $failures $runs"
}
export -f run_one
export bin out retries

seq "$first" $((first + count - 1)) |
  xargs -P "$jobs" -I{} bash -c 'run_one {}' |
  sort -n |
  while read -r seed failures runs; do
    echo "seed $seed failed $failures of $runs: FELIX_SIM_SEED=$seed cargo test -p felix-sim --test seeds replay -- --nocapture" |
      tee -a "$out/failed.txt"
  done

failed=$(wc -l < "$out/failed.txt" | tr -d ' ')
echo "$failed of $count seeds failed (seeds $first to $((first + count - 1)))"
[ "$failed" -eq 0 ]
