#!/usr/bin/env bash
# Run a range of Raft group simulator seeds, a few at a time, one process
# per seed.
#
#   scripts/sim-sweep.sh <first seed> <count> [out dir]
#
# Each seed runs the `raft_seeds` batch of
# crates/testing/felix-sim/tests/raft_seeds.rs on that one seed, so its
# outcome lands in <out>/seed-<n>.log.
#
# FELIX_SIM_MODE picks the nemeses (docs/simulation.md, "Raft groups"):
# `raft` (default) and `raft-clocks`, the second with skewed timers;
# `raft-wipe` and `raft-restore` run them with members stopped and a shard
# directory or data root wiped or put back from an older copy.
# `raft-power-loss`, `raft-relaxed-power-loss`, `raft-majority-cut` and
# `raft-majority-cut-relaxed` cut members' power.
#
# A failing seed is sorted by what its log says. Its categories are
# `history`, `stalled` (a run still going at its virtual bound), `group` (any
# other group finding), `too-little` (a seed that exercised too little to
# count) or `crash`.
#
#   real bug  any category but `stalled`. Listed in <out>/failed.txt. The
#             script exits 1.
#   stall     only `stalled`. Listed in <out>/stalls.txt. The script still
#             exits 0.
#
# A seed with a real bug runs again up to FELIX_SIM_RETRIES more times
# (default 1) to say how often it fails. Each line names the categories and the
# command that replays the seed. Logs of seeds that passed are deleted; the
# list files exist only when they have lines.
#
# FELIX_SIM_JOBS sets how many seeds run at once (default 2). A seed holds one
# core most of the time; running more than the machine has makes the stall
# watchdog report a busy machine as a stuck run. FELIX_SIM_BIN names a test
# binary built already (the `raft_seeds` one); without it the script builds
# one the way `task sim:raft-replay` does.
set -euo pipefail

first=${1:?first seed}
count=${2:?seed count}
out=${3:-target/sim-sweep}
jobs=${FELIX_SIM_JOBS:-2}
retries=${FELIX_SIM_RETRIES:-1}
mode=${FELIX_SIM_MODE:-raft}

for value in "$first" "$count" "$jobs" "$retries"; do
  [[ "$value" =~ ^[0-9]{1,9}$ ]] || { echo "not a number: $value" >&2; exit 2; }
done

# Mode flags go in the environment the test reads, and in the replay command.
test=raft_seeds
case "$mode" in
  raft) mode_env=(); mode_prefix="" ;;
  raft-clocks) mode_env=(FELIX_SIM_CLOCKS=bounded); mode_prefix="FELIX_SIM_CLOCKS=bounded " ;;
  raft-wipe) mode_env=(FELIX_SIM_WIPE=1); mode_prefix="FELIX_SIM_WIPE=1 " ;;
  raft-restore) mode_env=(FELIX_SIM_RESTORE=1); mode_prefix="FELIX_SIM_RESTORE=1 " ;;
  raft-power-loss) mode_env=(FELIX_SIM_POWER_LOSS=1); mode_prefix="FELIX_SIM_POWER_LOSS=1 " ;;
  raft-relaxed-power-loss) mode_env=(FELIX_SIM_POWER_LOSS=relaxed); mode_prefix="FELIX_SIM_POWER_LOSS=relaxed " ;;
  raft-majority-cut) mode_env=(FELIX_SIM_MAJORITY_CUT=1); mode_prefix="FELIX_SIM_MAJORITY_CUT=1 " ;;
  raft-majority-cut-relaxed) mode_env=(FELIX_SIM_POWER_LOSS=relaxed FELIX_SIM_MAJORITY_CUT=1); mode_prefix="FELIX_SIM_POWER_LOSS=relaxed FELIX_SIM_MAJORITY_CUT=1 " ;;
  *) echo "FELIX_SIM_MODE must be raft, raft-clocks, raft-wipe, raft-restore, raft-power-loss, raft-relaxed-power-loss, raft-majority-cut or raft-majority-cut-relaxed, got $mode" >&2; exit 2 ;;
esac
# The test one seed runs: the batch, on a range of one.
case_name=raft_seeds; case_flags=(--exact --ignored)

bin=${FELIX_SIM_BIN:-}
if [ -z "$bin" ]; then
  bin=$(RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}/tokio-unstable" \
    cargo test -p felix-sim --release --test "$test" --no-run --message-format=json |
    jq -r --arg test "$test" 'select(.profile.test == true and .target.name == $test) | .executable' |
    tail -1)
fi
[ -x "$bin" ] || { echo "no $test test binary: $bin" >&2; exit 2; }
# An exact name matching nothing would pass every seed without running one.
"$bin" --list "${case_flags[@]}" "$case_name" 2>/dev/null | grep -q "^$case_name: test$" ||
  { echo "$bin has no test named $case_name" >&2; exit 2; }

mkdir -p "$out"
: > "$out/failed.txt"
: > "$out/stalls.txt"

# The categories a log shows, comma separated, `crash` when the log names
# none.
categories() {
  local log=$1 found=""
  grep -q '^history: ' "$log" && found="history"
  grep -q '^group: stalled: ' "$log" && found+=" stalled"
  grep '^group: ' "$log" | grep -vq '^group: stalled: ' && found+=" group"
  grep -q 'checked too little' "$log" && found+=" too-little"
  found=$(echo $found | tr ' ' ',')
  echo "${found:-crash}"
}

# Whether every category is a tracked liveness one.
only_liveness() {
  local category
  for category in ${1//,/ }; do
    case "$category" in
      stalled) ;;
      *) return 1 ;;
    esac
  done
}

# The command that replays a seed, built from the mode: a run prints none
# that carries it.
replay_command() {
  local seed=$1
  echo "${mode_prefix}FELIX_SIM_SEED=$seed task sim:raft-replay"
}

# Runs one seed; prints "<kind> <seed> <categories> <failures> <runs>" when it
# failed, where kind is `bug` or `stall`.
run_one() {
  local seed=$1 failures=0 runs=0 kind=bug cats=""
  local log="$out/seed-$seed.log"
  : > "$log"
  while [ "$runs" -le "$retries" ]; do
    runs=$((runs + 1))
    echo "=== run $runs of seed $seed" >> "$log"
    if ! env "${mode_env[@]}" FELIX_SIM_SEED=$seed FELIX_SIM_SEED_START=$seed FELIX_SIM_SEEDS=1 \
      "$bin" "$case_name" "${case_flags[@]}" --nocapture >> "$log" 2>&1; then
      failures=$((failures + 1))
      if [ "$failures" -eq 1 ]; then
        cats=$(categories "$log")
        # A tracked stall is not run again: it costs minutes and says the same.
        if only_liveness "$cats"; then
          kind=stall
          break
        fi
      fi
    elif [ "$failures" -eq 0 ]; then
      rm -f "$log"
      return 0
    fi
  done
  echo "$kind $seed $cats $failures $runs"
}
export -f run_one categories only_liveness
export bin out retries mode test case_name
# Arrays do not cross into the child shell; the flags hold no spaces.
export CASE_FLAGS="${case_flags[*]}"
export -f replay_command
export mode_prefix
# Arrays do not cross into the child shell; pass the mode's variables by name.
export MODE_ENV="${mode_env[*]:-}"

seq "$first" $((first + count - 1)) |
  xargs -P "$jobs" -I{} bash -c 'mode_env=($MODE_ENV); case_flags=($CASE_FLAGS); run_one {}' |
  sort -k2,2n |
  while read -r kind seed cats failures runs; do
    line="seed $seed ($cats) failed $failures of $runs, mode $mode: $(replay_command "$seed")"
    if [ "$kind" = stall ]; then
      echo "stall $line" | tee -a "$out/stalls.txt"
    else
      echo "BUG   $line" | tee -a "$out/failed.txt"
    fi
  done

bugs=$(wc -l < "$out/failed.txt" | tr -d ' ')
stalls=$(wc -l < "$out/stalls.txt" | tr -d ' ')
[ -s "$out/failed.txt" ] || rm -f "$out/failed.txt"
[ -s "$out/stalls.txt" ] || rm -f "$out/stalls.txt"
echo "$bugs real bugs and $stalls tracked stalls in $count seeds (seeds $first to $((first + count - 1)), mode $mode)"
[ "$bugs" -eq 0 ]
