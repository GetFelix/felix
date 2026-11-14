#!/usr/bin/env bash
# Run a range of simulator seeds, a few at a time, one process per seed.
#
#   scripts/sim-sweep.sh <first seed> <count> [out dir]
#
# Each seed runs the `replay` test of crates/testing/felix-sim/tests/seeds.rs
# with FELIX_SIM_SEED set, so its outcome lands in <out>/seed-<n>.log. The
# Raft modes run the `raft_seeds` batch of tests/raft_seeds.rs on that one
# seed instead.
#
# FELIX_SIM_MODE picks the nemeses beyond the default faults: `off` (default),
# `clocks` (bounded clock skew), `placement` (placement faults),
# `power-loss` (power cuts in place of kills), `relaxed-power-loss` (the same
# on brokers that do not sync), `majority-cut` and `majority-cut-relaxed`
# (several brokers lose power at once) or `wipe` (shard directories and data
# directories emptied). `raft` and `raft-clocks` run Raft groups
# (docs/simulation.md, "Raft groups"), the second with skewed timers.
#
# A failing seed is sorted by what its log says:
#
#   real bug  a history violation, an undecided key, a safety finding (more
#             than one serving broker, two leaders in a generation, an
#             acknowledged write a leader does not hold), or a crash.
#             Listed in <out>/failed.txt. The script exits 1.
#   stall     only the liveness findings `stalled`, `election-settles` and
#             `final-reads`, the tracked ones in docs/simulation.md. Listed
#             in <out>/stalls.txt. The script still exits 0.
#
# A Raft seed's categories are `history`, `stalled` (a run still going at its
# virtual bound), `group` (any other group finding), `too-little` (a seed
# that exercised too little to count) or `crash`; only `stalled` is a stall.
#
# A seed with a real bug runs again up to FELIX_SIM_RETRIES more times
# (default 1) to say how often it fails. Each line names the categories and the
# command that replays the seed. Logs of seeds that passed are deleted; the
# list files exist only when they have lines.
#
# FELIX_SIM_JOBS sets how many seeds run at once (default 2). A seed holds one
# core most of the time; running more than the machine has makes the stall
# watchdog report a busy machine as a stuck run. FELIX_SIM_BIN names a test
# binary built already (the `raft_seeds` one for a Raft mode); without it the
# script builds one the way `task sim:replay` does.
set -euo pipefail

first=${1:?first seed}
count=${2:?seed count}
out=${3:-target/sim-sweep}
jobs=${FELIX_SIM_JOBS:-2}
retries=${FELIX_SIM_RETRIES:-1}
mode=${FELIX_SIM_MODE:-off}

for value in "$first" "$count" "$jobs" "$retries"; do
  [[ "$value" =~ ^[0-9]{1,9}$ ]] || { echo "not a number: $value" >&2; exit 2; }
done

# Mode flags go in the environment the test reads, and in the replay command
# when a log does not print one.
test=seeds
case "$mode" in
  off) mode_env=() ; mode_prefix="" ;;
  clocks) mode_env=(FELIX_SIM_CLOCKS=bounded); mode_prefix="FELIX_SIM_CLOCKS=bounded " ;;
  placement) mode_env=(FELIX_SIM_PLACEMENT=1); mode_prefix="FELIX_SIM_PLACEMENT=1 " ;;
  power-loss) mode_env=(FELIX_SIM_POWER_LOSS=1); mode_prefix="FELIX_SIM_POWER_LOSS=1 " ;;
  relaxed-power-loss) mode_env=(FELIX_SIM_POWER_LOSS=relaxed); mode_prefix="FELIX_SIM_POWER_LOSS=relaxed " ;;
  majority-cut) mode_env=(FELIX_SIM_MAJORITY_CUT=1); mode_prefix="FELIX_SIM_MAJORITY_CUT=1 " ;;
  majority-cut-relaxed) mode_env=(FELIX_SIM_POWER_LOSS=relaxed FELIX_SIM_MAJORITY_CUT=1); mode_prefix="FELIX_SIM_POWER_LOSS=relaxed FELIX_SIM_MAJORITY_CUT=1 " ;;
  wipe) mode_env=(FELIX_SIM_WIPE=1); mode_prefix="FELIX_SIM_WIPE=1 " ;;
  raft) test=raft_seeds; mode_env=(); mode_prefix="" ;;
  raft-clocks) test=raft_seeds; mode_env=(FELIX_SIM_CLOCKS=bounded); mode_prefix="FELIX_SIM_CLOCKS=bounded " ;;
  *) echo "FELIX_SIM_MODE must be off, clocks, placement, power-loss, relaxed-power-loss, majority-cut, majority-cut-relaxed, wipe, raft or raft-clocks, got $mode" >&2; exit 2 ;;
esac
# The test one seed runs: `replay` takes FELIX_SIM_SEED, the Raft batch a
# range of one.
if [ "$test" = seeds ]; then
  case_name=replay; case_flags=(--exact)
else
  case_name=raft_seeds; case_flags=(--exact --ignored)
fi

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

# The categories a log shows, comma separated: `history` for violations or an
# undecided key, otherwise each failing checker's name, `crash` when the log
# names none.
categories() {
  local log=$1 found=""
  if [ "$test" = raft_seeds ]; then
    grep -q '^history: ' "$log" && found="history"
    grep -q '^group: stalled: ' "$log" && found+=" stalled"
    grep '^group: ' "$log" | grep -vq '^group: stalled: ' && found+=" group"
    grep -q 'checked too little' "$log" && found+=" too-little"
    found=$(echo $found | tr ' ' ',')
    echo "${found:-crash}"
    return
  fi
  grep -qE '^[0-9]+ violation\(s\)|undecided' "$log" && found="history"
  found+=" $(grep -oE '^\[[a-z-]+\]' "$log" | tr -d '[]' | sort -u | tr '\n' ' ')"
  found=$(echo $found | tr ' ' ',')
  echo "${found:-crash}"
}

# Whether every category is a tracked liveness one.
only_liveness() {
  local category
  for category in ${1//,/ }; do
    case "$category" in
      stalled|election-settles|final-reads) ;;
      *) return 1 ;;
    esac
  done
}

# The command the failed run printed, or one built from the mode. A Raft run
# prints none that carries the mode, so its command is always built.
replay_command() {
  local seed=$1 log=$2 line
  if [ "$test" = raft_seeds ]; then
    echo "${mode_prefix}FELIX_SIM_SEED=$seed task sim:raft-replay"
    return
  fi
  line=$(grep -A1 'To run it again' "$log" | sed -n 2p | sed 's/^ *//' || true)
  echo "${line:-${mode_prefix}FELIX_SIM_SEED=$seed task sim:replay}"
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
    line="seed $seed ($cats) failed $failures of $runs, mode $mode: $(replay_command "$seed" "$out/seed-$seed.log")"
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
