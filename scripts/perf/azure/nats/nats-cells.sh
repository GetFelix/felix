#!/usr/bin/env bash
# NATS JetStream on session A's VMs, measured the way cells.sh measures Felix.
# Run after the Felix cells and install.sh. Results land beside Felix's in
# sessions/<session>-results/cells/nats-*/ and summarize.py reads both.
#
#   SESSION=v060-a STEPS=sweep ./nats-cells.sh     # find NATS's best shape
#   SESSION=v060-a STEPS=matrix RUN_TAG=best FAST_WINDOW=256 PUBS_PER_GEN=32 ./nats-cells.sh
#
# Every cell: one calibration per shape (cached), then the server restarted on
# a trimmed, empty /data/nats with a dropped page cache, the streams
# recreated, snapshots before and after, the 1 Hz sampler on nats-server and
# on every generator, and all generators running one continuous `nats bench`
# from a shared wall-clock start, interrupted at CELL_SECS. Throughput is the
# server's steady-state rate. See README.md "NATS comparison".
#
# Steps (STEPS, in order):
#   sweep   streams x fast-batch window in `always` mode (the 2-D grid), then
#           one knob at a time around the base in NATS_SWEEP_MODES: publishers
#           per generator, fast-batch flow, async window, sync publish, GOGC.
#           SWEEP_TRIALS each (default 1). 47 cells.
#   matrix  the cells quoted against Felix, TRIALS each (default 3): each
#           durability mode x payload x stream count, fast batch with flow 1
#           (the sliding-window pair for Felix's acked cells) and flow 64,
#           async stop-and-wait in NATS_MATRIX_ASYNC_MODES, and core publish.
#           With the defaults 43 shapes, 129 cells, about 9-10 h; trim with
#           the NATS_MATRIX_* lists and run it in parts.
#
# Shape knobs (nats-lib.sh): PUBS_PER_GEN=16, FAST_WINDOW=64, ASYNC_WINDOW=64,
# NATS_PROCS=1, NATS_SERVER_ENV, NATS_EXTRA_CONF, NATS_MEM_RETAIN, CELL_SECS=90,
# NATS_CALIB_SECS=15, NATS_MTU (default: what the Felix cells recorded).
set -uo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

: "${STEPS:=sweep matrix}"
: "${NATS_MATRIX_MODES:=always memory default}"
: "${NATS_MATRIX_ASYNC_MODES:=always}"
: "${NATS_MATRIX_PAYLOADS:=4096 256}"
# Felix's listener-sweep cells use 12 keys and its shape cells 48 (SHARDS);
# 64 is one stream per publisher, a NATS-only extra.
: "${NATS_MATRIX_STREAMS:=12 48 64}"
: "${NATS_CORE_PAYLOADS:=4096}"
: "${SWEEP_TRIALS:=1}"
: "${NATS_SWEEP_PAYLOAD:=4096}"
: "${NATS_SWEEP_GRID_STREAMS:=12 24 48 64}"
: "${NATS_SWEEP_GRID_WINDOWS:=16 64 256 1024 4000}"
: "${NATS_SWEEP_MODES:=always default}"
: "${NATS_SWEEP_STREAMS:=48}"
: "${NATS_SWEEP_PUBS:=8 32 64}"
: "${NATS_SWEEP_FLOWS:=16 64 256}"
# nats.go caps async publishes in flight at 4000 by default
# (PublishAsyncMaxPending) and nats bench does not raise it, so 4000 is the
# largest async window that does not stall.
: "${NATS_SWEEP_ASYNC_WINDOWS:=64 256 1024 4000}"
: "${NATS_SWEEP_GOGC:=200 400}"

EXPECT_MTU="$(expected_mtu)" || exit 1

# run <trials> <mode> <kind> <size> <pubs> <window> <flow> <streams> [env...]
run() {
  local n="$1" t name
  shift
  name="$(cell_name "$@")"
  for t in $(seq 1 "${n}"); do nats_cell "${name}-t${t}" "$@"; done
}

record_session nats-cells
apply_mtu "${EXPECT_MTU}" || exit 1
nats_par_on "${OUT}/system/nats" hostfacts "nats-agent hostfacts" "${NATS_VM}" "${LOADGEN_VMS[@]}" || true

for step in ${STEPS}; do
  case "${step}" in
  sweep)
    P="${NATS_SWEEP_PAYLOAD}"; C="${PUBS_PER_GEN}"; W="${FAST_WINDOW}"; S="${NATS_SWEEP_STREAMS}"
    # Under sync_interval: always an R1 file stream fsyncs every write under
    # its lock, so stream count and window interact; sweep them together.
    for s in ${NATS_SWEEP_GRID_STREAMS}; do
      for w in ${NATS_SWEEP_GRID_WINDOWS}; do run "${SWEEP_TRIALS}" always js-fast "${P}" "${C}" "${w}" 1 "${s}"; done
    done
    for mode in ${NATS_SWEEP_MODES}; do
      [ "${mode}" = always ] || run "${SWEEP_TRIALS}" "${mode}" js-fast "${P}" "${C}" "${W}" 1 "${S}"
      for c in ${NATS_SWEEP_PUBS}; do run "${SWEEP_TRIALS}" "${mode}" js-fast "${P}" "${c}" "${W}" 1 "${S}"; done
      for f in ${NATS_SWEEP_FLOWS}; do run "${SWEEP_TRIALS}" "${mode}" js-fast "${P}" "${C}" "${W}" "${f}" "${S}"; done
      for w in ${NATS_SWEEP_ASYNC_WINDOWS}; do run "${SWEEP_TRIALS}" "${mode}" js-async "${P}" "${C}" "${w}" 1 "${S}"; done
      run "${SWEEP_TRIALS}" "${mode}" js-sync "${P}" "${C}" 1 1 "${S}"
      for g in ${NATS_SWEEP_GOGC}; do run "${SWEEP_TRIALS}" "${mode}" js-fast "${P}" "${C}" "${W}" 1 "${S}" "GOGC=${g}"; done
    done
    ;;

  matrix)
    # Pairs (README.md "NATS comparison"): always <-> Felix OnCommit, memory
    # <-> Felix in-memory, default <-> Felix periodic. Fast flow 1 <-> acked
    # b1-f64, fast flow 64 <-> acked b64-f64, core <-> fire-and-forget.
    for mode in ${NATS_MATRIX_MODES}; do
      for p in ${NATS_MATRIX_PAYLOADS}; do
        for s in ${NATS_MATRIX_STREAMS}; do
          run "${TRIALS}" "${mode}" js-fast "${p}" "${PUBS_PER_GEN}" "${FAST_WINDOW}" 1 "${s}"
          run "${TRIALS}" "${mode}" js-fast "${p}" "${PUBS_PER_GEN}" "${FAST_WINDOW}" 64 "${s}"
          case " ${NATS_MATRIX_ASYNC_MODES} " in
            *" ${mode} "*) run "${TRIALS}" "${mode}" js-async "${p}" "${PUBS_PER_GEN}" "${ASYNC_WINDOW}" 1 "${s}" ;;
          esac
        done
      done
    done
    for p in ${NATS_CORE_PAYLOADS}; do
      run "${TRIALS}" default core "${p}" "${PUBS_PER_GEN}" 0 1 12
    done
    ;;

  *) echo "!! unknown step ${step}" >&2 ;;
  esac
done

finish
