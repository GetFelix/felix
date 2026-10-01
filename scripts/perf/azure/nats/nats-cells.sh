#!/usr/bin/env bash
# NATS JetStream on session A's VMs, measured the way cells.sh measures Felix.
# Run after the Felix cells and install.sh; results land beside Felix's in
# sessions/<session>-results/cells/nats-*/ and summarize.py reads both.
#
#   SESSION=v060-a ./nats-cells.sh                       # sweep, then matrix
#   SESSION=v060-a STEPS=sweep ./nats-cells.sh           # find NATS's best shape
#   SESSION=v060-a STEPS=matrix RUN_TAG=best NATS_STREAMS=8 PUBS_PER_GEN=32 ./nats-cells.sh
#
# Every cell: the server restarted on an empty /data/nats with a dropped page
# cache, the streams recreated, snapshots before and after, the 1 Hz sampler
# on nats-server (CPU ticks, stored bytes, received bytes) and on each
# generator, and all generators publishing from one wall-clock start for
# CELL_SECS. See README.md "NATS comparison" for what each cell pairs with.
#
# Steps (STEPS, in order):
#   sweep   one-at-a-time variations around the base shape, SWEEP_TRIALS each
#           (default 1), in each of NATS_SWEEP_MODES: streams, async window,
#           publishers per generator, publish API, fast-batch flow, GOGC.
#   matrix  the pairs quoted against Felix, TRIALS each (default 3), in each
#           of NATS_MATRIX_MODES (always, memory, default), using the shape
#           below: 57 cells, about 4 h at ~4 min a cell. The sweep is 38.
#
# Shape knobs (defaults match session A's Felix ingest cells):
#   NATS_STREAMS=12      streams (Felix: 12 keys over the stream's shards)
#   PUBS_PER_GEN=16      publishers per generator (64 total)
#   ASYNC_WINDOW=64      async publishes in flight per publisher
#   FAST_WINDOW=64       fast-batch acks outstanding per publisher
#   NATS_SERVER_ENV=""   process environment for nats-server, e.g. "GOGC=400"
#   NATS_EXTRA_CONF=""   server config lines appended at top level
#   CELL_SECS=90, START_DELAY_SECS=45, TRIALS=3, RESUME=1, RUN_TAG (as cells.sh)
#   CHUNK_SECS=10        nats bench run length inside a cell (see nats-agent)
set -uo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

: "${STEPS:=sweep matrix}"
: "${NATS_STREAMS:=12}"
: "${PUBS_PER_GEN:=16}"
: "${ASYNC_WINDOW:=64}"
: "${FAST_WINDOW:=64}"
: "${NATS_SERVER_ENV:=}"
: "${NATS_EXTRA_CONF:=}"
: "${CELL_SECS:=90}"
: "${CHUNK_SECS:=10}"
: "${NATS_MATRIX_MODES:=always memory default}"
: "${NATS_MATRIX_PAYLOADS:=4096 256}"
: "${NATS_CORE_PAYLOADS:=4096}"
: "${SWEEP_TRIALS:=1}"
: "${NATS_SWEEP_MODES:=always default}"
: "${NATS_SWEEP_PAYLOAD:=4096}"
: "${NATS_SWEEP_STREAMS:=4 8 24 48}"
: "${NATS_SWEEP_WINDOWS:=16 256 1024 4096}"
: "${NATS_SWEEP_PUBS:=8 32 64}"
: "${NATS_SWEEP_KINDS:=js-sync js-fast}"
: "${NATS_SWEEP_FLOWS:=1 16 256 1024}"
: "${NATS_SWEEP_GOGC:=200 400}"
NGEN="${#LOADGEN_VMS[@]}"

# nats_cell <name> <mode> <kind> <size> <pubs/gen> <window> <flow> <streams> [server env...]
nats_cell() {
  local name mode="$2" kind="$3" size="$4" pubs="$5" window="$6" flow="$7" streams="$8"
  name="$(tagged "$1")"
  shift 8
  local senv="${NATS_SERVER_ENV}${*:+ $*}" dir="${OUT}/cells/${name}" lg p pids=() rc=0 gi
  if [ "${RESUME}" = 1 ] && [ -e "${dir}/done" ]; then log "skip ${name} (done)"; return 0; fi
  rm -rf "${dir}"; mkdir -p "${dir}"

  # The instrument's shape in felix-loadgen's flag names, so cells.csv puts
  # NATS and Felix rows in the same columns: batch is records per ack and
  # in-flight what may be outstanding (0 is fire-and-forget).
  local batch=1 inflight="${window}"
  case "${kind}" in
    js-fast) batch="${flow}" ;;
    js-sync) inflight=1 ;;
    core) inflight=0 ;;
  esac
  local streams_cmd="nats-agent streams ${streams}"
  [ "${mode}" = memory ] && streams_cmd="nats-agent streams ${streams} memory"
  [ "${kind}" = core ] && streams_cmd=":"
  local env_lines
  # shellcheck disable=SC2086 # senv is a list of KEY=VALUE words
  env_lines="$(printf '%s\n' ${senv})"
  {
    echo "cell=${name}"
    echo "session=${SESSION}"
    echo "ref=nats-server-v${NATS_VERSION}"
    echo "loadgen_ref=natscli-v${NATS_CLI_VERSION}"
    echo "generators=${LOADGEN_VMS[*]}"
    echo "args=--scenario nats-${kind} --stream nats-${mode} --payload-bytes ${size} --batch ${batch} --in-flight ${inflight} --concurrency ${pubs} --keys ${streams} --duration-secs ${CELL_SECS}"
    echo "loadgen_env=CHUNK_SECS=${CHUNK_SECS}"
    echo "started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "override.NATS_SYNC=${mode}"
    echo "override.NATS_KIND=${kind}"
    [ "${kind}" != js-fast ] || echo "override.NATS_FAST_FLOW=${flow}"
    for kv in ${senv}; do echo "override.NATS_ENV_${kv}"; done
    printf '%s\n' "${NATS_EXTRA_CONF}" | sed '/^$/d; s/^/override.NATS_CONF=/'
  } > "${dir}/meta.env"
  log "cell ${name}: ${kind} ${mode} ${size} B, ${pubs}/gen, window ${window}, flow ${flow}, ${streams} streams${senv:+, ${senv}}"

  nats_agent_on "${NATS_VM}" "cat > /etc/nats/nats.env <<'ENV'
${env_lines}
ENV
cat > /etc/nats/extra.conf <<'CONF'
${NATS_EXTRA_CONF}
CONF
nats-agent reset ${mode}
${streams_cmd}" > "${dir}/${NATS_VM}.reset.txt" 2>&1 || {
    echo "!! ${name}: reset failed; see ${dir}/${NATS_VM}.reset.txt" >&2
    FAILED_CELLS="${FAILED_CELLS} ${name}"; return 0; }

  nats_par_on "${dir}" before "nats-agent snapshot --env
felix-agent sampler-start '${name}' nats-server nats" "${NATS_VM}" || rc=1
  nats_par_on "${dir}" armed "nats-agent hostfacts
felix-agent sampler-start '${name}' nats" "${LOADGEN_VMS[@]}" || true
  local start_at=$(( $(date +%s) + START_DELAY_SECS ))
  echo "start_at=${start_at}" >> "${dir}/meta.env"

  gi=0
  for lg in "${LOADGEN_VMS[@]}"; do
    ( nats_agent_on "${lg}" "ulimit -n 1048576 || true
CHUNK_SECS=${CHUNK_SECS} nats-agent bench '${name}' ${start_at} ${CELL_SECS} ${kind} ${size} ${pubs} ${window} ${flow} ${streams} ${gi}" \
        > "${dir}/${lg}.run.txt" 2>&1 ) &
    pids+=("$!")
    gi=$((gi + 1))
  done
  for p in "${pids[@]}"; do wait "${p}" || rc=1; done
  grep -h '^nats_cmd' "${dir}/${LOADGEN_VMS[0]}.run.txt" >> "${dir}/meta.env" || true

  nats_par_on "${dir}" after "felix-agent sampler-stop '${name}'
nats-agent snapshot" "${NATS_VM}" || rc=1
  nats_par_on "${dir}" after "felix-agent sampler-stop '${name}'" "${LOADGEN_VMS[@]}" || true
  fetch_series "${dir}" "${name}"

  cell_line "${dir}"
  local got; got="$(grep -l '^NATS_BENCH_JSON' "${dir}"/*.run.txt 2>/dev/null | wc -l | tr -d ' ')"
  if [ "${rc}" = 0 ] && [ "${got}" = "${NGEN}" ]; then
    touch "${dir}/done"
  else
    echo "!! ${name} incomplete (${got}/${NGEN} generator results); recorded and continuing" >&2
    FAILED_CELLS="${FAILED_CELLS} ${name}"
  fi
}

# cell_name <mode> <kind> <size> <pubs> <window> <flow> <streams> [env...]
cell_name() {
  local n="nats-$2" e
  [ "$2" = core ] || n="${n}-$1"
  n="${n}-p$3"
  case "$2" in
    js-async) n="${n}-w$5" ;;
    js-fast) n="${n}-f$6-w$5" ;;
  esac
  n="${n}-c$4-s$7"
  shift 7
  for e in ${NATS_SERVER_ENV} "$@"; do n="${n}-$(printf '%s' "${e}" | tr '[:upper:]' '[:lower:]' | tr '=' '_')"; done
  printf '%s' "${n}"
}

# run <trials> <mode> <kind> <size> <pubs> <window> <flow> <streams> [env...]
run() {
  local n="$1" t name
  shift
  name="$(cell_name "$@")"
  for t in $(seq 1 "${n}"); do nats_cell "${name}-t${t}" "$@"; done
}

record_session nats-cells
nats_par_on "${OUT}/system/nats" hostfacts "nats-agent hostfacts" "${NATS_VM}" "${LOADGEN_VMS[@]}" || true

for step in ${STEPS}; do
  case "${step}" in
  sweep)
    # One knob at a time around the base, so a knob that helps NATS shows up
    # without a full cross product. The best values go into the matrix run.
    P="${NATS_SWEEP_PAYLOAD}"; C="${PUBS_PER_GEN}"; W="${ASYNC_WINDOW}"; S="${NATS_STREAMS}"
    for mode in ${NATS_SWEEP_MODES}; do
      run "${SWEEP_TRIALS}" "${mode}" js-async "${P}" "${C}" "${W}" 1 "${S}"
      for s in ${NATS_SWEEP_STREAMS}; do run "${SWEEP_TRIALS}" "${mode}" js-async "${P}" "${C}" "${W}" 1 "${s}"; done
      for w in ${NATS_SWEEP_WINDOWS}; do run "${SWEEP_TRIALS}" "${mode}" js-async "${P}" "${C}" "${w}" 1 "${S}"; done
      for c in ${NATS_SWEEP_PUBS}; do run "${SWEEP_TRIALS}" "${mode}" js-async "${P}" "${c}" "${W}" 1 "${S}"; done
      for k in ${NATS_SWEEP_KINDS}; do
        case "${k}" in
          js-fast) for f in ${NATS_SWEEP_FLOWS}; do run "${SWEEP_TRIALS}" "${mode}" js-fast "${P}" "${C}" "${FAST_WINDOW}" "${f}" "${S}"; done ;;
          *) run "${SWEEP_TRIALS}" "${mode}" "${k}" "${P}" "${C}" "${W}" 1 "${S}" ;;
        esac
      done
      for g in ${NATS_SWEEP_GOGC}; do run "${SWEEP_TRIALS}" "${mode}" js-async "${P}" "${C}" "${W}" 1 "${S}" "GOGC=${g}"; done
    done
    ;;

  matrix)
    # Pairs with session A (README.md, "NATS comparison"): always <-> Felix
    # OnCommit, memory <-> Felix in-memory, default (page cache, fsync every
    # 2 min) <-> Felix periodic; async <-> acked b1 with a window, fast f1 <->
    # acked b1-f64, fast f64 <-> acked b64-f64, core <-> fire-and-forget.
    for mode in ${NATS_MATRIX_MODES}; do
      for p in ${NATS_MATRIX_PAYLOADS}; do
        run "${TRIALS}" "${mode}" js-async "${p}" "${PUBS_PER_GEN}" "${ASYNC_WINDOW}" 1 "${NATS_STREAMS}"
        run "${TRIALS}" "${mode}" js-fast "${p}" "${PUBS_PER_GEN}" "${FAST_WINDOW}" 1 "${NATS_STREAMS}"
        run "${TRIALS}" "${mode}" js-fast "${p}" "${PUBS_PER_GEN}" "${FAST_WINDOW}" 64 "${NATS_STREAMS}"
      done
    done
    for p in ${NATS_CORE_PAYLOADS}; do
      run "${TRIALS}" default core "${p}" "${PUBS_PER_GEN}" 0 1 "${NATS_STREAMS}"
    done
    ;;

  *) echo "!! unknown step ${step}" >&2 ;;
  esac
done

finish
