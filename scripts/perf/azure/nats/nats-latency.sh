#!/usr/bin/env bash
# Publish latency, Felix against NATS, on the same VMs on the same day.
# Run after install.sh (and after nats-cells.sh, which holds the broker VM).
#
#   SESSION=v060-a ./nats-latency.sh
#
# Each cell is felix-loadgen's pubsub latency case: one publisher on
# generator 0, one subscriber, batch 1, one publish in flight, LAT_WARMUP
# (2000) discarded and LAT_TOTAL (20000) measured, publish-to-ack and
# publish-to-delivery percentiles. NATS runs it through nats-latency
# (nats/latency/), a port of that scenario. See README.md "NATS comparison".
#
# Pairs (LAT_PAIRS):
#   oncommit  Felix perf-durable, on_commit fsync  <-> JetStream sync_interval: always
#   periodic  Felix perf-durable, periodic fsync   <-> JetStream default sync
#   inmem     Felix perf (in-memory)               <-> JetStream memory stream
#   core      (the inmem Felix cell)               <-> core NATS, no stream
#
# Felix and NATS alternate, Felix first on odd trials. Before a Felix cell
# NATS is stopped and both stores are wiped and trimmed; before a NATS cell
# nats_reset stops Felix, wipes, trims and starts NATS in the pair's mode.
#
# Steps (STEPS): build (install-latency.sh), cells. Knobs: LAT_PAIRS,
# LAT_PAYLOADS (0 256 1024 4096), LAT_WARMUP, LAT_TOTAL, TRIALS (3),
# FELIX_LAT_REF (main), FELIX_LAT_ENV and FELIX_LAT_CLIENT_ENV (session A's
# latency cells), NATS_SERVER_ENV, NATS_EXTRA_CONF, NATS_MTU.
set -uo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

: "${STEPS:=build cells}"
: "${LAT_PAIRS:=oncommit periodic inmem core}"
: "${LAT_PAYLOADS:=0 256 1024 4096}"
: "${LAT_WARMUP:=2000}"
: "${LAT_TOTAL:=20000}"
: "${FELIX_LAT_REF:=main}"
: "${FELIX_LAT_ENV:=FELIX_QUIC_LISTENERS=4 FELIX_PUB_INGRESS_WAIT=1}"
: "${FELIX_LAT_CLIENT_ENV:=FELIX_PUB_CONN_POOL=4}"

LG="${LOADGEN_VMS[0]}"

# felix_lat <name> <stream> <fsync mode> <payload>
felix_lat() {
  local name="$1" stream="$2" fsync="$3" p="$4"
  if [ "${RESUME}" = 1 ] && [ -e "${OUT}/cells/$(tagged "${name}")/done" ]; then log "skip ${name} (done)"; return 0; fi
  nats_agent_on "${NATS_VM}" "nats-agent stop
felix-agent wipe
nats-agent trim" > "${OUT}/system/nats/lat-felix-prep.txt" 2>&1 || {
    echo "!! could not hand the broker to Felix; see ${OUT}/system/nats/lat-felix-prep.txt" >&2
    FAILED_CELLS="${FAILED_CELLS} ${name}"; return 0; }
  # The wipe stopped Felix; forget the applied configuration so the cell's
  # configure starts it again.
  CURRENT_REF=""; CURRENT_OVERRIDES=""
  # shellcheck disable=SC2086 # FELIX_LAT_ENV is a list of KEY=VALUE words
  stage "${FELIX_LAT_REF}" ${FELIX_LAT_ENV} FELIX_DURABLE_FSYNC_MODE="${fsync}"
  CELL_LOADGEN_ENV="${FELIX_LAT_CLIENT_ENV}" cell "${name}" 1 --scenario pubsub --stream "${stream}" \
    --payload-bytes "${p}" --fanout 1 --batch 1 --warmup "${LAT_WARMUP}" --total "${LAT_TOTAL}"
}

# nats_lat <name> <always|default|memory> <js|core> <payload>: one cell laid
# out like nats_cell's, with nats-latency on generator 0.
nats_lat() {
  local name mode="$2" kind="$3" p="$4" dir rc=0 bad="" f mtu flags
  name="$(tagged "$1")"
  dir="${OUT}/cells/${name}"
  if [ "${RESUME}" = 1 ] && [ -e "${dir}/done" ]; then log "skip ${name} (done)"; return 0; fi
  rm -rf "${dir}"; mkdir -p "${dir}"
  {
    echo "cell=${name}"
    echo "session=${SESSION}"
    echo "ref=nats-server-v${NATS_VERSION}"
    echo "loadgen_ref=nats-latency $(cat "${here}/sessions/${SESSION}.nats-latency-ref" 2>/dev/null)"
    echo "generators=${LG}"
    echo "args=--scenario pubsub --stream nats-${mode} --payload-bytes ${p} --fanout 1 --batch 1 --warmup ${LAT_WARMUP} --total ${LAT_TOTAL}"
    echo "loadgen_env="
    echo "started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "expected_mtu=${EXPECT_MTU}"
    echo "override.NATS_SYNC=${mode}"
    echo "override.NATS_KIND=lat-${kind}"
    [ "${mode}" != memory ] || echo "override.NATS_MEM_RETAIN=${NATS_MEM_RETAIN}"
    for kv in ${NATS_SERVER_ENV}; do echo "override.NATS_ENV_${kv}"; done
    printf '%s\n' "${NATS_EXTRA_CONF}" | sed '/^$/d; s/^/override.NATS_CONF=/'
  } > "${dir}/meta.env"
  log "cell ${name}: ${kind} ${mode} ${p} B"

  # One R1 stream, bench0 on bench.0; none for core.
  local rkind=js-sync
  [ "${kind}" = core ] && rkind=core
  nats_reset "${dir}" "${mode}" "${rkind}" 1 "${NATS_SERVER_ENV}" || {
    echo "!! ${name}: reset failed; see ${dir}/${NATS_VM}.reset.txt" >&2
    FAILED_CELLS="${FAILED_CELLS} ${name}"; return 0; }
  nats_par_on "${dir}" before "nats-agent snapshot --env
felix-agent sampler-start '${name}' nats-server nats" "${NATS_VM}" || rc=1
  nats_par_on "${dir}" armed "nats-agent hostfacts
felix-agent sampler-start '${name}' nats-latency" "${LG}" || true

  flags="--server tls://${NATS_IP}:4222 --tlsca /etc/nats/tls/ca.pem --mode ${kind} --stream bench0 --subject bench.0 --payload-bytes ${p} --fanout 1 --batch 1 --warmup ${LAT_WARMUP} --total ${LAT_TOTAL}"
  echo "nats_cmd=nats-latency ${flags}" >> "${dir}/meta.env"
  run_on_str "${LG}" "set -eu
mkdir -p /var/tmp/nats-cells
o=/var/tmp/nats-cells/${name}.out; e=/var/tmp/nats-cells/${name}.err
s=\$(date +%s.%N)
nats-latency ${flags} --environment 'azure-${SESSION}-${name}' > \$o 2> \$e || { echo '!! case failed'; tail -15 \$e; exit 1; }
t=\$(date +%s.%N)
grep -v '^LOADGEN_JSON' \$o | tail -c 1200
grep '^LOADGEN_JSON' \$o | tail -1
echo gen.start=\$s
echo gen.end=\$t
echo __RUNOK__" > "${dir}/${LG}.run.txt" 2>&1 || rc=1

  nats_par_on "${dir}" after "felix-agent sampler-stop '${name}'
nats-agent snapshot" "${NATS_VM}" || rc=1
  nats_par_on "${dir}" after "felix-agent sampler-stop '${name}'" "${LG}" || true
  fetch_series "${dir}" "${name}"
  cell_line "${dir}"

  for f in "${dir}/${NATS_VM}.before.txt" "${dir}/${LG}.armed.txt"; do
    mtu="$(sed -n 's/^host\.mtu=//p' "${f}" | head -1)"
    [ "${mtu}" = "${EXPECT_MTU}" ] || bad="${bad} mtu:$(basename "${f}" | cut -d. -f1)=${mtu:-?}"
  done
  if [ "${rc}" = 0 ] && grep -q '^LOADGEN_JSON' "${dir}/${LG}.run.txt" && [ -z "${bad}" ]; then
    touch "${dir}/done"
  else
    echo "fail=${bad# }" >> "${dir}/meta.env"
    echo "!! ${name} failed${bad:+ (${bad# })}; recorded and continuing" >&2
    FAILED_CELLS="${FAILED_CELLS} ${name}"
  fi
}

# pair <pair> <payload> <trial> <felix-first:0|1>
pair() {
  local pr="$1" p="$2" t="$3" first="$4" fstream="" fsync="" nmode nkind
  case "${pr}" in
    oncommit) fstream=perf-durable; fsync=on_commit; nmode=always; nkind=js ;;
    periodic) fstream=perf-durable; fsync=periodic; nmode=default; nkind=js ;;
    inmem) fstream=perf; fsync=periodic; nmode=memory; nkind=js ;;
    # Its Felix row is the inmem cell.
    core) nmode=default; nkind=core ;;
    *) echo "!! unknown pair ${pr}" >&2; return 0 ;;
  esac
  local fname="felix-lat-${pr}-p${p}-t${t}" nname="nats-lat-${pr}-p${p}-t${t}"
  if [ -z "${fstream}" ]; then
    nats_lat "${nname}" "${nmode}" "${nkind}" "${p}"
  elif [ "${first}" = 1 ]; then
    felix_lat "${fname}" "${fstream}" "${fsync}" "${p}"
    nats_lat "${nname}" "${nmode}" "${nkind}" "${p}"
  else
    nats_lat "${nname}" "${nmode}" "${nkind}" "${p}"
    felix_lat "${fname}" "${fstream}" "${fsync}" "${p}"
  fi
}

mkdir -p "${OUT}/system/nats"
for step in ${STEPS}; do
  case "${step}" in
  build)
    "${nats_dir}/install-latency.sh" || exit 1
    ;;
  cells)
    EXPECT_MTU="$(expected_mtu)" || exit 1
    record_session nats-latency
    distribute_token || exit 1
    apply_mtu "${EXPECT_MTU}" || exit 1
    for t in $(seq 1 "${TRIALS}"); do
      for pr in ${LAT_PAIRS}; do
        for p in ${LAT_PAYLOADS}; do pair "${pr}" "${p}" "${t}" $(( t % 2 )); done
      done
    done
    # Leave the VM as install.sh left it: NATS up, Felix off.
    nats_agent_on "${NATS_VM}" "systemctl stop felix-broker || true
nats-agent reset default" > "${OUT}/system/nats/lat-final.txt" 2>&1 || true
    finish
    ;;
  *) echo "!! unknown step ${step}" >&2 ;;
  esac
done
