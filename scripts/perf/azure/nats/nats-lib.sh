#!/usr/bin/env bash
# Shared by the NATS comparison scripts. Sourced, never run.
#
# Runs on a live Felix session's VMs (session A: felixperf-broker-0 and the
# four generators), reusing cells.sh for the session inventory, the result
# layout, the sampler series fetch and the summary, so NATS cells land beside
# the Felix cells and summarize.py reads both.

nats_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
here="$(cd "${nats_dir}/.." && pwd)"
# shellcheck source=scripts/perf/azure/cells.sh
source "${here}/cells.sh"

# Pinned releases, checked against the SHA256SUMS published with each.
# https://github.com/nats-io/nats-server/releases/tag/v2.15.0
# https://github.com/nats-io/natscli/releases/tag/v0.5.0
: "${NATS_VERSION:=2.15.0}"
: "${NATS_SHA256:=5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92}"
: "${NATS_CLI_VERSION:=0.5.0}"
: "${NATS_CLI_SHA256:=d9fc93e9e9ab0310deff7e719a8a0da6f9d66f26d473f039aaee70155507aecc}"

[ "${#BROKER_VMS[@]}" -ge 1 ] || { echo "!! the session has no broker" >&2; return 1; }
[ "${#BROKER_VMS[@]}" -eq 1 ] || echo "!! ${#BROKER_VMS[@]} brokers; NATS runs as one server on ${BROKER_VMS[0]}" >&2
# shellcheck disable=SC2034 # read by the scripts that source this
NATS_VM="${BROKER_VMS[0]}"
# shellcheck disable=SC2034
NATS_IP="${BROKER_IP_LIST[0]}"

# nats_agent_on <vm> <commands>: agent_on with nats-agent installed too.
nats_agent_on() {
  run_on_str "$1" "set -e
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cat > /usr/local/sbin/felix-agent <<'FELIX_AGENT_EOF'
$(cat "${here}/remote/felix-agent.sh")
FELIX_AGENT_EOF
cat > /usr/local/sbin/nats-agent <<'NATS_AGENT_EOF'
$(cat "${nats_dir}/remote/nats-agent.sh")
NATS_AGENT_EOF
chmod 0755 /usr/local/sbin/felix-agent /usr/local/sbin/nats-agent
$2
echo __RUNOK__"
}

# nats_par_on <dir> <suffix> <commands> <vm>...: par_on for nats_agent_on.
nats_par_on() {
  local dir="$1" suffix="$2" cmds="$3" vm p rc=0
  shift 3
  local pids=()
  mkdir -p "${dir}"
  for vm in "$@"; do
    ( nats_agent_on "${vm}" "${cmds}" > "${dir}/${vm}.${suffix}.txt" 2>&1 ) &
    pids+=("$!")
  done
  for p in ${pids[@]+"${pids[@]}"}; do wait "${p}" || rc=1; done
  return "${rc}"
}

# ------------------------------------------------------------------ cells
# Shape knobs; defaults match session A's Felix ingest cells.
: "${PUBS_PER_GEN:=16}"
: "${FAST_WINDOW:=64}"
: "${ASYNC_WINDOW:=64}"
: "${NATS_PROCS:=1}"
: "${NATS_MAX_PROCS:=4}"
: "${NATS_GEN_BUSY_MAX:=85}"
: "${NATS_SERVER_ENV:=}"
: "${NATS_EXTRA_CONF:=}"
: "${CELL_SECS:=90}"
: "${NATS_CALIB_SECS:=15}"
: "${NATS_CALIB_MSGS:=50000000}"
# A Felix in-memory stream keeps the newest 1024 records per shard
# (DEFAULT_LOG_CAPACITY in felix-broker's broker.rs); NATS memory streams keep
# the same total.
: "${NATS_MEM_RETAIN:=$(( 1024 * ${SHARDS:-12} ))}"
NGEN="${#LOADGEN_VMS[@]}"

# expected_mtu: the NIC MTU the Felix cells ran with. NATS_MTU wins;
# otherwise the one value the Felix cells and sysinfo recorded.
expected_mtu() {
  local vals
  if [ -n "${NATS_MTU:-}" ]; then printf '%s' "${NATS_MTU}"; return 0; fi
  # shellcheck disable=SC2046 # find prints paths without spaces
  vals="$(cat "${OUT}"/system/*.sysinfo.txt \
      $(find "${OUT}/cells" -name 'felixperf-broker-*.before.txt' ! -path '*/nats-*' ! -path '*/ab-nats-*' 2>/dev/null) \
      2>/dev/null | sed -n 's/^nic\.mtu=\([0-9][0-9]*\)$/\1/p' | sort -u)"
  case "$(printf '%s\n' "${vals}" | grep -c .)" in
    1) printf '%s' "${vals}" ;;
    0) echo "!! the Felix cells did not record nic.mtu; set NATS_MTU to the MTU they ran with" >&2; return 1 ;;
    *) echo "!! the Felix cells ran at MTUs $(echo "${vals}" | tr '\n' ' '); set NATS_MTU to the paired cells' value" >&2; return 1 ;;
  esac
}

# apply_mtu <mtu>: set it on the broker and every generator (a no-op when it
# is already there), then prove the path carries it from each generator.
apply_mtu() {
  local want="$1"
  log "NIC MTU ${want} on every VM"
  nats_par_on "${OUT}/system/nats" mtu "nats-agent mtu ${want}" "${NATS_VM}" "${LOADGEN_VMS[@]}" || {
    echo "!! setting MTU ${want} failed; see ${OUT}/system/nats/" >&2; return 1; }
  nats_par_on "${OUT}/system/nats" mtucheck "nats-agent mtu-check ${NATS_IP} ${want}" "${LOADGEN_VMS[@]}" || {
    echo "!! a generator's path to ${NATS_IP} does not carry ${want}; see ${OUT}/system/nats/" >&2; return 1; }
}

# shape_key <mode> <kind> <size> <pubs> <window> <flow> <streams> [env...]
shape_key() { printf '%s' "$*" | tr ' =' '__'; }

# nats_reset <dir> <mode> <kind> <streams> <env>: restart on an empty store
# with this cell's server environment and config, and create the streams.
nats_reset() {
  local dir="$1" mode="$2" kind="$3" streams="$4" senv="$5" streams_cmd env_lines
  streams_cmd="nats-agent streams ${streams}"
  [ "${mode}" = memory ] && streams_cmd="nats-agent streams ${streams} memory ${NATS_MEM_RETAIN}"
  [ "${kind}" = core ] && streams_cmd=":"
  # shellcheck disable=SC2086 # senv is a list of KEY=VALUE words
  env_lines="$(printf '%s\n' ${senv})"
  nats_agent_on "${NATS_VM}" "cat > /etc/nats/nats.env <<'ENV'
${env_lines}
ENV
cat > /etc/nats/extra.conf <<'CONF'
${NATS_EXTRA_CONF}
CONF
nats-agent reset ${mode}
${streams_cmd}" > "${dir}/${NATS_VM}.reset.txt" 2>&1
}

# calibrate <mode> <kind> <size> <pubs> <window> <flow> <streams> <procs> <env>:
# print messages per client that outlast a CELL_SECS cell: a NATS_CALIB_SECS
# run on every generator, its rate read from the server's counters, x1.5 over
# CELL_SECS + 5 s. Cached per shape, so trials share one calibration.
calibrate() {
  local mode="$1" kind="$2" size="$3" pubs="$4" window="$5" flow="$6" streams="$7" procs="$8" senv="$9"
  local key dir f start_at lg gi=0 pids=() p key_m before after n
  key="$(shape_key "$@")"
  dir="${OUT}/nats-calib/${key}"; f="${dir}/per_client"
  if [ -s "${f}" ]; then cat "${f}"; return 0; fi
  mkdir -p "${dir}"
  nats_reset "${dir}" "${mode}" "${kind}" "${streams}" "${senv}" || return 1
  key_m=m.append_records
  [ "${kind}" = core ] && key_m=m.publish_requests
  nats_agent_on "${NATS_VM}" "nats-agent snapshot" > "${dir}/before.txt" 2>&1 || return 1
  start_at=$(( $(date +%s) + START_DELAY_SECS ))
  for lg in "${LOADGEN_VMS[@]}"; do
    ( nats_agent_on "${lg}" "nats-agent bench 'calib-${key}' ${start_at} ${NATS_CALIB_SECS} ${kind} ${size} ${pubs} ${window} ${flow} ${streams} ${gi} ${NATS_CALIB_MSGS} ${procs}" \
        > "${dir}/${lg}.run.txt" 2>&1 ) &
    pids+=("$!"); gi=$((gi + 1))
  done
  for p in "${pids[@]}"; do wait "${p}" || true; done
  nats_agent_on "${NATS_VM}" "nats-agent snapshot" > "${dir}/after.txt" 2>&1 || return 1
  before="$(sed -n "s/^${key_m}=//p" "${dir}/before.txt" | head -1)"
  after="$(sed -n "s/^${key_m}=//p" "${dir}/after.txt" | head -1)"
  n="$(awk -v a="${before}" -v b="${after}" -v s="${NATS_CALIB_SECS}" -v c="$((NGEN * pubs))" -v cs="${CELL_SECS}" \
    'BEGIN { if (b == "" || a == "" || b <= a) exit 1; printf "%.0f", 1.5 * (b - a) / s / c * (cs + 5) }')" || {
    echo "!! calibration of ${key} saw no progress; see ${dir}" >&2; return 1; }
  # nats bench sizes some per-client latency arrays by the count; keep each
  # under 1 GiB of address space.
  if [ "${n}" -gt 120000000 ]; then
    echo "!! ${key}: calibrated ${n} msgs per client, capped at 120000000; the run may end early" >&2
    n=120000000
  fi
  echo "${n}" > "${f}"
  echo "${n}"
}

# nats_cell <name> <mode> <kind> <size> <pubs/gen> <window> <flow> <streams> [server env...]
# One measured cell, laid out like cells.sh's. CELL_PROCS (default NATS_PROCS)
# splits each generator's publishers over processes; a cell whose generators
# pass NATS_GEN_BUSY_MAX % CPU is run again with twice the processes, up to
# NATS_MAX_PROCS, and flagged if still saturated. A cell whose NIC MTU on any
# VM differs from the Felix cells' fails.
nats_cell() {
  local name mode="$2" kind="$3" size="$4" pubs="$5" window="$6" flow="$7" streams="$8"
  local procs="${CELL_PROCS:-${NATS_PROCS}}" raw="$1"
  name="$(tagged "$1")"
  shift 8
  local senv="${NATS_SERVER_ENV}${*:+ $*}" dir="${OUT}/cells/${name}" lg p pids=() rc=0 gi per_client
  if [ "${RESUME}" = 1 ] && [ -e "${dir}/done" ]; then log "skip ${name} (done)"; return 0; fi
  [ "${kind}" = js-fast ] && procs=1
  per_client="$(calibrate "${mode}" "${kind}" "${size}" "${pubs}" "${window}" "${flow}" "${streams}" "${procs}" "${senv}")" || {
    FAILED_CELLS="${FAILED_CELLS} ${name}"; return 0; }
  rm -rf "${dir}"; mkdir -p "${dir}"

  # The shape in felix-loadgen's flag names, so cells.csv lines NATS and Felix
  # rows up: batch is records per ack, in-flight what may be outstanding.
  local batch=1 inflight="${window}"
  case "${kind}" in
    js-fast) batch="${flow}"; inflight=$(( window * flow )) ;;
    js-sync) inflight=1 ;;
    core) inflight=0 ;;
  esac
  {
    echo "cell=${name}"
    echo "session=${SESSION}"
    echo "ref=nats-server-v${NATS_VERSION}"
    echo "loadgen_ref=natscli-v${NATS_CLI_VERSION}"
    echo "generators=${LOADGEN_VMS[*]}"
    echo "args=--scenario nats-${kind} --stream nats-${mode} --payload-bytes ${size} --batch ${batch} --in-flight ${inflight} --concurrency ${pubs} --keys ${streams} --duration-secs ${CELL_SECS}"
    echo "loadgen_env=NATS_PROCS=${procs} MSGS_PER_CLIENT=${per_client}"
    echo "started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "expected_mtu=${EXPECT_MTU}"
    echo "override.NATS_SYNC=${mode}"
    echo "override.NATS_KIND=${kind}"
    [ "${kind}" != js-fast ] || echo "override.NATS_FAST_FLOW=${flow}"
    [ "${mode}" != memory ] || echo "override.NATS_MEM_RETAIN=${NATS_MEM_RETAIN}"
    [ "${procs}" = 1 ] || echo "override.NATS_PROCS=${procs}"
    for kv in ${senv}; do echo "override.NATS_ENV_${kv}"; done
    printf '%s\n' "${NATS_EXTRA_CONF}" | sed '/^$/d; s/^/override.NATS_CONF=/'
  } > "${dir}/meta.env"
  log "cell ${name}: ${kind} ${mode} ${size} B, ${pubs}/gen x ${procs} procs, window ${window}, flow ${flow}, ${streams} streams, ${per_client} msgs/client${senv:+, ${senv}}"

  nats_reset "${dir}" "${mode}" "${kind}" "${streams}" "${senv}" || {
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
nats-agent bench '${name}' ${start_at} ${CELL_SECS} ${kind} ${size} ${pubs} ${window} ${flow} ${streams} ${gi} ${per_client} ${procs}" \
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

  local got bad="" mtu f busy
  got="$(grep -l '^NATS_BENCH_JSON' "${dir}"/*.run.txt 2>/dev/null | wc -l | tr -d ' ')"
  grep -qs '"ran_out":true' "${dir}"/*.run.txt && bad="${bad} count-ran-out"
  for f in "${dir}/${NATS_VM}.before.txt" "${dir}"/*.armed.txt; do
    mtu="$(sed -n 's/^host\.mtu=//p' "${f}" | head -1)"
    [ "${mtu}" = "${EXPECT_MTU}" ] || bad="${bad} mtu:$(basename "${f}" | cut -d. -f1)=${mtu:-?}"
  done
  busy="$(cat "${dir}"/*.after.txt 2>/dev/null | sed -n 's/^s\.cpu_busy=//p' | sort -n | tail -1)"
  echo "gen_cpu_busy_max=${busy:-}" >> "${dir}/meta.env"
  if [ -n "${busy}" ] && awk -v b="${busy}" -v m="${NATS_GEN_BUSY_MAX}" 'BEGIN { exit !(b > m) }'; then
    if [ "${kind}" != js-fast ] && [ "${procs}" -lt "${NATS_MAX_PROCS}" ]; then
      log "   a generator was ${busy}% busy; running ${name} again with $((procs * 2)) processes per generator"
      CELL_PROCS=$((procs * 2)) nats_cell "${raw}" "${mode}" "${kind}" "${size}" "${pubs}" "${window}" "${flow}" "${streams}" "$@"
      return 0
    fi
    echo "flag.gen_saturated=${busy}" >> "${dir}/meta.env"
    echo "!! ${name}: a generator was ${busy}% busy; the client may be the limit" >&2
  fi
  if [ "${rc}" = 0 ] && [ "${got}" = "${NGEN}" ] && [ -z "${bad}" ]; then
    touch "${dir}/done"
  else
    echo "fail=${bad# }" >> "${dir}/meta.env"
    echo "!! ${name} failed (${got}/${NGEN} results${bad:+;${bad}}); recorded and continuing" >&2
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
