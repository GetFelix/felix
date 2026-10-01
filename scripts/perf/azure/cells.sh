#!/usr/bin/env bash
# Shared by the session drivers (session-a/b/c.sh, sweep.sh): configure the
# brokers, run one measured cell, record everything a row needs to be
# attributable. Sourced, never run.
#
# A cell is one felix-loadgen invocation, on one or more generators at once,
# bracketed by broker snapshots. Its directory under $OUT/cells/<name>/ holds:
#   meta.env                 the cell's arguments, build label and overrides
#   <broker>.before.txt      counters + running binary sha/ref + full FELIX_* env
#   <broker>.after.txt       counters + 1 Hz sampler summary (CPU, append MB/s)
#   <broker>.series.tsv      the sampler's raw 1 Hz counters (bytes, UDP, CPU ticks)
#   <loadgen>.run.txt        the instrument's output (LOADGEN_JSON, then
#                            gen.start/gen.end wall clock around the run)
#   <loadgen>.after.txt      generator CPU summary
#   <broker>.profile.txt     with profiling on: per-thread CPU, perf heads
#   <broker>.folded.gz       with profiling on: collapsed stacks
# summarize.py turns the directories into cells.csv and summary.md.
#
# Knobs, all environment:
#   TRIALS=3                 trials per cell (suffix -t1..-tN)
#   RESUME=1                 skip cells whose directory is marked done
#   WIPE_DURABLE=1           wipe /data/felix and drop caches before durable cells
#   EXTRA_BROKER_ENV="K=V …" applied on top of every configuration
#   EXTRA_LOADGEN_ENV="K=V …" exported on every generator for every cell
#   RUN_TAG=<tag>            inserted into every cell name (before -tN), so a
#                            re-run with different EXTRA_* lands beside the first
#   PROFILE=1 | PROFILE_CELLS=<regex>  perf + pidstat + folded stacks per cell
#   PROFILE_SECS=40          how long a profile records
#   SETTLE_SECS=8            pause after brokers report ready
#   START_DELAY_SECS=30      ingest cells: every generator starts publishing this
#                            long after the cell launches, at the same instant

here="${here:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
# shellcheck source=lib.sh
source "${here}/lib.sh"

: "${SESSION:?SESSION=<name>}"
# shellcheck disable=SC1090
source "${here}/sessions/${SESSION}.env"
export GROUP

: "${TRIALS:=3}"
: "${RESUME:=1}"
: "${WIPE_DURABLE:=1}"
: "${EXTRA_BROKER_ENV:=}"
: "${EXTRA_LOADGEN_ENV:=}"
: "${PROFILE:=0}"
: "${PROFILE_CELLS:=}"
: "${PROFILE_SECS:=40}"
: "${SETTLE_SECS:=8}"
: "${START_DELAY_SECS:=30}"
: "${OUT:=${here}/sessions/${SESSION}-results}"
mkdir -p "${OUT}/cells" "${OUT}/system"

IFS=',' read -ra BROKER_IP_LIST <<<"${BROKER_IPS}"
BROKER_VMS=()
for i in "${!BROKER_IP_LIST[@]}"; do BROKER_VMS+=("$(broker_vm "${i}")"); done
read -ra LOADGEN_VMS <<<"${LOADGENS}"
# The dialled address only; the client learns the other listener ports from
# AuthOk and spreads its connections across them.
BROKER_ADDRS="$(printf '%s:5000,' "${BROKER_IP_LIST[@]}")"; BROKER_ADDRS="${BROKER_ADDRS%,}"
TOKEN_FILE=/home/felix/felix-session/token
FAILED_CELLS=""

CURRENT_REF=""
CURRENT_OVERRIDES=""
# What was last asked for, kept even when applying it failed, so a wipe can
# re-apply it.
CONFIG_LABEL=""
CONFIG_ARGS=()

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# configure <label> [KEY=VALUE...]: put every broker on build <label> with
# the base knobs plus these (plus EXTRA_BROKER_ENV), restart, and wait until
# each reports /ready. A no-op when nothing changed, unless wipe is set.
# Set CONFIG_WIPE=1 to also wipe storage (used by cells on durable streams).
configure() {
  local label="$1" desired wipe_cmd="" rc=0
  shift
  CONFIG_LABEL="${label}"; CONFIG_ARGS=("$@")
  # shellcheck disable=SC2086 # EXTRA_BROKER_ENV is a list of words
  desired="$(merge_env "$(base_overrides)" "$@" ${EXTRA_BROKER_ENV})"
  if [ "${label}" = "${CURRENT_REF}" ] && [ "${desired}" = "${CURRENT_OVERRIDES}" ] \
      && [ "${CONFIG_WIPE:-0}" != 1 ]; then
    return 0
  fi
  [ "${CONFIG_WIPE:-0}" = 1 ] && wipe_cmd="felix-agent wipe"
  log "configure ${label}: $(printf '%s' "${desired}" | tr '\n' ' ')${wipe_cmd:+ (wipe)}"
  par_on "${OUT}/system/configure" last "felix-agent activate broker '${label}'
felix-agent env-replace <<'OVR'
${desired}
OVR
${wipe_cmd}
felix-agent restart" "${BROKER_VMS[@]}" || rc=1
  if [ "${rc}" = 0 ] && wait_ready; then
    CURRENT_REF="${label}"; CURRENT_OVERRIDES="${desired}"
    sleep "${SETTLE_SECS}"
    return 0
  fi
  echo "!! configure ${label} failed; see ${OUT}/system/configure/" >&2
  CURRENT_REF=""; CURRENT_OVERRIDES=""
  return 1
}

# stage <label> [KEY=VALUE...]: record a configuration without applying it.
# The next cell applies it; a durable cell applies it together with its wipe,
# which saves the restart configure-then-wipe would cost.
stage() {
  CONFIG_LABEL="$1"; shift
  CONFIG_ARGS=("$@")
}

# wait_ready: every broker's /ready, polled from inside the VNet. Durable
# brokers stay unready until the control plane has seeded their streams.
wait_ready() {
  local out
  out="$(agent_on "${LOADGEN_VMS[0]}" "felix-agent ready-wait ${READY_TIMEOUT:-180} ${BROKER_IP_LIST[*]}" 2>&1)" || {
    printf '%s\n' "${out}" | grep -E 'ready=' >&2 || printf '%s\n' "${out}" | tail -5 >&2
    return 1
  }
}

# distribute_token: the seed leaves the client token on generator 0 only.
distribute_token() {
  local b64 lg
  [ "${#LOADGEN_VMS[@]}" -gt 1 ] || return 0
  b64="$(run_on_str "${LOADGEN_VMS[0]}" "base64 -w0 < ${TOKEN_FILE}; echo; echo __RUNOK__" 2>/dev/null \
    | grep -E '^[A-Za-z0-9+/=]{200,}$' | head -1 || true)"
  [ -n "${b64}" ] || { echo "!! could not read the client token from ${LOADGEN_VMS[0]}" >&2; return 1; }
  for lg in "${LOADGEN_VMS[@]:1}"; do
    run_on_str "${lg}" "mkdir -p /home/felix/felix-session
printf '%s' '${b64}' | base64 -d > ${TOKEN_FILE}
chown -R felix:felix /home/felix/felix-session
chmod 600 ${TOKEN_FILE}
test -s ${TOKEN_FILE}
echo __RUNOK__" >/dev/null || { echo "!! token copy to ${lg} failed" >&2; return 1; }
  done
}

# record_session <driver>: session.json plus per-VM system facts, RTT and
# builds. Run once at the start of a driver.
record_session() {
  local driver="$1" vm rtt_out
  log "recording system facts"
  par_on "${OUT}/system" sysinfo "felix-agent sysinfo" \
    "${BROKER_VMS[@]}" "${LOADGEN_VMS[@]}" "$(cp_vm)" || true
  rtt_out="$(run_on_str "${LOADGEN_VMS[0]}" "for ip in ${BROKER_IP_LIST[*]}; do
  rtt=\$(ping -qc 20 \"\$ip\" | awk -F/ 'END { print \$5 }')
  printf '__RTT_BEGIN__%s=%s__RTT_END__\n' \"\$ip\" \"\${rtt:-null}\"
done
echo __RUNOK__" 2>/dev/null || true)"
  printf '%s\n' "${rtt_out}" | sed -n 's/.*__RTT_BEGIN__\(.*\)__RTT_END__.*/\1/p' > "${OUT}/system/rtt.txt"
  if [ -n "${ARTIFACT_BASE:-}" ]; then
    run_on_str "${LOADGEN_VMS[0]}" 'cat /srv/felix/BUILD_STATUS; echo __RUNOK__' 2>/dev/null \
      | grep -E ' (ok|failed) ' > "${OUT}/system/builds.txt" || true
  fi
  SESSION_DRIVER="${driver}" OUT="${OUT}" python3 - <<'PY'
import json, os, pathlib, datetime
out = pathlib.Path(os.environ["OUT"])
keys = ["SESSION", "TIER", "LOCATION", "RELEASE_TAG", "RELEASE_URL", "LOADGEN_SPEC", "BROKER_SPECS",
        "FP_SPECS", "BROKER_LABELS", "ACTIVE_REF", "BROKER_VM_SIZE", "LOADGEN_VM_SIZE", "LOADGEN0_VM_SIZE",
        "USE_LOCAL_NVME", "BROKER_DATA_DISK_GIB", "BROKER_LISTENERS", "SHARDS", "REPLICATION_FACTOR",
        "LOADGENS", "BROKER_IPS"]
doc = {k.lower(): os.environ.get(k, "") for k in keys}
doc["driver"] = os.environ["SESSION_DRIVER"]
doc["started_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
rtt = {}
p = out / "system" / "rtt.txt"
if p.exists():
    for line in p.read_text().splitlines():
        ip, _, v = line.partition("=")
        rtt[ip] = None if v in ("", "null") else float(v)
doc["rtt_ms"] = rtt
systems = {}
for f in sorted((out / "system").glob("*.sysinfo.txt")):
    kv = {}
    for line in f.read_text().splitlines():
        if "=" in line and line.split("=", 1)[0].count(" ") == 0:
            k, v = line.split("=", 1)
            kv[k] = v
    systems[f.name.split(".")[0]] = kv
doc["systems"] = systems
b = out / "system" / "builds.txt"
doc["builds"] = b.read_text().split("\n")[:-1] if b.exists() else []
path = out / "session.json"
if path.exists():
    old = json.loads(path.read_text())
    runs = old.get("runs", [])
else:
    runs = []
runs.append({"driver": doc["driver"], "started_utc": doc["started_utc"]})
doc["runs"] = runs
path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
print(f">> {path}")
PY
}
export SESSION TIER LOCATION RELEASE_TAG RELEASE_URL LOADGEN_SPEC BROKER_SPECS FP_SPECS BROKER_LABELS \
  ACTIVE_REF BROKER_VM_SIZE LOADGEN_VM_SIZE LOADGEN0_VM_SIZE USE_LOCAL_NVME BROKER_DATA_DISK_GIB \
  BROKER_LISTENERS SHARDS REPLICATION_FACTOR LOADGENS BROKER_IPS

# fio_baseline <label> <bs> <jobs> [<label> <bs> <jobs>...]: device baseline on
# every broker's /data, 30 s each, with the broker idle.
fio_baseline() {
  local cmds=""
  if [ "${RESUME}" = 1 ] && grep -qs '^fio\.' "${OUT}"/system/*.fio.txt; then
    log "fio baseline already recorded"; return 0
  fi
  while [ "$#" -ge 3 ]; do
    cmds="${cmds}felix-agent fio '$1' '$2' '$3' ${FIO_SECS:-30}
"
    shift 3
  done
  log "fio baseline"
  par_on "${OUT}/system" fio "${cmds}" "${BROKER_VMS[@]}" || true
  grep -h '^fio\.' "${OUT}"/system/*.fio.txt 2>/dev/null | sed 's/^/   /' || true
}

# tagged <name>: the cell name with RUN_TAG inserted before any -tN suffix.
tagged() {
  local name="$1"
  [ -n "${RUN_TAG:-}" ] || { printf '%s' "${name}"; return; }
  if [[ "${name}" =~ ^(.*)(-t[0-9]+)$ ]]; then
    printf '%s-%s%s' "${BASH_REMATCH[1]}" "${RUN_TAG}" "${BASH_REMATCH[2]}"
  else
    printf '%s-%s' "${name}" "${RUN_TAG}"
  fi
}

# pending <name>: whether the cell, or any of its TRIALS trials, still has
# to run. Lets a resumed driver skip a whole block, restart included.
pending() {
  local t
  [ "${RESUME}" = 1 ] || return 0
  [ -e "${OUT}/cells/$(tagged "$1")/done" ] && return 1
  for t in $(seq 1 "${TRIALS}"); do
    [ -e "${OUT}/cells/$(tagged "$1-t${t}")/done" ] || return 0
  done
  return 1
}

want_profile() {
  [ "${PROFILE}" = 1 ] && return 0
  [ -n "${PROFILE_CELLS}" ] && [[ "$1" =~ ${PROFILE_CELLS} ]]
}

# cell <name> <generators> [loadgen flags...]: one measured run on the first
# <generators> generators at once, each running the same flags. Set
# CELL_LOADGEN_ENV for per-cell client knobs. The broker configuration is
# whatever `configure` last applied.
cell() {
  local name ngen="$2" dir lg vm pids=() rc=0 lg_env="" kv
  name="$(tagged "$1")"
  shift 2
  dir="${OUT}/cells/${name}"
  if [ "${RESUME}" = 1 ] && [ -e "${dir}/done" ]; then
    log "skip ${name} (done)"
    return 0
  fi
  if [ -n "${CONFIG_LABEL}" ] && ! configure "${CONFIG_LABEL}" ${CONFIG_ARGS[@]+"${CONFIG_ARGS[@]}"}; then
    FAILED_CELLS="${FAILED_CELLS} ${name}"
    return 0
  fi
  if [ "${ngen}" -gt "${#LOADGEN_VMS[@]}" ]; then
    echo "!! ${name} wants ${ngen} generators; the session has ${#LOADGEN_VMS[@]}" >&2
    ngen="${#LOADGEN_VMS[@]}"
  fi
  local gens=("${LOADGEN_VMS[@]:0:${ngen}}")
  # Launching generators through run-command staggers them by seconds, so an
  # ingest cell gives them one wall-clock start instead (#723).
  local start_at=""
  case " $* " in *" --scenario ingest "*) start_at="$(( $(date +%s) + START_DELAY_SECS ))" ;; esac
  rm -rf "${dir}"; mkdir -p "${dir}"
  # shellcheck disable=SC2086 # both are lists of words
  for kv in ${CELL_LOADGEN_ENV:-} ${EXTRA_LOADGEN_ENV}; do lg_env="${lg_env}export ${kv}
"; done
  {
    echo "cell=${name}"
    echo "session=${SESSION}"
    echo "ref=${CURRENT_REF}"
    echo "loadgen_ref=$(cat "${here}/sessions/${SESSION}.loadgen-ref" 2>/dev/null || echo "${LOADGEN_SPEC:-}")"
    echo "generators=${gens[*]}"
    echo "args=$*"
    echo "start_at=${start_at}"
    echo "loadgen_env=$(printf '%s' "${lg_env}" | sed 's/^export //' | tr '\n' ' ')"
    echo "started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '%s\n' "${CURRENT_OVERRIDES}" | sed 's/^/override./'
  } > "${dir}/meta.env"
  log "cell ${name} (${ngen} gen): $*"

  par_on "${dir}" before "felix-agent snapshot --env
felix-agent sampler-start '${name}'" "${BROKER_VMS[@]}" || rc=1
  par_on "${dir}" armed "felix-agent sampler-start '${name}'" "${gens[@]}" || true
  if want_profile "${name}"; then
    par_on "${dir}" profstart "felix-agent profile-start '${name}' ${PROFILE_SECS}" "${BROKER_VMS[@]}" || true
  fi

  # The instrument's stderr goes to a file on the VM, and only the tail of
  # stdout comes back: run-command keeps the last ~4 KB of output.
  for lg in "${gens[@]}"; do
    ( run_on_str "${lg}" "set -eu
export FELIX_MTU_UPPER_BOUND=4096
${lg_env}ulimit -n 1048576 || true
mkdir -p /var/tmp/felix-cells
o=/var/tmp/felix-cells/${name}.out; e=/var/tmp/felix-cells/${name}.err
s=\$(date +%s.%N)
felix-loadgen --brokers '${BROKER_ADDRS}' --tenant perf --token-file '${TOKEN_FILE}' --environment 'azure-${SESSION}-${name}' $* ${start_at:+--start-at ${start_at}} > \$o 2> \$e || { echo '!! case failed'; tail -15 \$e; exit 1; }
t=\$(date +%s.%N)
echo gen.launch=\$s
# The steady-state window starts when publishing does, not at the launch.
[ -z '${start_at}' ] || s=\$(awk -v s=\$s -v a='${start_at}' 'BEGIN { printf \"%.3f\\n\", (a > s) ? a : s }')
grep -v '^LOADGEN_JSON' \$o | tail -c 1200
grep '^LOADGEN_JSON' \$o | tail -1
echo gen.start=\$s
echo gen.end=\$t
echo __RUNOK__" > "${dir}/${lg}.run.txt" 2>&1 ) &
    pids+=("$!")
  done
  for p in ${pids[@]+"${pids[@]}"}; do wait "${p}" || rc=1; done

  par_on "${dir}" after "felix-agent sampler-stop '${name}'
felix-agent snapshot" "${BROKER_VMS[@]}" || rc=1
  par_on "${dir}" after "felix-agent sampler-stop '${name}'" "${gens[@]}" || true
  fetch_series "${dir}" "${name}"
  if want_profile "${name}"; then
    par_on "${dir}" profile "felix-agent profile-report '${name}'
felix-agent profile-collapse '${name}'" "${BROKER_VMS[@]}" || true
    for vm in "${BROKER_VMS[@]}"; do
      if fetch_file "${vm}" "/var/tmp/felix-perf/${name}.folded.gz.b64" "${dir}/${vm}.folded.gz.b64"; then
        base64 -d < "${dir}/${vm}.folded.gz.b64" > "${dir}/${vm}.folded.gz" 2>/dev/null \
          || base64 -D < "${dir}/${vm}.folded.gz.b64" > "${dir}/${vm}.folded.gz"
        rm -f "${dir}/${vm}.folded.gz.b64"
      fi
    done
  fi

  cell_line "${dir}"
  if [ "${rc}" = 0 ] && grep -q '^LOADGEN_JSON' "${dir}"/*.run.txt; then
    touch "${dir}/done"
  else
    echo "!! ${name} incomplete; recorded and continuing" >&2
    FAILED_CELLS="${FAILED_CELLS} ${name}"
  fi
  return 0
}

# fetch_series <dir> <tag>: each broker's raw sampler series into the cell
# dir. A cell without one still summarizes, from the snapshots alone.
fetch_series() {
  local dir="$1" tag="$2" vm pids=() p
  for vm in "${BROKER_VMS[@]}"; do
    (
      b64="${dir}/${vm}.series.gz.b64"
      if ! fetch_file "${vm}" "/var/tmp/felix-samples/${tag}.series.gz.b64" "${b64}" 2>/dev/null \
          || ! { base64 -d < "${b64}" 2>/dev/null || base64 -D < "${b64}"; } | gunzip > "${dir}/${vm}.series.tsv"; then
        echo "!! no series from ${vm}" >&2
        rm -f "${dir}/${vm}.series.tsv"
      fi
      rm -f "${b64}"
    ) &
    pids+=("$!")
  done
  for p in ${pids[@]+"${pids[@]}"}; do wait "${p}" || true; done
}

# One line per cell on the console: the steady-state rates from the broker
# series (summarize.py --cell), or, for a cell without a series, what the
# brokers stored against what the clients think they sent.
cell_line() {
  local dir="$1" broker client line
  if line="$(python3 "${here}/summarize.py" --cell "${dir}" 2>/dev/null)" && [ -n "${line}" ]; then
    log "   ${line}"
    return 0
  fi
  broker="$(grep -h '^s.append_mb_s=' "${dir}"/felixperf-broker-*.after.txt 2>/dev/null \
    | awk -F= '{ s += $2 } END { if (NR) printf "%.1f", s; else printf "-" }')"
  client="$(grep -ho '"throughput_mb_s":[0-9.]*' "${dir}"/*.run.txt 2>/dev/null \
    | awk -F: '{ s += $2 } END { if (NR) printf "%.1f", s; else printf "-" }')"
  log "   broker append ${broker} MB/s, client ${client} MB/s"
}

# check_listeners <n>: the brokers bound n client sockets. A flat listener
# sweep must be told apart from "the listeners were never there".
check_listeners() {
  local want="$1" vm got
  par_on "${OUT}/system" listeners "felix-agent snapshot --env" "${BROKER_VMS[@]}" || true
  for vm in "${BROKER_VMS[@]}"; do
    got="$(sed -n 's/^listen.ports=//p' "${OUT}/system/${vm}.listeners.txt" | head -1)"
    if [ "$(printf '%s' "${got}" | tr ',' '\n' | grep -c .)" -ne "${want}" ]; then
      echo "!! ${vm} bound [${got}], wanted ${want} listeners" >&2
      return 1
    fi
    log "   ${vm} listeners: ${got}"
  done
}

# trials <name> <generators> [flags...]: the cell TRIALS times.
trials() {
  local name="$1" t
  shift
  for t in $(seq 1 "${TRIALS}"); do cell "${name}-t${t}" "$@"; done
}

# durable_cell <name> <generators> [flags...]: a cell on a durable stream.
# With WIPE_DURABLE=1 it first wipes /data/felix and drops the page cache,
# restarting the brokers on the staged configuration.
durable_cell() {
  local name
  name="$(tagged "$1")"
  if [ "${RESUME}" = 1 ] && [ -e "${OUT}/cells/${name}/done" ]; then
    log "skip ${name} (done)"; return 0
  fi
  CONFIG_WIPE="${WIPE_DURABLE}" configure "${CONFIG_LABEL}" ${CONFIG_ARGS[@]+"${CONFIG_ARGS[@]}"} || {
    FAILED_CELLS="${FAILED_CELLS} ${name}"; return 0; }
  cell "$@"
}

durable_trials() {
  local name="$1" t
  shift
  for t in $(seq 1 "${TRIALS}"); do durable_cell "${name}-t${t}" "$@"; done
}

# ingest_flags <stream> <keys> <publishers-per-generator>: keyed ingest for
# CELL_SECS seconds, so every cell runs as long whatever its shape. PAYLOAD
# (4096) and BATCH (64) shape the records; IN_FLIGHT > 0 sends acked batches
# with that many outstanding per publisher, 0 is fire-and-forget. --keys
# spreads batches over routing keys; 0 would put every record on shard 0.
ingest_flags() {
  echo "--scenario ingest --stream $1 --payload-bytes ${PAYLOAD:-4096} --batch ${BATCH:-64} --concurrency $3 --duration-secs ${CELL_SECS:-90} --keys $2${IN_FLIGHT:+ --in-flight ${IN_FLIGHT}}"
}

# shape_pass <tag> <stream> <durable:0|1> <generators> <publishers-per-generator>:
# keyed ingest across record shapes, so the write-up does not lead with the
# best case. Payload x batch x in-flight, where in-flight 0 is fire-and-forget
# and above 0 is acked (the rate a client that waits for its acks gets).
shape_pass() {
  local tag="$1" stream="$2" durable="$3" ngen="$4" per="$5" run=cell p b f t
  [ "${durable}" = 1 ] && run=durable_cell
  for t in $(seq 1 "${SHAPE_TRIALS:-1}"); do
    for p in ${SHAPE_PAYLOADS:-256 1024 4096}; do
      for b in ${SHAPE_BATCHES:-1 64}; do
        for f in ${SHAPE_IN_FLIGHT:-0 64}; do
          # shellcheck disable=SC2046 # ingest_flags prints words
          "${run}" "${tag}-p${p}-b${b}-f${f}-t${t}" "${ngen}" \
            $(PAYLOAD="${p}" BATCH="${b}" IN_FLIGHT="${f}" ingest_flags "${stream}" "${SHARDS}" "${per}")
        done
      done
    done
  done
}

# finalize_features <feature>...: finalize fleet features through the control
# plane, from broker 0, which holds the admin token. ONE-WAY: run every cell
# that needs the old behaviour first.
finalize_features() {
  local f script="set -eu
u=\$(sed -n 's/^FELIX_CONTROLPLANE_URL=//p' /etc/felix/broker.env)
t=\$(cat /etc/felix/node.token)"
  for f in "$@"; do
    script="${script}
curl -fsS -X POST -H \"Authorization: Bearer \$t\" \"\$u/v1/fleet/features/${f}/finalize?dry_run=false\"; echo"
  done
  script="${script}
curl -fsS -H \"Authorization: Bearer \$t\" \"\$u/v1/fleet/features\"; echo
echo __RUNOK__"
  log "finalize fleet features: $*"
  run_on_str "${BROKER_VMS[0]}" "${script}" > "${OUT}/system/fleet-features.txt" 2>&1
}

# write_path_pass <tag> <stream> <durable:0|1>: the #375/#425 shapes against
# one stream. Latency is batch 1 with a per-message ack, on one generator
# (pubsub publishes through the first broker only). Throughput is batch 64,
# fire-and-forget, so on a durable stream the broker's append rate is the
# number and the client's is an enqueue rate. The keyed ingest splits
# INGEST_PUBS total publishers over INGEST_GENS generators.
write_path_pass() {
  local tag="$1" stream="$2" durable="$3" run=trials p n
  [ "${durable}" = 1 ] && run=durable_trials
  for p in 0 256 4096; do
    "${run}" "${tag}-lat-p${p}" 1 --scenario pubsub --stream "${stream}" --payload-bytes "${p}" \
      --fanout 1 --batch 1 --warmup 2000 --total 20000
  done
  for p in 256 4096; do
    "${run}" "${tag}-tp-p${p}" 1 --scenario pubsub --stream "${stream}" --payload-bytes "${p}" \
      --fanout 1 --batch 64 --binary --warmup 2000 --total 500000
  done
  for n in ${INGEST_PUBS:-12 24}; do
    # shellcheck disable=SC2046 # ingest_flags prints words
    "${run}" "${tag}-ingest-c${n}" "${INGEST_GENS:-2}" \
      $(ingest_flags "${stream}" "${SHARDS}" $(( n / ${INGEST_GENS:-2} )))
  done
}

finish() {
  python3 "${here}/summarize.py" "${OUT}" || true
  if [ -n "${FAILED_CELLS}" ]; then
    echo "!! incomplete cells:${FAILED_CELLS}"
    echo "   re-run the same command to retry just those (RESUME=1 skips finished cells)"
  fi
  echo ">> results: ${OUT}/summary.md, ${OUT}/cells.csv"
}
