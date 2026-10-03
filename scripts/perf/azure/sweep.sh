#!/usr/bin/env bash
# A knob sweep in one line: every combination of the given values, TRIALS
# times each, as ordinary cells (same snapshots, same summary).
#
#   SESSION=<name> ./sweep.sh [--name <tag>] [--ref <label>] [--gens <n>] [--durable] [--profile] \
#     KNOB=v1,v2,... [client:KNOB=v1,...] ... -- <felix-loadgen flags>
#
# A FELIX_* knob goes to the brokers (on top of the calibrated base); a
# client: knob is exported on the generators. --durable wipes storage before
# each cell. Example:
#
#   SESSION=v060-a ./sweep.sh --name flushconc --durable \
#     FELIX_BROKER_PUB_FLUSH_CONCURRENCY=16,32,64 client:FELIX_PUB_CONN_POOL=4 -- \
#     --scenario ingest --stream perf-durable --payload-bytes 4096 --batch 64 \
#     --concurrency 16 --total 3200000 --keys 48
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

tag=sweep; ref="${ACTIVE_REF:-release}"; sweep_gens="${#LOADGEN_VMS[@]}"; durable=0
knobs=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --name) tag="$2"; shift 2 ;;
    --ref) ref="$2"; shift 2 ;;
    --gens) sweep_gens="$2"; shift 2 ;;
    --durable) durable=1; shift ;;
    --profile) PROFILE=1; shift ;;
    --) shift; break ;;
    *=*) knobs+=("$1"); shift ;;
    *) echo "!! unexpected ${1}; knobs are KEY=v1,v2 and loadgen flags follow --" >&2; exit 2 ;;
  esac
done
[ "$#" -gt 0 ] || { echo "!! no loadgen flags after --" >&2; exit 2; }
flags=("$@")

record_session "sweep ${tag}"
refresh_client_token || exit 1

# run_combo <index> <broker-kv...> ; client knobs accumulate in combo_client.
combo_client=""
run_combo() {
  local idx="$1" kv key values v short
  shift
  if [ "${idx}" -ge "${#knobs[@]}" ]; then
    local name="${tag}" b
    for b in "$@" ${combo_client}; do
      short="${b#FELIX_}"; short="${short#client:}"; short="${short#FELIX_}"
      name="${name}-${short}"
    done
    stage "${ref}" "$@"
    local t
    for t in $(seq 1 "${TRIALS}"); do
      if [ "${durable}" = 1 ]; then
        CELL_LOADGEN_ENV="${combo_client//client:/}" durable_cell "${name}-t${t}" "${sweep_gens}" "${flags[@]}"
      else
        CELL_LOADGEN_ENV="${combo_client//client:/}" cell "${name}-t${t}" "${sweep_gens}" "${flags[@]}"
      fi
    done
    return 0
  fi
  kv="${knobs[${idx}]}"; key="${kv%%=*}"; values="${kv#*=}"
  local saved="${combo_client}"
  for v in ${values//,/ }; do
    if [[ "${key}" == client:* ]]; then
      combo_client="${saved:+${saved} }${key}=${v}"
      run_combo $((idx + 1)) "$@"
    else
      run_combo $((idx + 1)) "$@" "${key}=${v}"
    fi
  done
  combo_client="${saved}"
}
run_combo 0

finish
