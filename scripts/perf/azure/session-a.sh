#!/usr/bin/env bash
# ingest_flags prints loadgen flags as words, split on purpose.
# shellcheck disable=SC2046
# Session A: one L8as_v4 broker on local NVMe (RAID0), four D4as_v5
# generators. Runs unattended; re-running resumes (finished cells are kept).
#
#   #557/#559  FELIX_QUIC_LISTENERS in {1,2,4,8} (and IO threads 6 at N=4) on
#              the in-memory and the durable OnCommit stream, keyed ingest.
#   #547       flush dispatch on NVMe: A0 = OLD_REF (spawn_blocking), A1 =
#              NEW_REF (flush thread), A2 = NEW_REF + io_uring; keys 1/12/48
#              plus one c1 latency cell per arm.
#   shapes     payload 256/1024/4096 x batch 1/64 x fire-and-forget/acked
#              (64 in flight), in-memory and durable OnCommit, at ARM_LISTENERS.
#   profiles   perf + pidstat + folded stacks at N=1 and N=4 on the
#              frame-pointer build, when one was built (FP_REFS=main).
#
# Provision with (see README):
#   SESSION=v060-a LOCATION=eastus2 BROKER_COUNT=1 BROKER_VM_SIZE=Standard_L8as_v4 \
#   USE_LOCAL_NVME=true LOADGEN_COUNT=4 LOADGEN0_VM_SIZE=Standard_D8as_v5 SHARDS=48 \
#   BROKER_REFS="main 8f1736eb" FP_REFS=main ./session.sh
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

: "${NEW_REF:=main}"
: "${OLD_REF:=8f1736eb}"
: "${FP_REF:=${NEW_REF}-fp}"
: "${PUBS_PER_GEN:=16}"
: "${LISTENER_SWEEP:=1 2 4 8}"
: "${SWEEP_KEYS:=12}"
: "${ARM_KEYS:=1 12 48}"
: "${ARM_LISTENERS:=4}"
# Which parts to run, in order. Re-run a subset with e.g. STEPS="arms".
: "${STEPS:=fio smoke sweep shapes profile arms}"

[ "${SHARDS}" -ge 48 ] || echo "!! the stream has ${SHARDS} shards; ARM_KEYS up to 48 wants SHARDS=48" >&2
record_session session-a
distribute_token || exit 1
NGEN="${#LOADGEN_VMS[@]}"

# Every sweep point uses the same client shape; only the pool size follows N
# so one client has a connection to every port.
sweep_env() { echo "FELIX_PUB_CONN_POOL=$(( $1 > 4 ? $1 : 4 ))"; }

for step in ${STEPS}; do
  case "${step}" in
  fio)
    fio_baseline seq256k-j1 256k 1 seq256k-j4 256k 4 seq256k-j8 256k 8 seq4k-j1 4k 1
    ;;

  smoke)
    # The first run of the source-build path: prove the listeners are bound
    # and that clients reach all of them before spending hours on a sweep.
    pending smoke-l4 || continue
    configure "${NEW_REF}" FELIX_QUIC_LISTENERS=4 FELIX_PUB_INGRESS_WAIT=1 || exit 1
    check_listeners 4 || exit 1
   
    CELL_LOADGEN_ENV="$(sweep_env 4)" cell smoke-l4 "${NGEN}" $(CELL_SECS=30 ingest_flags perf "${SWEEP_KEYS}" "${PUBS_PER_GEN}")
    grep -h '' "${OUT}/cells/smoke-l4/"*.after.txt | grep -E '^port\.50' | sed 's/^/   /'
    ;;

  sweep)
    for n in ${LISTENER_SWEEP}; do
      threads_set="0"
      [ "${n}" = 4 ] && threads_set="0 6"
      for th in ${threads_set}; do
        tag="l557-l${n}-io${th}"
        pending "${tag}-inmem" || pending "${tag}-dur" || continue
        configure "${NEW_REF}" FELIX_QUIC_LISTENERS="${n}" FELIX_IO_RUNTIME_THREADS="${th}" \
          FELIX_PUB_INGRESS_WAIT=1 FELIX_DURABLE_FSYNC_MODE=on_commit || continue
        check_listeners "${n}" || continue
        export CELL_LOADGEN_ENV; CELL_LOADGEN_ENV="$(sweep_env "${n}")"
       
        trials "${tag}-inmem" "${NGEN}" $(ingest_flags perf "${SWEEP_KEYS}" "${PUBS_PER_GEN}")
       
        durable_trials "${tag}-dur" "${NGEN}" $(ingest_flags perf-durable "${SWEEP_KEYS}" "${PUBS_PER_GEN}")
        unset CELL_LOADGEN_ENV
      done
    done
    ;;

  shapes)
    configure "${NEW_REF}" FELIX_QUIC_LISTENERS="${ARM_LISTENERS}" FELIX_PUB_INGRESS_WAIT=1 \
      FELIX_DURABLE_FSYNC_MODE=on_commit || continue
    export CELL_LOADGEN_ENV; CELL_LOADGEN_ENV="$(sweep_env "${ARM_LISTENERS}")"
    shape_pass a-shape-inmem perf 0 "${NGEN}" "${PUBS_PER_GEN}"
    shape_pass a-shape-dur perf-durable 1 "${NGEN}" "${PUBS_PER_GEN}"
    unset CELL_LOADGEN_ENV
    ;;

  profile)
    case " ${BROKER_LABELS} " in
      *" ${FP_REF} "*) ;;
      *) echo "!! ${FP_REF} is not installed (FP_REFS at provision, or deploy-ref.sh --fp); skipping profiles" >&2; continue ;;
    esac
    for n in 1 4; do
      pending "prof-l${n}-inmem" || continue
      configure "${FP_REF}" FELIX_QUIC_LISTENERS="${n}" FELIX_PUB_INGRESS_WAIT=1 || continue
     
      CELL_LOADGEN_ENV="$(sweep_env "${n}")" PROFILE=1 \
        cell "prof-l${n}-inmem" "${NGEN}" $(ingest_flags perf "${SWEEP_KEYS}" "${PUBS_PER_GEN}")
    done
    ;;

  arms)
    # Interleaved by trial, so drift over the session lands on every arm alike.
    export CELL_LOADGEN_ENV; CELL_LOADGEN_ENV="$(sweep_env "${ARM_LISTENERS}")"
    for t in $(seq 1 "${TRIALS}"); do
      for arm in A0 A1 A2; do
        case "${arm}" in
          A0) ref="${OLD_REF}"; uring=0 ;;
          A1) ref="${NEW_REF}"; uring=0 ;;
          A2) ref="${NEW_REF}"; uring=1 ;;
        esac
        stage "${ref}" FELIX_QUIC_LISTENERS="${ARM_LISTENERS}" FELIX_STORAGE_IO_URING="${uring}" \
          FELIX_DURABLE_FSYNC_MODE=on_commit FELIX_PUB_INGRESS_WAIT=1
        for k in ${ARM_KEYS}; do
         
          durable_cell "a547-${arm}-k${k}-t${t}" "${NGEN}" $(ingest_flags perf-durable "${k}" "${PUBS_PER_GEN}")
        done
        # One publisher, one in flight: p50 against fio's single-job flush is
        # the "<= 1.5x one device flush" budget.
        durable_cell "a547-${arm}-lat-t${t}" 1 --scenario pubsub --stream perf-durable --payload-bytes 256 \
          --fanout 1 --batch 1 --warmup 2000 --total 20000
      done
    done
    # Exploratory: deeper flush coalescing on a fast device, one trial.
    stage "${NEW_REF}" FELIX_QUIC_LISTENERS="${ARM_LISTENERS}" FELIX_STORAGE_IO_URING=0 \
      FELIX_DURABLE_FSYNC_MODE=on_commit FELIX_PUB_INGRESS_WAIT=1 FELIX_BROKER_PUB_FLUSH_CONCURRENCY=64
    durable_cell "a547-A1-flush64-k48-t1" "${NGEN}" $(ingest_flags perf-durable 48 "${PUBS_PER_GEN}")
    unset CELL_LOADGEN_ENV
    ;;

  *) echo "!! unknown step ${step}" >&2 ;;
  esac
done

finish
