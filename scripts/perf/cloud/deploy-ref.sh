#!/usr/bin/env bash
# Build any git ref on generator 0 and install it on every broker of a live
# session — no reprovision. With --activate, also switch the brokers to it
# and restart.
#
#   SESSION=<name> ./deploy-ref.sh <branch|tag|sha> [--fp] [--activate]
#
# --fp builds with frame pointers and line tables (label <name>-fp), which is
# what perf needs for stacks that resolve. The build takes generator 0's CPU
# for ~15-25 minutes, so it refuses to start while a felix-loadgen is running
# there (FORCE=1 overrides): a measurement taken beside a build is not one.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

ref="${1:?usage: deploy-ref.sh <ref> [--fp] [--activate]}"
shift
fp=0; activate=0
for a in "$@"; do
  case "${a}" in
    --fp) fp=1 ;;
    --activate) activate=1 ;;
    *) echo "!! unknown option ${a}" >&2; exit 2 ;;
  esac
done

spec="$(resolve_ref "${ref}")"
label="${spec%@*}"; [ "${fp}" = 1 ] && label="${label}-fp"
base="${ARTIFACT_BASE:-}"
if [ -z "${base}" ]; then
  : "${LOADGEN_PRIVATE_IP:?the inventory predates deploy-ref.sh; reprovision}"
  base="http://${LOADGEN_PRIVATE_IP}:8088"
fi
builder="${LOADGEN_VMS[0]}"

if [ "${FORCE:-0}" != 1 ] && run_on_str "${builder}" 'if pgrep -x felix-loadgen >/dev/null; then echo __BUSY_BEGIN__1__BUSY_END__; fi
echo __RUNOK__' | grep -q __BUSY_BEGIN__; then
  echo "!! felix-loadgen is running on ${builder}; wait for the run or set FORCE=1" >&2
  exit 1
fi

log "building ${label} (${spec#*@}) on ${builder}"
run_on_str "${builder}" "set -e
cat > /usr/local/sbin/felix-provision-loadgen.sh <<'FELIX_BUILDER_EOF'
$(cat "${here}/cloudinit/provision-loadgen.sh")
FELIX_BUILDER_EOF
chmod 0755 /usr/local/sbin/felix-provision-loadgen.sh
mkdir -p /srv/felix/.status
echo queued > /srv/felix/.status/${label}
nohup /usr/local/sbin/felix-provision-loadgen.sh build '${spec}' ${fp} > /var/log/felix-deploy-${label}.log 2>&1 &
echo __RUNOK__" >/dev/null

state=""
for _ in $(seq 1 240); do
  sleep 30
  state="$(run_on_str "${builder}" "printf '__ST_BEGIN__%s__ST_END__\n' \"\$(cat /srv/felix/.status/${label})\"
echo __RUNOK__" 2>/dev/null | extract_between ST || true)"
  case "${state}" in ok|failed) break ;; esac
done
if [ "${state}" != ok ]; then
  run_on_str "${builder}" "tail -30 /var/log/felix-build-${label}.log /var/log/felix-deploy-${label}.log; echo __RUNOK__" >&2 || true
  echo "!! build of ${label} ended as '${state:-timeout}'" >&2
  exit 1
fi

log "installing ${label} on ${#BROKER_VMS[@]} broker(s)"
par_on "${OUT}/system/deploy" "${label}" "felix-agent install-ref '${base}' '${label}'" "${BROKER_VMS[@]}"
grep -h '^installed\.' "${OUT}"/system/deploy/*."${label}".txt | sort -u | sed 's/^/   /'

inv="${here}/sessions/${SESSION}.env"
case " ${BROKER_LABELS:-} " in
  *" ${label} "*) ;;
  *)
    BROKER_LABELS="${BROKER_LABELS:+${BROKER_LABELS} }${label}"
    grep -v '^BROKER_LABELS=' "${inv}" > "${inv}.tmp"
    echo "BROKER_LABELS=\"${BROKER_LABELS}\"" >> "${inv}.tmp"
    mv "${inv}.tmp" "${inv}"
    ;;
esac

if [ "${activate}" = 1 ]; then
  "${here}/broker-env.sh" activate "${label}"
else
  echo ">> installed. Switch with: SESSION=${SESSION} ${here}/broker-env.sh activate ${label}"
fi
