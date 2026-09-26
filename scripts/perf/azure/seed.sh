#!/usr/bin/env bash
# Seed one perf session, orchestrated from the operator via `az vm run-command`
# (never SSH — see lib.sh). Three moves:
#
#   1. On the loadgen (inside the VNet, so it can reach the private control
#      plane): bootstrap the tenant with the real IdP, exchange the IdP token
#      for a Felix operator token, create the namespace/streams/cache. This is
#      the deployment's own flow, measured — not demo auth. seed-remote.sh is
#      the body; it frames the felix token on stdout.
#   2. On each broker: drop that token as /etc/felix/node.token and start the
#      broker. run-command reaches each broker directly, so no loadgen->broker
#      SSH (which had no key and rode the same DPI-blocked path) is needed.
#   3. Poll the control plane (from the loadgen) until every broker registers.
set -euo pipefail

: "${CONTROLPLANE_IP:?}"
: "${BROKER_COUNT:?}"
: "${BOOTSTRAP_TOKEN:?}"
: "${IDP_JWKS_URL:?set IDP_JWKS_URL}"
# Not required: the audience the tenant is registered with is derived from
# IDP_TOKEN itself. If it is set it is only used to flag a mismatch.
: "${IDP_TOKEN:?set IDP_TOKEN (a token from the IdP for the perf principal)}"
: "${GROUP:?}"

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"

TENANT="perf"
NAMESPACE="default"

# --- 0. install source builds (only when the session builds its own refs) ---
# Generator 0 built them during provisioning and serves them on :8088. The
# control plane runs one build, ACTIVE_REF unless CONTROLPLANE_REF says
# otherwise; it has to be up before the bootstrap below.
if [ -n "${ARTIFACT_BASE:-}" ]; then
  cp_ref="${CONTROLPLANE_REF:-${ACTIVE_REF}}"
  echo ">> control plane: installing ${cp_ref} from ${ARTIFACT_BASE}"
  agent_on "$(cp_vm)" "felix-agent install-ref '${ARTIFACT_BASE}' '${cp_ref}'
felix-agent activate controlplane '${cp_ref}'
/usr/local/sbin/felix-controlplane-env.sh
systemctl daemon-reload
systemctl enable felix-controlplane
systemctl restart felix-controlplane
i=0
until curl -s -o /dev/null http://${CONTROLPLANE_IP}:8080/; do
  i=\$((i + 1)); [ \$i -lt 60 ] || { journalctl -u felix-controlplane --no-pager | tail -20; exit 1; }
  sleep 2
done" | grep -E '^(installed|active)\.|!!' || { echo "!! control plane install failed" >&2; exit 1; }
fi

# --- 1. bootstrap + exchange + scopes, on the loadgen -----------------------
# A dash header assigns the values seed-remote.sh reads; the tokens are hex/
# base64url with no single quotes, so single-quoting them is safe. Everything
# rides run-command's HTTPS to Azure, never the terminal-visible network.
header="$(cat <<HDR
set -eu
CP='http://${CONTROLPLANE_IP}:8080'
BOOTSTRAP='http://${CONTROLPLANE_IP}:8081'
BOOTSTRAP_TOKEN='${BOOTSTRAP_TOKEN}'
IDP_TOKEN='${IDP_TOKEN}'
IDP_JWKS_URL='${IDP_JWKS_URL}'
IDP_AUDIENCE='${IDP_AUDIENCE:-}'
TENANT='${TENANT}'
NAMESPACE='${NAMESPACE}'
REPLICATION_FACTOR='${REPLICATION_FACTOR:-1}'
SHARDS='${SHARDS:-12}'
SEED_STREAMS=0
HDR
)"
# Capture without set -e aborting the assignment, so the remote message is
# always printed — a swallowed run-command message is a blind failure.
set +e
seed_out="$(run_on_str "$(loadgen_vm)" "${header}
$(cat "${here}/seed-remote.sh")")"
seed_rc=$?
set -e
printf '%s\n' "${seed_out}"
[ ${seed_rc} -eq 0 ] || { echo "!! loadgen seed step failed (see message above)" >&2; exit 1; }
token="$(printf '%s' "${seed_out}" | extract_between FTOKEN)"
[ -n "${token}" ] || { echo "!! seed did not return a felix token" >&2; exit 1; }

# --- 2. drop the node token on each broker and start it ---------------------
echo ">> node token to each broker, then start"
for i in $(seq 0 $((BROKER_COUNT - 1))); do
  # felix-broker-env.sh regenerates /etc/felix/broker.env before start (the
  # unit's ExecStartPre does too, but a required EnvironmentFile must already
  # exist when the unit activates, so we write it here first). reset-failed
  # clears any earlier give-up. `restart` (not just enable --now) is deliberate:
  # a re-seed drops a *fresh* token, and the broker reads its token once at
  # startup with no refresh, so an already-running broker must be restarted to
  # pick it up — enable --now is a no-op on an active unit.
  # Source builds are installed side by side and ACTIVE_REF is linked in, so a
  # later switch is a relink and a restart. The calibrated knobs go into
  # overrides.env before the first start, so no broker ever runs uncalibrated.
  install_cmds=""
  if [ -n "${ARTIFACT_BASE:-}" ]; then
    for label in ${BROKER_LABELS}; do
      install_cmds="${install_cmds}felix-agent install-ref '${ARTIFACT_BASE}' '${label}'
"
    done
    install_cmds="${install_cmds}felix-agent activate broker '${ACTIVE_REF}'
"
  fi
  broker_out="$(agent_on "$(broker_vm "${i}")" "mkdir -p /etc/felix
printf '%s' '${token}' > /etc/felix/node.token
chmod 600 /etc/felix/node.token
${install_cmds}felix-agent env-replace <<'OVR'
$(base_overrides)
OVR
felix-agent counters-install
/usr/local/sbin/felix-broker-env.sh
systemctl reset-failed felix-broker 2>/dev/null || true
systemctl enable felix-broker
systemctl restart felix-broker")" || {
    printf '%s\n' "${broker_out}" >&2
    echo "!! broker-${i} did not start (see message above)" >&2
    exit 1
  }
  echo "   broker-${i}: started"
done

# --- 3. wait for the brokers to register ------------------------------------
echo ">> waiting for the brokers to register and take their shards"
# The *admin* token, not the load generator's. The client-scoped token in
# felix-session/token deliberately carries no `node.view:cluster:*` -- that is
# #513's scoping working -- so polling /v1/nodes with it answers 403 forever,
# `|| echo 0` swallows it, and the loop reports "0/N registered" after burning
# its full five minutes. That false zero was mistaken for a flake on every
# session since #513; it is deterministic.
poll_out="$(run_on_str "$(loadgen_vm)" "set -eu
TOKEN=\$(curl -fsS -X POST 'http://${CONTROLPLANE_IP}:8080/v1/tenants/${TENANT}/token/exchange' \
  -H 'Authorization: Bearer ${IDP_TOKEN}' -H 'Content-Type: application/json' -d '{\"audience\":\"felix-controlplane\"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"felix_token\"])')
n=0
i=0
while [ \$i -lt 60 ]; do
  n=\$(curl -fsS 'http://${CONTROLPLANE_IP}:8080/v1/nodes' -H \"Authorization: Bearer \$TOKEN\" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get(\"items\", d.get(\"nodes\", []))))' 2>/dev/null || echo 0)
  [ \"\$n\" -ge ${BROKER_COUNT} ] && break
  i=\$((i + 1)); sleep 5
done
printf '__NODES_BEGIN__%s__NODES_END__\n' \"\$n\"
echo __RUNOK__")"
registered="$(printf '%s' "${poll_out}" | extract_between NODES)"
echo ">> ${registered:-0}/${BROKER_COUNT} brokers registered"
[ "${registered:-0}" -ge "${BROKER_COUNT}" ] || {
  echo "!! not all brokers registered; inspect with: az vm run-command invoke -g ${GROUP} -n $(broker_vm 0) --command-id RunShellScript --scripts 'journalctl -u felix-broker --no-pager | tail -50'" >&2
  exit 1
}
# --- 4. create the streams, now that every broker is registered -------------
#
# Placement runs once, when a stream is created, against the brokers the control
# plane can see at that instant -- and nothing moves a shard afterwards (#130).
# Creating them before the cluster is up hands every shard to whichever broker
# registered first, and the rest of the session measures one broker with spare
# machines attached. A two-broker session measured exactly that: 49/0.
#
# The remote script is idempotent, so this second pass re-bootstraps (409),
# re-exchanges, and this time creates the streams.
echo ">> creating streams now that ${BROKER_COUNT}/${BROKER_COUNT} brokers are registered"
stream_header="$(cat <<HDR2
CP='http://${CONTROLPLANE_IP}:8080'
BOOTSTRAP='http://${CONTROLPLANE_IP}:8081'
BOOTSTRAP_TOKEN='${BOOTSTRAP_TOKEN}'
IDP_TOKEN='${IDP_TOKEN}'
IDP_JWKS_URL='${IDP_JWKS_URL}'
IDP_AUDIENCE='${IDP_AUDIENCE:-}'
TENANT='${TENANT}'
NAMESPACE='${NAMESPACE}'
REPLICATION_FACTOR='${REPLICATION_FACTOR:-1}'
SHARDS='${SHARDS:-12}'
SEED_STREAMS=1
HDR2
)"
set +e
stream_out="$(run_on_str "$(loadgen_vm)" "${stream_header}
$(cat "${here}/seed-remote.sh")")"
set -e
printf '%s\n' "${stream_out}" | grep -vE '^__FTOKEN|^eyJ' || true
case "${stream_out}" in
  *__RUNOK__*) ;;
  *) echo "!! stream creation did not complete" >&2; exit 1 ;;
esac

# The distribution is the point of the reordering, so report it rather than
# assume it: an uneven split here means placement still has a problem.
echo ">> shard ownership"
run_on_str "$(loadgen_vm)" "ADMIN=\$(curl -s -X POST '${CP_URL:-http://${CONTROLPLANE_IP}:8080}/v1/tenants/${TENANT}/token/exchange' -H 'Authorization: Bearer ${IDP_TOKEN}' -H 'Content-Type: application/json' -d '{\"audience\":\"felix-controlplane\"}' | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"felix_token\"])')
curl -s '${CP_URL:-http://${CONTROLPLANE_IP}:8080}/v1/shard-assignments/changes?since=0' -H \"Authorization: Bearer \$ADMIN\" | python3 -c '
import json,sys,collections
items=json.load(sys.stdin)[\"items\"]
cur={}
for it in items:
    k=it[\"key\"]; kk=(k[\"stream\"],k[\"shard\"],k[\"kind\"])
    if it[\"op\"]==\"assigned\": cur[kk]=it[\"assignment\"][\"leader\"]
    else: cur.pop(kk,None)
c=collections.Counter(cur.values())
print(\"   per broker:\", dict(c))'
echo __RUNOK__" 2>/dev/null | grep -E "per broker" | tee -a "${ASSIGNMENTS_OUT:-/dev/null}" \
  || echo "   (could not read assignments)"

echo ">> seeded: tenant ${TENANT}, token on the loadgen at ~/felix-session/token"
