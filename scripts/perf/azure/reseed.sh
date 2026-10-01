#!/usr/bin/env bash
# Seed a running session again: a new tenant, streams, caches and tokens, on
# the VMs and builds it already has. The session control plane keeps its state
# in memory, so a restart of it (an OS update, a reboot) leaves every token in
# the session invalid and the brokers never ready. Usage: SESSION=<name> ./reseed.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
: "${SESSION:?SESSION=<name>}"
session="${SESSION}"
set -a
# shellcheck disable=SC1091
source "${here}/session.env"
# session.env may name another session.
SESSION="${session}"
# shellcheck disable=SC1090
source "${here}/sessions/${SESSION}.env"
set +a
# The same IdP derivation session.sh does.
if [ -z "${IDP_TOKEN:-}" ] && [ -n "${IDP_TENANT_ID:-}" ]; then
  export IDP_SCOPE="${IDP_SCOPE:-${IDP_AUDIENCE:-}}"
  : "${IDP_SCOPE:?set IDP_SCOPE (the app Application ID URI, or the bare client id)}"
  export IDP_JWKS_URL="${IDP_JWKS_URL:-https://login.microsoftonline.com/${IDP_TENANT_ID}/discovery/v2.0/keys}"
  IDP_TOKEN="$("${here}/idp-token.sh")"
  export IDP_TOKEN
  echo ">> minted an IdP token for scope ${IDP_SCOPE}"
fi
IFS=',' read -ra broker_ip_list <<<"${BROKER_IPS}"
CONTROLPLANE_IP="${CONTROLPLANE_IP}" \
BROKER_COUNT="${#broker_ip_list[@]}" \
BOOTSTRAP_TOKEN="${BOOTSTRAP_TOKEN}" \
IDP_JWKS_URL="${IDP_JWKS_URL:-}" \
IDP_AUDIENCE="${IDP_AUDIENCE:-}" \
IDP_TOKEN="${IDP_TOKEN:-}" \
GROUP="${GROUP}" \
ARTIFACT_BASE="${ARTIFACT_BASE}" \
BROKER_LABELS="${BROKER_LABELS}" \
ACTIVE_REF="${ACTIVE_REF}" \
BROKER_LISTENERS="${BROKER_LISTENERS:-1}" \
ASSIGNMENTS_OUT="${here}/sessions/${SESSION}-results/system/assignments.txt" \
  bash "${here}/seed.sh"
