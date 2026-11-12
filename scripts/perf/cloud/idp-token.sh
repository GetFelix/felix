#!/usr/bin/env bash
# Mint a fresh IdP access token via the client-credentials grant, and print it
# to stdout. Nothing else in the suite handles a raw JWT: the operator sets
# three secrets, this turns them into the token the exchange verifies, and it
# can be re-run whenever a token expires (Entra tokens last ~60-90 min, so a
# long session refreshes rather than pastes).
#
# App-only tokens carry the app's service-principal identity as `sub`/`oid`,
# which is exactly the stable principal Felix authorizes — no user, no
# interactive login, no MFA in the path.
set -euo pipefail

: "${IDP_TENANT_ID:?set IDP_TENANT_ID (the Entra directory/tenant GUID)}"
: "${IDP_CLIENT_ID:?set IDP_CLIENT_ID (the felix-perf app registration's client ID)}"
: "${IDP_CLIENT_SECRET:?set IDP_CLIENT_SECRET (a client secret on that app)}"
# What to request a token FOR. `IDP_AUDIENCE` is the old name and still works.
: "${IDP_SCOPE:=${IDP_AUDIENCE:-}}"
: "${IDP_SCOPE:?set IDP_SCOPE (the app's Application ID URI, api://<client-id>, or the bare client id -- both work)}"

# The v2 client-credentials grant.
#
# This scope decides what is requested; it does NOT decide the `aud` the token
# comes back with, and the two differ in practice. An app-only credential can
# issue a **v1** token even through the v2 endpoint, and v1 carries
# `aud=api://<client-id>` where v2 carries the bare client-id GUID. Registering
# the tenant against the requested value rather than the received one is how
# every exchange ends up answering `401 invalid token`, so `seed-remote.sh`
# derives the audience from the minted token -- as it already does the issuer,
# for the same reason.
token=$(curl -fsS -X POST \
  "https://login.microsoftonline.com/${IDP_TENANT_ID}/oauth2/v2.0/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=${IDP_CLIENT_ID}" \
  --data-urlencode "client_secret=${IDP_CLIENT_SECRET}" \
  --data-urlencode "scope=${IDP_SCOPE}/.default" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')

[ -n "${token}" ] || { echo "no access_token in the IdP response" >&2; exit 1; }
printf '%s' "${token}"
