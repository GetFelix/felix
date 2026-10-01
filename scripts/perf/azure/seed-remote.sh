# POSIX sh body, run ON the load generator under dash as root via run-command.
# seed.sh prepends a header assigning CP / BOOTSTRAP / BOOTSTRAP_TOKEN /
# IDP_TOKEN / IDP_JWKS_URL / IDP_AUDIENCE / TENANT / NAMESPACE /
# REPLICATION_FACTOR / BROKER_COUNT, then concatenates this file. Keep it
# dash-clean: no arrays, no pipefail, no bashisms.
#
# The flow is the deployment's own, not a harness shortcut:
#   1. bootstrap-initialize the tenant with the real IdP's issuer + JWKS
#   2. exchange the IdP token for the Felix operator token
#   3. create the namespace, streams, and cache scope the matrix uses
# The client token and its refresh token go to ~felix/felix-session for the
# generators. With BROKER_COUNT > 0 each broker also gets its own exchange,
# framed on stdout for seed.sh to drop as that broker's node credential.
#
# Every step is idempotent (409 "already exists" is tolerated) so a re-run
# after a partial seed converges instead of wedging, and every non-2xx prints
# its status and body so a failure is legible in the run-command message rather
# than a bare stall.

RESP=/tmp/felix-seed-resp

# field <name>: one string field of the last JSON response.
field() {
  python3 -c 'import json,sys; print(json.load(open("/tmp/felix-seed-resp"))[sys.argv[1]])' "$1"
}

# post_ok <url> [curl args...] — POST, reading any JSON body from stdin via
# `-d @-`. Succeeds on 2xx and on 409 (already exists); prints status+body and
# fails otherwise.
post_ok() {
  _url=$1
  shift
  _code=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$_url" "$@" -d @-)
  case "$_code" in
    2*) return 0 ;;
    409)
      echo "   (already exists: HTTP 409, continuing)"
      # Bootstrap-initialize is exactly-once: a 409 means the tenant keeps the
      # issuer and audience the FIRST run registered, and this one's are
      # discarded. Silently reusing a wrong audience is a 401 on every exchange
      # afterwards with nothing pointing here, so say it out loud.
      case "$_url" in
        */initialize)
          echo "   !! the tenant's registered issuer/audience are unchanged." >&2
          echo "   !! if they are wrong, clear the control plane's store and re-seed." >&2
          ;;
      esac
      ;;
    *) echo "!! POST $_url -> HTTP $_code" >&2; cat "$RESP" >&2; echo >&2; return 1 ;;
  esac
}

# Derive principal (iss#sub), issuer and audience FROM the token, in one
# decode — an app-only Entra credential issues a v1 token
# (iss=https://sts.windows.net/<tenant>/, aud=api://<client-id>) even through
# the v2 endpoint (aud=bare GUID), so assuming either would reject a valid
# token or register the wrong audience. What the token says beats what we
# asked for.
CLAIMS=$(python3 - "$IDP_TOKEN" <<'PY'
import base64, json, sys
p = sys.argv[1].split('.')[1]; p += '=' * (-len(p) % 4)
c = json.loads(base64.urlsafe_b64decode(p))
aud = c.get('aud')
print(c['iss'])
print(c.get('sub') or c.get('oid') or '')
print(aud[0] if isinstance(aud, list) else (aud or ''))
PY
)
ISS=$(echo "$CLAIMS" | sed -n '1p')
SUB=$(echo "$CLAIMS" | sed -n '2p')
AUD=$(echo "$CLAIMS" | sed -n '3p')
[ -n "$AUD" ] || { echo "!! token has no aud claim" >&2; exit 1; }
if [ -n "${IDP_AUDIENCE:-}" ] && [ "$IDP_AUDIENCE" != "$AUD" ]; then
  echo ">> note: registering the token's aud ($AUD), not IDP_AUDIENCE ($IDP_AUDIENCE)"
fi
# The control plane keys RBAC on principal_id = hex(sha256(issuer|subject))
# (services/felix-controlplane-service/src/auth/principal.rs), NOT a human-readable string.
# Registering the admin/role grouping under anything else yields a validated
# token with zero permissions (403 "no permissions"). Compute the same id here
# so the seed's groupings match what the exchange evaluates. subject_claim is
# "sub", so use the token's sub — the same claim the validator maps.
PRINCIPAL=$(python3 - "$ISS" "$SUB" <<'PY'
import hashlib, sys
print(hashlib.sha256(sys.argv[1].encode() + b"|" + sys.argv[2].encode()).hexdigest())
PY
)

echo ">> bootstrap-initialize tenant $TENANT (issuer $ISS, aud $AUD, principal $PRINCIPAL)"
post_ok "$BOOTSTRAP/internal/bootstrap/tenants/$TENANT/initialize" \
  -H "X-Felix-Bootstrap-Token: $BOOTSTRAP_TOKEN" \
  -H 'Content-Type: application/json' <<JSON
{
  "display_name": "Perf",
  "idp_issuers": [{
    "issuer": "$ISS",
    "audiences": ["$AUD"],
    "jwks_url": "$IDP_JWKS_URL",
    "claim_mappings": { "subject_claim": "sub" }
  }],
  "initial_admin_principals": ["$PRINCIPAL"],
  "policies": [
    { "subject": "role:perf", "object": "stream:$TENANT/$NAMESPACE/*", "action": "stream.publish" },
    { "subject": "role:perf", "object": "stream:$TENANT/$NAMESPACE/*", "action": "stream.subscribe" },
    { "subject": "role:perf", "object": "cache:$TENANT/$NAMESPACE/*", "action": "cache.read" },
    { "subject": "role:perf", "object": "cache:$TENANT/$NAMESPACE/*", "action": "cache.write" },
    { "subject": "role:perf", "object": "cluster:*", "action": "node.manage" },
    { "subject": "role:perf", "object": "cluster:*", "action": "node.view" }
  ],
  "groupings": [{ "user": "$PRINCIPAL", "role": "role:perf" }]
}
JSON

# Two tokens come out of the exchange, because the broker validates a client
# token by rejecting the WHOLE token if any action in it is not client-facing:
#   - the ADMIN token (full perms) creates the namespace/streams/cache; each
#     broker's node credential is an exchange of the same kind (below);
#   - the CLIENT token is narrowed to stream/cache actions only, and is what the
#     load generator authenticates with — an admin token carrying node.* is
#     refused at the broker's client control stream ("invalid action: node.view").
echo ">> exchange the IdP token for the Felix admin token"
CODE=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$CP/v1/tenants/$TENANT/token/exchange" \
  -H "Authorization: Bearer $IDP_TOKEN" -H 'Content-Type: application/json' \
  -d '{"audience":"felix-controlplane"}')
[ "$CODE" = 200 ] || { echo "!! admin token exchange -> HTTP $CODE" >&2; cat "$RESP" >&2; echo >&2; exit 1; }
TOKEN=$(field felix_token)
[ -n "$TOKEN" ] || { echo "!! exchange returned no felix_token" >&2; exit 1; }

echo ">> namespace and cache scope"
post_ok "$CP/v1/tenants/$TENANT/namespaces" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' <<JSON
{ "namespace": "$NAMESPACE", "display_name": "Perf" }
JSON
# Four streams, the two durability settings crossed with the two consistency
# levels, so a run can price each one against the others without reseeding.
# The -quorum pair is what #425 needs: every published figure before this was
# RF=1 Leader, which is not the configuration the docs recommend.
#
# A quorum of one is just the leader, so at REPLICATION_FACTOR=1 the -quorum
# streams measure the same path as their siblings -- seed them anyway, so the
# stream names are stable across tiers and a run script does not have to know
# the replication factor to know what to ask for.
# Streams are created only when SEED_STREAMS=1, which seed.sh sets on a second
# pass *after* every broker has registered.
#
# Placement happens once, when a stream is created, against whatever brokers the
# control plane can see at that instant -- and nothing ever moves a shard
# afterwards (#130). Create the streams while only the first broker has
# registered and it takes all of them; the rest of the cluster then sits idle
# for the whole session with no remedy short of deleting the streams, which
# destroys their data. A two-broker session measured exactly that: 49/0.
if [ "${SEED_STREAMS:-1}" = "1" ]; then
for stream in perf perf-durable perf-quorum perf-durable-quorum; do
  case "$stream" in
    *durable*) durable=true ;;
    *)         durable=false ;;
  esac
  case "$stream" in
    *quorum*) consistency=Quorum ;;
    *)        consistency=Leader ;;
  esac
  # Delete first so a reseed can change the shard count -- a fresh seed 404s
  # harmlessly. SHARDS spreads the write load across brokers: a single shard
  # pins the whole stream (and all its ingest) to one broker, which on fast
  # local disk caps throughput at one broker's write bandwidth. Default 12 (4
  # per broker on a 3-broker tier). Replicated when the tier has zones.
  curl -s -X DELETE "$CP/v1/tenants/$TENANT/namespaces/$NAMESPACE/streams/$stream" \
    -H "Authorization: Bearer $TOKEN" >/dev/null 2>&1 || true
  post_ok "$CP/v1/tenants/$TENANT/namespaces/$NAMESPACE/streams" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' <<STREAM
{
  "stream": "$stream",
  "kind": "Stream",
  "shards": $SHARDS,
  "replication_factor": $REPLICATION_FACTOR,
  "retention": { "max_age_seconds": null, "max_size_bytes": null },
  "consistency": "$consistency",
  "delivery": "AtLeastOnce",
  "durable": $durable
}
STREAM
done
else
  echo "   (skipping streams: they are created after the brokers register)"
fi
post_ok "$CP/v1/tenants/$TENANT/namespaces/$NAMESPACE/caches" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' <<JSON
{ "cache": "perf", "display_name": "Perf" }
JSON
# Replicated like the quorum streams, for the Quorum read path (lease or
# majority round, depending on the fleet features).
post_ok "$CP/v1/tenants/$TENANT/namespaces/$NAMESPACE/caches" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' <<JSON
{ "cache": "perf-quorum", "display_name": "Perf quorum", "shards": $SHARDS,
  "replication_factor": $REPLICATION_FACTOR, "consistency": "Quorum" }
JSON

echo ">> exchange a client-scoped token for the load generator"
# The exchange narrows to exactly the requested actions (filter_permissions),
# so this token carries only stream/cache perms — no node.* — and the broker
# accepts it on the client control stream.
CODE=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$CP/v1/tenants/$TENANT/token/exchange" \
  -H "Authorization: Bearer $IDP_TOKEN" -H 'Content-Type: application/json' \
  -d '{"requested":["stream.publish","stream.subscribe","cache.read","cache.write"]}')
[ "$CODE" = 200 ] || { echo "!! client token exchange -> HTTP $CODE" >&2; cat "$RESP" >&2; echo >&2; exit 1; }
CLIENT_TOKEN=$(field felix_token)
CLIENT_REFRESH=$(field refresh_token)
[ -n "$CLIENT_TOKEN" ] && [ -n "$CLIENT_REFRESH" ] \
  || { echo "!! client exchange returned no felix_token/refresh_token" >&2; exit 1; }
mkdir -p /home/felix/felix-session
( umask 077
  printf '%s' "$CLIENT_TOKEN" > /home/felix/felix-session/token
  printf '%s' "$CLIENT_REFRESH" > /home/felix/felix-session/refresh )
chown -R felix:felix /home/felix/felix-session
chmod 600 /home/felix/felix-session/token /home/felix/felix-session/refresh

# One exchange per broker, framed for seed.sh to drop as that broker's node
# credential. Refresh tokens rotate and a reused one revokes its whole chain,
# so brokers sharing one would lock each other out at the first refresh. The
# controlplane audience is what the broker's refresh asks for; node auth needs
# node.manage/node.view, which the client token deliberately lacks.
i=0
while [ "$i" -lt "${BROKER_COUNT:-0}" ]; do
  CODE=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$CP/v1/tenants/$TENANT/token/exchange" \
    -H "Authorization: Bearer $IDP_TOKEN" -H 'Content-Type: application/json' \
    -d '{"audience":"felix-controlplane"}')
  [ "$CODE" = 200 ] || { echo "!! broker-$i token exchange -> HTTP $CODE" >&2; cat "$RESP" >&2; echo >&2; exit 1; }
  printf '__FTOKEN_%s_BEGIN__%s__FTOKEN_%s_END__\n' "$i" "$(field felix_token)" "$i"
  printf '__FREFRESH_%s_BEGIN__%s__FREFRESH_%s_END__\n' "$i" "$(field refresh_token)" "$i"
  i=$((i + 1))
done
rm -f "$RESP"
echo __RUNOK__
