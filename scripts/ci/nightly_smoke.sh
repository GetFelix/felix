#!/usr/bin/env bash
# Publish and subscribe through the control plane, broker and felixctl images.
#
#   scripts/ci/nightly_smoke.sh <registry/owner> <tag>
#
# Everything runs on the host network: the control plane only allows dev
# tokens with bootstrap bound to loopback, and that loopback has to be the one
# curl reaches.
set -euo pipefail

REG=${1:?registry, e.g. ghcr.io/getfelix}
TAG=${2:?image tag}
T=t1
NS=smoke
S=orders
BT=smoke-bootstrap-token
CP=http://127.0.0.1:8443
BOOT=http://127.0.0.1:9095/internal/bootstrap/tenants/$T
MESSAGE="hello from ${TAG}"

work=$(mktemp -d)
# The images run as uid 65532 and write the broker certificate and the
# subscriber's output here.
chmod 777 "$work"

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    for c in smoke-cp smoke-broker smoke-sub; do
      echo "--- logs: $c"
      docker logs "$c" 2>&1 | tail -n 80 || true
    done
  fi
  docker rm -f smoke-cp smoke-broker smoke-sub > /dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 60); do
    curl -fsS "$1" > /dev/null 2>&1 && return 0
    sleep 1
  done
  echo "::error::$1 never became ready"
  return 1
}

# Metrics on 9091, so the broker keeps 8080. The images' HEALTHCHECK probes
# 8080, which on a shared network would be the wrong process.
docker run -d --name smoke-cp --network host --no-healthcheck \
  -e FELIX_CONTROLPLANE_STORAGE_BACKEND=memory \
  -e FELIX_CONTROLPLANE_BIND=127.0.0.1:8443 \
  -e FELIX_CONTROLPLANE_METRICS_BIND=127.0.0.1:9091 \
  -e FELIX_BOOTSTRAP_ENABLED=true \
  -e FELIX_BOOTSTRAP_BIND_ADDR=127.0.0.1:9095 \
  -e FELIX_BOOTSTRAP_TOKEN=$BT \
  -e FELIX_BOOTSTRAP_DEV_TOKENS=true \
  "$REG/felix-controlplane:$TAG"
wait_ready http://127.0.0.1:9091/ready

json=(-fsS -H 'Content-Type: application/json')
curl "${json[@]}" -H "X-Felix-Bootstrap-Token: $BT" -X POST "$BOOT/initialize" -d @- <<EOF
{"display_name":"$T","idp_issuers":[],"initial_admin_principals":["p:admin"],
 "policies":[
  {"subject":"role:broker","object":"cluster:*","action":"node.view"},
  {"subject":"role:client","object":"namespace:$T/*","action":"ns.manage"}],
 "groupings":[{"user":"p:broker","role":"role:broker"},{"user":"p:client","role":"role:client"}]}
EOF
echo

token() {
  curl "${json[@]}" -H "X-Felix-Bootstrap-Token: $BT" -X POST "$BOOT/dev-token" -d "$1" | jq -er .felix_token
}
node_token=$(token '{"principal":"p:broker","audience":"felix-controlplane"}')
admin_token=$(token '{"principal":"p:client","audience":"felix-controlplane","requested":["ns.manage","stream.manage"]}')
# A broker refuses a token carrying any action it does not know, so the
# client's broker token asks for the data-path actions only.
token '{"principal":"p:client","audience":"felix-broker","requested":["stream.publish","stream.subscribe"]}' > "$work/client.jwt"
chmod 644 "$work/client.jwt"

curl "${json[@]}" -H "Authorization: Bearer $admin_token" -X POST "$CP/v1/tenants/$T/namespaces" \
  -d "{\"namespace\":\"$NS\",\"display_name\":\"$NS\"}"
echo
curl "${json[@]}" -H "Authorization: Bearer $admin_token" -X POST "$CP/v1/tenants/$T/namespaces/$NS/streams" -d @- <<EOF
{"stream":"$S","kind":"Stream","shards":1,"replication_factor":1,
 "retention":{"max_age_seconds":null,"max_size_bytes":null},
 "consistency":"Leader","delivery":"AtLeastOnce","durable":false}
EOF
echo

# No TLS material: the broker makes a self-signed certificate for localhost
# and exports it for felixctl to trust.
docker run -d --name smoke-broker --network host --no-healthcheck -v "$work:/work" \
  -e FELIX_CONTROLPLANE_URL=$CP \
  -e FELIX_NODE_TOKEN="$node_token" \
  -e FELIX_QUIC_BIND=127.0.0.1:5000 \
  -e FELIX_BROKER_METRICS_BIND=127.0.0.1:8080 \
  -e FELIX_TLS_CERT_EXPORT=/work/broker-cert.pem \
  "$REG/felix-broker:$TAG"
wait_ready http://127.0.0.1:8080/ready

felixctl=(--network host -v "$work:/work" "$REG/felixctl:$TAG"
  --brokers 127.0.0.1:5000 --tenant "$T" -n "$NS"
  --token-file /work/client.jwt --ca-file /work/broker-cert.pem)

docker run -d --name smoke-sub "${felixctl[@]}" sub $S --count 1

# The subscriber starts at the live tail, so publish until it has seen one
# rather than guessing when it is attached.
for _ in $(seq 1 30); do
  [ "$(docker inspect -f '{{.State.Running}}' smoke-sub)" = true ] || break
  docker run --rm "${felixctl[@]}" pub $S "$MESSAGE"
  sleep 2
done

if [ "$(docker inspect -f '{{.State.Running}}' smoke-sub)" = true ]; then
  echo "::error::the subscriber received nothing in 60 seconds"
  exit 1
fi
code=$(docker inspect -f '{{.State.ExitCode}}' smoke-sub)
out=$(docker logs smoke-sub 2>&1)
echo "$out"
if [ "$code" -ne 0 ] || ! grep -qF "$MESSAGE" <<< "$out"; then
  echo "::error::the subscriber exited ${code} without printing the message"
  exit 1
fi
echo "round trip ok"
