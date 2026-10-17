#!/usr/bin/env bash
# Bring up compose.yaml and do one publish/subscribe round trip through it:
# subscribe with the stack's felixctl service, publish from the host with the
# credentials copied out the way the docs say.
#
#   scripts/ci/compose_smoke.sh
#
# CONTAINER_ENGINE=podman runs it under Podman (with the docker-compose
# provider). FELIX_VERSION picks the image tag, as it does for compose.yaml;
# CI sets it to images it built and loaded, and COMPOSE_PULL=never makes sure
# nothing is pulled in their place.
set -euo pipefail

ENGINE=${CONTAINER_ENGINE:-docker}
cd "$(dirname "$0")/../.."
compose() { "$ENGINE" compose "$@"; }

FELIXCTL_IMAGE=$(compose config --images | grep -m1 /felixctl:)
MESSAGE="hello from compose $$"
work=$(mktemp -d)
# felixctl's image runs as uid 65532 and reads the copied files from here.
chmod 755 "$work"
sub_pid=

cleanup() {
  status=$?
  [ -n "$sub_pid" ] && kill "$sub_pid" 2> /dev/null || true
  if [ "$status" -ne 0 ]; then
    compose ps -a || true
    compose logs --no-color 2>&1 | tail -n 200 || true
  fi
  compose down -v > /dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

compose up -d --pull "${COMPOSE_PULL:-missing}"

for _ in $(seq 1 90); do
  compose logs felix-dev-ready 2> /dev/null | grep -q 'Felix is up' && break
  sleep 2
done
compose logs --no-color felix-dev-ready | grep -q 'Felix is up' || {
  echo "::error::the stack never printed its connection settings"
  exit 1
}
compose logs --no-color felix-dev-ready

# The host side, as the printed instructions do it.
compose exec -T felix-broker cat /var/lib/felix/client.jwt > "$work/felix-dev.jwt"
compose exec -T felix-broker cat /var/lib/felix/broker-cert.pem > "$work/felix-dev.pem"
chmod 644 "$work"/*
host_felixctl=("$ENGINE" run --rm --network host -v "$work:/work:ro" "$FELIXCTL_IMAGE"
  --brokers 127.0.0.1:5000 --tenant dev
  --token-file /work/felix-dev.jwt --ca-file /work/felix-dev.pem)

compose run --rm -T felixctl sub orders --count 1 > "$work/sub.out" 2>&1 &
sub_pid=$!

# The subscriber starts at the live tail, so publish until it has seen one
# rather than guessing when it is attached.
for _ in $(seq 1 30); do
  kill -0 "$sub_pid" 2> /dev/null || break
  "${host_felixctl[@]}" pub orders "$MESSAGE" || true
  sleep 2
done

if kill -0 "$sub_pid" 2> /dev/null; then
  echo "::error::the subscriber received nothing in 60 seconds"
  cat "$work/sub.out"
  exit 1
fi
code=0
wait "$sub_pid" || code=$?
sub_pid=
cat "$work/sub.out"
if [ "$code" -ne 0 ] || ! grep -qF "$MESSAGE" "$work/sub.out"; then
  echo "::error::the subscriber exited ${code} without printing the message"
  exit 1
fi
echo "round trip ok"
