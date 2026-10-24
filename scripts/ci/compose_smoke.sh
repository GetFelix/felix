#!/usr/bin/env bash
# Bring up compose.yaml and do one publish/subscribe round trip through it:
# subscribe with the stack's felixctl service, publish from the host with the
# credentials copied out the way the docs say. Then run the commands the
# use-case pages' "Try it" sections give: a retained cache watch, a counter,
# a replay with offsets and a consumer group redelivery.
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
# Under pipefail, `cmd | grep -q` fails when grep exits on its match before
# cmd finishes writing (SIGPIPE, 141). Reading all of the input avoids that.
has() { grep -F -- "$1" > /dev/null; }

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
  compose logs felix-dev-ready 2> /dev/null | has 'Felix is up' && break
  sleep 2
done
compose logs --no-color felix-dev-ready | has 'Felix is up' || {
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

ctl() { compose run --rm -T felixctl "$@"; }
fail() { echo "::error::$1"; exit 1; }

# Wait up to 60 seconds for $2 to appear in file $1.
wait_for() {
  for _ in $(seq 1 30); do
    grep -q "$2" "$1" 2> /dev/null && return 0
    sleep 2
  done
  return 1
}

# A watcher gets the current value first, then the next change.
ctl cache put config checkout blue
ctl cache watch config --key checkout --retained --count 2 > "$work/watch.out" 2>&1 &
sub_pid=$!
wait_for "$work/watch.out" "checkout.blue" || fail "the retained value never arrived"
ctl cache put config checkout green
code=0
wait "$sub_pid" || code=$?
sub_pid=
cat "$work/watch.out"
[ "$code" -eq 0 ] && grep -q "checkout.green" "$work/watch.out" \
  || fail "the cache watch missed the update"
ctl cache get config checkout | has green || fail "cache get did not return the last put"

# A prefix watch sees a delete of a key that existed.
ctl cache put config flag/beta-search on
ctl cache watch config --prefix flag/ --retained --count 2 > "$work/prefix.out" 2>&1 &
sub_pid=$!
wait_for "$work/prefix.out" "flag/beta-search.on" || fail "the prefix watch never showed the retained key"
ctl cache del config flag/beta-search
code=0
wait "$sub_pid" || code=$?
sub_pid=
cat "$work/prefix.out"
[ "$code" -eq 0 ] && grep -qF $'flag/beta-search\t(deleted)' "$work/prefix.out" \
  || fail "the prefix watch missed the delete"

# A counter keeps a running total.
ctl counter add stats page-views 1 > /dev/null
total=$(ctl counter add stats page-views 1 | tr -d '[:space:]')
[ "$total" = 2 ] || fail "counter add returned '$total', not 2"

# A durable stream replays from the start, with offsets.
ctl sub orders --from earliest --count 1 --format offsets | tee "$work/replay.out"
[ -s "$work/replay.out" ] || fail "replay from earliest printed nothing"

# A consumer group hands a nacked record out again with a higher attempt count.
ctl group create orders workers --from earliest
first=$(ctl group poll orders workers --max 1 --wait-ms 5000 | head -n 1 | cut -f1)
[ -n "$first" ] || fail "the group claimed nothing"
ctl group nack orders workers "$first"
second=$(ctl group poll orders workers --max 1 --wait-ms 5000 | head -n 1 | cut -f1)
[ "${second%:*}" = "${first%:*}" ] && [ "${second##*:}" = 2 ] \
  || fail "after a nack the group claimed '$second' after '$first'"
ctl group ack orders workers "$second"

# A token minted for tenant dev is refused for another tenant.
if timeout 60 "$ENGINE" compose run --rm -T felixctl --tenant acme pub orders 'hello' > "$work/tenant.out" 2>&1; then
  fail "a dev token published as tenant acme"
fi
cat "$work/tenant.out"
grep -Eqi 'auth rejected' "$work/tenant.out" \
  || fail "the cross-tenant publish failed, but not with a refusal"

# A key written with a TTL is gone once it lapses.
ctl cache put config lease/job-1 worker-a --ttl-ms 10000
ctl cache get config lease/job-1 | has worker-a || fail "the leased key was not readable"
sleep 12
if ctl cache get config lease/job-1 > /dev/null 2>&1; then
  fail "the leased key outlived its TTL"
fi
echo "try-it commands ok"
