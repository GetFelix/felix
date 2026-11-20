#!/usr/bin/env bash
# Run a three-broker Felix cluster, kill things in it, and check that nothing
# acknowledged was lost. See README.md. Works with Docker and with Podman.
#
#   ./run.sh up                 start the cluster and the checker, follow its output
#   ./run.sh watch              follow the checker again
#   ./run.sh kill-leader        SIGKILL the broker leading the stream's shard
#   ./run.sh restart <broker>   start a stopped broker (broker-1, broker-2, broker-3)
#   ./run.sh kill-controlplane  SIGKILL the control plane
#   ./run.sh restart-controlplane
#   ./run.sh evidence           election log lines and promotion metrics from the brokers
#   ./run.sh receipt            ask the checker for a receipt now
#   ./run.sh scenario           the whole sequence, hands off
#   ./run.sh down               final receipt, then remove everything
#
# Everything is scoped to the compose project below. Nothing outside it is
# touched.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Fixed on purpose: `down -v` removes this project's volumes, so the name must
# not come from the caller's environment.
readonly COMPOSE_PROJECT_NAME=felix-failover
export COMPOSE_PROJECT_NAME
unset COMPOSE_FILE COMPOSE_PROFILES COMPOSE_PATH_SEPARATOR COMPOSE_ENV_FILES
cd "$here"

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  engine=docker
elif command -v podman >/dev/null 2>&1; then
  engine=podman
  # podman-compose lacks what this file needs; use the docker-compose provider.
  export PODMAN_COMPOSE_PROVIDER="${PODMAN_COMPOSE_PROVIDER:-docker-compose}"
  export PODMAN_COMPOSE_WARNING_LOGS=false
else
  echo "run.sh: need docker or podman with docker-compose 2.20 or later" >&2
  exit 1
fi
# `docker` can be Podman's CLI in disguise; either way, compose is a subcommand.
compose() { "$engine" compose -p "$COMPOSE_PROJECT_NAME" -f "$here/compose.yaml" "$@"; }

ctl() { compose --profile cli run --rm -T felixctl "$@"; }

say() { printf '\n== %s\n' "$*"; }

# The broker leading shard 0 of the stream, as felixctl reports it.
# Asked of the brokers alone, so it still works with the control plane down.
leader() {
  compose --profile cli run --rm -T -e FELIX_CONTROLPLANE_URL= -e FELIX_CONTROLPLANE_TOKEN_FILE= \
    felixctl topology orders 2>/dev/null | awk '$1 == "0" { print $2; exit }'
}

stamp() { date '+%H:%M:%S'; }

running() { compose ps --status running --services 2>/dev/null | grep -qx "$1"; }

# Wait for the checker to be producing, so a kill lands on live traffic.
wait_for_checker() {
  for _ in $(seq 1 180); do
    if compose logs --no-log-prefix --tail 5 checker 2>/dev/null | grep -q 'acknowledged'; then
      return 0
    fi
    sleep 1
  done
  echo "run.sh: the checker did not start; see: $engine compose -p $COMPOSE_PROJECT_NAME logs" >&2
  return 1
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  up)
    say "starting the cluster (the first run builds the checker image, a few minutes)"
    compose up -d --build
    wait_for_checker
    say "running. Ctrl-C stops following; the cluster keeps running"
    echo "  in another terminal: ./run.sh kill-leader"
    compose logs -f --no-log-prefix --tail 5 checker
    ;;
  watch)
    compose logs -f --no-log-prefix --tail 20 checker
    ;;
  leader)
    leader
    ;;
  kill-leader)
    victim="$(leader)"
    [[ -n "$victim" ]] || { echo "run.sh: could not find the leader" >&2; exit 1; }
    echo "$(stamp) killing $victim (SIGKILL)"
    compose kill "$victim"
    ;;
  restart)
    broker="${1:?usage: ./run.sh restart broker-1|broker-2|broker-3}"
    case "$broker" in broker-[123]) ;; *) echo "run.sh: not a broker: $broker" >&2; exit 1 ;; esac
    echo "$(stamp) starting $broker"
    compose up -d --no-build --no-deps "$broker"
    ;;
  kill-controlplane)
    echo "$(stamp) killing controlplane (SIGKILL)"
    compose kill controlplane
    ;;
  restart-controlplane)
    echo "$(stamp) starting controlplane"
    compose up -d --no-build --no-deps controlplane
    ;;
  evidence)
    for b in broker-1 broker-2 broker-3; do
      echo "== $b"
      compose logs --no-log-prefix "$b" 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' \
        | grep -E 'standing for election|majority took the fence|shard now serving' \
        | cut -c1-240 || true
      compose exec -T "$b" wget -qO- http://127.0.0.1:8080/metrics 2>/dev/null \
        | grep -E '^felix_broker_promotions_opened_total' || true
    done
    ;;
  receipt)
    running checker || { echo "run.sh: the checker is not running" >&2; exit 1; }
    before="$(compose exec -T checker cat /out/receipt.txt 2>/dev/null || true)"
    compose kill -s USR1 checker >/dev/null
    for _ in $(seq 1 90); do
      now="$(compose exec -T checker cat /out/receipt.txt 2>/dev/null || true)"
      if [[ -n "$now" && "$now" != "$before" ]]; then
        printf '%s\n' "$now"
        # Not a pass unless the verdict line says so.
        if grep -qE '^verdict +(VIOLATED|FAILED)' <<<"$now"; then exit 1; fi
        exit 0
      fi
      sleep 1
    done
    echo "run.sh: no receipt after 90 s" >&2
    exit 1
    ;;
  scenario)
    wait_for_checker
    say "steady state for 15 s"
    sleep 15
    say "1. kill the leader; the control plane is up and names the new one"
    first="$(leader)"
    "$0" kill-leader
    sleep 25
    say "restart $first"
    "$0" restart "$first"
    sleep 30
    say "2. kill the control plane"
    "$0" kill-controlplane
    sleep 5
    say "3. kill the new leader with the control plane down; the replicas elect"
    second="$(leader)"
    "$0" kill-leader
    sleep 50
    say "bring the control plane and $second back"
    "$0" restart-controlplane
    sleep 15
    "$0" restart "$second"
    sleep 30
    say "receipt"
    "$0" receipt
    ;;
  down)
    status=0
    if running checker; then
      say "final receipt"
      compose kill -s TERM checker >/dev/null || true
      for _ in $(seq 1 150); do running checker || break; sleep 1; done
      compose logs --no-log-prefix --tail 40 checker 2>/dev/null | sed -n '/==== failover receipt (final)/,$p'
      # The checker exits 0 only when every acknowledged write was found once.
      cid="$(compose ps -aq checker 2>/dev/null | head -1)"
      status="$("$engine" inspect --format '{{.State.ExitCode}}' "$cid" 2>/dev/null || echo 1)"
    fi
    say "removing the $COMPOSE_PROJECT_NAME project"
    compose --profile cli down -v --remove-orphans
    if [[ "$status" != 0 ]]; then
      echo "run.sh: the checker did not pass (exit $status)" >&2
    fi
    exit "$status"
    ;;
  logs)
    compose logs --no-log-prefix "$@"
    ;;
  ps)
    compose ps -a
    ;;
  *)
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
esac
