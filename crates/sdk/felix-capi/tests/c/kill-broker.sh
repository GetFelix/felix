#!/usr/bin/env bash
# Kill the felix-broker process listening on a UDP port, and only that one.
#
#   kill-broker.sh host:port
#
# Exits 0 when a broker was killed, 1 when none was found, 2 on bad input.
# Never matches by name alone: the port picks the process, and the name only
# keeps a client socket that happens to use the port from being chosen.
set -euo pipefail

addr="${1:-}"
port="${addr##*:}"
case "$port" in
  '' | *[!0-9]*)
    echo "usage: kill-broker.sh host:port" >&2
    exit 2
    ;;
esac
if ! command -v lsof >/dev/null 2>&1; then
  echo "lsof is not installed" >&2
  exit 1
fi
pid="$(lsof -nP -a -c felix-broker -iUDP:"$port" -t 2>/dev/null | head -n 1 || true)"
if [ -z "$pid" ]; then
  echo "no felix-broker is listening on UDP port $port" >&2
  exit 1
fi
kill -9 "$pid"
