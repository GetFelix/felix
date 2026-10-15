#!/usr/bin/env bash
# Print the container engine to use: `docker` or `podman`, or nothing when
# neither can run a container right now.
#
# CONTAINER_ENGINE wins when set. Otherwise the first of docker and podman
# whose `version` succeeds; `version` reaches the daemon or the Podman machine,
# so an installed CLI with nothing behind it does not count.
set -u

if [ -n "${CONTAINER_ENGINE:-}" ]; then
  if "$CONTAINER_ENGINE" version >/dev/null 2>&1; then
    echo "$CONTAINER_ENGINE"
  else
    echo "CONTAINER_ENGINE=$CONTAINER_ENGINE is set but '$CONTAINER_ENGINE version' fails" >&2
  fi
  exit 0
fi

for engine in docker podman; do
  if "$engine" version >/dev/null 2>&1; then
    echo "$engine"
    exit 0
  fi
done

# The usual reason on macOS: the VM is installed but stopped.
if command -v podman >/dev/null 2>&1 && [ "$(uname -s)" = Darwin ]; then
  echo "podman is installed but not answering; start it with 'podman machine start'" >&2
fi
exit 0
