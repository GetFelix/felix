#!/usr/bin/env bash
# End the session: delete everything it created (Azure: the resource group;
# GCP: every resource labelled for it, then the network). The last line of
# every run -- nothing in a session is meant to survive it. DRY_RUN=1 prints
# the commands instead.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"
group="felix-perf-${SESSION}"
provider_check
provider_teardown
