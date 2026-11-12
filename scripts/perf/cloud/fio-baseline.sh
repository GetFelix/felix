#!/usr/bin/env bash
# Device baseline on every broker's /data: sequential buffered writes with an
# fdatasync after each (the OnCommit pattern), 30 s per test, broker idle.
# The session drivers run their own; this is for a session in progress.
#
#   SESSION=<name> ./fio-baseline.sh [<label> <bs> <jobs>]...
#
# Default: 256k at 1/4/8 jobs and 4k at 1 job. FIO_SECS changes the length.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

if [ "$#" -eq 0 ]; then
  set -- seq256k-j1 256k 1 seq256k-j4 256k 4 seq256k-j8 256k 8 seq4k-j1 4k 1
fi
fio_baseline "$@"
