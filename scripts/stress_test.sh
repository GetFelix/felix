#!/usr/bin/env bash
# Run one unit test many times with every core busy, the load a timing bug
# needs to show itself. Usage: stress_test.sh <package> <test path> <runs>
set -euo pipefail

package=$1
test=$2
runs=$3
parallel=${STRESS_PARALLEL:-8}

binary=$(cargo test -p "$package" --lib --no-run --message-format=json 2>/dev/null |
  python3 -c 'import json,sys
for line in sys.stdin:
    m = json.loads(line)
    if m.get("reason") == "compiler-artifact" and m.get("executable") and m["target"]["kind"] == ["lib"]:
        print(m["executable"])')

burners=()
for _ in $(seq "$(getconf _NPROCESSORS_ONLN)"); do
  (while :; do :; done) &
  burners+=($!)
done
trap 'kill "${burners[@]}" 2>/dev/null || true' EXIT

failures=$(mktemp)
per_loop=$(((runs + parallel - 1) / parallel))
loops=()
for _ in $(seq "$parallel"); do
  (
    for _ in $(seq "$per_loop"); do
      if ! out=$("$binary" --exact "$test" --test-threads 1 2>&1); then
        echo "$out" | grep -E "panicked|assert" | head -3 >>"$failures"
        echo FAIL >>"$failures"
      fi
    done
  ) &
  loops+=($!)
done
wait "${loops[@]}"

failed=$(grep -c '^FAIL$' "$failures" || true)
echo "$test: $failed of $((per_loop * parallel)) runs failed under load"
grep -v "^FAIL$" "$failures" | sort | uniq -c | head -5 || true
[ "$failed" -eq 0 ]
