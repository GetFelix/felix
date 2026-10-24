#!/usr/bin/env bash
# Build the C test against the header and both library forms, then run it.
#
#   tests/c/run.sh offline   no broker needed
#   tests/c/run.sh fixture   against `felix-cluster client-fixture`, which
#                            must already be built (as CI's Python job does).
#                            Writes the conformance results document to
#                            $FELIX_CONFORMANCE_RESULTS when that is set.
set -euo pipefail

mode="${1:-offline}"
crate="$(cd "$(dirname "$0")/../.." && pwd)"
repo="$(cd "$crate/../../.." && pwd)"
target="${CARGO_TARGET_DIR:-$repo/target}/debug"
work="$(mktemp -d)"
fixture_pid=""
cleanup() {
  if [ -n "$fixture_pid" ]; then kill "$fixture_pid" 2>/dev/null || true; wait "$fixture_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

cd "$repo"
cargo build --locked -q -p felix-capi
# What a static link needs from the system differs per platform; rustc says.
# Without --color never, CARGO_TERM_COLOR=always leaves an escape code on the
# last library name.
native="$(cargo rustc --color never --locked -p felix-capi --lib --crate-type staticlib -- --print native-static-libs 2>&1 \
  | sed -n 's/.*native-static-libs: //p' | tail -n 1)"
if [ -z "$native" ]; then
  echo "rustc did not report the static library's native dependencies" >&2
  exit 1
fi

cflags=(-std=c11 -pthread -Wall -Wextra -Werror -I "$crate/include")
case "$(uname -s)" in
  Darwin) shared="$target/libfelix.dylib" ;;
  *) shared="$target/libfelix.so" ;;
esac
cc "${cflags[@]}" "$crate/tests/c/pubsub.c" "$shared" -Wl,-rpath,"$target" -o "$work/pubsub-shared"
# shellcheck disable=SC2086 # the native library list is meant to split
cc "${cflags[@]}" "$crate/tests/c/pubsub.c" "$target/libfelix.a" $native -o "$work/pubsub-static"

if [ "$mode" = offline ]; then
  "$work/pubsub-shared" offline
  "$work/pubsub-static" offline
  exit 0
fi

RUST_LOG=warn "$target/felix-cluster" client-fixture \
  --out "$work/fixture.json" --ca-file "$work/ca.pem" >"$work/fixture.log" 2>&1 &
fixture_pid=$!
for _ in $(seq 1 600); do
  if [ -s "$work/fixture.json" ] && [ -s "$work/ca.pem" ] &&
    python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$work/fixture.json" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$fixture_pid" 2>/dev/null; then
    echo "the fixture exited during startup:" >&2
    cat "$work/fixture.log" >&2
    exit 1
  fi
  sleep 0.2
done
if [ ! -s "$work/fixture.json" ]; then
  echo "the fixture was not ready in time" >&2
  cat "$work/fixture.log" >&2
  exit 1
fi

# The test reads the fixture as key=value lines, which C parses without a JSON
# library. The catalogue's fault steps ride along as fault.<id>=<json> for the
# fault scenarios.
python3 -I - "$work/fixture.json" >"$work/fixture.env" <<'PY'
import json, sys
f = json.load(open(sys.argv[1]))
def emit(key, value):
    value = str(value)
    if "\n" in value or "\r" in value:
        raise SystemExit(f"{key} has a line break; the fixture file is line based")
    print(f"{key}={value}")
emit("addrs", ",".join(f["addrs"]))
for key in ("tenant_id", "namespace", "token", "ca_file", "durable_stream",
            "missing_stream", "unauthorized_token", "group_operator_token",
            "cache", "single_shard_cache"):
    emit(key, f[key])
for key in ("movable_stream", "quorum_stream", "control_url", "link_addr",
            "link_stream"):
    if f.get(key):
        emit(key, f[key])
for fault in f.get("faults", []):
    step = {k: v for k, v in fault.items() if k != "id"}
    emit(f"fault.{fault['id']}", json.dumps(step, separators=(",", ":")))
PY

"$work/pubsub-shared" offline
"$work/pubsub-static" offline

# Each build appends its outcomes to its own file; they are merged below, so a
# scenario passes only if it passed in both.
status=0
FELIX_CONFORMANCE_LINES="$work/lines-shared" "$work/pubsub-shared" fixture "$work/fixture.env" || status=1
FELIX_CONFORMANCE_LINES="$work/lines-static" "$work/pubsub-static" fixture "$work/fixture.env" || status=1

if [ -n "${FELIX_CONFORMANCE_RESULTS:-}" ]; then
  version="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$repo/Cargo.toml" | head -n 1)"
  python3 -I - "$FELIX_CONFORMANCE_RESULTS" "${version:-unknown}" "$work/lines-shared" "$work/lines-static" <<'PY'
import json, os, sys

destination, version, *paths = sys.argv[1:]
# Worst outcome wins: a scenario that failed or was skipped in either build
# has not been shown to hold in both.
rank = {"pass": 0, "skip": 1, "fail": 2}
merged = {}
for path in paths:
    if not os.path.exists(path):
        continue
    for line in open(path, encoding="utf-8"):
        scenario, status, detail = (line.rstrip("\n").split("\t", 2) + ["", ""])[:3]
        if status not in rank:
            continue
        held = merged.get(scenario)
        if held is None or rank[status] > rank[held["status"]]:
            outcome = {"id": scenario, "status": status}
            if detail:
                outcome["detail"] = detail
            merged[scenario] = outcome
document = {
    "client": "felix-capi (C)",
    "version": version,
    "outcomes": sorted(merged.values(), key=lambda outcome: outcome["id"]),
}
os.makedirs(os.path.dirname(os.path.abspath(destination)), exist_ok=True)
with open(destination, "w", encoding="utf-8") as out:
    json.dump(document, out, indent=2)
    out.write("\n")
print(f"conformance results written to {destination}")
PY
fi
exit "$status"
