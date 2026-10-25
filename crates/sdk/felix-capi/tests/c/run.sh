#!/usr/bin/env bash
# Build the C test against the header and both library forms, then run it.
#
#   tests/c/run.sh offline   no broker needed
#   tests/c/run.sh fixture   against `felix-cluster client-fixture`, which
#                            must already be built (as CI's Python job does).
#                            Writes the conformance results document to
#                            $FELIX_CONFORMANCE_RESULTS when that is set.
#                            The scenarios that kill a broker each get a
#                            fixture of their own, started and stopped here.
set -euo pipefail

mode="${1:-offline}"
crate="$(cd "$(dirname "$0")/../.." && pwd)"
repo="$(cd "$crate/../../.." && pwd)"
target="${CARGO_TARGET_DIR:-$repo/target}/debug"
work="$(mktemp -d)"
fixture_pid=""
disposable_pid=""
cleanup() {
  for pid in "$disposable_pid" "$fixture_pid"; do
    if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
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

# Starts `felix-cluster client-fixture` over $1 and waits until it has written
# its fixture document and certificates. The process id goes into the variable
# named by $2 as soon as it exists. Returns 1 rather
# than exiting, so a disposable fixture that fails to start fails its scenario
# and not the whole run.
start_fixture() {
  local dir="$1" pid
  mkdir -p "$dir"
  RUST_LOG=warn "$target/felix-cluster" client-fixture \
    --out "$dir/fixture.json" --ca-file "$dir/ca.pem" >"$dir/fixture.log" 2>&1 &
  pid=$!
  # Recorded before the startup wait, so the trap stops it if killed during it.
  printf -v "$2" %s "$pid"
  for _ in $(seq 1 600); do
    if [ -s "$dir/fixture.json" ] && [ -s "$dir/ca.pem" ] &&
      python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$dir/fixture.json" 2>/dev/null; then
      write_env "$dir/fixture.json" "$dir/fixture.env" || return 1
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "the fixture exited during startup:" >&2
      cat "$dir/fixture.log" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "the fixture was not ready in time" >&2
  cat "$dir/fixture.log" >&2
  return 1
}

# The test reads the fixture as key=value lines, which C parses without a JSON
# library. The catalogue's fault steps ride along as fault.<id>=<json> for the
# fault scenarios, and kill_helper names the script that kills one broker.
write_env() {
  python3 -I - "$1" "$crate/tests/c/kill-broker.sh" >"$2" <<'PY'
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
if f.get("records_publishers"):
    emit("records_publishers", "true")
if f.get("no_commit_addrs"):
    emit("no_commit_addrs", ",".join(f["no_commit_addrs"]))
emit("kill_helper", sys.argv[2])
for fault in f.get("faults", []):
    step = {k: v for k, v in fault.items() if k != "id"}
    emit(f"fault.{fault['id']}", json.dumps(step, separators=(",", ":")))
PY
}

start_fixture "$work/shared" fixture_pid || exit 1

"$work/pubsub-shared" offline
"$work/pubsub-static" offline

# Each build appends its outcomes to its own file; they are merged below, so a
# scenario passes only if it passed in both.
status=0
# The scenarios that kill a broker each get a fixture of their own, because a
# cluster someone else damaged would fail every scenario after it, and the
# shared and static builds each need an undamaged one.
run_build() {
  local build="$1" scenario dir
  FELIX_CONFORMANCE_LINES="$work/lines-$build" "$work/pubsub-$build" fixture "$work/shared/fixture.env" || status=1
  for scenario in broker_loss lost_shard; do
    dir="$work/$scenario-$build"
    if start_fixture "$dir" disposable_pid; then
      FELIX_CONFORMANCE_LINES="$work/lines-$build" "$work/pubsub-$build" disposable "$dir/fixture.env" "$scenario" || status=1
    else
      status=1
    fi
    kill "$disposable_pid" 2>/dev/null || true
    wait "$disposable_pid" 2>/dev/null || true
    disposable_pid=""
  done
}
run_build shared
run_build static

if [ -n "${FELIX_CONFORMANCE_RESULTS:-}" ]; then
  version="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$repo/Cargo.toml" | head -n 1)"
  python3 -I - "$FELIX_CONFORMANCE_RESULTS" "${version:-unknown}" "$work/lines-shared" "$work/lines-static" <<'PY'
import json, os, sys

destination, version, *paths = sys.argv[1:]
builds = ("shared", "static")
rank = {"pass": 0, "skip": 1, "fail": 2}

def read(path):
    outcomes = {}
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            scenario, status, detail = (line.rstrip("\n").split("\t", 2) + ["", ""])[:3]
            if status not in rank:
                continue
            held = outcomes.get(scenario)
            if held is None or rank[status] > rank[held[0]]:
                outcomes[scenario] = (status, detail)
    return outcomes

per_build = dict(zip(builds, (read(path) for path in paths)))
if not any(per_build.values()):
    # Nothing was recorded: leave no document, so verify reports it missing.
    print("no build recorded any outcome; writing no results", file=sys.stderr)
    sys.exit(0)

# Worst outcome wins, and an id one build never recorded (it crashed, or never
# started) fails: it has not been shown to hold in both.
merged = {}
for scenario in sorted(set().union(*per_build.values())):
    worst = None
    for build in builds:
        held = per_build[build].get(scenario)
        if held is None:
            held = ("fail", f"not recorded by the {build} build")
        if worst is None or rank[held[0]] > rank[worst[0]]:
            worst = held
    outcome = {"id": scenario, "status": worst[0]}
    if worst[1]:
        outcome["detail"] = worst[1]
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
