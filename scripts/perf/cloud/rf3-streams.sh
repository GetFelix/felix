#!/usr/bin/env bash
# Control-plane stream helpers shared by campaign-rf3.sh and run-ab.sh: recreate a
# stream, read replication state, finalize incarnations. Sourced after cells.sh
# (LOADGEN_VMS, CONTROLPLANE_IP, OUT, SHARDS) and lib.sh; never run.

# note <text>: the campaign's progress line when it has one, else the plain log.
note() { if declare -F plog >/dev/null; then plog "$@"; else log "$@"; fi; }

cp_remote() {
  agent_on "${LOADGEN_VMS[0]}" "u='http://${CONTROLPLANE_IP}:8080'
felix-agent token-refresh \"\$u\" /home/felix/felix-session/admin.token /home/felix/felix-session/admin.refresh 1800 >/dev/null 2>&1 || true
t=\$(cat /home/felix/felix-session/admin.token)
$1"
}

read -r -d '' REPL_PY <<'PY' || true
import collections, json, sys
d = json.load(sys.stdin)
items = d.get("items", [])
by = collections.defaultdict(collections.Counter)
for it in items:
    name = it.get("stream") or "?"
    if str(it.get("kind", "")).lower() == "cache":
        name = "cache/" + name
    by[name][(it.get("leader") or "none")[-1:]] += 1
parts = ";".join("%s:%s" % (s, ",".join("%s=%d" % (n, c) for n, c in sorted(by[s].items()))) for s in sorted(by))
print("__REPL_BEGIN__under=%d shards=%d %s__REPL_END__" % (d.get("under_replicated", 0), len(items), parts))
PY

# repl_status: "under=<n> shards=<n> <stream>:<broker>=<leaders>,..;..", empty on failure.
repl_status() {
  cp_remote "cat > /tmp/repl.py <<'PYEOF'
${REPL_PY}
PYEOF
curl -fsS -H \"Authorization: Bearer \$t\" \"\$u/v1/placement/replication\" | python3 /tmp/repl.py
echo __RUNOK__" 2>/dev/null | extract_between REPL
}

# leaders_even <status> <stream>: every broker leads, and no two differ by more than one.
leaders_even() {
  printf '%s\n' "$1" | tr ' ;' '\n\n' | grep "^$2:" | head -1 | cut -d: -f2 | tr ',' '\n' \
    | awk -F= -v n="${NBROKERS}" '{ c[NR] = $2 } END { if (NR != n) exit 1; lo = hi = c[1]; for (i = 2; i <= NR; i++) { if (c[i] < lo) lo = c[i]; if (c[i] > hi) hi = c[i] } exit (hi - lo > 1) }'
}

# reset_streams <stream>...: delete and recreate on the control plane, the way
# seed-remote.sh creates them. Recreating frees the disk and re-places the leaders.
reset_streams() {
  local s list="$*" body
  # Not $(cat <<'BODY'): bash 3.2 mis-parses a quoted heredoc inside $() and
  # expands the remote script's variables here.
  IFS= read -r -d '' body <<'BODY' || true
for s in $STREAMS; do
  case "$s" in *durable*|perf-failover) d=true ;; *) d=false ;; esac
  curl -s -X DELETE -H "Authorization: Bearer $t" "$u/v1/tenants/perf/namespaces/default/streams/$s" >/dev/null 2>&1 || true
  sleep 3
  code=$(curl -s -o /tmp/reset.resp -w '%{http_code}' -X POST "$u/v1/tenants/perf/namespaces/default/streams" \
    -H "Authorization: Bearer $t" -H 'Content-Type: application/json' \
    -d "{\"stream\":\"$s\",\"kind\":\"Stream\",\"shards\":$SHARDS,\"replication_factor\":$RF,\"retention\":{\"max_age_seconds\":null,\"max_size_bytes\":null},\"consistency\":\"Quorum\",\"delivery\":\"AtLeastOnce\",\"durable\":$d}")
  case "$code" in 2*) echo "created.$s=$code" ;; *) echo "!! create $s -> $code"; cat /tmp/reset.resp; exit 1 ;; esac
done
BODY
  cp_remote "STREAMS='${list}'; SHARDS=${SHARDS}; RF=3
${body}
echo __RUNOK__" > "${OUT}/system/reset-streams.txt" 2>&1 || { note "WARN stream recreate failed: $(tail -3 "${OUT}/system/reset-streams.txt" | tr '\n' ' ')"; return 1; }
}

# finalize_incarnations: each cell deletes and recreates its durable stream, and
# reusing a deleted name needs stream and cache incarnations (#1447). A new
# cluster finalizes them itself; this covers one seeded before its brokers
# registered. Finalizing an enabled feature again changes nothing.
finalize_incarnations() {
  local f out
  for f in stream_incarnations cache_incarnations; do
    out="$(cp_remote "curl -fsS -X POST -H \"Authorization: Bearer \$t\" \"\$u/v1/fleet/features/${f}/finalize\" >/dev/null && echo __RUNOK__" 2>&1)" \
      || { note "WARN could not finalize ${f}: $(printf '%s' "${out}" | tail -2 | tr '\n' ' ')"; return 1; }
  done
  note "setup: stream and cache incarnations finalized"
}

