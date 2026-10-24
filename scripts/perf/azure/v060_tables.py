#!/usr/bin/env python3
"""Markdown tables for the v0.6.0 performance write-up, straight from cell directories.

    v060_tables.py <results-dir> [<results-dir> ...]

Groups trials of a cell (name minus -tN) and prints, per group: median across
finished trials with the min to max spread, the broker's accepted payload rate,
wire bytes, durable append rate, broker cores, UDP receive-buffer drops, the
shed share from ack-proof.txt, and ack and delivery latency (p50/p99/p999,
median across generators, then across trials). Gate pairs (s0-new-* against
s0-p9-*) get their own comparison table. Read-only; re-run as cells finish.
"""

import json
import re
import statistics as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import summarize  # noqa: E402

TRIAL = re.compile(r"^(?P<group>.+)-t(?P<t>\d+)$")


def latencies(cdir):
    """(ack, delivery) dicts of p50/p99/p999, median over the generators' JSON lines."""
    acks, dels = [], []
    for run in cdir.glob("*.run.txt"):
        for line in run.read_text(errors="replace").splitlines():
            if not line.startswith("LOADGEN_JSON"):
                continue
            try:
                j = json.loads(line.split(" ", 1)[1])
            except (IndexError, ValueError):
                continue
            a = j.get("ack_latency_us") or j.get("batch_ack_latency_us")
            if a:
                acks.append(a)
            if j.get("delivery_latency_us"):
                dels.append(j["delivery_latency_us"])

    def med(rs):
        return {k: st.median(r.get(k, 0) for r in rs) for k in ("p50", "p99", "p999")} if rs else None

    return med(acks), med(dels)


def shed_pct(cdir):
    p = cdir / "ack-proof.txt"
    if not p.exists():
        return None
    m = re.search(r"shed=(-?[0-9.]+)%", p.read_text())
    return float(m.group(1)) if m else None


def load(root):
    groups = {}
    for d in sorted((root / "cells").glob("*")):
        if not d.is_dir():
            continue
        m = TRIAL.match(d.name)
        group = m.group("group") if m else d.name
        r = summarize.cell_row(d) or {}
        done = bool(r.get("done")) and not r.get("legacy")
        ack, dl = latencies(d) if done else (None, None)
        groups.setdefault(group, []).append({
            "done": done,
            "payload": r.get("ss_publish_mb_s"),
            "wire": r.get("ss_ingress_mb_s"),
            "append": r.get("ss_append_mb_s"),
            "cores": r.get("ss_cores"),
            "drops": r.get("ss_drops_s"),
            "durable": "durable" in (r.get("stream") or ""),
            "shed": shed_pct(d),
            "ack": ack,
            "del": dl,
        })
    return groups


def mid(vals, digits=0):
    vals = [v for v in vals if isinstance(v, (int, float))]
    if not vals:
        return "-"
    m = st.median(vals)
    if len(vals) > 1 and max(vals) - min(vals) > 0.005 * abs(m):
        return f"{m:.{digits}f} ({min(vals):.{digits}f}-{max(vals):.{digits}f})"
    return f"{m:.{digits}f}"


def pcts(trials, key):
    rs = [t[key] for t in trials if t[key]]
    if not rs:
        return "-"
    return "/".join(f"{st.median(r[k] for r in rs):.0f}" for k in ("p50", "p99", "p999"))


def table(groups, names):
    out = ["| Cell | Trials | Payload MB/s | Wire MB/s | Append MB/s | Cores | UDP drops/s | Shed % "
           "| Ack p50/p99/p999 us | Delivery p50/p99/p999 us |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    for g in names:
        ts = groups[g]
        ok = [t for t in ts if t["done"]]
        durable = any(t["durable"] for t in ts)
        out.append("| " + " | ".join([
            g, f"{len(ok)}/{len(ts)}",
            mid([t["payload"] for t in ok]), mid([t["wire"] for t in ok]),
            mid([t["append"] for t in ok]) if durable else "-",
            mid([t["cores"] for t in ok], 2), mid([t["drops"] for t in ok]),
            mid([t["shed"] for t in ok], 1), pcts(ok, "ack"), pcts(ok, "del"),
        ]) + " |")
    return "\n".join(out)


def gate(groups):
    rows = ["| Shape | preview.9 MB/s | This build MB/s | Change |", "|---|---|---|---|"]
    for g in sorted(groups):
        if not g.startswith("s0-new-"):
            continue
        old = groups.get("s0-p9-" + g[len("s0-new-"):])
        if not old:
            continue
        key = "append" if any(t["durable"] for t in groups[g]) else "payload"
        n = [t[key] for t in groups[g] if t["done"] and t[key]]
        o = [t[key] for t in old if t["done"] and t[key]]
        if not n or not o:
            continue
        rows.append(f"| {g[len('s0-new-'):]} | {st.median(o):.0f} | {st.median(n):.0f} | "
                    f"{100 * (st.median(n) / st.median(o) - 1):+.1f}% |")
    return "\n".join(rows)


def main():
    for arg in sys.argv[1:] or ["."]:
        root = Path(arg)
        groups = load(root)
        if not groups:
            print(f"no cells under {root}/cells\n")
            continue
        print(f"# {root.name}\n")
        if any(g.startswith("s0-new-") for g in groups):
            print("## Gate: this build against preview.9 (payload, or append for durable)\n")
            print(gate(groups) + "\n")
        prefixes = {}
        for g in groups:
            if not g.startswith(("s0-new-", "s0-p9-")):
                prefixes.setdefault(g.split("-", 1)[0], []).append(g)
        for p, names in prefixes.items():
            print(f"## {p}\n")
            print(table(groups, sorted(names)) + "\n")


if __name__ == "__main__":
    main()
