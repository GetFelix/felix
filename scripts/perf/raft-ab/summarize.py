#!/usr/bin/env python3
"""Turn a raft-ab run directory into a markdown table: per-round values, the
mean of each side, and the hybrid -> raft change. Usage: summarize.py <run dir>."""
import csv
import json
import os
import sys

run = sys.argv[1]
rows = list(csv.DictReader(open(os.path.join(run, "cells.tsv")), delimiter="\t"))
env = open(os.path.join(run, "run.env")).read().strip()

# (label, getter, higher is better)
INGEST = [
    ("publish throughput (msg/s)", lambda d: d["throughput_msg_s"], True),
    ("publish throughput (MB/s)", lambda d: d["throughput_mb_s"], True),
    ("batch ack p50 (us)", lambda d: (d.get("batch_ack_latency_us") or {}).get("p50"), False),
    ("batch ack p99 (us)", lambda d: (d.get("batch_ack_latency_us") or {}).get("p99"), False),
    ("failed sends", lambda d: d.get("failed_sends"), False),
]
FANOUT = [
    ("publish throughput (msg/s)", lambda d: d["publish_throughput_msg_s"], True),
    ("delivered throughput (msg/s)", lambda d: d["delivered_throughput_msg_s"], True),
    ("ack p50 (us)", lambda d: (d.get("ack_latency_us") or {}).get("p50"), False),
    ("ack p99 (us)", lambda d: (d.get("ack_latency_us") or {}).get("p99"), False),
    ("delivery p99 (us)", lambda d: (d.get("delivery_latency_us") or {}).get("p99"), False),
]
SHAPE = {
    "oncommit": "Quorum, fsync on_commit (the fair comparison)",
    "leader": "Leader, fsync periodic",
    "quorum": "Quorum, fsync periodic",
    "fanout": "Quorum, fsync on_commit, pubsub fanout",
}


def load(r):
    p = os.path.join(run, "cells", "r%s-%s-%s" % (r["round"], r["cell"], r["side"]), "loadgen.json")
    if r["status"] != "ok" or not os.path.exists(p):
        return None
    return json.load(open(p))


rounds = sorted({int(r["round"]) for r in rows})
cells = []
for r in rows:
    if r["cell"] not in cells:
        cells.append(r["cell"])
data = {(int(r["round"]), r["cell"], r["side"]): (r["status"], load(r)) for r in rows}


def fmt(v):
    if v is None:
        return "-"
    return "%.0f" % v if abs(v) >= 100 else "%.2f" % v


print("# Hybrid vs Raft shard engine, RF 3, three GCP build boxes\n")
print("```\n%s\n```\n" % env)
for cell in cells:
    metrics = FANOUT if cell == "fanout" else INGEST
    print("## %s: %s\n" % (cell, SHAPE.get(cell, cell)))
    head = ["metric", "side"] + ["r%d" % r for r in rounds] + ["mean", "hybrid -> raft"]
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    for label, get, _ in metrics:
        means = {}
        for side in ("hybrid", "raft"):
            vals, cols = [], []
            for r in rounds:
                st, d = data.get((r, cell, side), ("missing", None))
                v = None
                if d is not None:
                    try:
                        v = get(d)
                    except (KeyError, TypeError):
                        v = None
                cols.append(fmt(v) if d is not None else st)
                if v is not None:
                    vals.append(float(v))
            means[side] = sum(vals) / len(vals) if vals else None
            delta = ""
            if side == "raft" and means["hybrid"] and means["raft"] is not None:
                delta = "%+.1f%%" % (100.0 * (means["raft"] - means["hybrid"]) / means["hybrid"])
            print("| %s | %s | %s | %s | %s |" % (label, side, " | ".join(cols), fmt(means[side]), delta))
    print()

bad = [r for r in rows if r["status"] != "ok"]
if bad:
    print("Cells not measured: " + ", ".join("r%s-%s-%s (%s)" % (r["round"], r["cell"], r["side"], r["status"]) for r in bad) + "\n")

print("""Caveats:
- Leader acks on Raft wait for the commit until #2436 lands, so the Leader cell compares
  a leader-only ack (hybrid) with a majority ack (Raft).
- The Quorum cell runs fsync periodic, which Raft does not honour yet: a Raft member's log
  fsyncs on commit until the 4e wiring. The on_commit cell is the like-for-like one.
- Shared build boxes (other lanes' disks, page cache, network), not release-grade hardware:
  read the deltas, not the absolute numbers.
- Throughput is what publishers sent and the brokers acknowledged; delivered throughput
  (fanout cell only) counts subscriber deliveries and differs by the fanout factor.
  Batched runs measure a throughput profile; the ack latencies are per batch.""")
