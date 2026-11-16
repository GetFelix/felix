#!/usr/bin/env python3
"""Turn a raft-parity run directory into results.json and results.md and decide
the gate: Raft >= hybrid on every cell, within measured run-to-run noise.

    summarize.py <run dir> [--strict]

--strict exits 1 when any cell fails. The files are written either way.
"""
import csv
import json
import math
import os
import sys

# A cell whose measured noise is smaller than this still gets this much slack:
# three rounds cannot show a spread below it.
NOISE_FLOOR = float(os.environ.get("NOISE_FLOOR", "0.03"))
# Width of the noise band in standard errors of the mean ratio.
NOISE_K = float(os.environ.get("NOISE_K", "2"))
# Noise never buys more than this: a side that swings wildly is a failed measurement,
# not a pass.
NOISE_CAP = float(os.environ.get("NOISE_CAP", "0.10"))

# metric id -> (label, getter on the loadgen JSON, higher is better)
def _lat(key, q):
    return lambda d: (d.get(key) or {}).get(q)


def _ratio(d, num, den):
    return d[num] / d[den] if d.get(num) is not None and d.get(den) else None


# Metrics that are not compared Raft against hybrid: a count that must be zero on both
# engines, or a ratio that must reach a floor on both. Anything else fails the cell.
MUST_BE_ZERO = {"resume_errors", "short_reads", "bad_first", "gaps"}
FLOORS = {"completeness": 1.0, "rate_attain": 0.95}

METRICS = {
    "msg_s": ("throughput (msg/s)", lambda d: d.get("throughput_msg_s"), True),
    "ack_p50": ("ack p50 (us)", _lat("batch_ack_latency_us", "p50"), False),
    "ack_p99": ("ack p99 (us)", _lat("batch_ack_latency_us", "p99"), False),
    "ack_p999": ("ack p999 (us)", _lat("batch_ack_latency_us", "p999"), False),
    "failed": ("failed sends", lambda d: d.get("failed_sends"), False),
    "pub_msg_s": ("publish throughput (msg/s)", lambda d: d.get("publish_throughput_msg_s"), True),
    "deliv_msg_s": ("delivered throughput (msg/s)", lambda d: d.get("delivered_throughput_msg_s"), True),
    "pubsub_ack_p99": ("ack p99 (us)", _lat("ack_latency_us", "p99"), False),
    "deliv_p99": ("delivery p99 (us)", _lat("delivery_latency_us", "p99"), False),
    "deliv_p50": ("delivery p50 (us)", _lat("delivery_latency_us", "p50"), False),
    "gaps": ("offset gaps (dropped records)", lambda d: d.get("gaps"), False),
    "replay_msg_s": ("replayed throughput (msg/s)", lambda d: d.get("replayed_throughput_msg_s"), True),
    "resume_p50": ("resume to first record p50 (us)", _lat("resume_latency_us", "p50"), False),
    "resume_p99": ("resume to first record p99 (us)", _lat("resume_latency_us", "p99"), False),
    "replay_p99": ("resume to last record p99 (us)", _lat("replay_latency_us", "p99"), False),
    "resume_errors": ("failed resumes", lambda d: d.get("errors"), False),
    "short_reads": ("short reads", lambda d: d.get("short_reads"), False),
    "bad_first": ("first records below the resume offset", lambda d: d.get("bad_first_offsets"), False),
    "completeness": ("consumed / produced records", lambda d: _ratio(d, "consumed_records", "produced_records"), True),
    "rate_attain": ("publisher rate / target", lambda d: _ratio(d, "publish_throughput_msg_s", "publish_target_msg_s"), True),
    "consume_msg_s": ("consumed throughput (msg/s)", lambda d: d.get("consumed_throughput_msg_s"), True),
    "first_commit": ("leader kill to first commit (ms)", lambda d: d.get("first_commit_ms"), False),
    "caught_up": ("leader kill to subscriber caught up (ms)", lambda d: d.get("caught_up_ms"), False),
}

# cell id -> (title, [(metric id, gates the verdict)])
LAT = [("ack_p50", True), ("ack_p99", True), ("ack_p999", True), ("msg_s", False), ("failed", True)]
TPUT = [("msg_s", True), ("ack_p99", True), ("ack_p50", False), ("failed", True)]
# Kafka and MQTT report no failed-send count.
TPUT_NOFAIL = [m for m in TPUT if m[0] != "failed"]
MODES = [("oncommit", "Quorum, fsync on_commit"), ("quorum", "Quorum, fsync periodic"),
         ("leader", "Leader, fsync periodic")]
CELLS = {}
for topo, topo_name in (("single", "single node"), ("rf3", "RF 3")):
    for mode, mode_name in MODES:
        CELLS["%s-%s-lat" % (topo, mode)] = ("%s, %s, batch 1 latency" % (topo_name, mode_name), LAT)
        CELLS["%s-%s-tput" % (topo, mode)] = ("%s, %s, batched throughput" % (topo_name, mode_name), TPUT)
CELLS["rf3-failover"] = ("RF 3, Quorum on_commit, leader kill recovery",
                         [("first_commit", True), ("caught_up", True)])
CELLS["rf3-kafka"] = ("RF 3, Quorum on_commit, Kafka produce", TPUT)
CELLS["rf3-mqtt"] = ("RF 3, Quorum on_commit, MQTT QoS 1 publish", TPUT_NOFAIL)
CELLS["rf3-fanout"] = ("RF 3, Quorum on_commit, consumer fanout",
                       [("deliv_msg_s", True), ("pub_msg_s", False), ("pubsub_ack_p99", True),
                        ("deliv_p99", True)])
CELLS["rf3-kafka-tail"] = ("RF 3, Quorum on_commit, Kafka consumer tailing a steady produce",
                           [("consume_msg_s", True), ("completeness", True), ("rate_attain", True),
                            ("pub_msg_s", False)])
CELLS["rf3-resume"] = ("RF 3, Quorum on_commit, resume and replay on the leader under steady produce",
                       [("replay_msg_s", True), ("resume_p50", True), ("resume_p99", True),
                        ("replay_p99", False), ("resume_errors", True), ("short_reads", True),
                        ("bad_first", True), ("rate_attain", True), ("pub_msg_s", False)])
CELLS["rf3-follower-tail"] = ("RF 3, Quorum on_commit, tail subscribe on a follower under steady produce",
                              [("deliv_msg_s", True), ("deliv_p50", True), ("deliv_p99", True),
                               ("gaps", True), ("rate_attain", True), ("pub_msg_s", False)])


def mean_sd(vals):
    n = len(vals)
    m = sum(vals) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1)) if n > 1 else None
    return m, sd


def compare(raft, hybrid, higher):
    """Verdict for one metric from the per-round values of each side."""
    if not raft or not hybrid:
        return {"verdict": "FAIL", "reason": "no data on %s" % ("raft" if not raft else "hybrid"),
                "ratio": None, "tolerance": None}
    rm, rsd = mean_sd(raft)
    hm, hsd = mean_sd(hybrid)
    if not higher and hm == 0 and rm == 0:
        # A count that should be zero (failed sends) and is, on both sides.
        return {"verdict": "PASS", "ratio": 1.0, "tolerance": 0.0, "noise": None, "rounds": len(raft),
                "raft": {"mean": 0.0, "sd": None, "min": 0, "max": 0, "values": raft},
                "hybrid": {"mean": 0.0, "sd": None, "min": 0, "max": 0, "values": hybrid}}
    if rm <= 0 or hm <= 0:
        return {"verdict": "FAIL", "reason": "non-positive mean", "ratio": None, "tolerance": None}
    # Ratio is always Raft's standing: above 1 means Raft is better.
    ratio = rm / hm if higher else hm / rm
    n = min(len(raft), len(hybrid))
    if rsd is None or hsd is None:
        noise = None
    else:
        noise = NOISE_K * math.sqrt((rsd / rm) ** 2 / len(raft) + (hsd / hm) ** 2 / len(hybrid))
    tol = max(min(noise or 0.0, NOISE_CAP), NOISE_FLOOR)
    ok = ratio >= 1.0 - tol
    return {"verdict": "PASS" if ok else "FAIL", "ratio": ratio, "tolerance": tol,
            "noise": noise, "rounds": n,
            "raft": {"mean": rm, "sd": rsd, "min": min(raft), "max": max(raft), "values": raft},
            "hybrid": {"mean": hm, "sd": hsd, "min": min(hybrid), "max": max(hybrid), "values": hybrid}}


def _side(vals):
    return {"mean": sum(vals) / len(vals), "sd": None, "min": min(vals), "max": max(vals), "values": vals}


def compare_exact(raft, hybrid, ok, rule):
    """An absolute rule that has to hold on both sides in every round. Not a ratio:
    a side that breaks it fails the cell however the other side did."""
    if not raft or not hybrid:
        return {"verdict": "FAIL", "reason": "no data on %s" % ("raft" if not raft else "hybrid"),
                "ratio": None, "tolerance": None}
    held = all(ok(v) for v in raft + hybrid)
    return {"verdict": "PASS" if held else "FAIL", "ratio": None, "tolerance": None, "rule": rule,
            "rounds": min(len(raft), len(hybrid)), "raft": _side(raft), "hybrid": _side(hybrid)}


def load_run(run):
    with open(os.path.join(run, "cells.tsv")) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    with open(os.path.join(run, "run.env")) as f:
        env = dict(line.split(" ", 1) for line in f.read().splitlines() if " " in line)
    data = {}
    for r in rows:
        p = os.path.join(run, "cells", "r%s-%s-%s" % (r["round"], r["cell"], r["side"]), "loadgen.json")
        d = None
        if r["status"] == "ok" and os.path.exists(p):
            with open(p) as f:
                d = json.load(f)
        data.setdefault((r["cell"], r["side"]), []).append((r["round"], r["status"], d))
    return rows, env, data


def summarize(run):
    rows, env, data = load_run(run)
    seen = []
    for r in rows:
        if r["cell"] not in seen:
            seen.append(r["cell"])
    cells = []
    for cid in [c for c in CELLS if c in seen] + [c for c in seen if c not in CELLS]:
        title, plan = CELLS.get(cid, (cid, [("msg_s", True)]))
        cell = {"id": cid, "title": title, "metrics": [], "notes": []}
        for side in ("hybrid", "raft"):
            bad = [(rd, st) for rd, st, d in data.get((cid, side), []) if d is None]
            if bad:
                cell["notes"].append("%s not measured: %s" % (
                    side, ", ".join("r%s %s" % b for b in bad)))
        for mid, gates in plan:
            label, get, higher = METRICS[mid]
            side_vals = {}
            for side in ("hybrid", "raft"):
                vals = []
                for _, _, d in data.get((cid, side), []):
                    v = None
                    if d is not None:
                        try:
                            v = get(d)
                        except (KeyError, TypeError, AttributeError):
                            v = None
                    if v is not None:
                        vals.append(float(v))
                side_vals[side] = vals
            if mid in MUST_BE_ZERO:
                res = compare_exact(side_vals["raft"], side_vals["hybrid"], lambda v: v == 0, "= 0")
            elif mid in FLOORS:
                floor = FLOORS[mid]
                res = compare_exact(side_vals["raft"], side_vals["hybrid"], lambda v: v >= floor,
                                    ">= %g" % floor)
            else:
                res = compare(side_vals["raft"], side_vals["hybrid"], higher)
            res.update({"id": mid, "label": label, "higher_is_better": higher, "gates": gates})
            cell["metrics"].append(res)
        gating = [m for m in cell["metrics"] if m["gates"]]
        # Every scheduled round on both sides must have measured: a dropped round is
        # a failure, not a smaller sample.
        incomplete = [(side, rd, st) for side in ("hybrid", "raft")
                      for rd, st, d in data.get((cid, side), []) if d is None]
        if incomplete:
            cell["notes"].append("incomplete: a cell with an unmeasured round fails")
        cell["verdict"] = "PASS" if (gating and not incomplete
                                     and all(m["verdict"] == "PASS" for m in gating)) else "FAIL"
        cells.append(cell)
    overall = "PASS" if cells and all(c["verdict"] == "PASS" for c in cells) else "FAIL"
    return {"env": env, "noise_floor": NOISE_FLOOR, "noise_k": NOISE_K, "verdict": overall, "cells": cells}


def fmt(v):
    if v is None:
        return "-"
    return "%.0f" % v if abs(v) >= 100 else "%.2f" % v


def markdown(res):
    out = ["# Raft vs hybrid parity matrix\n", "Verdict: **%s**\n" % res["verdict"],
           "```\n%s\n```\n" % "\n".join("%s %s" % kv for kv in res["env"].items()),
           "Gate: Raft >= hybrid on every gating metric within the measured noise. The ratio is "
           "Raft's standing (above 1 is better, for latency too). The tolerance is "
           "between %g%% and %g%%, %g standard errors of the ratio.\n" % (res["noise_floor"] * 100, NOISE_CAP * 100, res["noise_k"]),
           "| cell | verdict |\n|---|---|"]
    for c in res["cells"]:
        out.append("| %s | %s |" % (c["title"], c["verdict"]))
    out.append("")
    for c in res["cells"]:
        out.append("## %s: %s\n" % (c["id"], c["verdict"]))
        out.append("| metric | gates | hybrid mean (sd) | raft mean (sd) | ratio | tolerance | verdict |")
        out.append("|---|---|---|---|---|---|---|")
        for m in c["metrics"]:
            if m.get("raft") is None:
                if not m["gates"]:
                    continue
                out.append("| %s | %s | - | - | - | - | %s (%s) |" % (
                    m["label"], "yes" if m["gates"] else "no", m["verdict"], m["reason"]))
                continue
            def ms(side):
                s = m[side]
                return "%s (%s)" % (fmt(s["mean"]), fmt(s["sd"]) if s["sd"] is not None else "n=1")
            if m.get("rule"):
                out.append("| %s | %s | %s | %s | - | must be %s | %s |" % (
                    m["label"], "yes" if m["gates"] else "no", ms("hybrid"), ms("raft"),
                    m["rule"], m["verdict"] if m["gates"] else "info"))
                continue
            out.append("| %s | %s | %s | %s | %.3f | %.1f%% | %s |" % (
                m["label"], "yes" if m["gates"] else "no", ms("hybrid"), ms("raft"),
                m["ratio"], m["tolerance"] * 100, m["verdict"] if m["gates"] else "info"))
        for n in c["notes"]:
            out.append("\n%s" % n)
        out.append("")
    out.append("""Notes:
- Both sides ran the same binary on the same machines; FELIX_SHARD_ENGINE picks the engine.
  Odd rounds ran hybrid first, even rounds Raft first, each cell on a fresh cluster.
- `throughput` is what the publishers sent and the brokers acknowledged; `delivered
  throughput` (fanout only) counts subscriber deliveries and differs by the fanout factor.
- Batched cells measure a throughput profile; their ack latency is per batch and says
  nothing about request latency. The batch 1 cells are the latency profile.
- A gap that does not pass ships only with its root cause measured and shown to be an
  inherent safety cost.""")
    return "\n".join(out) + "\n"


def main(argv):
    run = argv[1]
    res = summarize(run)
    md = markdown(res)
    with open(os.path.join(run, "results.json"), "w") as f:
        json.dump(res, f, indent=2)
    with open(os.path.join(run, "results.md"), "w") as f:
        f.write(md)
    print(md)
    if "--strict" in argv and res["verdict"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
