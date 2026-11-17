#!/usr/bin/env python3
"""Turn a raft-parity run directory into results.json and results.md and decide
the gate: Raft >= hybrid on every cell, within measured run-to-run noise.

A run that also measured the "baseline" side (a release build, run.sh SIDES) gets a second
gate: the current hybrid and the current Raft must each be no worse than the baseline on
every cell, by the same noise rule. A baseline cell marked n/a (the release cannot run it)
is left out of that gate; a baseline cell that failed to measure fails it.

    summarize.py <run dir> [--strict] [--raft-from <run dir>]

A side missing from the run is reported "not in this run" and is not a failure: the gates
cover only the sides present. --raft-from takes the raft side from another run dir (for the
baseline gate), so a baseline+hybrid run can be judged against a separate raft run.

--strict exits 1 when any cell fails. The files are written either way.
"""
import argparse
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


# What a loadgen document counts as a message in its window, first key present wins:
# acked publishes (ingest, MQTT, Kafka), deliveries (pubsub, subscribe), replayed records
# (resume), consumed records (Kafka tail).
MESSAGE_KEYS = ("acked_records", "received", "replayed", "consumed_records")
# Raft may use at most this much more broker CPU per message than hybrid.
CPU_TOLERANCE = float(os.environ.get("CPU_TOLERANCE", "0.10"))


def cpu_ms_per_kmsg(d):
    """Broker CPU milliseconds per 1000 messages over the measured window, summed over
    every broker: (utime + stime at the end - at the start) / ticks per second, divided
    by the messages the generator counted. `broker_cpu` is {"hz": n, "boxes": {box:
    [ticks before, ticks after]}}. None when a sample or the count is missing or a
    counter went backwards (a restarted broker)."""
    cpu = d.get("broker_cpu")
    if not cpu or not cpu.get("boxes") or not cpu.get("hz"):
        return None
    ticks = 0
    for before, after in cpu["boxes"].values():
        if before is None or after is None or after < before:
            return None
        ticks += after - before
    msgs = next((d[k] for k in MESSAGE_KEYS if d.get(k)), None)
    if not msgs:
        return None
    return ticks / cpu["hz"] * 1e6 / msgs


def read_cpu(cell_dir):
    """The `broker_cpu` of a cell from the cpu-before-box<N>.txt and cpu-after-box<N>.txt
    files run.sh wrote ("ticks=<n> hz=<n>"), or None if the cell has none."""
    boxes, hz = {}, None
    for name in sorted(os.listdir(cell_dir)):
        if not (name.startswith("cpu-before-box") and name.endswith(".txt")):
            continue
        box = name[len("cpu-before-box"):-len(".txt")]
        pair = []
        for phase in ("before", "after"):
            try:
                with open(os.path.join(cell_dir, "cpu-%s-box%s.txt" % (phase, box))) as f:
                    kv = dict(p.split("=", 1) for p in f.read().split() if "=" in p)
                pair.append(int(kv["ticks"]))
                hz = int(kv["hz"])
            except (OSError, KeyError, ValueError):
                pair.append(None)
        boxes[box] = pair
    return {"hz": hz, "boxes": boxes} if boxes else None


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
    "cpu_kmsg": ("broker CPU per 1000 messages (ms)", cpu_ms_per_kmsg, False),
    "first_commit": ("leader kill to first commit (ms)", lambda d: d.get("first_commit_ms"), False),
    "caught_up": ("leader kill to subscriber caught up (ms)", lambda d: d.get("caught_up_ms"), False),
}

# cell id -> (title, [(metric id, gates the verdict)])
LAT = [("ack_p50", True), ("ack_p99", True), ("ack_p999", True), ("msg_s", False), ("failed", True),
       ("cpu_kmsg", True)]
TPUT = [("msg_s", True), ("ack_p99", True), ("ack_p50", False), ("failed", True), ("cpu_kmsg", True)]
# A Kafka or MQTT latency cell: p50 and p99 gate, p999 is reported.
MQTT_LAT = [("ack_p50", True), ("ack_p99", True), ("ack_p999", False), ("msg_s", False),
            ("cpu_kmsg", True)]
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
                        ("deliv_p99", True), ("cpu_kmsg", True)])
CELLS["rf3-kafka-tail"] = ("RF 3, Quorum on_commit, Kafka consumer tailing a steady produce",
                           [("consume_msg_s", True), ("completeness", True), ("rate_attain", True),
                            ("pub_msg_s", False), ("cpu_kmsg", True)])
CELLS["rf3-mqtt-lat"] = ("RF 3, Quorum on_commit, MQTT QoS 1 publish, one connection, one in flight",
                         MQTT_LAT)
CELLS["rf3-mqtt-tail"] = ("RF 3, Quorum on_commit, MQTT subscribers tailing a steady QoS 1 publish",
                          [("deliv_msg_s", True), ("rate_attain", True), ("pub_msg_s", False),
                           ("cpu_kmsg", True)])
CELLS["rf3-resume"] = ("RF 3, Quorum on_commit, resume and replay on the leader under steady produce",
                       [("replay_msg_s", True), ("resume_p50", True), ("resume_p99", True),
                        ("replay_p99", False), ("resume_errors", True), ("short_reads", True),
                        ("bad_first", True), ("rate_attain", True), ("pub_msg_s", False),
                        ("cpu_kmsg", True)])
CELLS["rf3-follower-tail"] = ("RF 3, Quorum on_commit, tail subscribe on a follower under steady produce",
                              [("deliv_msg_s", True), ("deliv_p50", True), ("deliv_p99", True),
                               ("gaps", True), ("rate_attain", True), ("pub_msg_s", False),
                               ("cpu_kmsg", True)])


def mean_sd(vals):
    n = len(vals)
    m = sum(vals) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1)) if n > 1 else None
    return m, sd


def _missing(cur, ref, names):
    """No verdict when a side has no values. An empty reference leaves nothing to judge
    against (N/A); an empty current side against a reference that has data is a failure."""
    if not ref:
        return {"verdict": "N/A", "reason": "no baseline data (%s has none)" % names[1],
                "ratio": None, "tolerance": None}
    if not cur:
        return {"verdict": "FAIL", "reason": "no data on %s" % names[0], "ratio": None, "tolerance": None}
    return None


def compare(raft, hybrid, higher, fixed_tolerance=None, names=("raft", "hybrid")):
    """Verdict for one metric from the per-round values of each side. A fixed tolerance
    replaces the noise band: lower-is-better then passes while Raft's mean is at most
    (1 + tolerance) times hybrid's, higher-is-better while it is at least (1 - tolerance).
    `names` labels the two sides in the result: the current engine first, the reference second."""
    missing = _missing(raft, hybrid, names)
    if missing:
        return missing
    rm, rsd = mean_sd(raft)
    hm, hsd = mean_sd(hybrid)
    if not higher and hm == 0 and rm == 0:
        # A count that should be zero (failed sends) and is, on both sides.
        return {"verdict": "PASS", "ratio": 1.0, "tolerance": 0.0, "noise": None, "rounds": len(raft),
                names[0]: {"mean": 0.0, "sd": None, "min": 0, "max": 0, "values": raft},
                names[1]: {"mean": 0.0, "sd": None, "min": 0, "max": 0, "values": hybrid}}
    if rm <= 0 or hm <= 0:
        return {"verdict": "FAIL", "reason": "non-positive mean", "ratio": None, "tolerance": None}
    # Ratio is always Raft's standing: above 1 means Raft is better.
    ratio = rm / hm if higher else hm / rm
    n = min(len(raft), len(hybrid))
    if rsd is None or hsd is None:
        noise = None
    else:
        noise = NOISE_K * math.sqrt((rsd / rm) ** 2 / len(raft) + (hsd / hm) ** 2 / len(hybrid))
    if fixed_tolerance is not None:
        tol = fixed_tolerance
        ok = rm <= hm * (1.0 + tol) if not higher else rm >= hm * (1.0 - tol)
    else:
        tol = max(min(noise or 0.0, NOISE_CAP), NOISE_FLOOR)
        ok = ratio >= 1.0 - tol
    return {"verdict": "PASS" if ok else "FAIL", "ratio": ratio, "tolerance": tol,
            "noise": noise, "rounds": n,
            names[0]: {"mean": rm, "sd": rsd, "min": min(raft), "max": max(raft), "values": raft},
            names[1]: {"mean": hm, "sd": hsd, "min": min(hybrid), "max": max(hybrid), "values": hybrid}}


def _side(vals):
    return {"mean": sum(vals) / len(vals), "sd": None, "min": min(vals), "max": max(vals), "values": vals}


def compare_exact(raft, hybrid, ok, rule, names=("raft", "hybrid")):
    """An absolute rule that has to hold on both sides in every round. Not a ratio:
    a side that breaks it fails the cell however the other side did."""
    missing = _missing(raft, hybrid, names)
    if missing:
        return missing
    held = all(ok(v) for v in raft + hybrid)
    return {"verdict": "PASS" if held else "FAIL", "ratio": None, "tolerance": None, "rule": rule,
            "rounds": min(len(raft), len(hybrid)), names[0]: _side(raft), names[1]: _side(hybrid)}


def judge(mid, cur, ref, higher, names):
    """One metric's verdict: an exact rule for the correctness counters, the noise rule
    for the rest. The same rules apply against hybrid and against the baseline."""
    if mid in MUST_BE_ZERO:
        return compare_exact(cur, ref, lambda v: v == 0, "= 0", names)
    if mid in FLOORS:
        floor = FLOORS[mid]
        return compare_exact(cur, ref, lambda v: v >= floor, ">= %g" % floor, names)
    return compare(cur, ref, higher, CPU_TOLERANCE if mid == "cpu_kmsg" else None, names)


def _read_rows(run):
    with open(os.path.join(run, "cells.tsv")) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_run(run, raft_from=None):
    """Rows, env and per-cell data of a run. `raft_from` is another run dir whose raft rows
    stand in for this run's (a run that had no raft side)."""
    rows = [dict(r, dir=run) for r in _read_rows(run)]
    with open(os.path.join(run, "run.env")) as f:
        env = dict(line.split(" ", 1) for line in f.read().splitlines() if " " in line)
    if raft_from:
        rows = [r for r in rows if r["side"] != "raft"]
        rows += [dict(r, dir=raft_from) for r in _read_rows(raft_from) if r["side"] == "raft"]
        env["raft_from"] = raft_from
        with open(os.path.join(raft_from, "run.env")) as f:
            for line in f.read().splitlines():
                if " " in line:
                    k, v = line.split(" ", 1)
                    env["raft_from." + k] = v
        env["raft_from.rounds"] = str(len({r["round"] for r in rows if r["dir"] == raft_from}))
    env["rounds"] = str(len({r["round"] for r in rows if r["dir"] == run}))
    data = {}
    for r in rows:
        p = os.path.join(r["dir"], "cells", "r%s-%s-%s" % (r["round"], r["cell"], r["side"]), "loadgen.json")
        d = None
        if r["status"] == "ok" and os.path.exists(p):
            with open(p) as f:
                d = json.load(f)
            cpu = read_cpu(os.path.dirname(p))
            if cpu and "broker_cpu" not in d:
                d["broker_cpu"] = cpu
        data.setdefault((r["cell"], r["side"]), []).append((r["round"], r["status"], d))
    return rows, env, data


SIDES = ("hybrid", "raft")
CURRENT = ("hybrid", "raft")


def _values(data, cid, side, get):
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
    return vals


def baseline_state(data, cid):
    """How the baseline side stands on a cell: None (never scheduled), "na" (every round
    marked n/a), "ok", or "incomplete" (a round that was not measured and was not n/a)."""
    rounds = data.get((cid, "baseline"), [])
    if not rounds:
        return None
    if all(st == "na" for _, st, _ in rounds):
        return "na"
    return "ok" if all(d is not None for _, _, d in rounds) else "incomplete"


def summarize(run, raft_from=None):
    rows, env, data = load_run(run, raft_from)
    seen = []
    for r in rows:
        if r["cell"] not in seen:
            seen.append(r["cell"])
    has_baseline = any(r["side"] == "baseline" for r in rows)
    present = {r["side"] for r in rows}
    # Without both hybrid and raft there is no Raft-vs-hybrid comparison to make.
    pair = "hybrid" in present and "raft" in present
    cur = tuple(sd for sd in CURRENT if sd in present)
    cells = []
    for cid in [c for c in CELLS if c in seen] + [c for c in seen if c not in CELLS]:
        title, plan = CELLS.get(cid, (cid, [("msg_s", True)]))
        cell = {"id": cid, "title": title, "metrics": [], "notes": []}
        for side in SIDES + (("baseline",) if has_baseline else ()):
            bad = [(rd, st) for rd, st, d in data.get((cid, side), []) if d is None and st != "na"]
            if bad:
                cell["notes"].append("%s not measured: %s" % (
                    side, ", ".join("r%s %s" % b for b in bad)))
        bstate = baseline_state(data, cid) if has_baseline else None
        if bstate == "na":
            cell["notes"].append("baseline n/a: the release cannot run this cell")
        sides_here = {r["side"] for r in rows if r["cell"] == cid}
        # With --raft-from only cells both runs contain can be judged against each other.
        partial = bool(raft_from) and pair and not {"hybrid", "raft"} <= sides_here
        if partial:
            cell["notes"].append("not judged: %s is not in both runs" % cid)
        for mid, gates in plan:
            label, get, higher = METRICS[mid]
            side_vals = {side: _values(data, cid, side, get) for side in SIDES}
            if not pair or partial:
                res = {"verdict": "N/A", "reason": "%s not in this run" % (
                    "raft" if "raft" not in (present if not partial else sides_here) else "hybrid"),
                    "ratio": None, "tolerance": None}
                if bstate in ("ok", "incomplete") and not partial:
                    base = _values(data, cid, "baseline", get)
                    res["vs_baseline"] = {
                        side: judge(mid, side_vals[side], base, higher, ("current", "baseline"))
                        for side in cur}
            else:
                res = judge(mid, side_vals["raft"], side_vals["hybrid"], higher, ("raft", "hybrid"))
                if bstate in ("ok", "incomplete"):
                    base = _values(data, cid, "baseline", get)
                    # The baseline is the reference side; a current engine is compared to it
                    # the way Raft is compared to hybrid.
                    res["vs_baseline"] = {
                        side: judge(mid, side_vals[side], base, higher, ("current", "baseline"))
                        for side in cur}
            res.update({"id": mid, "label": label, "higher_is_better": higher, "gates": gates})
            cell["metrics"].append(res)
        gating = [m for m in cell["metrics"] if m["gates"]]
        # Every scheduled round on both sides must have measured: a dropped round is
        # a failure, not a smaller sample.
        incomplete = [(side, rd, st) for side in SIDES
                      for rd, st, d in data.get((cid, side), []) if d is None]
        if incomplete:
            cell["notes"].append("incomplete: a cell with an unmeasured round fails")
        judged = [m for m in gating if m["verdict"] != "N/A"]
        if not pair or partial:
            cell["verdict"] = "N/A"
        elif incomplete or any(m["verdict"] != "PASS" for m in judged):
            cell["verdict"] = "FAIL"
        else:
            cell["verdict"] = "PASS" if judged else "N/A"
        if has_baseline:
            fails = []
            if bstate == "incomplete":
                fails.append("baseline had an unmeasured round")
            for side in cur:
                if incomplete and any(s_ == side for s_, _, _ in incomplete):
                    fails.append("%s had an unmeasured round" % side)
            for m in gating:
                for side, r in (m.get("vs_baseline") or {}).items():
                    if r["verdict"] == "FAIL":
                        fails.append("%s %s" % (side, m["id"]))
            compared = any(r["verdict"] != "N/A" for m in gating for r in (m.get("vs_baseline") or {}).values())
            cell["baseline_verdict"] = ("N/A" if bstate in (None, "na") or partial else
                                        "FAIL" if fails else "PASS" if compared else "N/A")
            cell["baseline_failures"] = fails
        cells.append(cell)
    judged_cells = [c for c in cells if c["verdict"] != "N/A"]
    overall = ("N/A" if not pair or (cells and not judged_cells) else
               "PASS" if cells and all(c["verdict"] == "PASS" for c in judged_cells) else "FAIL")
    res = {"sides": sorted(present), "env": env, "noise_floor": NOISE_FLOOR, "noise_k": NOISE_K, "verdict": overall, "cells": cells}
    warnings = []
    if raft_from and env.get("rounds") != env.get("raft_from.rounds"):
        warnings.append("round counts differ: %s round(s) in the main run, %s in the --raft-from run" % (
            env.get("rounds"), env.get("raft_from.rounds")))
    res["warnings"] = warnings
    if has_baseline:
        judged = [c for c in cells if c["baseline_verdict"] != "N/A"]
        res["baseline_verdict"] = "PASS" if judged and all(c["baseline_verdict"] == "PASS" for c in judged) else "FAIL"
    return res


def fmt(v):
    if v is None:
        return "-"
    return "%.0f" % v if abs(v) >= 100 else "%.2f" % v


def cpu_table(res):
    """Broker CPU per 1000 messages, hybrid against Raft, one row per cell that has it."""
    rows = []
    for c in res["cells"]:
        for m in c["metrics"]:
            if m["id"] == "cpu_kmsg" and m.get("raft") and m.get("hybrid"):
                h, r = m["hybrid"]["mean"], m["raft"]["mean"]
                rows.append("| %s | %s | %s | %.3f | %s |" % (
                    c["id"], fmt(h), fmt(r), r / h if h else float("nan"), m["verdict"]))
    if not rows:
        return []
    return ["Broker CPU per 1000 messages (ms, all brokers; Raft over hybrid must be at most "
            "%.2f):\n" % (1 + CPU_TOLERANCE),
            "| cell | hybrid | raft | raft / hybrid | verdict |", "|---|---|---|---|---|"] + rows + [""]


def baseline_section(res):
    """The "versus the release baseline" tables, or nothing for a run without that side."""
    if "baseline_verdict" not in res:
        return []
    ref = res["env"].get("baseline_ref", "the baseline").strip()
    out = ["## Versus %s\n" % ref,
           "Gate: the current hybrid and the current Raft must each be no worse than %s on every "
           "gating metric, by the same noise rule. Ratios are the current engine's standing (above "
           "1 is better). Verdict: **%s**\n" % (ref, res["baseline_verdict"]),
           "| cell | verdict | failing |", "|---|---|---|"]
    for c in res["cells"]:
        out.append("| %s | %s | %s |" % (c["id"], c["baseline_verdict"],
                                         ", ".join(c["baseline_failures"]) or "-"))
    out.append("")
    for c in res["cells"]:
        rows = [m for m in c["metrics"] if m.get("vs_baseline")]
        if not rows:
            continue
        out.append("### %s: %s\n" % (c["id"], c["baseline_verdict"]))
        out.append("| metric | gates | baseline mean | hybrid ratio | raft ratio | tolerance | hybrid | raft |")
        out.append("|---|---|---|---|---|---|---|---|")
        absent = "not in this run"
        for m in rows:
            vb = m["vs_baseline"]
            gate = "yes" if m["gates"] else "no"
            done = [x for x in vb.values() if x.get("ratio") is not None]
            if not done:
                out.append("| %s | %s | - | - | - | - | %s | %s |" % (
                    m["label"], gate, vb["hybrid"]["verdict"] if "hybrid" in vb else absent,
                    vb["raft"]["verdict"] if "raft" in vb else absent))
                continue
            def v(x):
                return x["verdict"] if m["gates"] else "info"
            def ratio(side):
                x = vb.get(side)
                return absent if x is None else ("-" if x.get("ratio") is None else "%.3f" % x["ratio"])
            def verdict(side):
                return absent if side not in vb else v(vb[side])
            out.append("| %s | %s | %s | %s | %s | %.1f%% | %s | %s |" % (
                m["label"], gate, fmt(done[0]["baseline"]["mean"]), ratio("hybrid"), ratio("raft"),
                max(x["tolerance"] for x in done) * 100, verdict("hybrid"), verdict("raft")))
        out.append("")
    return out


def markdown(res):
    out = ["# Raft vs hybrid parity matrix\n", "Verdict: **%s**\n" % (
        res["verdict"] if res["verdict"] != "N/A" else
        "N/A (needs hybrid and raft; only %s in this run)" % ", ".join(res["sides"])
        if not {"hybrid", "raft"} <= set(res["sides"]) else "N/A (no cell had data on both sides)"),
           "```\n%s\n```\n" % "\n".join("%s %s" % kv for kv in res["env"].items())]
    out.extend("WARNING: %s\n" % w for w in res.get("warnings", []))
    out += [
           "Gate: Raft >= hybrid on every gating metric within the measured noise. The ratio is "
           "Raft's standing (above 1 is better, for latency too). The tolerance is "
           "between %g%% and %g%%, %g standard errors of the ratio. Broker CPU per 1000 messages "
           "is gated apart from that: Raft may use at most %g%% more than hybrid.\n" % (res["noise_floor"] * 100, NOISE_CAP * 100, res["noise_k"],
                                     CPU_TOLERANCE * 100),
           "| cell | verdict |\n|---|---|"]
    for c in res["cells"]:
        out.append("| %s | %s |" % (c["title"], c["verdict"]))
    out.append("")
    out.extend(cpu_table(res))
    out.extend(baseline_section(res))
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
- Hybrid and Raft ran the same binary on the same machines; FELIX_SHARD_ENGINE picks the engine.
  A baseline side, when present, ran the release's own broker and control plane, and the same
  loadgen as every other side.
  The order of the sides rotates by round, each cell on a fresh cluster.
- `throughput` is what the publishers sent and the brokers acknowledged; `delivered
  throughput` (fanout only) counts subscriber deliveries and differs by the fanout factor.
- Batched cells measure a throughput profile; their ack latency is per batch and says
  nothing about request latency. The batch 1 cells are the latency profile.
- A gap that does not pass ships only with its root cause measured and shown to be an
  inherent safety cost.""")
    return "\n".join(out) + "\n"


def main(argv):
    ap = argparse.ArgumentParser(description="Summarize a Raft vs hybrid parity run.")
    ap.add_argument("run", help="run directory holding cells.tsv and run.env")
    ap.add_argument("--raft-from", help="another run dir whose raft rows stand in for this run's")
    ap.add_argument("--strict", action="store_true", help="exit 1 when a verdict is FAIL")
    args = ap.parse_args(argv[1:])
    run = args.run
    res = summarize(run, args.raft_from)
    md = markdown(res)
    with open(os.path.join(run, "results.json"), "w") as f:
        json.dump(res, f, indent=2)
    with open(os.path.join(run, "results.md"), "w") as f:
        f.write(md)
    print(md)
    if args.strict and "FAIL" in (res["verdict"], res.get("baseline_verdict")):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
