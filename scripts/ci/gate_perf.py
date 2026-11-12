#!/usr/bin/env python3
"""Pass/fail rules of the release gate for the perf cells (docs/release-gate.md, section 3).

  gate_perf.py compare RESULTS.jsonl [--mode release|engine] [--rounds N] [--aa]
                       [--expect-cells A,B] [--min-rounds N] [--md OUT] [--json OUT]
  gate_perf.py failover-max OUTPUT.txt

RESULTS has one JSON object per trial:

  {"side": "baseline" | "head" | "baseline_aa", "cell": "rf3-1k", "round": 1,
   "msg_s": 12345.0, "p50_us": 400.0, "p99_us": 900.0}   throughput cell
  {"side": ..., "cell": "failover-<variant>", "round": 1, "outage_max_ms": 2100.0}
  {"side": ..., "cell": "move-<variant>", "round": 1, "pause_max_ms": 800.0}

`baseline_aa` is a second run of the baseline, paired with the first one as the
validity check.

Release mode is the paired rule. Each round gives one ratio, candidate over
baseline. The figure is the median ratio, with the exact order-statistic
interval for a median: 6 rounds read the smallest and largest, 12 the 2nd, 24
the 6th. Throughput passes when the lower end is above 0.97 and fails when the
upper end is below it. p99 passes when the upper end is below 1.10 and fails
when the lower end is above it. Any other lower-is-better figure is turned
over (baseline over candidate) and judged like throughput against its own
bound. Otherwise the cell is undecided; the caller adds rounds (6, 12, 24). A
cell undecided at 24 rounds fails as not measured. A missing or partial round
fails.

Engine mode keeps the engine A/B rule: medians, throughput at least 0.90 and
p99 at most 1.20 of the other engine. Failover and the move pause use the
paired rule in both modes.

Exit status: 0 all pass, 1 a failure, 2 nothing failed but a cell is undecided.
"""
import argparse
import json
import math
import re
import statistics
import sys

# Rounds the paired rule may decide at, and the rank (1-based) of the interval's
# ends: the k-th smallest and k-th largest ratio. The interval covers the median
# with probability 1 - 2 * P(Binomial(n, 1/2) <= k - 1): 96.9%, 99.37%, 99.34%.
RANKS = {6: 1, 12: 2, 24: 6}
MAX_ROUNDS = 24

# The bounds of the engine A/B.
ENGINE_THROUGHPUT_FLOOR = 0.90
ENGINE_P99_CEILING = 1.20

# A metric's rule: (bound, higher_is_better, A/A half-width margin).
THROUGHPUT = (0.97, True, 0.03)
P99 = (1.10, False, 0.10)
# Lower-is-better figures judged like throughput, baseline over candidate.
TURNED_OVER = {"outage_max_ms", "pause_max_ms", "p50_us"}
TURNED_OVER_97 = (0.97, True, 0.03)
TURNED_OVER_95 = (0.95, True, 0.05)


def load(path):
    cells = {}
    with open(path, encoding="utf-8") as lines:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            cells.setdefault(row["cell"], {}).setdefault(row["side"], []).append(row)
    return cells


def median(rows, key):
    values = [row[key] for row in rows if row.get(key) is not None]
    return statistics.median(values) if values else None


def by_round(rows, key):
    return {r.get("round"): r[key] for r in rows if r.get(key) is not None and r[key] > 0}


def interval(ratios):
    """The exact interval for the median of `ratios`, or None off the 6/12/24 ladder."""
    k = RANKS.get(len(ratios))
    if k is None:
        return None
    ordered = sorted(ratios)
    return ordered[k - 1], ordered[-k]


def coverage(n, k):
    """Probability that the k-th smallest to k-th largest of n covers the median."""
    return 1 - 2 * sum(math.comb(n, i) for i in range(k)) / 2**n


def decide(lo, hi, bound, higher_is_better):
    """'pass', 'fail' or 'undecided' for an interval against its bound."""
    if higher_is_better:
        return "pass" if lo > bound else "fail" if hi < bound else "undecided"
    return "pass" if hi < bound else "fail" if lo > bound else "undecided"


def cell_metrics(cell, mode):
    """The (metric key, rule) pairs a cell is judged on."""
    if cell.startswith("failover"):
        return [("outage_max_ms", TURNED_OVER_95)]
    if cell.startswith("move"):
        return [("pause_max_ms", TURNED_OVER_95)]
    if cell.endswith("-b1"):
        # Batch 1: request latency only, no throughput.
        p50 = (0.90, True, 0.10) if mode == "engine" else TURNED_OVER_97
        return [("p50_us", p50), ("p99_us", P99)]
    return [("msg_s", THROUGHPUT), ("p99_us", P99)]


def verdict_row(cell, key, bound, higher_is_better, margin):
    return {"cell": cell, "metric": key, "bound": bound, "higher_is_better": higher_is_better,
            "margin": margin, "baseline": None, "head": None, "median": None, "lo": None,
            "hi": None, "rounds": 0, "planned": 0, "aa_half_width": None,
            "status": "fail", "reason": "", "ok": False}


def judge_paired(cell, sides, key, rule, planned, require_aa):
    bound, higher_is_better, margin = rule
    v = verdict_row(cell, key, bound, higher_is_better, margin)
    base, head = by_round(sides.get("baseline", []), key), by_round(sides.get("head", []), key)
    common = sorted(base.keys() & head.keys(), key=str)
    v["planned"] = planned
    v["rounds"] = len(common)
    if common:
        v["baseline"] = statistics.median(base[r] for r in common)
        v["head"] = statistics.median(head[r] for r in common)
    if not common:
        v["reason"] = "no rounds measured"
        return v
    if len(common) < planned:
        v["reason"] = f"partial: {len(common)} of {planned} rounds measured on both sides"
        return v
    if len(common) not in RANKS:
        v["reason"] = f"{len(common)} rounds; the rule decides at 6, 12 or 24"
        return v
    ratios = [base[r] / head[r] if key in TURNED_OVER else head[r] / base[r] for r in common]
    lo, hi = interval(ratios)
    v["median"], v["lo"], v["hi"] = statistics.median(ratios), lo, hi
    status = decide(lo, hi, bound, higher_is_better)
    if status == "undecided" and len(common) >= MAX_ROUNDS:
        status, v["reason"] = "fail", "undecided at 24 rounds: not measured, rerun on a dedicated runner"
    if status == "pass":
        aa = by_round(sides.get("baseline_aa", []), key)
        aa_common = sorted(aa.keys() & base.keys(), key=str)
        if len(aa_common) == len(common) and len(aa_common) in RANKS:
            a_lo, a_hi = interval([aa[r] / base[r] for r in aa_common])
            v["aa_half_width"] = (a_hi - a_lo) / 2
            if v["aa_half_width"] > margin:
                status = "void"
                v["reason"] = f"A/A half width {v['aa_half_width']:.3f} over {margin:.2f}: runner not steady"
        elif require_aa:
            status, v["reason"] = "void", "no complete A/A pair"
    v["status"] = status
    v["ok"] = status == "pass"
    return v


def judge_engine(cell, sides, key, min_rounds):
    """Engine A/B: medians of the rounds both sides completed."""
    higher = key == "msg_s" or key in TURNED_OVER
    bound = ENGINE_P99_CEILING if key == "p99_us" else ENGINE_THROUGHPUT_FLOOR
    v = verdict_row(cell, key, bound, higher, 0.0)
    base, head = by_round(sides.get("baseline", []), key), by_round(sides.get("head", []), key)
    common = sorted(base.keys() & head.keys(), key=str)
    v["rounds"] = v["planned"] = len(common)
    if not common or len(common) < min_rounds:
        v["reason"] = f"{len(common)} of {min_rounds} rounds"
        return v
    v["baseline"] = statistics.median(base[r] for r in common)
    v["head"] = statistics.median(head[r] for r in common)
    v["median"] = v["baseline"] / v["head"] if key in TURNED_OVER else v["head"] / v["baseline"]
    ok = v["median"] >= bound if higher else v["median"] <= bound
    v["status"], v["ok"] = ("pass" if ok else "fail"), ok
    return v


def judge(cells, mode="release", expected=(), rounds=None, min_rounds=1, require_aa=False):
    """One verdict per (cell, metric). `rounds` is the rounds attempted; by default
    the highest round number seen in the cell, so a round one side lost is partial."""
    cells = dict(cells)
    for cell in expected:
        cells.setdefault(cell, {})
    verdicts = []
    for cell, sides in sorted(cells.items()):
        seen = {r.get("round") for rows in sides.values() for r in rows}
        planned = rounds if rounds is not None else len(seen)
        for key, rule in cell_metrics(cell, mode):
            paired_rule = mode == "release" or cell.startswith(("failover", "move"))
            if paired_rule:
                verdicts.append(judge_paired(cell, sides, key, rule, planned, require_aa))
            else:
                verdicts.append(judge_engine(cell, sides, key, min_rounds))
    return verdicts


def render(verdicts):
    lines = [
        "| Cell | Metric | Baseline | Head | Median ratio | Interval | Bound | Rounds | A/A half width | Result |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    fmt = lambda x: "n/a" if x is None else f"{x:,.1f}"
    ratio = lambda x: "n/a" if x is None else f"{x:.3f}"
    for v in verdicts:
        bound = (">" if v["higher_is_better"] else "<") + f" {v['bound']:.2f}"
        interval_text = "n/a" if v["lo"] is None else f"{v['lo']:.3f} to {v['hi']:.3f}"
        result = "pass" if v["ok"] else v["status"].upper()
        if v["reason"]:
            result += f" ({v['reason']})"
        lines.append(
            f"| {v['cell']} | {v['metric']} | {fmt(v['baseline'])} | {fmt(v['head'])} "
            f"| {ratio(v['median'])} | {interval_text} | {bound} | {v['rounds']} "
            f"| {ratio(v['aa_half_width'])} | {result} |"
        )
    return "\n".join(lines) + "\n"


UNITS = {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1000.0}


def failover_max_ms(text):
    """The outage max in ms from `failover heal: outage: ... max Some(2.1s); ...`."""
    match = re.search(r"failover heal: outage: .*? max Some\(([0-9.]+)(ns|µs|us|ms|s)\)", text)
    if not match:
        return None
    return float(match.group(1)) * UNITS[match.group(2)]


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    compare = sub.add_parser("compare")
    compare.add_argument("results")
    compare.add_argument("--mode", choices=["release", "engine"], default="release")
    compare.add_argument("--rounds", type=int,
                         help="rounds attempted; fewer on both sides is a partial round and fails")
    compare.add_argument("--aa", action="store_true",
                         help="a cell without a complete baseline A/A pair is void")
    compare.add_argument("--expect-cells", default="",
                         help="comma-separated cells that must have results")
    compare.add_argument("--min-rounds", type=int, default=1,
                         help="engine mode: rounds both sides must complete for each cell")
    compare.add_argument("--md")
    compare.add_argument("--json")
    failover = sub.add_parser("failover-max")
    failover.add_argument("output")
    args = parser.parse_args(argv)

    if args.command == "failover-max":
        with open(args.output, encoding="utf-8") as handle:
            value = failover_max_ms(handle.read())
        if value is None:
            sys.exit("no `failover heal: outage:` line with a max in the output")
        print(f"{value:.1f}")
        return 0

    expected = [c for c in args.expect_cells.split(",") if c]
    verdicts = judge(load(args.results), args.mode, expected, args.rounds, args.min_rounds, args.aa)
    if not verdicts:
        sys.exit("no results")
    failed = any(v["status"] in ("fail", "void") for v in verdicts)
    undecided = any(v["status"] == "undecided" for v in verdicts)
    status = "fail" if failed else "undecided" if undecided else "pass"
    table = render(verdicts)
    if args.md:
        with open(args.md, "w", encoding="utf-8") as out:
            out.write(table)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as out:
            json.dump({"status": status, "verdicts": verdicts}, out, indent=1)
    print(table)
    return {"pass": 0, "fail": 1, "undecided": 2}[status]


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
