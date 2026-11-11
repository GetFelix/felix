#!/usr/bin/env python3
"""Pass/fail rules of the Raft no-regression gate for the perf cells.

  gate_perf.py compare RESULTS.jsonl [--expect-cells A,B] [--min-rounds N]
                       [--failover-slack FRACTION] [--md OUT] [--json OUT]
  gate_perf.py failover-max OUTPUT.txt

RESULTS has one JSON object per trial:

  {"side": "baseline" | "head", "cell": "rf3-1k", "round": 1,
   "msg_s": 12345.0, "p99_us": 900.0}                 throughput cell
  {"side": ..., "cell": "failover-<variant>", "round": 1, "outage_max_ms": 2100.0}

Each side's figure for a cell is the median over the rounds both sides
completed, so a trial lost on one side drops that round for both. A cell with
fewer than --min-rounds such rounds, or an --expect-cells cell with no rows,
fails: a missing measurement is never a pass. The gate
(docs/replication-engine.md, "The no-regression gate"): throughput within 10%
of the baseline, p99 within 20%, failover time no worse.
"""
import argparse
import json
import re
import statistics
import sys

THROUGHPUT_FLOOR = 0.90
P99_CEILING = 1.20


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


def paired(sides, key):
    """Each side's rows for the rounds in which both sides measured `key`."""
    def by_round(rows):
        return {r.get("round"): r for r in rows if r.get(key) is not None}

    base, head = by_round(sides.get("baseline", [])), by_round(sides.get("head", []))
    common = sorted(base.keys() & head.keys(), key=str)
    return [base[r] for r in common], [head[r] for r in common]


def judge(cells, failover_slack=0.0, expected=(), min_rounds=1):
    """One verdict per (cell, metric): {cell, metric, baseline, head, ratio, rounds, ok}."""
    cells = dict(cells)
    for cell in expected:
        cells.setdefault(cell, {})
    verdicts = []
    for cell, sides in sorted(cells.items()):
        if cell.startswith("failover"):
            metrics = [("outage_max_ms", 1.0 + failover_slack, False)]
        else:
            metrics = [("msg_s", THROUGHPUT_FLOOR, True), ("p99_us", P99_CEILING, False)]
        for key, limit, higher_is_better in metrics:
            base_rows, head_rows = paired(sides, key)
            rounds = len(base_rows)
            base = median(base_rows, key)
            head = median(head_rows, key)
            if base is None or head is None or base <= 0 or rounds < min_rounds:
                ok, ratio = False, None
                if base is not None and head is not None and base > 0:
                    ratio = head / base
            else:
                ratio = head / base
                ok = ratio >= limit if higher_is_better else ratio <= limit
            verdicts.append(
                {"cell": cell, "metric": key, "baseline": base, "head": head,
                 "ratio": ratio, "limit": limit, "rounds": rounds,
                 "min_rounds": min_rounds, "ok": ok}
            )
    return verdicts


def render(verdicts):
    lines = [
        "| Cell | Metric | Baseline | Head | Head / baseline | Limit | Result |",
        "|---|---|---|---|---|---|---|",
    ]
    for v in verdicts:
        fmt = lambda x: "missing" if x is None else f"{x:,.1f}"
        ratio = "n/a" if v["ratio"] is None else f"{v['ratio']:.3f}"
        bound = (">= " if v["metric"] == "msg_s" else "<= ") + f"{v['limit']:.2f}"
        result = "pass" if v["ok"] else "FAIL"
        if v["rounds"] < v["min_rounds"]:
            result += f" ({v['rounds']} of {v['min_rounds']} rounds)"
        lines.append(
            f"| {v['cell']} | {v['metric']} | {fmt(v['baseline'])} | {fmt(v['head'])} "
            f"| {ratio} | {bound} | {result} |"
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
    compare.add_argument("--failover-slack", type=float, default=0.0)
    compare.add_argument("--expect-cells", default="",
                         help="comma-separated cells that must have results")
    compare.add_argument("--min-rounds", type=int, default=1,
                         help="rounds both sides must complete for each cell")
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
    verdicts = judge(load(args.results), args.failover_slack, expected, args.min_rounds)
    if not verdicts:
        sys.exit("no results")
    ok = all(v["ok"] for v in verdicts)
    table = render(verdicts)
    if args.md:
        with open(args.md, "w", encoding="utf-8") as out:
            out.write(table)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as out:
            json.dump({"status": "pass" if ok else "fail", "verdicts": verdicts}, out, indent=1)
    print(table)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
