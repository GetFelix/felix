#!/usr/bin/env python3
"""Pass/fail rules of the Raft no-regression gate for the perf cells.

  gate_perf.py compare RESULTS.jsonl [--failover-slack FRACTION] [--md OUT] [--json OUT]
  gate_perf.py failover-max OUTPUT.txt

RESULTS has one JSON object per trial:

  {"side": "baseline" | "head", "cell": "rf3-1k", "round": 1,
   "msg_s": 12345.0, "p99_us": 900.0}                 throughput cell
  {"side": ..., "cell": "failover-<variant>", "round": 1, "outage_max_ms": 2100.0}

Each side's figure for a cell is the median over its rounds. The gate
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


def judge(cells, failover_slack=0.0):
    """One verdict per (cell, metric): {cell, metric, baseline, head, ratio, ok}."""
    verdicts = []
    for cell, sides in sorted(cells.items()):
        if cell.startswith("failover"):
            metrics = [("outage_max_ms", 1.0 + failover_slack, False)]
        else:
            metrics = [("msg_s", THROUGHPUT_FLOOR, True), ("p99_us", P99_CEILING, False)]
        for key, limit, higher_is_better in metrics:
            base = median(sides.get("baseline", []), key)
            head = median(sides.get("head", []), key)
            if base is None or head is None or base <= 0:
                ok, ratio = False, None
            else:
                ratio = head / base
                ok = ratio >= limit if higher_is_better else ratio <= limit
            verdicts.append(
                {"cell": cell, "metric": key, "baseline": base, "head": head,
                 "ratio": ratio, "limit": limit, "ok": ok}
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
        lines.append(
            f"| {v['cell']} | {v['metric']} | {fmt(v['baseline'])} | {fmt(v['head'])} "
            f"| {ratio} | {bound} | {'pass' if v['ok'] else 'FAIL'} |"
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

    verdicts = judge(load(args.results), args.failover_slack)
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
