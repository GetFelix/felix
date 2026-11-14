#!/usr/bin/env python3
"""Compare runs of the metadata Raft benchmark (docs/metadata-raft-design.md, "Perf gate").

  metadata_bench.py compare --base B1.json B2.json ... --head H1.json H2.json ... [--floor FRACTION] [--md OUT]
  metadata_bench.py table RESULT.json [RESULT.json ...]

Each file is the output of `tests/metadata_bench.rs`: {"cells": {cell: {metric: number}}}.
Pass several runs per side, taken alternately (base, head, base, head, ...) so
runner drift lands on both. Each metric is the median of its runs. A cell's
noise band is the spread of the base runs ((max - min) / median), never under
--floor (10% by default; 30% for tail latencies). HEAD fails a cell only when it is worse than BASE
by more than the band. A cell HEAD does not report fails: a missing
measurement is never a pass. A metric named with --waive is still shown,
marked "waived", and never fails; each waiver needs a written reason
(docs/metadata-raft-benchmark.md). Throughput, `ok` and `rounds` are better higher;
everything else (times, sizes, costs, `failed`, `failed_rounds`) is better lower.
"""
import argparse
import json
import statistics
import sys

HIGHER_IS_BETTER = {"ops_per_s", "ok", "rounds"}
# A tail over a few hundred samples moves a lot between identical runs, so
# these get a wider floor than the means and medians.
TAIL = {"p99_ms", "max_ms", "longest_write_ms"}
TAIL_FLOOR = 0.30


def flatten(path):
    with open(path, encoding="utf-8") as handle:
        cells = json.load(handle)["cells"]
    return {
        f"{cell} {metric}": float(value)
        for cell, metrics in cells.items()
        for metric, value in metrics.items()
        if isinstance(value, (int, float, bool))
    }


def collect(paths):
    """Per metric, the value from every run that reports it."""
    runs = {}
    for path in paths:
        for name, value in flatten(path).items():
            runs.setdefault(name, []).append(value)
    return runs


def worse_by(metric, base, head):
    """How much worse HEAD is than BASE, as a fraction of BASE. Negative is better."""
    higher = metric.split()[-1] in HIGHER_IS_BETTER
    if base == 0:
        if head == 0:
            return 0.0
        return float("-inf") if higher == (head > 0) else float("inf")
    change = (head - base) / abs(base)
    return -change if higher else change


def compare(args):
    base, head = collect(args.base), collect(args.head)
    rows, failed = [], False
    for name in sorted(base):
        metric = name.split()[-1]
        runs = base[name]
        reference = statistics.median(runs)
        if name not in head:
            rows.append((name, reference, None, None, None, "FAIL (missing)"))
            failed = True
            continue
        band = max(args.floor, TAIL_FLOOR) if metric in TAIL else args.floor
        if reference != 0:
            band = max(band, (max(runs) - min(runs)) / abs(reference))
        value = statistics.median(head[name])
        worse = worse_by(metric, reference, value)
        verdict = "FAIL" if worse > band else "ok"
        if verdict == "FAIL" and name in args.waive:
            verdict = "waived"
        failed |= verdict == "FAIL"
        rows.append((name, reference, value, worse, band, verdict))
    lines = [
        "| Cell and metric | Base (median) | Head (median) | Worse by | Noise band | |",
        "|---|---|---|---|---|---|",
    ]
    for name, b, h, worse, band, verdict in rows:
        head_text = "missing" if h is None else f"{h:.4g}"
        worse_text = "" if worse is None else f"{worse * 100:+.1f}%"
        band_text = "" if band is None else f"{band * 100:.1f}%"
        lines.append(f"| {name} | {b:.4g} | {head_text} | {worse_text} | {band_text} | {verdict} |")
    text = "\n".join(lines) + "\n"
    print(text)
    if args.md:
        with open(args.md, "w", encoding="utf-8") as out:
            out.write(text)
    return 1 if failed else 0


def table(args):
    lines = ["| Cell and metric | Value |", "|---|---|"]
    medians = {name: statistics.median(runs) for name, runs in collect(args.results).items()}
    lines += [f"| {name} | {value:.4g} |" for name, value in sorted(medians.items())]
    print("\n".join(lines))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    cmp_parser = sub.add_parser("compare")
    cmp_parser.add_argument("--base", nargs="+", required=True)
    cmp_parser.add_argument("--head", nargs="+", required=True)
    cmp_parser.add_argument("--floor", type=float, default=0.10)
    cmp_parser.add_argument("--md")
    cmp_parser.add_argument(
        "--waive",
        action="append",
        default=[],
        metavar="'CELL METRIC'",
        help="a cell metric that is reported but does not fail the compare",
    )
    cmp_parser.set_defaults(run=compare)
    table_parser = sub.add_parser("table")
    table_parser.add_argument("results", nargs="+")
    table_parser.set_defaults(run=table)
    args = parser.parse_args()
    sys.exit(args.run(args))


if __name__ == "__main__":
    main()
