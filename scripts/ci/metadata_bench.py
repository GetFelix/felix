#!/usr/bin/env python3
"""Compare runs of the metadata Raft benchmark (docs/metadata-raft-design.md, "Perf gate").

  metadata_bench.py compare BASE.json HEAD.json [--aa BASE2.json] [--floor FRACTION] [--md OUT]
  metadata_bench.py table RESULT.json

Each file is the output of `tests/metadata_bench.rs`: {"cells": {cell: {metric: number}}}.
A metric is better when higher (`ops_per_s`) or lower (everything else that
is a time, size or cost). `--aa` is a second run of the baseline; the gap
between the two runs of one cell is that cell's noise band, and HEAD fails a
cell only when it is worse than BASE by more than the band (at least
--floor, 5% by default). A cell HEAD does not report fails: a missing
measurement is never a pass. `failed` counts and `ok` flags must not get worse.
"""
import argparse
import json
import sys

HIGHER_IS_BETTER = {"ops_per_s"}
SKIP = {"rounds"}


def flatten(path):
    with open(path, encoding="utf-8") as handle:
        cells = json.load(handle)["cells"]
    return {
        f"{cell} {metric}": float(value)
        for cell, metrics in cells.items()
        for metric, value in metrics.items()
        if metric not in SKIP and isinstance(value, (int, float, bool))
    }


def worse_by(metric, base, head):
    """How much worse HEAD is than BASE, as a fraction of BASE. Negative is better."""
    if base == 0:
        return 0.0 if head == 0 else float("inf")
    change = (head - base) / base
    return -change if metric.split()[-1] in HIGHER_IS_BETTER else change


def compare(args):
    base, head = flatten(args.base), flatten(args.head)
    aa = flatten(args.aa) if args.aa else {}
    rows, failed = [], False
    for name in sorted(base):
        metric = name.split()[-1]
        if name not in head:
            rows.append((name, base[name], None, None, None, "FAIL (missing)"))
            failed = True
            continue
        band = args.floor
        if name in aa:
            band = max(band, abs(worse_by(metric, base[name], aa[name])))
        worse = worse_by(metric, base[name], head[name])
        verdict = "FAIL" if worse > band else "ok"
        failed |= verdict == "FAIL"
        rows.append((name, base[name], head[name], worse, band, verdict))
    lines = [
        "| Cell and metric | Base | Head | Worse by | Noise band | |",
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
    lines += [f"| {name} | {value:.4g} |" for name, value in sorted(flatten(args.result).items())]
    print("\n".join(lines))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    cmp_parser = sub.add_parser("compare")
    cmp_parser.add_argument("base")
    cmp_parser.add_argument("head")
    cmp_parser.add_argument("--aa")
    cmp_parser.add_argument("--floor", type=float, default=0.05)
    cmp_parser.add_argument("--md")
    cmp_parser.set_defaults(run=compare)
    table_parser = sub.add_parser("table")
    table_parser.add_argument("result")
    table_parser.set_defaults(run=table)
    args = parser.parse_args()
    sys.exit(args.run(args))


if __name__ == "__main__":
    main()
