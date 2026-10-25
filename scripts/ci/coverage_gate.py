#!/usr/bin/env python3
"""Line coverage from an lcov file, and the pull request gate on it.

  coverage_gate.py percent LCOV
  coverage_gate.py check LCOV BASELINE [--tolerance POINTS]

Both sides use the same figure (sum of LH over sum of LF), so the comparison
does not depend on how any summary table rounds.
"""
import argparse
import sys


def totals(path):
    found = hit = 0
    with open(path, encoding="utf-8") as lcov:
        for line in lcov:
            if line.startswith("LF:"):
                found += int(line[3:])
            elif line.startswith("LH:"):
                hit += int(line[3:])
    if found == 0:
        sys.exit(f"{path} has no line data")
    return found, hit


def percent(path):
    found, hit = totals(path)
    return 100.0 * hit / found


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("percent").add_argument("lcov")
    check = sub.add_parser("check")
    check.add_argument("lcov")
    check.add_argument("baseline")
    check.add_argument("--tolerance", type=float, default=0.1)
    args = parser.parse_args()

    if args.cmd == "percent":
        print(f"{percent(args.lcov):.2f}")
        return

    now = percent(args.lcov)
    with open(args.baseline, encoding="utf-8") as baseline:
        base = float(baseline.read().strip())
    print(f"line coverage: {now:.2f}% (main: {base:.2f}%, allowed drop: {args.tolerance} points)")
    if base - now > args.tolerance:
        print(f"::error::line coverage fell {base - now:.2f} points below main", file=sys.stderr)
        sys.exit(1)


main()
