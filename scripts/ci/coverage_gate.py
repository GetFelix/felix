#!/usr/bin/env python3
"""Line coverage from an lcov file, and the pull request gate on it.

  coverage_gate.py percent LCOV
  coverage_gate.py files LCOV            (per-file {path: [found, hit]} JSON)
  coverage_gate.py check LCOV BASELINE [--tolerance POINTS] [--baseline-files JSON]

Both sides use the same figure (sum of LH over sum of LF), so the comparison
does not depend on how any summary table rounds.
"""
import argparse
import json
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


def per_file(path):
    """{source file: [lines found, lines hit]}, from the same LF/LH records."""
    files = {}
    current = None
    with open(path, encoding="utf-8") as lcov:
        for line in lcov:
            if line.startswith("SF:"):
                current = files.setdefault(line[3:].strip(), [0, 0])
            elif current is not None and line.startswith("LF:"):
                current[0] += int(line[3:])
            elif current is not None and line.startswith("LH:"):
                current[1] += int(line[3:])
    return files


def load_files(path):
    """The baseline summary, or None when absent, empty or unreadable: the
    breakdown is a diagnostic and must never fail the gate."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (TypeError, OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def explain(base, now, top=15):
    """Log lines naming the files whose coverage moved against the baseline."""
    out = []

    def section(title, rows):
        out.append(title)
        out.extend(f"  {delta:+d}  {path}" for delta, path in rows[:top])
        if not rows:
            out.append("  none")

    def missed(f, p):
        return f[p][0] - f[p][1]

    common = base.keys() & now.keys()
    rows = [(missed(now, p) - missed(base, p), p) for p in common]
    section("newly missed lines (PR minus main):",
            sorted([r for r in rows if r[0] > 0], reverse=True))
    rows = [(now[p][1] - base[p][1], p) for p in common]
    section("hit lines lost:", sorted([r for r in rows if r[0] < 0]))
    section("files added (missed lines):",
            sorted(((missed(now, p), p) for p in now.keys() - base.keys()), reverse=True))
    section("files removed (hit lines lost):",
            sorted((-base[p][1], p) for p in base.keys() - now.keys()))
    return out


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
    check.add_argument("--baseline-files")
    sub.add_parser("files").add_argument("lcov")
    args = parser.parse_args()

    if args.cmd == "percent":
        print(f"{percent(args.lcov):.2f}")
        return

    if args.cmd == "files":
        print(json.dumps(per_file(args.lcov), sort_keys=True, separators=(",", ":")))
        return

    now = percent(args.lcov)
    with open(args.baseline, encoding="utf-8") as baseline:
        base = float(baseline.read().strip())
    print(f"line coverage: {now:.2f}% (main: {base:.2f}%, allowed drop: {args.tolerance} points)")
    # Printed on a pass too, so a slow slide shows up before it trips the gate.
    base_files = load_files(args.baseline_files)
    if base_files is not None:
        print("::group::files that moved against main")
        print("\n".join(explain(base_files, per_file(args.lcov))))
        print("::endgroup::")
    if base - now > args.tolerance:
        print(f"::error::line coverage fell {base - now:.2f} points below main", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
