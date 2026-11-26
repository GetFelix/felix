#!/usr/bin/env python3
"""Diagnostic: rank where one broker arm spends per-message cost that another does not.

Reads `perf script` text dumps named `<arm>__<cell>.b<broker>.txt` and the
profile test's result files `<arm>.json` (for messages per recorded cell),
sums every broker of a cell, and prints, per cell, the symbols, threads and
short call paths whose cost per message differs most between two arms.

    prof_diff.py DIR ARM_A ARM_B [CELL...]
"""

import collections
import glob
import json
import os
import re
import sys

HASH = re.compile(r"::h[0-9a-f]{16}$")


def short(sym):
    sym = HASH.sub("", sym.strip())
    # Generic arguments make Rust names unreadable in a table.
    out, depth = [], 0
    for ch in sym:
        if ch == "<":
            depth += 1
            if depth == 1:
                out.append("<..>")
            continue
        if ch == ">" and depth:
            depth -= 1
            continue
        if depth == 0:
            out.append(ch)
    return "".join(out)[:110]


def fold(path):
    """Per symbol self and inclusive cost, per thread, per leaf path."""
    self_, incl, thread, paths = (collections.Counter() for _ in range(4))
    total = 0
    with open(path, errors="replace") as fh:
        header, frames = None, []

        def flush():
            nonlocal total
            if header is None:
                return
            toks = header.split()
            period = 1
            for t in reversed(toks):
                t = t.rstrip(":")
                if t.isdigit():
                    period = int(t)
                    break
            comm = re.sub(r"[-_]?\d+$", "", toks[0]) if toks else "?"
            total += period
            thread[comm] += period
            names = [short(f) for f in frames] or ["[unknown]"]
            self_[names[0]] += period
            for n in set(names):
                incl[n] += period
            paths[" <- ".join(names[:3])] += period

        for line in fh:
            if not line.strip():
                flush()
                header, frames = None, []
            elif line[0] in " \t":
                parts = line.strip().split(None, 1)
                if len(parts) == 2:
                    frames.append(re.sub(r"\+0x[0-9a-f]+$", "", parts[1].split(" (")[0]))
            else:
                flush()
                header, frames = line, []
        flush()
    return total, self_, incl, thread, paths


def messages(directory, arm, cell):
    path = os.path.join(directory, f"{arm}.json")
    try:
        rows = json.load(open(path))
    except (OSError, ValueError):
        return None
    for row in rows:
        if row.get("label") == cell and row.get("record"):
            return row["messages"]
    return None


def load(directory, arm, cell):
    agg = [0, collections.Counter(), collections.Counter(), collections.Counter(), collections.Counter()]
    files = sorted(glob.glob(os.path.join(directory, f"{arm}__{cell}.b*.txt")))
    for f in files:
        t, s, i, th, p = fold(f)
        agg[0] += t
        for k in range(1, 5):
            agg[k].update((s, i, th, p)[k - 1])
    msgs = messages(directory, arm, cell)
    if not files or not msgs:
        return None
    scale = 1.0 / msgs
    return [agg[0] * scale] + [{k: v * scale for k, v in c.items()} for c in agg[1:]]


def table(title, a, b, gap, unit, n=10):
    rows = sorted(set(a) | set(b), key=lambda k: a.get(k, 0) - b.get(k, 0), reverse=True)[:n]
    print(f"\n  {title}")
    print(f"  | {'A-B':>9} | share | {'A':>9} | {'B':>9} | name")
    for k in rows:
        d = a.get(k, 0) - b.get(k, 0)
        share = d / gap * 100 if gap else 0
        print(f"  | {d / 1e3:8.1f}k | {share:4.0f}% | {a.get(k, 0) / 1e3:8.1f}k | {b.get(k, 0) / 1e3:8.1f}k | {k}")


def main():
    directory, arm_a, arm_b, *cells = sys.argv[1:]
    if not cells:
        cells = sorted({os.path.basename(f).split("__")[1].split(".b")[0]
                        for f in glob.glob(os.path.join(directory, f"{arm_a}__*.txt"))})
    unit = os.environ.get("PROF_UNIT", "samples")
    for cell in cells:
        a, b = load(directory, arm_a, cell), load(directory, arm_b, cell)
        if a is None or b is None:
            print(f"\n== {cell}: missing data for {arm_a if a is None else arm_b}")
            continue
        gap = a[0] - b[0]
        print(f"\n== {cell}: {arm_a} {a[0] / 1e3:.0f}k {unit}/msg, {arm_b} {b[0] / 1e3:.0f}k, gap {gap / 1e3:+.0f}k ({gap / b[0] * 100:+.0f}%)")
        table("threads", a[3], b[3], gap, unit)
        table("self, by symbol", a[1], b[1], gap, unit)
        table("inclusive, by symbol", a[2], b[2], gap, unit, n=15)
        table("leaf <- caller <- caller", a[4], b[4], gap, unit)


if __name__ == "__main__":
    main()
