#!/usr/bin/env python3
"""Diagnostic: tabulate interleaved A/B passes of the native profile test.

    ab_table.py DIR ARM...

Reads DIR/<pass>-<arm slug>.json and prints, per cell and arm, instructions
per message summed over brokers (mean of passes, with the spread), mean CPU
per broker and msg/s, then each arm's gap to the first `:hybrid` arm.
"""

import glob
import json
import os
import statistics
import sys


def slug(arm):
    return arm.replace("/", "_").replace(":", "_")


def main():
    directory, *arms = sys.argv[1:]
    data = {}
    cells = []
    for arm in arms:
        for path in sorted(glob.glob(os.path.join(directory, f"*-{slug(arm)}.json"))):
            try:
                rows = json.load(open(path))
            except (OSError, ValueError):
                continue
            for row in rows:
                if row.get("record"):
                    continue
                label = row["label"]
                if label not in cells:
                    cells.append(label)
                brokers = row["brokers"]
                instr = sum(b["perf_per_msg"].get("instructions", 0) or 0 for b in brokers)
                cpu = statistics.mean(b["cpu_pct"] for b in brokers)
                data.setdefault((label, arm), []).append((instr, cpu, row["msg_s"]))
    hybrid = next((a for a in arms if a.endswith(":hybrid")), None)
    print("| cell | arm | instr/msg (passes) | CPU/broker | msg/s | vs hybrid |")
    print("|---|---|---|---|---|---|")
    for cell in cells:
        ref = data.get((cell, hybrid))
        ref_i = statistics.mean(x[0] for x in ref) if ref else None
        ref_c = statistics.mean(x[1] for x in ref) if ref else None
        for arm in arms:
            v = data.get((cell, arm))
            if not v:
                print(f"| {cell} | {arm} | - | - | - | - |")
                continue
            ins = [x[0] for x in v]
            mi = statistics.mean(ins)
            mc = statistics.mean(x[1] for x in v)
            ms = statistics.mean(x[2] for x in v)
            spread = f"{min(ins) / 1e3:.0f}-{max(ins) / 1e3:.0f}k" if len(ins) > 1 else ""
            if ref_i and arm != hybrid and mi:
                gap = f"instr {(mi / ref_i - 1) * 100:+.0f}%, cpu {(mc / ref_c - 1) * 100:+.0f}%"
            elif ref_c and arm != hybrid:
                gap = f"cpu {(mc / ref_c - 1) * 100:+.0f}%"
            else:
                gap = ""
            print(f"| {cell} | {arm} | {mi / 1e3:.0f}k {spread} | {mc:.0f}% | {ms:.0f} | {gap} |")


if __name__ == "__main__":
    main()
