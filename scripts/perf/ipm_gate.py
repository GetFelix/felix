#!/usr/bin/env python3
"""Compare broker instructions per message with the committed baseline.

  ipm_gate.py check  RESULT.json [--allow-big-drop]
                                   exit 1 if any baseline cell is missing, over by more than the
                                   tolerance, or more than 30% under (--allow-big-drop for a real win)
  ipm_gate.py update RESULT.json   rewrite the baseline's section for this architecture

RESULT.json comes from crates/testing/felix-cluster/tests/instructions_per_message.rs.
The count is the instructions the broker executes in user space, as callgrind
reports them; kernel time is not included.
"""
import json
import math
import os
import sys
import tomllib
from pathlib import Path

BASELINE = Path(__file__).with_name("ipm-baseline.toml")


def load_baseline():
    with open(BASELINE, "rb") as f:
        return tomllib.load(f)


def summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(text + "\n")
    print(text)


def render(arch, result):
    lines = [f"[{arch}]", f'commit = "{result["commit"]}"', f'toolchain = "{result["toolchain"]}"']
    for name, cell in sorted(result["cells"].items()):
        if finite(cell.get("user_per_message")):
            lines.append(f"{name} = {round(cell['user_per_message'])}")
    return "\n".join(lines)


def finite(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def check(result, allow_big_drop):
    base = load_baseline()
    arch = result["arch"]
    tol = float(base["tolerance_pct"])
    drop_limit = float(base.get("max_drop_pct", 30.0))
    if not result.get("cells"):
        summary("The result has no cells.")
        return 1
    if arch not in base:
        summary(f"No baseline for {arch} in {BASELINE.name}. Proposed section:\n\n```toml\n{render(arch, result)}\n```")
        return 1
    section = base[arch]
    rows = ["| cell | baseline | measured | change | spread |", "|---|---|---|---|---|"]
    failed, lower = [], []
    expected = [k for k in section if k not in ("commit", "toolchain")]
    if not expected:
        summary(f"The {arch} baseline has no cells.")
        return 1
    for name in sorted(expected):
        want = section[name]
        cell = result["cells"].get(name)
        if cell is None:
            failed.append(f"{name}: the run produced no measurement for this baseline cell")
            continue
        got = cell.get("user_per_message")
        if not finite(got) or not finite(want):
            failed.append(f"{name}: measured {got!r} / baseline {want!r} is not a positive finite number")
            continue
        change = (got - want) / want * 100.0
        rows.append(f"| {name} | {want} | {got:.0f} | {change:+.2f}% | {cell.get('user_spread_pct', float('nan')):.2f}% |")
        if change > tol:
            failed.append(f"{name}: {got:.0f} instructions/message is {change:+.2f}% over the baseline {want} (tolerance {tol}%)")
        elif change < -drop_limit and not allow_big_drop:
            failed.append(
                f"{name}: {got:.0f} instructions/message is {change:+.2f}% under the baseline {want}; "
                f"a drop this large is more likely a broken measurement than a gain. "
                f"If it is real, rerun with --allow-big-drop and lower the baseline"
            )
        elif change < -tol:
            lower.append(f"{name}: {change:+.2f}%")
    for name in sorted(set(result["cells"]) - set(expected)):
        failed.append(f"{name}: measured but has no baseline value")
    summary(f"### Broker instructions per message ({arch}, tolerance +{tol}%)\n\n" + "\n".join(rows))
    for note in lower:
        print(f"::warning::instructions per message fell below the baseline ({note}); if the gain is real, lower the baseline in this PR")
        summary(f"Lower than baseline: {note}. Check whether the baseline is stale.")
    if failed:
        for msg in failed:
            print(f"::error::{msg}")
        summary(
            "Regression. Remove the cost, or raise the baseline in this PR with a sentence on what the extra instructions buy:\n\n"
            "```\npython3 scripts/perf/ipm_gate.py update <ipm-result.json from this run's artifact>\n```\n\n"
            f"Proposed section:\n\n```toml\n{render(arch, result)}\n```"
        )
        return 1
    return 0


def update(result):
    base = load_baseline()
    arch = result["arch"]
    for name, cell in result["cells"].items():
        if not finite(cell.get("user_per_message")):
            print(f"cell {name} has no usable value")
            return 1
    sections = {k: v for k, v in base.items() if isinstance(v, dict) and k != arch}
    out = [
        "# Broker instructions per message: written by scripts/perf/ipm_gate.py update.",
        f'tolerance_pct = {base["tolerance_pct"]}',
        "",
    ]
    for key in sorted(sections):
        sec = sections[key]
        out.append(f"[{key}]")
        out.append(f'commit = "{sec["commit"]}"')
        out.append(f'toolchain = "{sec["toolchain"]}"')
        out += [f"{k} = {v}" for k, v in sorted(sec.items()) if k not in ("commit", "toolchain")]
        out.append("")
    out.append(render(arch, result))
    BASELINE.write_text("\n".join(out) + "\n")
    print(f"updated {arch} in {BASELINE}")
    return 0


def main():
    args = [a for a in sys.argv[1:] if a != "--allow-big-drop"]
    allow = "--allow-big-drop" in sys.argv[1:]
    if len(args) != 2 or args[0] not in ("check", "update"):
        print(__doc__)
        return 2
    with open(args[1]) as f:
        result = json.load(f)
    for key in ("arch", "commit", "toolchain"):
        if not result.get(key):
            print(f"the result has no {key}")
            return 1
    return check(result, allow) if args[0] == "check" else update(result)


if __name__ == "__main__":
    sys.exit(main())
