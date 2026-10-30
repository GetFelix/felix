#!/usr/bin/env python3
"""Compare broker instructions per message with the committed baseline.

  ipm_gate.py check  RESULT.json   exit 1 if any cell is over baseline by more than the tolerance
  ipm_gate.py update RESULT.json   rewrite the baseline's section for this architecture

RESULT.json comes from crates/testing/felix-cluster/tests/instructions_per_message.rs.
The count is the instructions the broker executes in user space, as callgrind
reports them; kernel time is not included.
"""
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

BASELINE = Path(__file__).with_name("ipm-baseline.toml")


def load_baseline():
    with open(BASELINE, "rb") as f:
        return tomllib.load(f)


def toolchain():
    return subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()


def summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(text + "\n")
    print(text)


def render(arch, result, commit):
    lines = [f"[{arch}]", f'commit = "{commit}"', f'toolchain = "{toolchain()}"']
    for name, cell in sorted(result["cells"].items()):
        lines.append(f"{name} = {round(cell['user_per_message'])}")
    return "\n".join(lines)


def check(result):
    base = load_baseline()
    arch = result["arch"]
    tol = float(base["tolerance_pct"])
    if arch not in base:
        summary(f"No baseline for {arch} in {BASELINE.name}. Proposed section:\n\n```toml\n{render(arch, result, 'unknown')}\n```")
        return 1
    section = base[arch]
    rows = ["| cell | baseline | measured | change | spread |", "|---|---|---|---|---|"]
    failed, lower = [], []
    for name, cell in sorted(result["cells"].items()):
        want = section.get(name)
        got = cell["user_per_message"]
        if want is None:
            failed.append(f"{name}: no baseline value")
            continue
        change = (got - want) / want * 100.0
        rows.append(
            f"| {name} | {want} | {got:.0f} | {change:+.2f}% | {cell['user_spread_pct']:.2f}% |"
        )
        if change > tol:
            failed.append(f"{name}: {got:.0f} instructions/message is {change:+.2f}% over the baseline {want} (tolerance {tol}%)")
        elif change < -tol:
            lower.append(f"{name}: {change:+.2f}%")
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
            f"Proposed section:\n\n```toml\n{render(arch, result, '<commit>')}\n```"
        )
        return 1
    return 0


def update(result):
    base = load_baseline()
    arch = result["arch"]
    commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
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
    out.append(render(arch, result, commit))
    BASELINE.write_text("\n".join(out) + "\n")
    print(f"updated {arch} in {BASELINE}")
    return 0


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("check", "update"):
        print(__doc__)
        return 2
    with open(sys.argv[2]) as f:
        result = json.load(f)
    return check(result) if sys.argv[1] == "check" else update(result)


if __name__ == "__main__":
    sys.exit(main())
