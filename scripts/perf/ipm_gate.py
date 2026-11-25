#!/usr/bin/env python3
"""Compare broker instructions per message with the committed baseline.

  ipm_gate.py check  RESULT.json [--allow-big-drop]
                                   exit 1 if any baseline cell is missing, over by more than the
                                   tolerance, or more than 30% under (--allow-big-drop for a real win)
  ipm_gate.py update RESULT.json [RESULT.json ...]
                                   rewrite the baseline's section for this architecture from the
                                   median of several runs of one commit (three or more)

RESULT.json comes from crates/testing/felix-cluster/tests/instructions_per_message.rs.
The count is the instructions the broker executes in user space, as callgrind
reports them; kernel time is not included. Each cell's figure is the
per-message term of a fit of instructions = a * messages + c * seconds over
reps of different duration, so time-driven work (timers) does not read as
per-message cost on a slow runner.

Besides the tolerance, a cell fails when its unpaced reps slowed down by more
than max_slowdown_pct from first to last (the broker or runner is degrading,
and that is a finding of its own), or when the fit's worst residual is above
the tolerance (the two-term model does not describe the run, so a change of
that size could not be told apart from the misfit).
"""
import json
import math
import os
import statistics
import sys
import tomllib
from pathlib import Path

BASELINE = Path(__file__).with_name("ipm-baseline.toml")
METHOD = "time-fit"
META = ("commit", "toolchain", "method", "runs")


def load_baseline():
    with open(BASELINE, "rb") as f:
        return tomllib.load(f)


def summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(text + "\n")
    print(text)


# A cell with this suffix repeats the cell named by the rest, on a broker with
# the MQTT listener on and idle. It is held to that cell in the same run, so it
# has no baseline value of its own.
PAIRED_SUFFIX = "_mqtt_idle"


def is_paired(name):
    return name.endswith(PAIRED_SUFFIX)


def render(arch, result, runs=1):
    lines = [
        f"[{arch}]",
        f'commit = "{result["commit"]}"',
        f'toolchain = "{result["toolchain"]}"',
        f'method = "{METHOD}"',
        f"runs = {runs}",
    ]
    for name, cell in sorted(result["cells"].items()):
        if finite(cell.get("user_per_message")) and not is_paired(name):
            lines.append(f"{name} = {round(cell['user_per_message'])}")
    return "\n".join(lines)


def detail(cell):
    rate = cell.get("time_ir_per_second", float("nan")) / 1e6
    return (
        f"{rate:.2f} M/s | {cell.get('fit_worst_residual_pct', float('nan')):.2f}% | "
        f"{cell.get('slowdown_pct', float('nan')):+.1f}%"
    )


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
    max_slowdown = float(base.get("max_slowdown_pct", 20.0))
    rows = [
        "| cell | baseline | measured | change | time cost under load | fit residual | slowdown |",
        "|---|---|---|---|---|---|---|",
    ]
    failed, lower = [], []
    if section.get("method") != METHOD:
        failed.append(
            f"the {arch} baseline was recorded with method {section.get('method', 'idle-subtracted minimum')!r}, "
            f"not {METHOD!r}; re-record it from several runs of one commit"
        )
    for name, cell in sorted(result["cells"].items()):
        if cell.get("method") != METHOD:
            failed.append(f"{name}: measured with method {cell.get('method')!r}, not {METHOD!r}")
        slowdown = cell.get("slowdown_pct")
        if isinstance(slowdown, (int, float)) and slowdown > max_slowdown:
            secs = [round(r["secs"], 1) for r in cell.get("reps", []) if not r.get("paced")]
            failed.append(
                f"{name}: the same unpaced rep took {slowdown:+.1f}% longer at the end of the cell than at the start "
                f"({secs} s; bound {max_slowdown}%). The broker or the runner is degrading while it runs; "
                f"the per-message figure has the time removed, but a slowdown like this is a finding of its own"
            )
        residual = cell.get("fit_worst_residual_pct")
        if isinstance(residual, (int, float)) and residual > tol:
            failed.append(
                f"{name}: the time fit misses a rep by {residual:.2f}%, more than the {tol}% tolerance, "
                f"so the figure cannot resolve a change of that size"
            )
    expected = [k for k in section if k not in META]
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
        rows.append(f"| {name} | {want} | {got:.0f} | {change:+.2f}% | {detail(cell)} |")
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
    for name in sorted(n for n in set(result["cells"]) - set(expected) if not is_paired(n)):
        failed.append(f"{name}: measured but has no baseline value")
    for name in sorted(n for n in result["cells"] if is_paired(n)):
        base_name = name[: -len(PAIRED_SUFFIX)]
        got = result["cells"][name].get("user_per_message")
        want = result["cells"].get(base_name, {}).get("user_per_message")
        if not finite(got) or not finite(want):
            failed.append(f"{name}: needs a measurement for itself and for {base_name} in the same run")
            continue
        change = (got - want) / want * 100.0
        rows.append(f"| {name} (against {base_name}, same run) | {want:.0f} | {got:.0f} | {change:+.2f}% | {detail(result['cells'][name])} |")
        if change > tol:
            failed.append(
                f"{name}: {got:.0f} instructions/message is {change:+.2f}% over {base_name} ({want:.0f}) in the same run "
                f"(tolerance {tol}%): an idle MQTT listener is costing the QUIC path"
            )
    summary(f"### Broker instructions per message ({arch}, tolerance +{tol}%)\n\n" + "\n".join(rows))
    for note in lower:
        print(f"::warning::instructions per message fell below the baseline ({note}); if the gain is real, lower the baseline in this PR")
        summary(f"Lower than baseline: {note}. Check whether the baseline is stale.")
    if failed:
        for msg in failed:
            print(f"::error::{msg}")
        summary(
            "Regression. Remove the cost, or raise the baseline in this PR with a sentence on what the extra instructions buy:\n\n"
            "```\npython3 scripts/perf/ipm_gate.py update <ipm-result.json from three or more runs of one commit>\n```\n\n"
            f"Proposed section:\n\n```toml\n{render(arch, result)}\n```"
        )
        return 1
    return 0


def update(results):
    base = load_baseline()
    arch = results[0]["arch"]
    for result in results:
        if (result["arch"], result["commit"], result["toolchain"]) != (arch, results[0]["commit"], results[0]["toolchain"]):
            print("every result must come from the same architecture, commit and toolchain")
            return 1
        for name, cell in result["cells"].items():
            if not finite(cell.get("user_per_message")) or cell.get("method") != METHOD:
                print(f"cell {name} has no usable {METHOD} value")
                return 1
    if len(results) < 3:
        print("a baseline needs the median of at least three runs of one commit")
        return 1
    names = set.intersection(*(set(r["cells"]) for r in results))
    merged = dict(results[0])
    merged["cells"] = {
        name: {"user_per_message": statistics.median(r["cells"][name]["user_per_message"] for r in results)}
        for name in names
    }
    sections = {k: v for k, v in base.items() if isinstance(v, dict) and k != arch}
    out = ["# Broker instructions per message: written by scripts/perf/ipm_gate.py update."]
    out += [f"{k} = {v}" for k, v in base.items() if not isinstance(v, dict)]
    out.append("")
    for key in sorted(sections):
        sec = sections[key]
        out.append(f"[{key}]")
        for k in META:
            if k in sec:
                out.append(f'{k} = "{sec[k]}"' if isinstance(sec[k], str) else f"{k} = {sec[k]}")
        out += [f"{k} = {v}" for k, v in sorted(sec.items()) if k not in META]
        out.append("")
    out.append(render(arch, merged, len(results)))
    BASELINE.write_text("\n".join(out) + "\n")
    print(f"updated {arch} in {BASELINE} from the median of {len(results)} runs")
    return 0


def main():
    args = [a for a in sys.argv[1:] if a != "--allow-big-drop"]
    allow = "--allow-big-drop" in sys.argv[1:]
    if len(args) < 2 or args[0] not in ("check", "update") or (args[0] == "check" and len(args) != 2):
        print(__doc__)
        return 2
    results = []
    for path in args[1:]:
        with open(path) as f:
            result = json.load(f)
        for key in ("arch", "commit", "toolchain"):
            if not result.get(key):
                print(f"{path}: the result has no {key}")
                return 1
        results.append(result)
    return check(results[0], allow) if args[0] == "check" else update(results)


if __name__ == "__main__":
    sys.exit(main())
