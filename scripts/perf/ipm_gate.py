#!/usr/bin/env python3
"""Compare broker instructions per message with the committed baseline.

  ipm_gate.py check  RESULT.json [--allow-big-drop]
                                   exit 1 if the measurement failed, the run was paced differently
                                   from the baseline, or any baseline cell is missing, over by more
                                   than the tolerance, or more than 30% under (--allow-big-drop for
                                   a real win)
  ipm_gate.py update RESULT.json...
                                   rewrite the baseline's section for this architecture from the
                                   median of at least 5 runs of one commit

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

# One run's figure moves with the runner it lands on, so a baseline is the
# median of several runs of one commit.
MIN_BASELINE_RUNS = 5

# Keys of an architecture's section that are not cells.
META = ("commit", "toolchain", "runs", "pace")

METHOD = """\
# Each value is the median, over at least five workflow runs of the one commit
# named in its section, of that run's median rep. Every rep runs on a fresh
# broker; every cell is paced at the rate under [<arch>.pace] for a fixed time,
# so the broker's per-second work is the same in every rep and is part of the
# value.
# Raise or lower a value only this way: dispatch the Instructions per message
# workflow five times on one commit and run
#   python3 scripts/perf/ipm_gate.py update <the five ipm-result.json files>
# See CONTRIBUTING.md."""


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


def paces(result):
    """The cells the run paced, and at what rate."""
    return {
        name: cell["pace_msg_s"]
        for name, cell in result["cells"].items()
        if cell.get("pace_msg_s") is not None and not is_paired(name)
    }


def render(arch, commit, toolchain, runs, values, pace):
    lines = [f"[{arch}]", f'commit = "{commit}"', f'toolchain = "{toolchain}"', f"runs = {runs}"]
    lines += [f"{name} = {round(value)}" for name, value in sorted(values.items())]
    if pace:
        lines += ["", f"[{arch}.pace]"] + [f"{name} = {rate}" for name, rate in sorted(pace.items())]
    return "\n".join(lines)


def render_result(result):
    values = {
        name: cell["user_per_message"]
        for name, cell in result["cells"].items()
        if finite(cell.get("user_per_message")) and not is_paired(name)
    }
    return render(result["arch"], result["commit"], result["toolchain"], 1, values, paces(result))


def median(values):
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


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
    # A rep that could not hold its pace, or reps that slowed one after
    # another, did not measure what the baseline did. Saying so apart from a
    # regression keeps a slow runner from reading as a costlier broker.
    if result.get("measurement_failures"):
        for msg in result["measurement_failures"]:
            print(f"::error::measurement failed: {msg}")
        summary(
            f"### Broker instructions per message ({arch}): measurement failed\n\n"
            "This is not a regression. The run did not measure what the baseline did:\n\n"
            + "\n".join(f"- {msg}" for msg in result["measurement_failures"])
        )
        return 1
    if arch not in base:
        summary(f"No baseline for {arch} in {BASELINE.name}. Proposed section (from one run; a baseline needs {MIN_BASELINE_RUNS}):\n\n```toml\n{render_result(result)}\n```")
        return 1
    section = base[arch]
    want_pace = section.get("pace", {})
    if paces(result) != want_pace:
        summary(
            f"The run paced {paces(result)} (messages a second) but the {arch} baseline was "
            f"recorded at {want_pace}. Per-second work differs between the two, so they are not "
            f"comparable. Record a new baseline at the new pace."
        )
        return 1
    rows = ["| cell | baseline | measured (median rep) | change | rep spread |", "|---|---|---|---|---|"]
    failed, lower = [], []
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
        rows.append(f"| {name} (against {base_name}, same run) | {want:.0f} | {got:.0f} | {change:+.2f}% | {result['cells'][name].get('user_spread_pct', float('nan')):.2f}% |")
        if change > tol:
            failed.append(
                f"{name}: {got:.0f} instructions/message is {change:+.2f}% over {base_name} ({want:.0f}) in the same run "
                f"(tolerance {tol}%): an idle MQTT listener is costing the QUIC path"
            )
    summary(
        f"### Broker instructions per message ({arch}, tolerance +{tol}%)\n\n"
        f"CPU: {result.get('cpu', 'not recorded')}\n\n" + "\n".join(rows)
    )
    for note in lower:
        print(f"::warning::instructions per message fell below the baseline ({note}); if the gain is real, lower the baseline in this PR")
        summary(f"Lower than baseline: {note}. Check whether the baseline is stale.")
    if failed:
        for msg in failed:
            print(f"::error::{msg}")
        summary(
            "Regression. Remove the cost, or raise the baseline in this PR with a sentence on what the extra instructions buy. "
            f"A baseline comes from {MIN_BASELINE_RUNS} or more workflow_dispatch runs of the PR's head commit, never from one run:\n\n"
            "```\npython3 scripts/perf/ipm_gate.py update <ipm-result.json from each run's artifact>\n```\n\n"
            f"This run alone would give:\n\n```toml\n{render_result(result)}\n```"
        )
        return 1
    return 0


def update(results):
    if len(results) < MIN_BASELINE_RUNS:
        print(f"a baseline needs at least {MIN_BASELINE_RUNS} runs of one commit, got {len(results)}")
        return 1
    first = results[0]
    arch = first["arch"]
    for key in ("arch", "commit", "toolchain"):
        if any(r[key] != first[key] for r in results):
            print(f"the runs disagree on {key}: {sorted({r[key] for r in results})}")
            return 1
    if any(paces(r) != paces(first) for r in results):
        print("the runs were paced differently")
        return 1
    for r in results:
        if r.get("measurement_failures"):
            print(f"a run's measurement failed, so it cannot be part of a baseline: {r['measurement_failures']}")
            return 1
    names = sorted(n for n in first["cells"] if not is_paired(n))
    values = {}
    print(f"{arch} at {first['commit']}, {len(results)} runs (each the median of its reps):")
    print("  cpus: " + ", ".join(r.get("cpu", "not recorded") for r in results))
    for name in names:
        per_run = [r["cells"].get(name, {}).get("user_per_message") for r in results]
        if not all(finite(v) for v in per_run):
            print(f"cell {name} has no usable value in every run")
            return 1
        values[name] = median(per_run)
        spread = (max(per_run) - min(per_run)) / min(per_run) * 100.0
        listed = ", ".join(f"{v:.0f}" for v in per_run)
        print(f"  {name}: median {values[name]:.0f}, run-to-run spread {spread:.2f}% ({listed})")
    base = load_baseline()
    sections = {k: v for k, v in base.items() if isinstance(v, dict) and k != arch}
    out = [
        "# Broker instructions per message: written by scripts/perf/ipm_gate.py update.",
        METHOD,
        f'tolerance_pct = {base["tolerance_pct"]}',
        "",
    ]
    for key in sorted(sections):
        sec = sections[key]
        cells = {k: v for k, v in sec.items() if k not in META}
        out.append(render(key, sec["commit"], sec["toolchain"], sec.get("runs", 1), cells, sec.get("pace", {})))
        out.append("")
    out.append(render(arch, first["commit"], first["toolchain"], len(results), values, paces(first)))
    BASELINE.write_text("\n".join(out) + "\n")
    print(f"updated {arch} in {BASELINE}")
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
