#!/usr/bin/env python3
"""Compare broker instructions per message with the committed baseline.

  ipm_gate.py check  RESULT.json [--allow-big-drop]
                                   exit 1 if the measurement failed, a cell the baseline lists (or the
                                   idle-MQTT cell) is missing, a cell has fewer reps than the baseline's,
                                   or a cell is over by more than the tolerance or more than 30% under
                                   (--allow-big-drop for a real win); exit 3 if the baseline was
                                   recorded at another pace (refresh it; not a regression); exit 4 if
                                   a cell is more than the tolerance under the baseline (the PR that
                                   earned the gain lowers the baseline)
  ipm_gate.py update [--commit SHA] [--run-ids ID,ID,...] RESULT.json...
                                   rewrite the baseline's section for this architecture from the
                                   median of at least 5 runs of one commit; --commit refuses a result
                                   measured at any other commit

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

# Exit code of a check whose baseline was taken at another pace. It is not 1,
# which means a regression or a failed measurement.
EXIT_STALE_BASELINE = 3

# Exit code of a check with a cell more than the tolerance below its baseline.
EXIT_IMPROVED = 4

# Reps a cell runs per run, and the fewest a baseline or a check accepts: a
# median of two is not a median of five.
MIN_REPS = 5

# Keys of an architecture's section that are not cells.
META = ("commit", "toolchain", "runs", "reps", "run_ids", "cpus", "pace")

# Baseline cells that have an idle-MQTT twin the gate requires in every run.
MQTT_PAIRED = ("b64",)

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


def reps_of(cell):
    return len(cell.get("reps") or [])


def cpus_of(results):
    return sorted({r["cpu"] for r in results if r.get("cpu")})


def render(arch, meta, values, pace):
    """One architecture's section. `meta` holds commit, toolchain, runs and,
    when known, reps, run_ids and cpus."""
    lines = [f"[{arch}]", f"commit = {json.dumps(meta['commit'])}", f"toolchain = {json.dumps(meta['toolchain'])}"]
    lines.append(f"runs = {meta['runs']}")
    if meta.get("reps") is not None:
        lines.append(f"reps = {meta['reps']}")
    if meta.get("run_ids"):
        lines.append(f"run_ids = [{', '.join(str(i) for i in meta['run_ids'])}]")
    if meta.get("cpus"):
        lines.append(f"cpus = [{', '.join(json.dumps(c) for c in meta['cpus'])}]")
    lines += [f"{name} = {round(value)}" for name, value in sorted(values.items())]
    if pace:
        lines += ["", f"[{arch}.pace]"] + [f"{name} = {rate}" for name, rate in sorted(pace.items())]
    return "\n".join(lines)


def result_meta(result):
    reps = min((reps_of(c) for c in result["cells"].values()), default=0)
    return {
        "commit": result["commit"],
        "toolchain": result["toolchain"],
        "runs": 1,
        "reps": reps or None,
        "cpus": cpus_of([result]),
    }


def render_result(result):
    values = {
        name: cell["user_per_message"]
        for name, cell in result["cells"].items()
        if finite(cell.get("user_per_message")) and not is_paired(name)
    }
    return render(result["arch"], result_meta(result), values, paces(result))


def median(values):
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def finite(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def measured_rate(cell):
    """The rate the load generator actually issued, from the reps."""
    rates = [r.get("issued_msg_s") for r in cell.get("reps") or [] if finite(r.get("issued_msg_s"))]
    return median(rates) if rates else None


def describe_cells(result, section):
    """Each cell's count and issued rate, against the baseline where it has one."""
    lines = []
    for name, cell in sorted(result["cells"].items()):
        got = cell.get("user_per_message")
        if not finite(got):
            lines.append(f"- {name}: no count")
            continue
        text = f"- {name}: {got:.0f} instructions/message"
        want = section.get(name) if section else None
        if finite(want):
            text += f" against a baseline of {want} ({(got - want) / want * 100.0:+.2f}%)"
        rate = measured_rate(cell)
        if rate is not None:
            text += f", issued {rate:.2f} msg/s of {cell.get('pace_msg_s')}"
        lines.append(text)
    return "\n".join(lines)


def check(result, allow_big_drop):
    base = load_baseline()
    arch = result["arch"]
    tol = float(base["tolerance_pct"])
    drop_limit = float(base.get("max_drop_pct", 30.0))
    if not result.get("cells"):
        summary("The result has no cells.")
        return 1
    # A rep that could not hold its pace, or reps that slowed one after
    # another, did not measure what the baseline did. The counts are printed
    # anyway: a rep that slowed because the broker got much costlier looks the
    # same, and only the count tells the two apart.
    if result.get("measurement_failures"):
        for msg in result["measurement_failures"]:
            print(f"::error::measurement failed: {msg}")
        section = base.get(arch)
        summary(
            f"### Broker instructions per message ({arch}): measurement failed\n\n"
            "The run did not hold its pace, so it is not a clean comparison with the baseline:\n\n"
            + "\n".join(f"- {msg}" for msg in result["measurement_failures"])
            + "\n\nWhat it did measure:\n\n"
            + describe_cells(result, section)
            + "\n\nA slow runner misses the pace and rerunning once clears it. A pace failure that repeats "
            "may be a real slowdown, too large for the pace to absorb: compare the counts above with the "
            "baseline, and if they are well over it, treat it as a regression."
        )
        return 1
    if arch not in base:
        summary(f"No baseline for {arch} in {BASELINE.name}. Proposed section (from one run; a baseline needs {MIN_BASELINE_RUNS}):\n\n```toml\n{render_result(result)}\n```")
        return 1
    section = base[arch]
    expected = [k for k in section if k not in META]
    if not expected:
        summary(f"The {arch} baseline has no cells.")
        return 1
    # Every cell the baseline lists must be measured, and so must the idle-MQTT
    # twin of each cell that has one: a harness edit that drops a cell must not
    # turn into a pass.
    required = expected + [n + PAIRED_SUFFIX for n in expected if n in MQTT_PAIRED]
    missing = [n for n in required if n not in result["cells"]]
    if missing:
        for name in missing:
            print(f"::error::{name}: the run produced no measurement for this cell")
        summary(
            f"### Broker instructions per message ({arch}): cells missing\n\n"
            + "\n".join(f"- {name}: the baseline requires this cell and the run has none" for name in missing)
        )
        return 1
    want_pace = section.get("pace", {})
    if paces(result) != want_pace:
        msg = (
            f"baseline predates the fixed pace: refresh needed. This run paced {paces(result)} "
            f"(messages a second) but the {arch} baseline was recorded at {want_pace}. Per-second "
            f"work differs between the two, so they are not comparable. This is not a regression: "
            f"run scripts/perf/ipm_refresh.sh (see CONTRIBUTING.md)."
        )
        print(f"::error title=Baseline needs a refresh, not a regression::{msg}")
        summary(f"### Broker instructions per message ({arch}): baseline needs a refresh\n\n{msg}")
        return EXIT_STALE_BASELINE
    rows = ["| cell | baseline | measured (median rep) | change | rep spread |", "|---|---|---|---|---|"]
    failed, improved = [], []
    want_reps = int(section.get("reps", MIN_REPS))
    for name in sorted(required):
        cell = result["cells"][name]
        if reps_of(cell) < want_reps:
            failed.append(f"{name}: measured over {reps_of(cell)} reps, the baseline's is {want_reps}; a median of fewer is not comparable")
    for name in sorted(expected):
        want = section[name]
        cell = result["cells"][name]
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
            improved.append(f"{name}: {got:.0f} instructions/message is {change:+.2f}% under the baseline {want} (tolerance {tol}%)")
    for name in sorted(n for n in set(result["cells"]) - set(expected) if not is_paired(n)):
        failed.append(f"{name}: measured but has no baseline value")
    for name in sorted(n for n in result["cells"] if is_paired(n)):
        base_name = name[: -len(PAIRED_SUFFIX)]
        got = result["cells"][name].get("user_per_message")
        want = result["cells"].get(base_name, {}).get("user_per_message")
        if not finite(got) or not finite(want):
            failed.append(f"{name}: needs a measurement for itself and for {base_name} in the same run")
            continue
        if result["cells"][name].get("pace_msg_s") != result["cells"][base_name].get("pace_msg_s"):
            failed.append(f"{name}: paced differently from {base_name}, so the two are not comparable")
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
    for note in improved:
        print(f"::error::improved beyond the tolerance: {note}")
    if failed:
        for msg in failed:
            print(f"::error::{msg}")
        summary(
            "Regression or failed measurement. Remove the cost, or raise the baseline in this PR with a sentence on what the extra instructions buy. "
            f"A baseline comes from {MIN_BASELINE_RUNS} or more workflow_dispatch runs of the PR's head commit, never from one run:\n\n"
            "```\nscripts/perf/ipm_refresh.sh <branch>\n```\n\n"
            f"This run alone would give:\n\n```toml\n{render_result(result)}\n```"
        )
        return 1
    if improved:
        summary(
            "Improved beyond the tolerance: refresh the baseline in this PR. A gain left out of the "
            "baseline lets a later change spend it unseen.\n\n"
            + "\n".join(f"- {note}" for note in improved)
            + "\n\n```\nscripts/perf/ipm_refresh.sh <branch>\n```"
        )
        return EXIT_IMPROVED
    return 0


def update(results, commit=None, run_ids=None):
    if len(results) < MIN_BASELINE_RUNS:
        print(f"a baseline needs at least {MIN_BASELINE_RUNS} runs of one commit, got {len(results)}")
        return 1
    first = results[0]
    arch = first["arch"]
    for key in ("arch", "commit", "toolchain"):
        if any(r[key] != first[key] for r in results):
            print(f"the runs disagree on {key}: {sorted({r[key] for r in results})}")
            return 1
    if commit is not None and first["commit"] != commit:
        print(f"the results were measured at {first['commit']}, not at {commit}")
        return 1
    if run_ids is not None and len(set(run_ids)) != len(results):
        print(f"{len(results)} results but {len(set(run_ids))} distinct run ids")
        return 1
    if any(paces(r) != paces(first) for r in results):
        print("the runs were paced differently")
        return 1
    for r in results:
        if r.get("measurement_failures"):
            print(f"a run's measurement failed, so it cannot be part of a baseline: {r['measurement_failures']}")
            return 1
    reps = min(reps_of(c) for r in results for c in r["cells"].values())
    if reps < MIN_REPS:
        print(f"a baseline needs at least {MIN_REPS} reps in every cell of every run, got {reps}")
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
    ]
    if "max_drop_pct" in base:
        out.append(f'max_drop_pct = {base["max_drop_pct"]}')
    out.append("")
    for key in sorted(sections):
        sec = sections[key]
        cells = {k: v for k, v in sec.items() if k not in META}
        meta = {k: sec.get(k) for k in ("commit", "toolchain", "reps", "run_ids", "cpus")}
        meta["runs"] = sec.get("runs", 1)
        out.append(render(key, meta, cells, sec.get("pace", {})))
        out.append("")
    meta = {
        "commit": first["commit"],
        "toolchain": first["toolchain"],
        "runs": len(results),
        "reps": reps,
        "run_ids": sorted(run_ids) if run_ids else None,
        "cpus": cpus_of(results),
    }
    out.append(render(arch, meta, values, paces(first)))
    BASELINE.write_text("\n".join(out) + "\n")
    print(f"updated {arch} in {BASELINE}")
    return 0


def main():
    argv = sys.argv[1:]
    allow = "--allow-big-drop" in argv
    argv = [a for a in argv if a != "--allow-big-drop"]
    options = {}
    for flag in ("--commit", "--run-ids"):
        if flag in argv:
            i = argv.index(flag)
            if i + 1 >= len(argv):
                print(__doc__)
                return 2
            options[flag] = argv[i + 1]
            del argv[i : i + 2]
    if len(argv) < 2 or argv[0] not in ("check", "update") or (argv[0] == "check" and (len(argv) != 2 or options)):
        print(__doc__)
        return 2
    results = []
    for path in argv[1:]:
        try:
            with open(path) as f:
                result = json.load(f)
        except OSError as e:
            # The measure step died before it wrote anything.
            summary(f"No result to compare: {e}. The measurement did not run to the point of writing one.")
            return 1
        for key in ("arch", "commit", "toolchain"):
            if not result.get(key):
                print(f"{path}: the result has no {key}")
                return 1
        results.append(result)
    if argv[0] == "check":
        return check(results[0], allow)
    run_ids = [int(i) for i in options["--run-ids"].split(",")] if "--run-ids" in options else None
    return update(results, options.get("--commit"), run_ids)


if __name__ == "__main__":
    sys.exit(main())
