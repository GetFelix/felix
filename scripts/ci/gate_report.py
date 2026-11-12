#!/usr/bin/env python3
"""Assemble the no-regression gate report from the parts' artifacts.

  gate_report.py ARTIFACT_DIR REPO_DIR [--engine ENGINE] [--sha SHA] [--run-url URL] [--out FILE]

Each part (history, perf, failover, groups, and the priced-only liveness) uploads an artifact `gate-result-<part>-<engine>` holding
`result.json` ({"part", "engine", "status", "summary"}) and `section.md`. A status is
`pass`, `fail`, `unavailable` (the engine does not exist yet), `n/a` (the criterion
does not apply to this engine) or `skipped` (the part was not asked for). The gate passes only when every part passes.
Criterion 7, the size of each protocol, is counted from REPO_DIR and never fails.
The parts in PRICED are shown in the report and never change its verdict.
"""
import argparse
import json
import pathlib
import sys

# Criterion numbers of docs/replication-engine.md, "The no-regression gate".
PARTS = [
    ("history", "1. Correctness"),
    ("perf", "2, 3. Throughput and p99"),
    ("failover", "4, 5. Failover and control plane down"),
    ("groups", "6. Groups per broker"),
]

# Measured and shown, never part of the verdict: features that are off by
# default and are priced before anything turns them on.
PRICED = [
    ("liveness", "Store liveness in shadow mode, on against off (priced only)"),
]


def read_parts(root, engine):
    found = {}
    for result in sorted(pathlib.Path(root).glob("*/result.json")):
        data = json.loads(result.read_text(encoding="utf-8"))
        if data.get("engine") != engine:
            continue
        section = result.parent / "section.md"
        data["section"] = section.read_text(encoding="utf-8") if section.exists() else ""
        data["dir"] = result.parent
        found[data["part"]] = data
    return found


def overall(parts):
    statuses = [parts.get(name, {}).get("status", "missing") for name, _ in PARTS]
    statuses = [s for s in statuses if s != "n/a"]
    if any(s in ("fail", "missing") for s in statuses):
        return "FAIL" if "fail" in statuses else "INCOMPLETE"
    if "unavailable" in statuses:
        return "NOT AVAILABLE"
    if "skipped" in statuses:
        return "INCOMPLETE"
    return "PASS"


def count_lines(paths):
    total = 0
    for path in paths:
        try:
            total += sum(1 for _ in path.open(encoding="utf-8", errors="replace"))
        except OSError:
            pass
    return total


def size_section(repo):
    repo = pathlib.Path(repo)
    src = repo / "crates/server/felix-replication/src"
    rust = [
        p for p in src.rglob("*.rs")
        if "tests" not in p.relative_to(src).parts and p.name != "tests.rs"
    ]
    formal = repo / "docs/formal"
    specs = sorted(formal.glob("FelixShard*.tla"))
    configs = sorted(formal.glob("FelixShard*.cfg"))
    return (
        "### 7. Size (reported only)\n\n"
        f"- `felix-replication/src`, non-test Rust: {count_lines(rust):,} lines in {len(rust)} files\n"
        f"- `docs/formal/FelixShard*.tla`: {count_lines(specs):,} lines in {len(specs)} specs, "
        f"{len(configs)} configs ({count_lines(configs):,} lines)\n"
    )


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("artifacts")
    parser.add_argument("repo")
    parser.add_argument("--engine", default="hybrid")
    parser.add_argument("--sha", default="")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--out", default="gate-report.md")
    args = parser.parse_args(argv)

    parts = read_parts(args.artifacts, args.engine)
    verdict = overall(parts)
    lines = [f"# No-regression gate: {args.engine}", ""]
    if args.sha:
        lines.append(f"Commit `{args.sha}`. " + (f"Run: {args.run_url}" if args.run_url else ""))
        lines.append("")
    lines += [f"**Result: {verdict}**", "", "| Criterion | Status | Summary |", "|---|---|---|"]
    for name, title in PARTS:
        part = parts.get(name)
        lines.append(
            f"| {title} | {part['status'] if part else 'missing'} | "
            f"{(part or {}).get('summary', 'no artifact')} |"
        )
    for name, title in PRICED:
        part = parts.get(name)
        if part:
            lines.append(f"| {title} | {part['status']} | {part.get('summary', '')} |")
    lines.append("")
    for name, _ in PARTS + PRICED:
        part = parts.get(name)
        if part and part["section"].strip():
            lines += [part["section"].rstrip(), ""]
    lines.append(size_section(args.repo))
    pathlib.Path(args.out).write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    return 0 if verdict in ("PASS", "NOT AVAILABLE") else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
