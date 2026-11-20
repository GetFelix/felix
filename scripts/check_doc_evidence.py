#!/usr/bin/env python3
"""Verify that every test a doc cites as evidence actually exists, and that
`file.rs:NNN` line citations still point at the symbol they name.

The architecture docs make normative claims and back them with test names, in
blockquotes like:

    > `a_quorum_acknowledged_record_survives_its_leader` -- the acknowledged
    > record is still there after its leader is killed.

A citation naming a test that no longer exists is worse than no citation: the
claim reads as verified and nothing checks it. Renaming or deleting a test is
easy and updating the prose that cites it is easy to forget, so this closes that
gap rather than relying on remembering.

A blockquote counts as evidence only when it *begins* with a backticked name.
That is how the convention is written, and it is what separates a citation from
an ordinary callout -- `docs/broker-config.md` has blockquotes naming config
keys, and those are not claims about tests.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOCS = [REPO / "docs", REPO / "docs-site" / "src" / "content" / "docs"]

# An evidence line: a blockquote whose content opens with a backticked name.
EVIDENCE = re.compile(r"^\s*>\s*`[a-z_]")
CITATION = re.compile(r"`([a-z_][a-z0-9_]{6,})`")
# A path or a module reference, which names a place rather than a test.
NOT_A_TEST = re.compile(r"[./:]")


def cited() -> dict[str, list[str]]:
    """Every test name cited, mapped to where it was cited."""
    found: dict[str, list[str]] = {}
    for root in DOCS:
        for path in sorted(root.rglob("*.md")) + sorted(root.rglob("*.mdx")):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                if not EVIDENCE.match(line):
                    continue
                for name in CITATION.findall(line):
                    if NOT_A_TEST.search(name):
                        continue
                    where = f"{path.relative_to(REPO)}:{number}"
                    found.setdefault(name, []).append(where)
    return found


def defined() -> set[str]:
    """Every function defined anywhere in the workspace's Rust sources."""
    out = subprocess.run(
        ["git", "grep", "-hoE", r"fn [a-z_][a-z0-9_]*", "--", "*.rs"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {line[3:] for line in out.splitlines()}


# `file.rs:NNN`, optionally with a path, optionally followed by the backticked
# symbol it points at: `ship.rs:20` `ship_once`.
LINE_CITATION = re.compile(
    r"`((?:[\w.-]+/)*[\w.-]+\.rs):(\d+)`(?:[ ,]+(?:`(?:fn |struct |enum |trait )?([A-Za-z_][\w:]*)`))?"
)
# A table row whose first cell names the symbol: | `quorum::ShardServing` | `quorum.rs:991` |
ROW_SYMBOL = re.compile(r"^\|\s*`([A-Za-z_][\w:]*)`\s*\|")


# Citations into a dependency pinned at a revision (the consensus engine), not
# into this repo, so there is no tracked file to check them against.
EXTERNAL = ("engine/engine_impl.rs", "engine/engine_config.rs")


def tracked_rs() -> dict[str, list[Path]]:
    """Every tracked Rust file, keyed by its repo-relative path."""
    out = subprocess.run(
        ["git", "ls-files", "--", "*.rs"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout
    return {line: [REPO / line] for line in out.splitlines()}


def resolve(cited_path: str, files: dict[str, list[Path]]) -> list[Path]:
    """Files a cited path can mean: a path suffix match, so `driver.rs` finds every driver.rs."""
    return [f[0] for rel, f in files.items() if rel == cited_path or rel.endswith("/" + cited_path)]


def check_lines() -> tuple[int, list[str]]:
    """Check `file.rs:NNN` citations: the file exists, the line is in it, and a
    symbol named alongside the citation is on that line. A citation with no
    symbol can only be range-checked; cite one so drift is caught."""
    files = tracked_rs()
    cache: dict[Path, list[str]] = {}
    problems: list[str] = []
    total = 0
    for root in DOCS:
        for path in sorted(root.rglob("*.md")) + sorted(root.rglob("*.mdx")):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                row = ROW_SYMBOL.match(line)
                for index, m in enumerate(LINE_CITATION.finditer(line)):
                    first = index == 0
                    total += 1
                    where = f"{path.relative_to(REPO)}:{number}"
                    cited_path, at, symbol = m.group(1), int(m.group(2)), m.group(3)
                    if symbol is None and row and first and m.start() > row.end():
                        symbol = row.group(1)
                    if cited_path in EXTERNAL:
                        continue
                    candidates = resolve(cited_path, files)
                    if not candidates:
                        problems.append(f"{where}: cites {cited_path}:{at}, no such file")
                        continue
                    leaf = symbol.split("::")[-1] if symbol else None
                    ok = False
                    for c in candidates:
                        lines = cache.setdefault(c, c.read_text().splitlines())
                        if at > len(lines):
                            continue
                        if leaf is None or re.search(rf"\b{re.escape(leaf)}\b", lines[at - 1]):
                            ok = True
                    if not ok:
                        what = f"`{leaf}` is not on line {at}" if leaf else f"line {at} is past the end"
                        problems.append(f"{where}: cites {cited_path}:{at}, but {what}")
    return total, problems


def main() -> int:
    names = cited()
    known = defined()
    missing = {name: where for name, where in names.items() if name not in known}

    for name, where in sorted(missing.items()):
        for place in where:
            print(f"\033[31mFAIL\033[0m {place}: cites `{name}`, which no test defines")
    print(f"{len(names)} cited test(s) checked, {len(missing)} missing")

    total, problems = check_lines()
    for problem in problems:
        print(f"\033[31mFAIL\033[0m {problem}")
    print(f"{total} line citation(s) checked, {len(problems)} stale")
    return 1 if missing or problems else 0


if __name__ == "__main__":
    sys.exit(main())
