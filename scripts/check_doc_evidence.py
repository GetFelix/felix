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


# `file.rs:NNN` or a range `file.rs:N-M`, optionally with a path, optionally
# followed by the backticked symbol it points at: `ship.rs:20` `ship_once`.
# A bare `:N` right after a citation (`quorum.rs:37`, `:618`) cites the same file.
LINE_CITATION = re.compile(
    r"`(?:((?:[\w.-]+/)*[\w.-]+\.rs):|:(?=\d))(\d+)(?:-(\d+))?`"
    r"(?:[ ,]+(?:`(?:fn |struct |enum |trait )?([A-Za-z_][\w:]*)`))?"
)
# Text allowed between a citation and a bare follow-on `:N` for it to count.
FOLLOW_GAP = re.compile(r"^(?:[\s,;/]|and|or)*$")
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
    """Files a cited path can mean: the exact path, else every path-suffix match."""
    if cited_path in files:
        return [files[cited_path][0]]
    return [f[0] for rel, f in files.items() if rel.endswith("/" + cited_path)]


def check_lines() -> tuple[int, list[str], list[str]]:
    """Check `file.rs:N` and `file.rs:N-M` citations: the file is unambiguous,
    the lines are in it, and a symbol named alongside the citation is on that
    line (or inside that range). A citation with no symbol can only be
    range-checked; it is reported as a warning so drift stays visible."""
    files = tracked_rs()
    cache: dict[Path, list[str]] = {}
    problems: list[str] = []
    warnings: list[str] = []
    total = 0
    for root in DOCS:
        for path in sorted(root.rglob("*.md")) + sorted(root.rglob("*.mdx")):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                row = ROW_SYMBOL.match(line)
                last_path: str | None = None
                last_end = 0
                first = True
                for m in LINE_CITATION.finditer(line):
                    where = f"{path.relative_to(REPO)}:{number}"
                    cited_path = m.group(1)
                    if cited_path is None:
                        if last_path is None or not FOLLOW_GAP.match(line[last_end : m.start()]):
                            continue
                        cited_path = last_path
                    total += 1
                    at = int(m.group(2))
                    end = int(m.group(3)) if m.group(3) else at
                    symbol = m.group(4)
                    last_path, last_end = cited_path, m.end()
                    if symbol is None and row and first and m.start() > row.end():
                        symbol = row.group(1)
                    first = False
                    if cited_path in EXTERNAL:
                        continue
                    span = f"{at}-{end}" if end != at else f"{at}"
                    if end < at:
                        problems.append(f"{where}: cites {cited_path}:{span}, the range runs backwards")
                        continue
                    candidates = resolve(cited_path, files)
                    if not candidates:
                        problems.append(f"{where}: cites {cited_path}:{span}, no such file")
                        continue
                    if len(candidates) > 1:
                        problems.append(
                            f"{where}: cites {cited_path}:{span}, ambiguous path, cite more of it "
                            f"({len(candidates)} files match)"
                        )
                        continue
                    lines = cache.setdefault(candidates[0], candidates[0].read_text().splitlines())
                    if end > len(lines):
                        problems.append(f"{where}: cites {cited_path}:{span}, but the file has {len(lines)} lines")
                        continue
                    if symbol is None:
                        warnings.append(f"{where}: cites {cited_path}:{span} with no symbol beside it")
                        continue
                    leaf = symbol.split("::")[-1]
                    pattern = re.compile(rf"\b{re.escape(leaf)}\b")
                    if not any(pattern.search(l) for l in lines[at - 1 : end]):
                        where_ = f"on line {at}" if end == at else f"in lines {at}-{end}"
                        problems.append(f"{where}: cites {cited_path}:{span}, but `{leaf}` is not {where_}")
    return total, problems, warnings


def main() -> int:
    names = cited()
    known = defined()
    missing = {name: where for name, where in names.items() if name not in known}

    for name, where in sorted(missing.items()):
        for place in where:
            print(f"\033[31mFAIL\033[0m {place}: cites `{name}`, which no test defines")
    print(f"{len(names)} cited test(s) checked, {len(missing)} missing")

    total, problems, warnings = check_lines()
    for warning in warnings:
        print(f"\033[33mWARN\033[0m {warning}")
    for problem in problems:
        print(f"\033[31mFAIL\033[0m {problem}")
    print(f"{total} line citation(s) checked, {len(problems)} stale, {len(warnings)} without a symbol")
    return 1 if missing or problems else 0


if __name__ == "__main__":
    sys.exit(main())
