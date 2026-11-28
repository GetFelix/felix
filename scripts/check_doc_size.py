#!/usr/bin/env python3
"""Keep the system overview short enough to read in ten minutes, and stop
docs/per-shard-raft.md from growing.

The overview is a newcomer's first page; it stays under OVERVIEW_WORDS. The
per-shard-raft ceiling is a ratchet: it is the page's size today, to be lowered
to its target when the page is cut, never raised.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OVERVIEW_WORDS = 1600
PER_SHARD_RAFT_LINES = 8940


def words(text: str) -> int:
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    return len(text.split())


def main() -> int:
    problems = []
    for rel in ("docs/overview.md", "docs-site/src/content/docs/architecture/overview.md"):
        n = words((ROOT / rel).read_text())
        print(f"{rel}: {n} words (limit {OVERVIEW_WORDS})")
        if n > OVERVIEW_WORDS:
            problems.append(f"{rel} is {n} words, over {OVERVIEW_WORDS}")
    lines = len((ROOT / "docs/per-shard-raft.md").read_text().splitlines())
    print(f"docs/per-shard-raft.md: {lines} lines (ceiling {PER_SHARD_RAFT_LINES})")
    if lines > PER_SHARD_RAFT_LINES:
        problems.append(f"docs/per-shard-raft.md grew to {lines} lines, over {PER_SHARD_RAFT_LINES}")
    for p in problems:
        print(f"\033[31mFAIL\033[0m {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
