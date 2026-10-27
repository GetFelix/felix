#!/usr/bin/env python3
"""Verify that `reference/metrics.md` lists exactly the metrics the code registers.

A metric name is a string literal, so it is found the same way the environment
variable check finds variables: by searching the Rust source. Two shapes count:
any quoted `felix_*` literal (the named constants and the helper functions that
pass a name on), and the literal first argument of a metrics macro (the
unprefixed stage timers, which have no common prefix to search for).

Tests, benchmarks, demos and the soak binary are skipped: a name there is a
fixture or a reader of someone else's metric, not a registration.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REFERENCE = REPO / "docs-site/src/content/docs/reference/metrics.md"

# Quoted `felix_*` literals that are not metric names: the Python binding's
# helper module and the control plane's token format.
NOT_METRICS = {"felix_token", "felix_token_call"}

EXCLUDE = [
    ":!*/tests/*",
    ":!*/tests.rs",
    ":!*_tests.rs",
    ":!crates/testing",
    ":!*/benches/*",
    ":!demos",
    ":!*/bin/soak/*",
]

LITERAL = re.compile(r'"(felix_[a-z][a-z0-9_]*)"')
MACRO_FIRST_ARG = re.compile(r'\b(?:t_)?(?:counter|gauge|histogram)!\(\s*"([a-z][a-z0-9_]*)"')


def registered() -> set[str]:
    files = subprocess.run(
        ["git", "grep", "-l", "-E", r'"[a-z][a-z0-9_]*"', "--", "*.rs", *EXCLUDE],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    names: set[str] = set()
    for path in files:
        text = (REPO / path).read_text()
        names.update(LITERAL.findall(text))
        names.update(MACRO_FIRST_ARG.findall(text))
    return names - NOT_METRICS


def documented() -> set[str]:
    rows = re.findall(r"^\| `([a-z][a-z0-9_]*)` \|", REFERENCE.read_text(), re.M)
    return set(rows)


def main() -> int:
    code = registered()
    page = documented()
    missing = sorted(code - page)
    stale = sorted(page - code)
    for name in missing:
        print(f"\033[31mFAIL\033[0m {name} is registered by the code and not on the metrics page")
    for name in stale:
        print(f"\033[31mFAIL\033[0m {name} is on the metrics page and the code does not register it")
    print(f"{len(code)} metric(s) checked, {len(missing)} undocumented, {len(stale)} stale")
    return 1 if missing or stale else 0


if __name__ == "__main__":
    sys.exit(main())
