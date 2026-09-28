#!/usr/bin/env python3
"""Verify `felix_common::env_registry::KNOWN_VARS` lists every `FELIX_*` read.

The registry is what turns a mistyped variable into a warning naming the
setting that is quietly using its default. A variable added to the code and not
to the list inverts that: the real name gets reported as unknown, which trains
an operator to ignore the warning entirely.

Sibling to `check_env_reference.py`, which checks the *documentation* covers
every variable. This checks the *runtime* does.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REGISTRY = REPO / "crates/server/felix-common/src/env_registry.rs"

# Not variables at all. The detector matches any quoted FELIX_* literal in Rust,
# and these are the TypeScript binding's error codes (`crates/sdk/felix-typescript`),
# which ride on an error message and reach JavaScript as `err.kind`. They are
# public API, documented in that package's `index.d.ts` and README, so they are
# named here rather than renamed to dodge a heuristic. Kept in step with the
# same set in `check_env_reference.py`.
NOT_VARIABLES = {
    "FELIX_AUTH",
    "FELIX_CONNECTION",
    "FELIX_CURSOR",
    "FELIX_ERROR",
    "FELIX_INVALID",
    "FELIX_NOT_FOUND",
    "FELIX_OUTCOME_UNKNOWN",
    "FELIX_OVERLOADED",
    "FELIX_SHARD_UNAVAILABLE",
}


def read_by_code() -> set[str]:
    out = subprocess.run(
        # Test files are excluded: a name that appears only in one is a
        # fixture — a deliberately wrong name, or a setting exercised by a
        # harness — not a knob an operator has. Counting them made
        # `FELIX_QUIC_BINDD` look like something to document.
        [
            "git",
            "grep",
            "-hoE",
            r'"FELIX_[A-Z0-9_]+"',
            "--",
            "*.rs",
            ":!*_tests.rs",
            ":!*/tests.rs",
            ":!*/tests/*",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {line.strip('"') for line in out.splitlines()}


def registered() -> set[str]:
    body = REGISTRY.read_text()
    listing = body[body.index("KNOWN_VARS: &[&str] = &[") : body.index("];")]
    return set(re.findall(r'"(FELIX_[A-Z0-9_]+)"', listing))


def stated_count() -> int | None:
    """The count the registry's header gives, which drifted once already."""
    match = re.search(r"Configuration is (\d+) environment variables", REGISTRY.read_text())
    return int(match.group(1)) if match else None


def main() -> int:
    # The registry's own entries are string literals in a .rs file, so
    # `read_by_code` finds them too. That is what makes the two sets directly
    # comparable: a name in the registry is "read by the code" by construction,
    # and anything else in that set is read by something real.
    expected = read_by_code() - NOT_VARIABLES
    listed = registered()

    missing = sorted(expected - listed)
    stale = sorted(listed - expected)

    for name in missing:
        print(f"read by the code and not in KNOWN_VARS: {name}")
    for name in stale:
        print(f"in KNOWN_VARS and read by nothing: {name}")

    stated = stated_count()
    miscounted = stated != len(listed)
    if stated is None:
        print(f"{REGISTRY.name}: header no longer says how many variables KNOWN_VARS holds")
    elif miscounted:
        print(f"{REGISTRY.name}: header says {stated} variables, KNOWN_VARS holds {len(listed)}")

    print(
        f"{len(expected)} variable(s) checked, "
        f"{len(missing)} missing, {len(stale)} stale"
    )
    return 1 if missing or stale or miscounted else 0


if __name__ == "__main__":
    sys.exit(main())
