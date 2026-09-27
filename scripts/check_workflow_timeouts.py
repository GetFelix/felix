#!/usr/bin/env python3
"""Every workflow job must set `timeout-minutes`.

Run via `task ci:timeouts`.

Without one a job inherits GitHub's six-hour default, so a hung test or a
broker that never exits holds a runner for hours before anyone hears about it.
A reusable-workflow call (`uses:` at job level) has no runner of its own and is
exempt.
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github/workflows"

JOB = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")


def jobs(text: str) -> dict[str, list[str]]:
    """Map each job id to the lines of its body (the lines indented under it)."""
    out: dict[str, list[str]] = {}
    in_jobs = False
    current: str | None = None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" "):
            in_jobs = line.startswith("jobs:")
            current = None
            continue
        if not in_jobs:
            continue
        m = JOB.match(line)
        if m:
            current = m.group(1)
            out[current] = []
        elif current is not None:
            out[current].append(line)
    return out


def check() -> list[str]:
    failures: list[str] = []
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        for job, body in jobs(wf.read_text()).items():
            keys = {l.strip().split(":", 1)[0] for l in body if re.match(r"^    [a-z-]+:", l)}
            if "uses" in keys:
                continue
            if "timeout-minutes" not in keys:
                failures.append(f"{wf.relative_to(REPO_ROOT)}: job `{job}` has no timeout-minutes")
    return failures


def main() -> int:
    failures = check()
    if failures:
        print("Workflow timeout check FAILED:\n", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print("Every workflow job sets timeout-minutes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
