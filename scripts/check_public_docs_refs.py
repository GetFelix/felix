#!/usr/bin/env python3
"""Keep internal project tracking out of the published documentation.

The docs site is read by people deploying Felix. Milestone labels — "M13
complete", "Replication is M5", "Packaged charts are M9's job" — mean nothing
to them: they name rows on a project board this reader has never seen, and
they date the page the moment the milestone closes. A capability statement
("this is available, here is what it survives") says the same thing to the
reader who matters and does not go stale on a board update.

Unlike the status claims in `check_status_claims.py`, this one can be a hard
gate: a milestone label in a published page is wrong on the day it is written,
so there is nothing to weigh.

Issue links are deliberately allowed. A public issue URL is a reasonable
citation for "the tracking discussion lives here" and stays meaningful to an
outside reader; a milestone number does not.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PUBLIC_DOCS = REPO / "docs-site" / "src" / "content" / "docs"

# Pages generated from CHANGELOG.md, which records history as written and is
# not checked here. They are not in git, so only a local build has them.
GENERATED = (
    PUBLIC_DOCS / "release-notes" / "index.md",
    PUBLIC_DOCS / "release-notes" / "releases",
)

# `M` followed by one or two digits, as its own word.
MILESTONE = re.compile(r"\bM\d{1,2}\b")

# Things that match the shape without being milestones. Narrow on purpose: an
# entry here is a promise that this token is not project tracking.
ALLOWED = (
    # Apple silicon, in the benchmark provenance lines.
    "M4 Max",
    # Mermaid's own keyword for a point-in-time marker on a gantt chart.
    ":milestone",
)


def offending_lines() -> list[tuple[Path, int, str]]:
    found: list[tuple[Path, int, str]] = []
    for path in sorted(PUBLIC_DOCS.rglob("*")):
        if path.suffix.lower() not in {".md", ".mdx"} or not path.is_file():
            continue
        if any(path == g or g in path.parents for g in GENERATED):
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not MILESTONE.search(line):
                continue
            if any(allowed in line for allowed in ALLOWED):
                continue
            # Inline SVG path data ("M52 22v6") is coordinates, not a milestone.
            if "<path" in line or ' d="' in line:
                continue
            found.append((path, number, line.strip()))
    return found


def main() -> int:
    found = offending_lines()
    for path, number, line in found:
        trimmed = line if len(line) <= 140 else line[:137] + "..."
        print(f"{path.relative_to(REPO)}:{number}: {trimmed}")
    if found:
        print(
            f"\n{len(found)} published page(s) name an internal milestone. "
            "Say what the reader can do and what it is proven to survive, not "
            "which milestone delivered it. If the token is not a milestone, add "
            "it to ALLOWED in scripts/check_public_docs_refs.py."
        )
        return 1
    print("no internal milestone labels in the published docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
