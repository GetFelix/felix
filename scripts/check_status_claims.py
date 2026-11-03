#!/usr/bin/env python3
"""Find claims about what Felix has *not* built, so they can be re-read when it is.

`check_doc_evidence.py` proves a cited test exists. It cannot prove the prose
still matches, and the claims that rot worst are not the ones citing tests —
they are the ones saying a thing does not exist yet. Those are written once and
then outlive the feature shipping, because nothing about shipping a feature
makes anyone grep the docs for sentences that said it was missing.

That is a real failure, not a hypothetical: metadata Raft shipped across
#345-#354 and five pages went on saying it "is not started", including an SVG's
alt text reused on three of them.

Normative-language checkers do not find these. A sweep for "always / never /
guarantees / cannot / must" across the docs site returns ~321 sentences and
would have caught none of the Raft drift; widening it to "is" returns ~1709,
which is a report nobody reads. The phrases below return far fewer, and they
are the ones that go stale on a milestone close.

Two modes:

  --report        list every such claim, grouped by file. The point is to make
                  the uncovered surface visible; it exits 0.
  --check-issues  for claims that cite an issue, ask GitHub whether that issue
                  is closed. A closed issue under a "not built" sentence is
                  drift, and this is the only fully automatic signal for it.
                  Skipped without `gh`, so CI and offline both work.

Run `--report` when closing a milestone; that is the moment these go wrong.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Where prose lives. The SVGs matter too: their `aria-label` is the text a
# screen reader gets, and it is copied between pages rather than referenced.
SEARCH_ROOTS = [
    REPO / "docs",
    REPO / "docs-site" / "src" / "content" / "docs",
]
SEARCH_SUFFIXES = {".md", ".mdx", ".svg"}

# Phrases that assert something does not exist. Deliberately narrow: each one
# is a promise about the state of the project rather than about behaviour, and
# it is the project's state that changes without the sentence changing with it.
CLAIM = re.compile(
    r"\b("
    r"not started"
    r"|not implemented"
    r"|not built"
    r"|not yet (?:built|implemented|started|supported)"
    r"|is the intended way"
    r"|remains the intended"
    r"|when that lands"
    r"|has not shipped"
    r"|is planned and not"
    # "deferred" and "pending the <thing>" describe a decision to not build
    # something yet, which is the same claim in different words — and they were
    # missed on the first pass of this very cleanup: three pages still called
    # metadata Raft "deferred", and one called it experimental "pending the M13
    # chaos pass" that had already run. Four extra matches repo-wide, all of
    # them real, which is the ratio this list is selected for.
    r"|deferred"
    r"|pending the"
    r")\b",
    re.IGNORECASE,
)

# A bare #123 is a Felix issue; a GitHub URL names its own repo.
ISSUE = re.compile(r"github\.com/([\w.-]+/[\w.-]+)/issues/(\d+)|#(\d{2,5})")
FELIX_REPO = "GetFelix/felix"


LINKED = re.compile(r"\[#\d+\]\((https?://[^)]*)\)")


def cited_issues(line: str) -> list[tuple[str, str]]:
    # A link's label repeats its URL's number; let the URL name the repo.
    line = LINKED.sub(r"\1", line)
    return [
        (repo or FELIX_REPO, url_number or bare)
        for repo, url_number, bare in ISSUE.findall(line)
    ]


def claims() -> list[tuple[Path, int, str]]:
    found: list[tuple[Path, int, str]] = []
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.suffix.lower() not in SEARCH_SUFFIXES or not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            for number, line in enumerate(text.splitlines(), 1):
                if CLAIM.search(line):
                    found.append((path, number, line.strip()))
    return found


def closed_issues(issues: set[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """(repo, number) -> state, for the issues cited by status claims."""
    if not issues or not shutil.which("gh"):
        return {}
    states: dict[tuple[str, str], str] = {}
    for repo, number in sorted(issues):
        try:
            out = subprocess.run(
                ["gh", "issue", "view", number, "-R", repo, "--json", "state,title"],
                capture_output=True,
                text=True,
                timeout=20,
                cwd=REPO,
            )
        except (subprocess.SubprocessError, OSError):
            continue
        if out.returncode != 0:
            continue
        try:
            states[(repo, number)] = json.loads(out.stdout)["state"]
        except (json.JSONDecodeError, KeyError):
            continue
    return states


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        action="store_true",
        help="list every unbuilt-feature claim and exit 0",
    )
    parser.add_argument(
        "--check-issues",
        action="store_true",
        help="fail when a claim cites an issue that is already closed",
    )
    args = parser.parse_args()
    if not args.report and not args.check_issues:
        args.report = True

    found = claims()

    if args.report:
        by_file: dict[Path, list[tuple[int, str]]] = {}
        for path, number, line in found:
            by_file.setdefault(path, []).append((number, line))
        for path in sorted(by_file):
            print(f"\n{path.relative_to(REPO)}")
            for number, line in by_file[path]:
                marker = "  " if cited_issues(line) else "? "
                trimmed = line if len(line) <= 120 else line[:117] + "..."
                print(f"  {marker}{number:>5}: {trimmed}")
        anchored = sum(1 for _, _, line in found if cited_issues(line))
        print(
            f"\n{len(found)} claim(s) that something is not built; "
            f"{anchored} cite an issue, {len(found) - anchored} do not (marked ?)."
        )
        print(
            "A claim citing an issue can be checked automatically "
            "(--check-issues); one that does not has to be re-read by hand."
        )

    if args.check_issues:
        cited = {
            issue
            for _, _, line in found
            for issue in cited_issues(line)
        }
        states = closed_issues(cited)
        if not states:
            print("skipping issue check: gh unavailable or no cited issues")
            return 0
        stale = [
            (path, number, line)
            for path, number, line in found
            for issue in cited_issues(line)
            if states.get(issue) == "CLOSED"
        ]
        for path, number, line in stale:
            print(f"{path.relative_to(REPO)}:{number}: {line}")
        if stale:
            print(
                f"\n{len(stale)} claim(s) say something is not built while the "
                "issue tracking it is closed. Re-read the prose against what "
                "shipped."
            )
            return 1
        print(f"{len(states)} cited issue(s) checked, none closed under a claim")

    return 0


if __name__ == "__main__":
    sys.exit(main())
