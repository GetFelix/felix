#!/usr/bin/env python3
"""Verify every version field agrees, and matches the tag being released.

A tag and a version that disagree ship artifacts labelled with neither, and
nothing else notices: the archive is named from the tag, each crate is built
from its own manifest, and both succeed. The wheel, the npm package and the
Helm chart each carry a version of their own, so bumping the workspace is not
enough -- which is the same shape as the lockfiles that sat at 0.4.0-preview
through two releases because nothing looked.

Run with a tag to check a release (`v0.5.0`), or with no argument to check only
that the files agree with each other, which is what a pull request wants.

With a tag it also checks the image tags the docs tell readers to pull. Those
name the newest release, not the tree, because between releases `main` carries
a version that has no images yet. Checking them when a release is tagged is
what keeps them from going stale.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

#: Every file carrying a version that must track the release, and how to read
#: it. The Helm chart's own `version` is deliberately absent: it versions the
#: chart, not the app, and moves on its own schedule.
SOURCES: list[tuple[str, str]] = [
    ("Cargo.toml", "cargo"),
    ("crates/sdk/felix-python/Cargo.toml", "cargo"),
    ("crates/sdk/felix-typescript/Cargo.toml", "cargo"),
    ("crates/sdk/felix-typescript/package.json", "npm"),
    ("deploy/helm/felix/Chart.yaml", "chart"),
]


def read(path: str, kind: str) -> str | None:
    text = (REPO / path).read_text()
    if kind == "cargo":
        # The first `version = "..."` is the package's own; dependency versions
        # follow it and are not what this checks.
        match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.M)
    elif kind == "npm":
        return json.loads(text).get("version")
    else:
        match = re.search(r'^appVersion:\s*"?([^"\s]+)"?', text, re.M)
    return match.group(1) if match else None


#: Where the docs pin a released image, as `ghcr.io/gabloe/<image>:<version>`.
DOC_ROOTS = ["docs", "docs-site/src/content/docs", "crates/tools/felixctl/README.md"]
IMAGE_PIN = re.compile(r"ghcr\.io/gabloe/felix[a-z-]*:(\d[0-9A-Za-z.+-]*)")


def stale_doc_pins(expected: str) -> list[str]:
    stale = []
    for root in DOC_ROOTS:
        base = REPO / root
        files = [base] if base.is_file() else sorted(base.rglob("*.md*"))
        for path in files:
            for number, line in enumerate(path.read_text().splitlines(), 1):
                for match in IMAGE_PIN.finditer(line):
                    if match.group(1) != expected:
                        stale.append(f"{path.relative_to(REPO)}:{number}: {match.group(0)}")
    return stale


def main() -> int:
    tag = sys.argv[1] if len(sys.argv) > 1 else None
    expected = tag[1:] if tag and tag.startswith("v") else tag

    found: dict[str, str | None] = {path: read(path, kind) for path, kind in SOURCES}
    missing = [path for path, version in found.items() if version is None]
    for path in missing:
        print(f"FAIL {path}: no version found where one was expected")

    versions = {version for version in found.values() if version is not None}
    disagree = len(versions) > 1
    if disagree:
        print("FAIL the version fields do not agree:")
        for path, version in found.items():
            print(f"       {version}  {path}")

    mismatched = expected is not None and versions != {expected}
    if mismatched:
        print(f"FAIL tag {tag} does not match the versions in the tree:")
        for path, version in found.items():
            marker = " " if version == expected else "<-"
            print(f"     {marker} {version}  {path}")

    stale = stale_doc_pins(expected) if expected is not None else []
    if stale:
        print(f"FAIL the docs pin images other than {expected}:")
        for pin in stale:
            print(f"       {pin}")

    if missing or disagree or mismatched:
        print("\nBump every file above, then run `task lock:refresh`.")
        return 1
    if stale:
        return 1

    agreed = versions.pop()
    print(f"{len(SOURCES)} version field(s) checked, all at {agreed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
