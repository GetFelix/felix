#!/usr/bin/env python3
"""Fail when a docs page or the README pins a preview of the current release
line that is not the newest tagged release.

compose.yaml's `${FELIX_VERSION:-X}` default is that version: the release step
bumps it when a tag is cut, and this check then lists every docs pin to bump.
The workspace version is the next, untagged preview, so it is the wrong source.
Previews of other lines (0.6.0-preview.9 in a history note) are left alone, and
so are the release notes generated from the changelog. A line that needs an old
pin on purpose carries `version-pin: allow`.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs-site" / "src" / "content" / "docs"
EXTRA = [REPO / "README.md"]
# Generated from the changelog, except the hand-written upgrade page.
HAND_WRITTEN = "upgrading-to-0-7.md"
SUFFIXES = {".md", ".mdx"}
ALLOW = "version-pin: allow"
PREVIEW = re.compile(r"(?<![\d.])(\d+\.\d+\.\d+)-preview\.(\d+)\b")


def released_version(compose: str) -> str:
    m = re.search(r"\$\{FELIX_VERSION:-([^}]+)\}", compose)
    if not m:
        raise SystemExit("no FELIX_VERSION default in compose.yaml")
    return m.group(1)


def stale_pins(text: str, current: str) -> list[tuple[int, str]]:
    base = current.split("-", 1)[0]
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        if ALLOW in line:
            continue
        for m in PREVIEW.finditer(line):
            if m.group(1) == base and m.group(0) != current:
                found.append((number, m.group(0)))
    return found


def pages() -> list[Path]:
    out = [
        p
        for p in sorted(DOCS.rglob("*"))
        if p.suffix in SUFFIXES and (p.relative_to(DOCS).parts[0] != "release-notes" or p.name == HAND_WRITTEN)
    ]
    return out + [p for p in EXTRA if p.exists()]


def main() -> int:
    current = released_version((REPO / "compose.yaml").read_text(encoding="utf-8"))
    bad = 0
    for path in pages():
        for number, pin in stale_pins(path.read_text(encoding="utf-8"), current):
            print(f"{path.relative_to(REPO)}:{number}: {pin}, but the newest tagged release is {current}")
            bad += 1
    if bad:
        print(f"\n{bad} stale version pin(s). Use {current}, or add `{ALLOW}` to the line.")
        return 1
    print(f"version pins ok (newest tag {current})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
