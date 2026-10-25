#!/usr/bin/env python3
"""Fail if a function exported by felix.h has no [LibraryImport] declaration in sdks/dotnet.

The C# SDK claims parity with the C ABI, so a new export must be bound, or listed in
ALLOWLIST with the reason it is not.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
HEADER = ROOT / "crates/sdk/felix-capi/include/felix.h"
SOURCES = ROOT / "sdks/dotnet/src"

# export -> why the C# SDK does not bind it.
ALLOWLIST = {
    "felix_client_connect": "connect goes through felix_client_connect_with, which takes the same arguments plus the options struct",
}

EXPORT = re.compile(r"^[A-Za-z][\w \*]*?\b(felix_\w+)\(", re.M)
BOUND = re.compile(r"\bpartial\s+[\w\*<>?, ]+?\s+(felix_\w+)\(")


def exports(text: str) -> set[str]:
    # Declarations start at column 0 and are not typedefs; comments never match.
    return {m.group(1) for m in EXPORT.finditer(text) if not m.group(0).startswith("typedef")}


def main() -> int:
    exported = exports(HEADER.read_text())
    bound = set()
    for path in SOURCES.rglob("*.cs"):
        bound |= set(BOUND.findall(path.read_text()))
    if not exported:
        print("no exports found in felix.h; the check is broken", file=sys.stderr)
        return 2
    missing = sorted(exported - bound - ALLOWLIST.keys())
    stale = sorted(name for name in ALLOWLIST if name not in exported or name in bound)
    for name in missing:
        print(f"felix.h exports {name}, which sdks/dotnet does not bind (bind it, or allowlist it with a reason)")
    for name in stale:
        print(f"ALLOWLIST lists {name}, which is no longer an unbound export; remove it")
    if missing or stale:
        return 1
    print(f"{len(exported)} exports: {len(exported & bound)} bound, {len(ALLOWLIST)} allowlisted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
