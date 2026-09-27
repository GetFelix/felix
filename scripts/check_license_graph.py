#!/usr/bin/env python3
"""Fail if a permissively licensed crate depends on an AGPL crate.

Run via `task publish:check`.

`check_publish_metadata.py` checks each crate's own licence label. That says
nothing about what the crate links: an Apache-2.0 crate with a normal or build
dependency on an AGPL crate is AGPL in practice once built, whatever its
manifest says. This walks the dependency graph instead of trusting the label.

Only first-party crates are walked. Third-party licences are cargo-deny's job,
and `deny.toml` allows AGPL only because our own server crates use it.
Dev-dependencies are skipped: they are never part of what a crate ships.
Optional dependencies count, since some feature combination links them.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

PERMISSIVE = {"Apache-2.0", "MIT", "Apache-2.0 OR MIT", "MIT OR Apache-2.0"}
COPYLEFT_MARKER = "AGPL"

# Crates with their own [workspace] that are still first-party and permissive.
# The Python and Node bindings ship to PyPI and npm under Apache-2.0.
EXTRA_MANIFESTS = (
    "crates/sdk/felix-python/Cargo.toml",
    "crates/sdk/felix-typescript/Cargo.toml",
)


def metadata(manifest: pathlib.Path | None = None) -> dict:
    cmd = ["cargo", "metadata", "--format-version", "1", "--no-deps"]
    if manifest is not None:
        cmd += ["--manifest-path", str(manifest)]
    raw = subprocess.run(
        cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout
    return json.loads(raw)


def first_party_packages() -> dict[str, dict]:
    meta = metadata()
    ids = set(meta["workspace_members"])
    packages = {p["name"]: p for p in meta["packages"] if p["id"] in ids}
    for rel in EXTRA_MANIFESTS:
        manifest = REPO_ROOT / rel
        if manifest.is_file():
            for p in metadata(manifest)["packages"]:
                packages.setdefault(p["name"], p)
    return packages


def shipped_first_party_deps(pkg: dict, packages: dict[str, dict]) -> list[str]:
    # `kind` is null for normal, "build" for build, "dev" for dev-dependencies.
    # A path dependency is how every first-party edge is declared.
    return sorted(
        {
            d["name"]
            for d in pkg["dependencies"]
            if d["kind"] != "dev" and d.get("path") and d["name"] in packages
        }
    )


def copyleft_path(
    start: str, packages: dict[str, dict]
) -> list[str] | None:
    """Shortest first-party path from `start` to an AGPL crate, or None."""
    parents: dict[str, str | None] = {start: None}
    queue = [start]
    while queue:
        name = queue.pop(0)
        if name != start and COPYLEFT_MARKER in (packages[name].get("license") or ""):
            path = [name]
            while parents[path[-1]] is not None:
                path.append(parents[path[-1]])
            return list(reversed(path))
        for dep in shipped_first_party_deps(packages[name], packages):
            if dep not in parents:
                parents[dep] = name
                queue.append(dep)
    return None


def check(packages: dict[str, dict]) -> list[str]:
    failures = []
    for name, pkg in sorted(packages.items()):
        if pkg.get("license") not in PERMISSIVE:
            continue
        path = copyleft_path(name, packages)
        if path:
            failures.append(
                f"{name} is {pkg['license']} but links AGPL code: "
                f"{' -> '.join(path)}. Relabel it AGPL-3.0-only, or move the "
                f"AGPL-dependent part into a separate crate."
            )
    return failures


def main() -> int:
    packages = first_party_packages()
    failures = check(packages)
    if failures:
        print("Licence dependency-graph check FAILED:\n", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print("\nSee LICENSING.md.", file=sys.stderr)
        return 1
    permissive = sum(1 for p in packages.values() if p.get("license") in PERMISSIVE)
    print(
        f"Licence dependency-graph check passed: none of {permissive} permissive "
        f"first-party crates links an AGPL crate."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
