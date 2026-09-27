#!/usr/bin/env python3
"""Keep vendored crates visible to the advisory database.

Run via `task deny`.

A crate patched in from vendor/ is a path crate, so cargo-deny and cargo-audit
only match advisories against registry sources and skip it. vendor/VENDORED.toml
names the upstream version each vendored copy was taken from. This script:

- fails if that name or version disagrees with the vendored manifest, the
  root [patch.crates-io] entry, or Cargo.lock, so the tracking file cannot go
  stale on an upgrade;
- with `--advisory-crate DIR`, writes a throwaway crate at DIR that depends on
  each upstream version from crates.io, for `cargo-deny check advisories` to
  run against. That is how an advisory on the upstream release still reaches us.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import tomllib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TRACKING = REPO_ROOT / "vendor" / "VENDORED.toml"


def load_toml(path: pathlib.Path) -> dict:
    return tomllib.loads(path.read_text())


def check(entries: list[dict]) -> list[str]:
    failures: list[str] = []
    root = load_toml(REPO_ROOT / "Cargo.toml")
    patches = root.get("patch", {}).get("crates-io", {})
    lock = load_toml(REPO_ROOT / "Cargo.lock").get("package", [])
    tracked = {e["name"] for e in entries}

    for name, spec in sorted(patches.items()):
        path = spec.get("path") if isinstance(spec, dict) else None
        if path and path.startswith("vendor/") and name not in tracked:
            failures.append(
                f"{name}: patched in from {path} but not listed in "
                f"{TRACKING.relative_to(REPO_ROOT)}, so no advisory can match it."
            )

    for entry in entries:
        name, version = entry["name"], entry["version"]
        where = f"{name} {version} ({TRACKING.relative_to(REPO_ROOT)})"

        spec = patches.get(name)
        expected_path = f"vendor/{name}"
        if not isinstance(spec, dict) or spec.get("path") != expected_path:
            failures.append(
                f"{where}: root Cargo.toml has no [patch.crates-io] entry pointing "
                f"at {expected_path}. Remove the entry here if the patch is gone."
            )

        manifest = REPO_ROOT / expected_path / "Cargo.toml"
        if not manifest.is_file():
            failures.append(f"{where}: {manifest.relative_to(REPO_ROOT)} is missing.")
        else:
            vendored = load_toml(manifest)["package"]["version"]
            if vendored != version:
                failures.append(
                    f"{where}: the vendored copy is {vendored}. Bump the version "
                    f"here to match, so advisories are checked against the "
                    f"release that is actually vendored."
                )

        locked = [p for p in lock if p["name"] == name]
        if not any(p["version"] == version and "source" not in p for p in locked):
            got = ", ".join(
                f"{p['version']} from {p.get('source', 'a path')}" for p in locked
            ) or "nothing"
            failures.append(
                f"{where}: Cargo.lock resolves {got}; expected the patched path "
                f"crate at {version}."
            )
    return failures


def write_advisory_crate(entries: list[dict], out: pathlib.Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "src").mkdir(exist_ok=True)
    (out / "src" / "lib.rs").write_text("")
    deps = "\n".join(
        f'{e["name"]} = {{ version = "={e["version"]}", default-features = false }}'
        for e in entries
    )
    # Its own [workspace] keeps the repository's [patch] from applying, so these
    # resolve to the registry releases the advisory database knows about.
    (out / "Cargo.toml").write_text(
        "[workspace]\n\n"
        "[package]\n"
        'name = "felix-vendored-advisories"\n'
        'version = "0.0.0"\n'
        'edition = "2024"\n'
        "publish = false\n\n"
        f"[dependencies]\n{deps}\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--advisory-crate",
        type=pathlib.Path,
        help="also write a crate depending on the upstream versions to this directory",
    )
    args = parser.parse_args()

    entries = load_toml(TRACKING).get("crate", [])
    failures = check(entries)
    if failures:
        print("Vendored-crate check FAILED:\n", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    if args.advisory_crate:
        write_advisory_crate(entries, args.advisory_crate)
    names = ", ".join(f"{e['name']} {e['version']}" for e in entries) or "none"
    print(f"Vendored-crate check passed: {names}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
