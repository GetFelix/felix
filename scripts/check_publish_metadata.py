#!/usr/bin/env python3
"""Enforce the licence split and crates.io publish-readiness across the workspace.

Run via `task publish:check`.

# Why this exists
The licence split in LICENSING.md is a legal boundary that lives in fifteen
separate manifests. Nothing structural stops a new crate from silently landing on
the wrong side of it, and the failure is invisible until someone reads the
manifest. This turns the LICENSING.md table into an assertion.

# Why it shells out to `cargo metadata`
Manifests use workspace inheritance (`license.workspace = true`), so parsing the
raw TOML sees the string "workspace" rather than the resolved licence and would
validate nothing while appearing to pass. `cargo metadata` reports resolved
values, which is the only trustworthy source.
"""

from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# The authoritative table, mirroring LICENSING.md. Adding a crate to the
# workspace without adding it here is itself a failure: the point is that
# classification is deliberate rather than inherited by accident.
APACHE = {
    "felix-wire",
    "felix-client",
    "felix-transport",
    "felix-common",
}
COPYLEFT = {
    "felix-broker",
    "felix-storage",
    "felix-authz",
    "felix-router",
    "felix-kafka",
    "felix-replication",
    "felix-cluster",
    "felix-conformance",
    "felix-loadgen",
    "felixctl",
    "felix-broker-service",
    "felix-controlplane-service",
}

# Everything except the client SDK and the two crates it is built on, and
# felixctl with the felix-loadgen it links. The server libraries are only ever
# built into the services, and the rest are service binaries or dev/CI tools.
NOT_PUBLISHABLE = {
    "felix-authz",
    "felix-broker",
    "felix-common",
    "felix-router",
    "felix-kafka",
    "felix-replication",
    "felix-storage",
    "felix-broker-service",
    "felix-controlplane-service",
    "felix-conformance",
    "felix-cluster",
}

# Publishable crates the release workflow does not publish yet. They are held
# to every other rule here; only the publish-order check skips them. Take a
# crate out of this set in the change that adds it to the workflow's list.
RELEASE_PENDING = {
    "felix-loadgen",
    "felixctl",
}

# crates.io hard requirement is `description`; the rest are discoverability
# fields we want set before a first publish rather than bolted on after.
REQUIRED_FIELDS = ("description", "repository", "keywords", "categories")


def workspace_members() -> list[dict]:
    raw = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    meta = json.loads(raw)
    ids = set(meta["workspace_members"])
    return [p for p in meta["packages"] if p["id"] in ids]


def check() -> list[str]:
    failures: list[str] = []
    packages = workspace_members()
    seen = {p["name"] for p in packages}

    unclassified = seen - APACHE - COPYLEFT
    for name in sorted(unclassified):
        failures.append(
            f"{name}: not listed in this script's licence table. Add it to APACHE or "
            f"COPYLEFT and to the table in LICENSING.md — a new crate must be "
            f"classified deliberately."
        )

    stale = (APACHE | COPYLEFT) - seen
    for name in sorted(stale):
        failures.append(
            f"{name}: listed in this script's licence table but is no longer a "
            f"workspace member; remove it here and from LICENSING.md."
        )

    for pkg in sorted(packages, key=lambda p: p["name"]):
        name = pkg["name"]
        expected = (
            "Apache-2.0" if name in APACHE else "AGPL-3.0-only" if name in COPYLEFT else None
        )
        actual = pkg.get("license")
        if expected and actual != expected:
            failures.append(
                f"{name}: licence is {actual!r}, expected {expected!r} per LICENSING.md."
            )

        # `publish` is null when unrestricted, or a list of allowed registries.
        # An empty list is how `publish = false` surfaces here.
        publishable = pkg.get("publish") != []
        should_publish = name not in NOT_PUBLISHABLE
        if publishable and not should_publish:
            failures.append(
                f"{name}: must set `publish = false` — only the client SDK and the "
                f"crates it depends on are published."
            )
        if not publishable and should_publish:
            failures.append(
                f"{name}: has `publish = false` but is expected to be publishable."
            )

        # Only crates that could actually reach crates.io need the metadata.
        if should_publish:
            for field in REQUIRED_FIELDS:
                if not pkg.get(field):
                    failures.append(
                        f"{name}: missing `{field}`. crates.io rejects a publish "
                        f"without `description`; the rest are required before a "
                        f"first publish."
                    )

        # `cargo package` only bundles files inside the crate directory, so a
        # crate without its own LICENSE ships with no licence text even though
        # the repository root has one.
        crate_dir = pathlib.Path(pkg["manifest_path"]).parent
        if not (crate_dir / "LICENSE").is_file():
            failures.append(
                f"{name}: no LICENSE file in {crate_dir.relative_to(REPO_ROOT)}/. "
                f"cargo package does not reach the repository root."
            )

        readme = pkg.get("readme")
        if should_publish and readme and not (crate_dir / readme).is_file():
            failures.append(
                f"{name}: readme is {readme!r} but that file does not exist in "
                f"{crate_dir.relative_to(REPO_ROOT)}/."
            )

    return failures


RELEASE_WORKFLOW = REPO_ROOT / ".github/workflows/release.yml"


def check_publish_order(packages: list[dict]) -> list[str]:
    """The release workflow's publish list must be every publishable crate, in
    an order crates.io can actually resolve.

    crates.io resolves each dependency against the registry as the crate is
    uploaded, so a crate published before something it depends on fails — and
    it fails halfway through a release, with some crates already permanent.
    The workflow's sequence is the only thing preventing that, and nothing
    about adding a crate to the workspace forces anyone to revisit it.
    """
    failures: list[str] = []
    text = RELEASE_WORKFLOW.read_text()
    match = re.search(r"for crate in (.*?); do", text, re.S)
    if not match:
        return [
            f"{RELEASE_WORKFLOW.relative_to(REPO_ROOT)}: no `for crate in ...; do` "
            f"publish loop found. This check reads the order out of it."
        ]

    listed = match.group(1).replace("\\\n", " ").split()
    by_name = {p["name"]: p for p in packages}
    expected = {n for n in by_name if n not in NOT_PUBLISHABLE | RELEASE_PENDING}

    for name in sorted(expected - set(listed)):
        failures.append(
            f"{name}: publishable but missing from the release workflow's crates.io "
            f"publish list."
        )
    for name in sorted(set(listed) - expected):
        failures.append(
            f"{name}: in the release workflow's crates.io publish list but is not a "
            f"publishable workspace member."
        )
    if failures:
        return failures

    published: set[str] = set()
    for name in listed:
        needs = {
            d["name"]
            for d in by_name[name]["dependencies"]
            # `kind` is null for a normal dependency; dev- and build-dependencies
            # do not have to be on crates.io ahead of it.
            if d["kind"] is None and d["name"] in expected
        }
        for missing in sorted(needs - published):
            failures.append(
                f"{name}: published before {missing}, which it depends on. The list "
                f"in {RELEASE_WORKFLOW.relative_to(REPO_ROOT)} must be a topological "
                f"sort of the workspace's internal dependencies."
            )
        published.add(name)
    return failures


def main() -> int:
    packages = workspace_members()
    failures = check() + check_publish_order(packages)
    if failures:
        print("Publish-readiness check FAILED:\n", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(
            "\nSee LICENSING.md for the authoritative licence table.",
            file=sys.stderr,
        )
        return 1
    print(
        f"Publish-readiness check passed for {len(packages)} workspace members, "
        f"and the crates.io publish order resolves."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
