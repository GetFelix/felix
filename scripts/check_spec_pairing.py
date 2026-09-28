#!/usr/bin/env python3
"""Fail a change to the code the TLA+ model describes that leaves the model alone.

`check_spec_evidence.py` catches a spec citing a test that no longer exists. It
cannot catch the drift that actually happened: #268 changed the replication
protocol without touching a test the spec cited, and without touching the spec,
and the model went on describing the old design for three weeks (#598).

So this forces the confrontation. A change touching the modelled paths below
must also touch `docs/formal/`, or say why it does not with a line

    Spec-Unaffected: <reason>

in a commit message or the pull request description. It is deliberately blunt:
most edits to these files are not protocol changes, and the marker is how you
say so. What it buys is that nobody changes the protocol without being asked
whether the model still describes it. It says nothing about whether a spec
change is *right*.

Usage:

    check_spec_pairing.py --base <rev>     # diff <rev>...HEAD

`PR_BODY`, if set, is searched for the marker along with the commit messages.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys

# The implementation of what docs/formal/FelixShard.tla models: the lease and
# the heartbeats that renew it, the control plane's expiry clock, the write
# paths that check the lease and the generation, the replication and quorum
# mark, the reports and the control plane's answer to them, promotion, and the
# planned handoff.
MODELLED = (
    "services/felix-broker-service/src/cluster/lease",
    "services/felix-broker-service/src/cluster/membership",
    "crates/server/felix-replication/src",
    "services/felix-broker-service/src/serving",
    "services/felix-broker-service/src/shards/lifecycle",
    "services/felix-controlplane-service/src/api/nodes/reports",
    "services/felix-controlplane-service/src/cluster/membership",
    "services/felix-controlplane-service/src/cluster/placement",
)
SPEC = "docs/formal/"
MARKER = re.compile(r"^\s*Spec-Unaffected:\s*\S", re.MULTILINE)


def modelled(path: str) -> bool:
    if not any(path == f"{p}.rs" or path.startswith(f"{p}/") for p in MODELLED):
        return False
    # Tests and metrics describe the protocol; they do not change it. A renamed
    # evidence test is check_spec_evidence.py's to catch.
    name = path.rsplit("/", 1)[-1]
    return name not in ("tests.rs", "metrics.rs") and "/tests/" not in path


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True
    ).stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True, help="revision to diff against")
    args = parser.parse_args()

    changed = git("diff", "--name-only", f"{args.base}...HEAD").splitlines()
    touched = sorted(p for p in changed if modelled(p))
    if not touched:
        print("spec pairing: no modelled code changed")
        return 0
    if any(p.startswith(SPEC) for p in changed):
        print(f"spec pairing: {len(touched)} modelled file(s) changed, and so did {SPEC}")
        return 0

    messages = git("log", "--format=%B", f"{args.base}..HEAD")
    if MARKER.search(messages) or MARKER.search(os.environ.get("PR_BODY", "")):
        print(
            f"spec pairing: {len(touched)} modelled file(s) changed, "
            "marked Spec-Unaffected"
        )
        return 0

    print("\033[31mFAIL\033[0m this change touches code the TLA+ model describes:")
    for path in touched:
        print(f"  {path}")
    print(
        f"\nand nothing under {SPEC}. If it changes the protocol -- the lease, the\n"
        "quorum mark, reports, promotion or handoff -- update the model. If it does\n"
        "not, say so with a line in a commit message or the PR description:\n\n"
        "  Spec-Unaffected: <why>\n\n"
        "See docs/formal/README.md."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
