#!/usr/bin/env python3
"""Pick the CI jobs a pull request's changed files can affect.

Reads changed paths on stdin (one per line) and prints `key=value` lines for
$GITHUB_OUTPUT: one `true`/`false` switch per gated job, plus the package
lists the test job runs. See "How pull request CI picks its jobs" in
CONTRIBUTING.md.

A crate is affected when it changed or depends on one that did, through the
workspace's path dependencies (`cargo metadata --no-deps`). A change only
under a crate's tests (tests/, benches/, examples/, `tests.rs` or a `tests/`
module, all `#[cfg(test)]` here) affects that crate alone: no dependent
compiles it. A path nothing below claims runs everything.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys

JOBS = (
    "test", "sim", "cluster", "images", "compose", "chart",
    "python", "typescript", "fuzz", "coverage",
)

# Run every job: they change how everything builds or how jobs are chosen.
FULL = re.compile(
    r"^(Cargo\.(toml|lock)|rust-toolchain(\.toml)?|Taskfile\.yml|rustfmt\.toml)$"
    r"|^\.cargo/|(^|/)build\.rs$|^vendor/"
    r"|^\.github/workflows/(ci|coverage)\.yml$|^\.github/actions/"
    r"|^scripts/ci/(affected\.py|test_affected\.py|fixtures/)"
)

# Paths only `checks`, `spec-pairing` or the docs, formal and dotnet switches
# read; those keep their own triggers in ci.yml.
OTHER_JOBS = re.compile(
    r"^docs/|^docs-site/|\.md$|^sdks/dotnet/|^demos/|^githooks/|^\.devcontainer/"
    r"|^\.github/(workflows/|ISSUE_TEMPLATE/|dependabot\.yml$|companions\.txt$)"
    r"|^(\.gitignore|clippy\.toml|deny\.toml|LICENSE.*)$"
    r"|^scripts/(check_|test_check_|changelog_|test_changelog_|perf/|check-|check_tla\.sh$)"
    r"|^scripts/ci/(test_|gate|metadata_bench|nightly_smoke|azure-runners/)"
)

# Paths outside the workspace crates, by the jobs that read them. First match wins.
PATH_JOBS = [
    (r"^crates/sdk/felix-python/", {"python", "coverage"}),
    (r"^crates/sdk/felix-typescript/", {"typescript"}),
    (r"^deploy/|^scripts/(check_chart|check_observability_assets)\.py$"
     r"|^scripts/ci/requirements/chart-check\.txt$", {"chart"}),
    (r"^\.dockerignore$", {"images", "compose"}),
    (r"^compose\.yaml$|^scripts/ci/compose_smoke\.sh$", {"images", "compose"}),
    (r"^scripts/ci/requirements/binding\.txt$", {"python"}),
    (r"^scripts/check_npm_packages\.py$", {"typescript"}),
    (r"^scripts/ci/(coverage|coverage_gate)", {"coverage"}),
    (r"^scripts/fetch-released-controlplane\.sh$", {"coverage"}),
    (r"^scripts/stress_test\.sh$", {"test"}),
]

# Files outside a crate that its tests read: test-only changes of these crates.
# Every matching row applies.
DOC_TESTS = [
    # felix-common's deployment_yaml test checks image tags on every page, and
    # felix-capi compiles the C examples on any docs-site page.
    (r"^docs/|^docs-site/src/content/docs/", {"felix-common"}, set()),
    (r"^docs-site/src/content/docs/", {"felix-capi"}, set()),
    # felix-common's deployment_yaml test reads every Dockerfile.
    (r"^docker/", {"felix-common"}, {"images", "compose"}),
    (r"^scripts/perf/cloud/remote/local-idp\.py$", {"felix-controlplane-service"}, set()),
    (r"^docs-site/src/content/docs/(clients/|tutorials/)",
     {"felix-client", "felix-capi"}, {"python", "typescript"}),
    (r"^docs-site/src/content/docs/reference/felixctl\.md$", {"felixctl"}, set()),
]

TEST_ONLY = re.compile(r"^(tests|benches|examples)/|(^|/)tests(\.rs$|/)|\.md$")

# The crates under fuzz/ directories (the fuzz job and `task fuzz`).
FUZZED = {
    "felix-wire", "felix-mqtt-wire", "felix-mqtt", "felix-storage",
    "felix-kafka", "felix-replication",
}
# What the Python and Node jobs build and run besides their own binding.
SDK_HARNESS = {"felix-broker-service", "felix-conformance", "felix-cluster"}
# Crates whose lib tests `task test:residual` runs without the telemetry feature.
RESIDUAL_LIB = {"felix-broker", "felix-client", "felix-broker-service"}


class Workspace:
    def __init__(self, metadata: dict):
        root = metadata["workspace_root"].rstrip("/") + "/"
        members = set(metadata.get("workspace_members", []))
        self.dirs: dict[str, str] = {}
        self.files: dict[str, str] = {}
        self.libs: set[str] = set()
        self.deps: dict[str, set[str]] = {}
        names = set()
        for pkg in metadata["packages"]:
            if members and pkg["id"] not in members:
                continue
            name = pkg["name"]
            names.add(name)
            self.dirs[_rel(os.path.dirname(pkg["manifest_path"]), root) + "/"] = name
            for target in pkg["targets"]:
                self.files[_rel(target["src_path"], root)] = name
                if "lib" in target["kind"] or "rlib" in target["kind"]:
                    self.libs.add(name)
            self.deps[name] = {d["name"] for d in pkg["dependencies"] if d.get("path")}
        self.names = names
        self.dependents: dict[str, set[str]] = {n: set() for n in names}
        for name, deps in self.deps.items():
            for dep in deps & names:
                if dep != name:
                    self.dependents[dep].add(name)

    def owner(self, path: str) -> tuple[str, str] | None:
        """The crate a path belongs to and the path inside that crate."""
        if path in self.files and not any(path.startswith(d) for d in self.dirs):
            return self.files[path], path
        best = max((d for d in self.dirs if path.startswith(d)), key=len, default=None)
        return (self.dirs[best], path[len(best):]) if best else None

    def closure(self, crates: set[str]) -> set[str]:
        seen, stack = set(crates), list(crates)
        while stack:
            for up in self.dependents.get(stack.pop(), ()):
                if up not in seen:
                    seen.add(up)
                    stack.append(up)
        return seen


def _rel(path: str, root: str) -> str:
    path = os.path.normpath(path)
    return path[len(root):] if path.startswith(root) else path


def select(files: list[str], metadata, event: str, labels: list[str]) -> dict:
    """`metadata` is the cargo metadata dict, or a function returning it."""
    reason = None
    if event != "pull_request":
        reason = f"event {event}"
    elif "ci:full" in labels:
        reason = "label ci:full"
    elif len(files) >= 300:
        reason = "300 or more files (the compare API's limit)"

    ws = None if reason else Workspace(metadata() if callable(metadata) else metadata)
    code, tests, jobs, fuzz_dirs = set(), set(), set(), False
    for path in files:
        if reason:
            break
        if FULL.search(path):
            reason = path
            break
        docs = [(c, j) for p, c, j in DOC_TESTS if re.search(p, path)]
        for crates, doc_jobs in docs:
            tests |= crates
            jobs |= doc_jobs
        if docs:
            continue
        owned = ws.owner(path)
        if owned:
            crate, inner = owned
            if crate == "felix-capi":
                jobs.add("python")  # which also runs the C ABI conformance harness
            if inner.startswith("fuzz/"):
                # Some crates' own tests replay the fuzz seeds.
                fuzz_dirs = True
                tests.add(crate)
            elif TEST_ONLY.search(inner):
                tests.add(crate)
            else:
                code.add(crate)
            continue
        hit = next((j for p, j in PATH_JOBS if re.search(p, path)), None)
        if hit is not None:
            jobs |= hit
            continue
        if OTHER_JOBS.search(path):
            continue
        reason = path
        break

    if reason:
        out = {job: True for job in JOBS}
        out.update(full=True, reason=reason, test_packages="", test_lib_packages="",
                   stress=True, crates="all")
        return out

    code_affected = ws.closure(code)
    affected = code_affected | tests
    out = {job: job in jobs for job in JOBS}
    out["coverage"] |= bool(affected)
    # The harness runs the broker, loadgen and felixctl binaries it does not link.
    binaries = {"felix-broker-service", "felix-loadgen", "felixctl"}
    out["sim"] = "felix-sim" in affected or "felixctl" in code_affected
    out["cluster"] = "felix-cluster" in affected or bool(binaries & code_affected)
    out["images"] |= bool(
        {"felix-broker-service", "felix-controlplane-service", "felixctl"} & code_affected)
    out["compose"] |= out["images"]
    # check_observability_assets.py reads metric names from all non-test Rust.
    out["chart"] |= bool(code)
    # Broker, storage and replication behaviour is what the conformance suites check.
    harness = bool(SDK_HARNESS & code_affected)
    out["python"] |= harness or bool({"felix-client", "felix-capi"} & code_affected)
    out["typescript"] |= harness or "felix-client" in code_affected
    out["fuzz"] |= fuzz_dirs or bool(FUZZED & code_affected)

    # `cargo test --workspace --exclude felix-cluster`, narrowed.
    every = ws.names - {"felix-cluster"}
    pkgs = sorted((affected - {"felix-cluster"}) & ws.libs)
    whole = set(pkgs) >= (every & ws.libs)
    out["test"] |= bool(pkgs) or "felix-storage" in affected
    out.update(
        full=False,
        reason="",
        crates=" ".join(sorted(affected)),
        test_packages="" if whole else " ".join(f"-p {p}" for p in pkgs),
        test_lib_packages=" ".join(f"-p {p}" for p in sorted(
            RESIDUAL_LIB if whole else affected & RESIDUAL_LIB)),
        stress="felix-storage" in affected or "test" in jobs,
    )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--metadata", help="cargo metadata JSON; default runs cargo")
    parser.add_argument("--event", default=os.environ.get("GITHUB_EVENT_NAME", "pull_request"))
    parser.add_argument("--labels", default="", help="space-separated PR labels")
    args = parser.parse_args()
    def metadata() -> dict:
        if args.metadata:
            with open(args.metadata) as f:
                return json.load(f)
        return json.loads(subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"]))

    files = [line.strip() for line in sys.stdin if line.strip()]
    out = select(files, metadata, args.event, args.labels.split())
    for key, value in out.items():
        print(f"{key}={str(value).lower() if isinstance(value, bool) else value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
