"""Tests for affected.py, against a trimmed copy of the workspace's metadata.

The fixture is `cargo metadata --format-version 1 --no-deps` cut down to the
fields affected.py reads, with paths relative to the workspace root.
`test_fixture_matches_the_workspace` fails when the crate graph drifts from it.
"""

import json
import os
import pathlib
import sys
import tomllib
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import affected  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
DEP_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")
METADATA = json.loads((HERE / "fixtures" / "affected-metadata.json").read_text())


def jobs(*files, event="pull_request", labels=()):
    out = affected.select(list(files), METADATA, event, list(labels))
    return out, {job for job in affected.JOBS if out[job]}


class Selection(unittest.TestCase):
    def assert_full(self, *files, **kw):
        out, ran = jobs(*files, **kw)
        self.assertTrue(out["full"], out)
        self.assertEqual(ran, set(affected.JOBS))
        self.assertEqual(out["test_packages"], "")

    def test_raft_test_only_change_runs_that_crates_tests(self):
        out, ran = jobs("crates/server/felix-replication/src/raft/tests/election.rs")
        self.assertEqual(ran, {"test", "coverage"})
        self.assertEqual(out["test_packages"], "-p felix-replication")
        self.assertEqual(out["test_lib_packages"], "")

    def test_raft_code_change_reaches_the_broker_but_not_the_sdks(self):
        _, ran = jobs("crates/server/felix-replication/src/raft/cache.rs")
        self.assertEqual(ran, set(affected.JOBS) - {"python", "typescript"})

    def test_wire_change_runs_everything_downstream(self):
        out, ran = jobs("crates/protocol/felix-wire/src/lib.rs")
        self.assertEqual(ran, set(affected.JOBS))
        self.assertFalse(out["full"])
        # felix-authz does not depend on the wire crate, so the list is narrowed.
        self.assertNotIn("felix-authz", out["test_packages"])

    def test_docs_only_change_runs_no_gated_job(self):
        _, ran = jobs("docs/protocol.md", "docs-site/src/content/docs/index.mdx")
        self.assertEqual(ran, set())

    def test_lockfile_change_runs_everything(self):
        self.assert_full("Cargo.lock")

    def test_helm_change_runs_only_the_chart(self):
        _, ran = jobs("deploy/helm/felix/values.yaml")
        self.assertEqual(ran, {"chart"})

    def test_metric_outside_the_broker_still_runs_the_chart(self):
        _, ran = jobs("crates/sdk/felix-client/src/lib.rs")
        self.assertIn("chart", ran)

    def test_python_sdk_change_runs_python_and_coverage(self):
        _, ran = jobs("crates/sdk/felix-python/src/producer.rs")
        self.assertEqual(ran, {"python", "coverage"})

    def test_unclassified_path_runs_everything(self):
        self.assert_full("docs/protocol.md", "some-new-dir/file.txt")
        self.assert_full("crates/server/felix-storage/build.rs")
        self.assert_full("rust-toolchain.toml")
        self.assert_full("scripts/ci/affected.py")
        self.assert_full("scripts/new-helper.sh")

    def test_push_and_label_run_everything(self):
        self.assert_full("docs/protocol.md", event="push")
        self.assert_full("deploy/helm/felix/values.yaml", labels=["ci:full"])

    def test_client_pages_run_the_example_tests(self):
        out, ran = jobs("docs-site/src/content/docs/clients/rust.md")
        self.assertEqual(ran, {"test", "coverage", "python", "typescript"})
        self.assertEqual(out["test_packages"], "-p felix-capi -p felix-client")

    def test_demo_binary_maps_to_the_broker_service(self):
        out, _ = jobs("demos/broker/cache_demo.rs")
        self.assertIn("felix-broker-service", out["crates"].split())

    def test_fuzz_change_runs_fuzz_and_the_crates_seed_tests(self):
        _, ran = jobs("crates/protocol/felix-wire/fuzz/fuzz_targets/frame.rs")
        self.assertEqual(ran, {"fuzz", "test", "coverage"})

    def test_cluster_suite_change_runs_the_cluster_jobs(self):
        out, ran = jobs("crates/testing/felix-cluster/tests/routing/main.rs")
        self.assertEqual(ran, {"cluster", "coverage"})
        self.assertFalse(out["test"])

    def test_compose_needs_images(self):
        _, ran = jobs("compose.yaml")
        self.assertEqual(ran, {"images", "compose"})

    def test_dockerfile_change_runs_the_test_that_reads_it(self):
        out, ran = jobs("docker/broker.Dockerfile")
        self.assertEqual(ran, {"images", "compose", "test", "coverage"})
        self.assertEqual(out["test_packages"], "-p felix-common")

    def test_felixctl_change_runs_the_cluster_and_sim_jobs(self):
        out, ran = jobs("crates/tools/felixctl/src/main.rs")
        self.assertTrue({"cluster", "sim", "images", "coverage"} <= ran, ran)
        # Bin-only: nothing for the doctest step, so no full residual run.
        self.assertFalse(out["test"])


class Fixture(unittest.TestCase):
    def test_fixture_matches_the_workspace(self):
        # From the manifests rather than cargo, so this needs no toolchain.
        root = HERE.parent.parent
        members = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["members"]
        live = {}
        for member in members:
            manifest = tomllib.loads((root / member / "Cargo.toml").read_text())
            tables = [manifest.get(k, {}) for k in DEP_TABLES]
            for target in manifest.get("target", {}).values():
                tables += [target.get(k, {}) for k in DEP_TABLES]
            live[manifest["package"]["name"]] = sorted({
                spec.get("package", key)
                for table in tables for key, spec in table.items()
                if isinstance(spec, dict) and "path" in spec})
        fixture = {p["name"]: sorted({d["name"] for d in p["dependencies"]})
                   for p in METADATA["packages"]}
        self.assertEqual(live, fixture,
                         "the crate graph changed; regenerate fixtures/affected-metadata.json")


if __name__ == "__main__":
    unittest.main()
