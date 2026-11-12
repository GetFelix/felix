import json
import os
import subprocess
import sys
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(__file__), "metadata_bench.py")


def write(directory, name, cells):
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as out:
        json.dump({"cells": cells}, out)
    return path


def run(*args):
    return subprocess.run([sys.executable, SCRIPT, *args], capture_output=True, text=True)


class CompareTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.count = 0

    def runs(self, *cells):
        paths = []
        for cell in cells:
            self.count += 1
            paths.append(write(self.dir.name, f"run{self.count}.json", cell))
        return paths

    def compare(self, base, head, *extra):
        return run("compare", "--base", *base, "--head", *head, *extra).returncode


def ops(value, **extra):
    return {"propose": {"ops_per_s": value, **extra}}


class Bands(CompareTest):
    def test_within_noise_passes_and_outside_fails(self):
        # The base runs spread about 10% around 1000, so 950 is inside the band.
        base = self.runs(ops(950), ops(1000), ops(1050))
        self.assertEqual(self.compare(base, self.runs(ops(950))), 0)
        self.assertEqual(self.compare(base, self.runs(ops(800))), 1)

    def test_the_median_ignores_one_wild_run(self):
        base = self.runs(ops(1000), ops(1005), ops(995))
        head = self.runs(ops(1000), ops(5000), ops(990))
        self.assertEqual(self.compare(base, head), 0)

    def test_a_wide_spread_in_the_base_widens_the_band(self):
        base = self.runs(ops(500), ops(1000), ops(1500))
        self.assertEqual(self.compare(base, self.runs(ops(600))), 0)

    def test_lower_is_better_for_latency(self):
        cell = lambda p99: {"propose": {"p99_ms": p99}}
        base = self.runs(cell(10))
        self.assertEqual(self.compare(base, self.runs(cell(12))), 1)
        self.assertEqual(self.compare(base, self.runs(cell(5))), 0)

    def test_a_missing_cell_fails(self):
        base = self.runs({"a": {"p99_ms": 1}})
        self.assertEqual(self.compare(base, self.runs({"b": {"p99_ms": 1}})), 1)


class Flags(CompareTest):
    def test_ok_going_from_one_to_zero_fails_and_zero_to_one_passes(self):
        cell = lambda ok: {"snapshot_install": {"ok": ok}}
        self.assertEqual(self.compare(self.runs(cell(True)), self.runs(cell(False))), 1)
        self.assertEqual(self.compare(self.runs(cell(False)), self.runs(cell(True))), 0)

    def test_a_failover_that_loses_rounds_fails(self):
        cell = lambda rounds, p50, lost: {
            "failover": {"p50_ms": p50, "rounds": rounds, "failed_rounds": lost}
        }
        base = self.runs(cell(5, 900, 0))
        # Every round failed: no latency was recorded, which must not read as fast.
        self.assertEqual(self.compare(base, self.runs(cell(0, 0, 5))), 1)
        self.assertEqual(self.compare(base, self.runs(cell(4, 900, 1))), 1)
        self.assertEqual(self.compare(base, self.runs(cell(5, 910, 0))), 0)


    def test_fewer_rounds_alone_fail(self):
        cell = lambda rounds: {"failover": {"rounds": rounds}}
        self.assertEqual(self.compare(self.runs(cell(5)), self.runs(cell(0))), 1)


class Table(CompareTest):
    def test_table_reports_the_median_of_the_runs(self):
        paths = self.runs(ops(1), ops(9), ops(5))
        out = run("table", *paths)
        self.assertEqual(out.returncode, 0)
        self.assertIn("| propose ops_per_s | 5 |", out.stdout)


if __name__ == "__main__":
    unittest.main()
