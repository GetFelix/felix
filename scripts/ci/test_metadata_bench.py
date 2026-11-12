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

    def files(self, base, head, aa=None):
        paths = [write(self.dir.name, "base.json", base), write(self.dir.name, "head.json", head)]
        if aa is not None:
            paths.append(write(self.dir.name, "aa.json", aa))
        return paths

    def test_within_noise_passes_and_outside_fails(self):
        cell = lambda ops, p99: {"propose": {"ops_per_s": ops, "p99_ms": p99}}
        base, head, aa = self.files(cell(1000, 10), cell(950, 10.4), cell(1100, 10))
        # The baseline runs differ by about 10% of their mean, so head 5% under
        # the mean is inside the band.
        self.assertEqual(run("compare", base, head, "--aa", aa).returncode, 0)
        base, head, aa = self.files(cell(1000, 10), cell(800, 10), cell(1010, 10))
        self.assertEqual(run("compare", base, head, "--aa", aa).returncode, 1)

    def test_lower_is_better_for_latency(self):
        cell = lambda p99: {"propose": {"p99_ms": p99}}
        base, head = self.files(cell(10), cell(12))
        self.assertEqual(run("compare", base, head).returncode, 1)
        base, head = self.files(cell(10), cell(5))
        self.assertEqual(run("compare", base, head).returncode, 0)

    def test_a_missing_cell_fails(self):
        base, head = self.files({"a": {"p99_ms": 1}}, {"b": {"p99_ms": 1}})
        self.assertEqual(run("compare", base, head).returncode, 1)


if __name__ == "__main__":
    unittest.main()
