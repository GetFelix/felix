"""Tests for ipm_gate.py: python3 -m unittest discover -s scripts/perf -p 'test_ipm_gate.py'"""
import contextlib
import io
import shutil
import tempfile
import tomllib
import unittest
from pathlib import Path

import ipm_gate

BASELINE = """\
tolerance_pct = 3.0

[aarch64]
commit = "abc"
toolchain = "rustc"
runs = 5
b1 = 300000
b64 = 9000

[aarch64.pace]
b1 = 20
"""


def result(b1, b64=9000.0, pace=20, failures=(), commit="abc"):
    return {
        "arch": "aarch64",
        "commit": commit,
        "toolchain": "rustc",
        "cells": {
            "b1": {"user_per_message": b1, "user_spread_pct": 1.0, "pace_msg_s": pace},
            "b64": {"user_per_message": b64, "user_spread_pct": 1.0, "pace_msg_s": None},
            "b64_mqtt_idle": {"user_per_message": b64, "user_spread_pct": 1.0, "pace_msg_s": None},
        },
        "measurement_failures": list(failures),
    }


class GateTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir)
        path = self.dir / "ipm-baseline.toml"
        path.write_text(BASELINE)
        self.old = ipm_gate.BASELINE
        ipm_gate.BASELINE = path
        self.addCleanup(setattr, ipm_gate, "BASELINE", self.old)

    def run_quietly(self, fn, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = fn(*args)
        return code, out.getvalue()

    def test_within_tolerance_passes(self):
        code, _ = self.run_quietly(ipm_gate.check, result(305000), False)
        self.assertEqual(code, 0)

    def test_over_tolerance_is_a_regression(self):
        code, out = self.run_quietly(ipm_gate.check, result(320000), False)
        self.assertEqual(code, 1)
        self.assertIn("Regression", out)

    def test_a_failed_measurement_is_not_reported_as_a_regression(self):
        code, out = self.run_quietly(ipm_gate.check, result(400000, failures=["b1 rep 2: fell behind"]), False)
        self.assertEqual(code, 1)
        self.assertIn("not a regression", out)
        self.assertNotIn("Regression.", out)

    def test_a_run_at_another_pace_is_not_compared(self):
        code, out = self.run_quietly(ipm_gate.check, result(300000, pace=40), False)
        self.assertEqual(code, ipm_gate.EXIT_STALE_BASELINE)
        self.assertEqual(code, 3)
        self.assertIn("refresh needed", out)
        self.assertIn("not a regression", out)

    def test_update_refuses_fewer_than_five_runs(self):
        code, _ = self.run_quietly(ipm_gate.update, [result(300000)] * 4)
        self.assertEqual(code, 1)
        self.assertEqual(ipm_gate.BASELINE.read_text(), BASELINE)

    def test_update_refuses_runs_of_different_commits(self):
        runs = [result(300000)] * 4 + [result(300000, commit="def")]
        code, _ = self.run_quietly(ipm_gate.update, runs)
        self.assertEqual(code, 1)

    def test_update_takes_the_median_run_and_keeps_the_pace(self):
        runs = [result(v, commit="new") for v in (310000, 290000, 300500, 299000, 350000)]
        code, _ = self.run_quietly(ipm_gate.update, runs)
        self.assertEqual(code, 0)
        written = tomllib.loads(ipm_gate.BASELINE.read_text())
        self.assertEqual(written["aarch64"]["b1"], 300500)
        self.assertEqual(written["aarch64"]["runs"], 5)
        self.assertEqual(written["aarch64"]["commit"], "new")
        self.assertEqual(written["aarch64"]["pace"], {"b1": 20})
        self.assertNotIn("b64_mqtt_idle", written["aarch64"])


if __name__ == "__main__":
    unittest.main()
