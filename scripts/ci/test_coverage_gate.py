"""Run with: python3 -m unittest discover -s scripts/ci"""
import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "coverage_gate", os.path.join(os.path.dirname(__file__), "coverage_gate.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def lcov(files):
    return "".join(
        f"SF:{p}\nDA:1,1\nLF:{f}\nLH:{h}\nend_of_record\n" for p, (f, h) in files.items())


class GateTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def write(self, name, text):
        path = os.path.join(self.dir.name, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path

    def main(self, *argv):
        out, code = io.StringIO(), 0
        old, sys.argv = sys.argv, ["coverage_gate.py", *argv]
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                gate.main()
        except SystemExit as e:
            code = e.code
        finally:
            sys.argv = old
        return code, out.getvalue()

    def run_check(self, base, now):
        lcov_path = self.write("lcov.info", lcov(now))
        base_lcov = self.write("base.info", lcov(base))
        pct = self.write("base.txt", f"{gate.percent(base_lcov):.2f}\n")
        files = self.write("base.json", json.dumps(gate.per_file(base_lcov)))
        return self.main("check", lcov_path, pct, "--baseline-files", files)

    def test_per_file_sums_records(self):
        path = self.write("a.info", lcov({"a.rs": (10, 7), "b.rs": (4, 4)}))
        self.assertEqual(gate.per_file(path), {"a.rs": [10, 7], "b.rs": [4, 4]})
        self.assertEqual(gate.totals(path), (14, 11))

    def test_failing_gate_names_the_file(self):
        code, out = self.run_check(
            {"a.rs": (100, 90), "b.rs": (100, 90)}, {"a.rs": (100, 80), "b.rs": (100, 90)})
        self.assertEqual(code, 1)
        self.assertIn("::group::", out)
        self.assertIn("+10  a.rs", out)
        self.assertNotIn("b.rs", out)

    def test_passing_gate_still_explains(self):
        code, out = self.run_check({"a.rs": (1000, 900)}, {"a.rs": (1000, 899)})
        self.assertEqual(code, 0)
        self.assertIn("+1  a.rs", out)

    def test_added_and_removed_files(self):
        _, out = self.run_check({"old.rs": (10, 10)}, {"new.rs": (10, 5)})
        self.assertIn("+5  new.rs", out)
        self.assertIn("-10  old.rs", out)

    def test_missing_baseline_files_is_quiet(self):
        lcov_path = self.write("lcov.info", lcov({"a.rs": (10, 10)}))
        pct = self.write("base.txt", "100.00\n")
        code, out = self.main("check", lcov_path, pct, "--baseline-files", "/nonexistent")
        self.assertEqual(code, 0)
        self.assertNotIn("::group::", out)


if __name__ == "__main__":
    unittest.main()
