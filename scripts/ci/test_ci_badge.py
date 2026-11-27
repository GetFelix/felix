"""Run with: python3 -m unittest discover -s scripts/ci"""
import importlib.util
import json
import os
import unittest

here = os.path.dirname(__file__)
spec = importlib.util.spec_from_file_location("ci_badge", os.path.join(here, "ci_badge.py"))
badge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(badge)

with open(os.path.join(here, "fixtures", "ci-runs.json")) as f:
    RUNS = json.load(f)["workflow_runs"]


class NewestFinished(unittest.TestCase):
    def test_skips_cancelled_and_skipped(self):
        self.assertEqual(badge.newest_finished(RUNS)["id"], 102)

    def test_older_run_is_not_newest(self):
        self.assertNotEqual(badge.newest_finished(RUNS)["id"], 101)

    def test_only_cancelled_gives_none(self):
        self.assertIsNone(badge.newest_finished([RUNS[0], RUNS[3]]))

    def test_svg_colors(self):
        self.assertIn("#4c1", badge.render_svg(True))
        self.assertIn("#e05d44", badge.render_svg(False))


if __name__ == "__main__":
    unittest.main()
