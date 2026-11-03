"""Run with: python3 -m unittest discover -s scripts/ci"""
import importlib.util
import os
import unittest

spec = importlib.util.spec_from_file_location(
    "check_version_pins",
    os.path.join(os.path.dirname(__file__), "..", "check_version_pins.py"))
pins = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pins)

CURRENT = "0.7.0-preview.2"


class StalePins(unittest.TestCase):
    def test_older_preview_of_the_line_is_flagged_with_its_line(self):
        text = "ok\ndocker pull x:0.7.0-preview.1\n"
        self.assertEqual(pins.stale_pins(text, CURRENT), [(2, "0.7.0-preview.1")])

    def test_v_prefixed_tag_is_flagged(self):
        self.assertEqual(pins.stale_pins("tag v0.7.0-preview.1", CURRENT), [(1, "0.7.0-preview.1")])

    def test_python_rc_spelling_is_checked_as_the_same_preview(self):
        self.assertEqual(pins.stale_pins("==0.7.0rc1", CURRENT), [(1, "0.7.0rc1")])
        self.assertEqual(pins.stale_pins("==0.7.0rc2", CURRENT), [])

    def test_current_version_passes(self):
        self.assertEqual(pins.stale_pins("x:0.7.0-preview.2", CURRENT), [])

    def test_preview_of_another_line_is_history(self):
        self.assertEqual(pins.stale_pins("since 0.6.0-preview.9", CURRENT), [])

    def test_opt_out_marker_skips_the_line(self):
        text = "0.7.0-preview.1 <!-- version-pin: allow -->"
        self.assertEqual(pins.stale_pins(text, CURRENT), [])

    def test_released_version_is_the_compose_default(self):
        yml = "image: ghcr.io/getfelix/felix-broker:${FELIX_VERSION:-0.7.0-preview.2}\n"
        self.assertEqual(pins.released_version(yml), CURRENT)


if __name__ == "__main__":
    unittest.main()
