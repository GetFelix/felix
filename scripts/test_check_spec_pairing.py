#!/usr/bin/env python3
"""Tests for check_spec_pairing.py. Run: python3 -m unittest scripts/test_check_spec_pairing.py"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_spec_pairing as sp  # noqa: E402


class StripComments(unittest.TestCase):
    def test_cfg_comment_edit_is_not_a_change(self):
        a = "CONSTANTS\n  N = 3 \\* three\nINVARIANT Safe\n"
        b = "CONSTANTS\n  N = 3 \\* a new remark\n\\* more\nINVARIANT Safe\n"
        self.assertEqual(sp.strip_comments(a, ".cfg"), sp.strip_comments(b, ".cfg"))

    def test_cfg_value_edit_is_a_change(self):
        a = "CONSTANTS\n  N = 3\n"
        b = "CONSTANTS\n  N = 4\n"
        self.assertNotEqual(sp.strip_comments(a, ".cfg"), sp.strip_comments(b, ".cfg"))

    def test_tla_block_and_line_comments_ignored(self):
        a = "(* old\n prose *)\nInit == x = 0 \\* start\n"
        b = "(* new prose *)\nInit ==   x = 0\n"
        self.assertEqual(sp.strip_comments(a, ".tla"), sp.strip_comments(b, ".tla"))

    def test_tla_definition_edit_is_a_change(self):
        a = "Init == x = 0\n"
        b = "Init == x = 1\n"
        self.assertNotEqual(sp.strip_comments(a, ".tla"), sp.strip_comments(b, ".tla"))


class Tokenizer(unittest.TestCase):
    def test_block_opener_inside_line_comment(self):
        a = "A == 1 \\* see (* x\nB == 2\nC==3 *)\nD==4"
        got = sp.strip_comments(a, ".tla")
        self.assertIn("B == 2", got)
        self.assertIn("C==3", got)

    def test_nested_blocks(self):
        a = "(* a (* b *) still comment *) X == 1"
        self.assertEqual(sp.strip_comments(a, ".tla"), "X == 1")

    def test_string_literal_keeps_markers(self):
        a = 'S == "a \\* b (* c"\nT == 2'
        got = sp.strip_comments(a, ".tla")
        self.assertIn("T == 2", got)
        self.assertIn('"a \\* b (* c"', got)

    def test_line_comment_inside_block(self):
        a = "(* x \\* y *) Z == 3"
        self.assertEqual(sp.strip_comments(a, ".tla"), "Z == 3")

    def test_cfg_block_comment(self):
        self.assertEqual(sp.strip_comments("(* note *)\nINVARIANT A", ".cfg"), "INVARIANT A")


class Modelled(unittest.TestCase):
    def test_new_paths(self):
        for path in (
            "crates/server/felix-common/src/fleet.rs",
            "crates/server/felix-broker/src/broker/commit.rs",
            "crates/server/felix-broker/src/broker/publish/completion.rs",
            "crates/server/felix-storage/src/cache/log_cache/index.rs",
        ):
            self.assertTrue(sp.modelled(path), path)

    def test_tests_excluded(self):
        self.assertFalse(sp.modelled("crates/server/felix-broker/src/broker/publish/tests.rs"))
        self.assertFalse(sp.modelled("crates/server/felix-common/src/fleet_other.rs"))


if __name__ == "__main__":
    unittest.main()
