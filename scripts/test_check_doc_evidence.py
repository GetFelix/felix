#!/usr/bin/env python3
"""Tests for check_doc_evidence.py line citations. Run: python3 -m unittest scripts/test_check_doc_evidence.py"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_doc_evidence as ev  # noqa: E402

SOURCE = "\n".join(["// one", "fn alpha() {}", "fn beta() {}", "// four", "// five"]) + "\n"


def run(doc: str, files: dict[str, str]):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        docs = root / "docs"
        docs.mkdir()
        (docs / "a.md").write_text(doc)
        tracked = {}
        for rel, text in files.items():
            f = root / rel
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(text)
            tracked[rel] = [f]
        with mock.patch.object(ev, "REPO", root), mock.patch.object(ev, "DOCS", [docs]), mock.patch.object(
            ev, "tracked_rs", lambda: tracked
        ):
            return ev.check_lines()


class LineCitations(unittest.TestCase):
    def test_symbol_on_cited_line_passes(self):
        _, problems, warnings = run("`src/x.rs:2` `alpha`\n", {"src/x.rs": SOURCE})
        self.assertEqual((problems, warnings), ([], []))

    def test_symbol_off_line_fails(self):
        _, problems, _ = run("`src/x.rs:2` `beta`\n", {"src/x.rs": SOURCE})
        self.assertEqual(len(problems), 1)

    def test_range_accepts_symbol_inside(self):
        _, problems, _ = run("`src/x.rs:2-3` `beta`\n", {"src/x.rs": SOURCE})
        self.assertEqual(problems, [])

    def test_range_rejects_symbol_outside_and_past_end(self):
        _, problems, _ = run("`src/x.rs:4-5` `beta`\n", {"src/x.rs": SOURCE})
        self.assertEqual(len(problems), 1)
        _, problems, _ = run("`src/x.rs:3-9`\n", {"src/x.rs": SOURCE})
        self.assertIn("5 lines", problems[0])

    def test_bare_follow_on_cites_the_same_file(self):
        total, problems, _ = run("`src/x.rs:2`, `:9`\n", {"src/x.rs": SOURCE})
        self.assertEqual(total, 2)
        self.assertEqual(len(problems), 1)

    def test_bare_port_is_not_a_citation(self):
        total, _, _ = run("reset `:22` to the cloud\n", {"src/x.rs": SOURCE})
        self.assertEqual(total, 0)

    def test_no_symbol_warns_instead_of_failing(self):
        _, problems, warnings = run("`src/x.rs:2`\n", {"src/x.rs": SOURCE})
        self.assertEqual((problems, len(warnings)), ([], 1))

    def test_ambiguous_suffix_fails(self):
        files = {"a/src/x.rs": SOURCE, "b/src/x.rs": SOURCE}
        _, problems, _ = run("`x.rs:2` `alpha`\n", files)
        self.assertIn("ambiguous path", problems[0])
        _, problems, _ = run("`a/src/x.rs:2` `alpha`\n", files)
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
