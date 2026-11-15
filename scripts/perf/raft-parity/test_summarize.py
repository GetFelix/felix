import json
import os
import tempfile
import unittest

import summarize as s


class Compare(unittest.TestCase):
    def test_equal_sides_pass(self):
        r = s.compare([100, 101, 99], [100, 100, 100], True)
        self.assertEqual(r["verdict"], "PASS")

    def test_a_gap_beyond_noise_fails(self):
        r = s.compare([70, 71, 70], [100, 101, 100], True)
        self.assertEqual(r["verdict"], "FAIL")
        self.assertLess(r["ratio"], 0.75)

    def test_lower_is_better_flips_the_ratio(self):
        faster = s.compare([50, 51, 50], [100, 100, 101], False)
        self.assertGreater(faster["ratio"], 1.9)
        self.assertEqual(faster["verdict"], "PASS")
        slower = s.compare([150, 151, 150], [100, 100, 101], False)
        self.assertEqual(slower["verdict"], "FAIL")

    def test_noisy_data_widens_the_tolerance(self):
        # An 8% gap, but hybrid itself swings 20%: not a measurable loss.
        r = s.compare([92, 91, 93], [100, 80, 120], True)
        self.assertEqual(r["verdict"], "PASS")
        self.assertGreater(r["tolerance"], 0.08)

    def test_huge_variance_cannot_buy_a_pass(self):
        r = s.compare([60, 40, 80], [100, 100, 100], True)
        self.assertEqual(r["verdict"], "FAIL")
        self.assertLessEqual(r["tolerance"], 0.10)

    def test_zero_failed_sends_pass(self):
        self.assertEqual(s.compare([0, 0], [0, 0], False)["verdict"], "PASS")
        self.assertEqual(s.compare([3, 0], [0, 0], False)["verdict"], "FAIL")

    def test_missing_side_fails(self):
        self.assertEqual(s.compare([], [1, 2], True)["verdict"], "FAIL")


class Run(unittest.TestCase):
    def write(self, cells):
        d = tempfile.mkdtemp()
        os.makedirs(os.path.join(d, "cells"))
        with open(os.path.join(d, "run.env"), "w") as f:
            f.write("rev abc\n")
        with open(os.path.join(d, "cells.tsv"), "w") as f:
            f.write("round\tcell\tside\tloadgen_box\tstatus\n")
            for rnd, cell, side, status, doc in cells:
                f.write("%d\t%s\t%s\t1\t%s\n" % (rnd, cell, side, status))
                if doc is not None:
                    p = os.path.join(d, "cells", "r%d-%s-%s" % (rnd, cell, side))
                    os.makedirs(p)
                    with open(os.path.join(p, "loadgen.json"), "w") as g:
                        json.dump(doc, g)
        return d

    def ingest(self, msg_s, p99):
        return {"throughput_msg_s": msg_s, "failed_sends": 0,
                "batch_ack_latency_us": {"p50": p99 / 2, "p99": p99, "p999": p99 * 2}}

    def test_refused_raft_cell_fails_the_matrix(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-mqtt", "hybrid", "ok", self.ingest(1000, 500)))
            rows.append((rnd, "rf3-mqtt", "raft", "stream-failed", None))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")
        self.assertIn("raft not measured", res["cells"][0]["notes"][0])

    def test_matching_cells_pass_and_render(self):
        rows = []
        for rnd in (1, 2, 3):
            for side in ("hybrid", "raft"):
                rows.append((rnd, "rf3-oncommit-tput", side, "ok", self.ingest(1000 + rnd, 500)))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "PASS")
        self.assertIn("rf3-oncommit-tput: PASS", s.markdown(res))

    def test_one_failed_round_fails_the_cell(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-oncommit-tput", "hybrid", "ok", self.ingest(1000, 500)))
            ok = rnd == 3
            rows.append((rnd, "rf3-oncommit-tput", "raft", "ok" if ok else "loadgen-failed",
                         self.ingest(1000, 500) if ok else None))
        self.assertEqual(s.summarize(self.write(rows))["verdict"], "FAIL")

    def test_failover_without_catch_up_fails(self):
        rows = []
        for rnd in (1, 2):
            rows.append((rnd, "rf3-failover", "hybrid", "ok", {"first_commit_ms": 900, "caught_up_ms": 1500}))
            rows.append((rnd, "rf3-failover", "raft", "ok", {"first_commit_ms": 800, "caught_up_ms": None}))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")


if __name__ == "__main__":
    unittest.main()
