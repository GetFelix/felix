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

    def resume(self, p50, p99, msg_s=50000, errors=0, short_reads=0, bad_first=0, held=20000):
        return {"replayed_throughput_msg_s": msg_s, "errors": errors, "short_reads": short_reads,
                "bad_first_offsets": bad_first,
                "publish_throughput_msg_s": held, "publish_target_msg_s": 20000,
                "resume_latency_us": {"p50": p50, "p99": p99, "p999": p99 * 2},
                "replay_latency_us": {"p50": p50 * 2, "p99": p99 * 2}}

    def tail(self, msg_s, p50, p99, gaps=0, held=20000):
        return {"delivered_throughput_msg_s": msg_s, "gaps": gaps,
                "publish_throughput_msg_s": held, "publish_target_msg_s": 20000,
                "delivery_latency_us": {"p50": p50, "p99": p99}}

    def test_slower_raft_resume_fails(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-resume", "hybrid", "ok", self.resume(300, 900)))
            rows.append((rnd, "rf3-resume", "raft", "ok", self.resume(300, 1800)))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")
        failed = [m["id"] for m in res["cells"][0]["metrics"] if m["gates"] and m["verdict"] == "FAIL"]
        self.assertEqual(failed, ["resume_p99"])

    def test_matching_resume_and_follower_tail_pass(self):
        rows = []
        for rnd in (1, 2, 3):
            for side in ("hybrid", "raft"):
                rows.append((rnd, "rf3-resume", side, "ok", self.resume(300, 900)))
                rows.append((rnd, "rf3-follower-tail", side, "ok", self.tail(20000, 800, 2500)))
        self.assertEqual(s.summarize(self.write(rows))["verdict"], "PASS")

    def test_a_dropped_record_on_a_follower_tail_fails(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-follower-tail", "hybrid", "ok", self.tail(20000, 800, 2500)))
            rows.append((rnd, "rf3-follower-tail", "raft", "ok", self.tail(20000, 800, 2500, gaps=4)))
        self.assertEqual(s.summarize(self.write(rows))["verdict"], "FAIL")

    def kafka_tail(self, consumed=20000, produced=20000, rate=20000, held=20000):
        return {"consumed_throughput_msg_s": rate, "consumed_records": consumed,
                "produced_records": produced, "publish_throughput_msg_s": held,
                "publish_target_msg_s": 20000}

    def verdicts(self, res):
        return {m["id"]: m["verdict"] for m in res["cells"][0]["metrics"] if m["gates"]}

    def test_kafka_tail_gates_consumed_throughput(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-kafka-tail", "hybrid", "ok", self.kafka_tail(rate=40000)))
            rows.append((rnd, "rf3-kafka-tail", "raft", "ok", self.kafka_tail(rate=20000)))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(self.verdicts(res)["consume_msg_s"], "FAIL")

    def test_kafka_tail_must_consume_everything_produced(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-kafka-tail", "hybrid", "ok", self.kafka_tail()))
            rows.append((rnd, "rf3-kafka-tail", "raft", "ok", self.kafka_tail(consumed=19000)))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(self.verdicts(res)["completeness"], "FAIL")

    def test_a_publisher_under_95_percent_of_target_invalidates_the_cell(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-resume", "hybrid", "ok", self.resume(300, 900)))
            rows.append((rnd, "rf3-resume", "raft", "ok", self.resume(300, 900, held=18000)))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(self.verdicts(res)["rate_attain"], "FAIL")

    def test_resume_correctness_counters_must_be_zero_on_both_engines(self):
        for field in ("errors", "short_reads", "bad_first"):
            for bad_side in ("hybrid", "raft"):
                rows = []
                for rnd in (1, 2, 3):
                    for side in ("hybrid", "raft"):
                        extra = {field: 1} if (side == bad_side and rnd == 2) else {}
                        rows.append((rnd, "rf3-resume", side, "ok", self.resume(300, 900, **extra)))
                res = s.summarize(self.write(rows))
                self.assertEqual(res["verdict"], "FAIL", (field, bad_side))

    def test_the_same_failure_on_both_engines_is_still_a_failure(self):
        # A ratio would call these equal; the absolute gates do not.
        cases = [("rf3-resume", self.resume(300, 900, short_reads=2), "short_reads"),
                 ("rf3-resume", self.resume(300, 900, errors=2), "resume_errors"),
                 ("rf3-resume", self.resume(300, 900, bad_first=2), "bad_first"),
                 ("rf3-resume", self.resume(300, 900, held=15000), "rate_attain"),
                 ("rf3-kafka-tail", self.kafka_tail(consumed=19000), "completeness"),
                 ("rf3-kafka-tail", self.kafka_tail(held=15000), "rate_attain")]
        for cell, data, metric in cases:
            rows = [(rnd, cell, side, "ok", data) for rnd in (1, 2, 3) for side in ("hybrid", "raft")]
            res = s.summarize(self.write(rows))
            self.assertEqual(self.verdicts(res)[metric], "FAIL", metric)
            self.assertEqual(res["verdict"], "FAIL", metric)

    def test_a_gap_on_the_hybrid_side_also_fails_the_follower_tail(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-follower-tail", "hybrid", "ok", self.tail(20000, 800, 2500, gaps=3)))
            rows.append((rnd, "rf3-follower-tail", "raft", "ok", self.tail(20000, 800, 2500, gaps=3)))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(self.verdicts(res)["gaps"], "FAIL")


if __name__ == "__main__":
    unittest.main()
