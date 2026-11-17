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
    @staticmethod
    def cpu(per_kmsg=50.0, msgs=1_000_000, boxes=2, hz=100):
        """The CPU fields of a loadgen document: `per_kmsg` ms of broker CPU per 1000
        messages, spread over `boxes` brokers."""
        ticks = round(per_kmsg * msgs / 1e6 * hz / boxes)
        return {"acked_records": msgs,
                "broker_cpu": {"hz": hz, "boxes": {str(b): [1000, 1000 + ticks] for b in range(boxes)}}}

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
                    if cell != "rf3-failover":
                        doc = {**self.cpu(), **doc}
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

    def mqtt_lat(self, p50, p99, p999=None):
        return {"batch_ack_latency_us": {"p50": p50, "p99": p99, "p999": p999 or p99 * 2},
                "throughput_msg_s": 900}

    def mqtt_tail(self, delivered=60000, held=15000):
        return {"scenario": "mqtt-subscribe", "received": 1_800_000,
                "delivered_throughput_msg_s": delivered,
                "publish_throughput_msg_s": held, "publish_target_msg_s": 15000}

    def test_mqtt_lat_gates_p50_and_p99_and_only_reports_p999(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-mqtt-lat", "hybrid", "ok", self.mqtt_lat(400, 900)))
            rows.append((rnd, "rf3-mqtt-lat", "raft", "ok", self.mqtt_lat(400, 900, p999=9000)))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "PASS")
        self.assertEqual(sorted(self.verdicts(res)), ["ack_p50", "ack_p99", "cpu_kmsg"])
        for slow, metric in (((800, 900), "ack_p50"), ((400, 1800), "ack_p99")):
            rows = []
            for rnd in (1, 2, 3):
                rows.append((rnd, "rf3-mqtt-lat", "hybrid", "ok", self.mqtt_lat(400, 900)))
                rows.append((rnd, "rf3-mqtt-lat", "raft", "ok", self.mqtt_lat(*slow)))
            res = s.summarize(self.write(rows))
            self.assertEqual(self.verdicts(res)[metric], "FAIL", metric)
            self.assertEqual(res["verdict"], "FAIL")

    def test_mqtt_tail_gates_delivered_throughput_and_publisher_rate(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-mqtt-tail", "hybrid", "ok", self.mqtt_tail()))
            rows.append((rnd, "rf3-mqtt-tail", "raft", "ok", self.mqtt_tail()))
        self.assertEqual(s.summarize(self.write(rows))["verdict"], "PASS")
        rows = [(rnd, "rf3-mqtt-tail", side, "ok", self.mqtt_tail(delivered=60000 if side == "hybrid" else 30000))
                for rnd in (1, 2, 3) for side in ("hybrid", "raft")]
        res = s.summarize(self.write(rows))
        self.assertEqual(self.verdicts(res)["deliv_msg_s"], "FAIL")
        # A publisher under 95% of its rate on both engines is an invalid cell, not a tie.
        rows = [(rnd, "rf3-mqtt-tail", side, "ok", self.mqtt_tail(held=14000))
                for rnd in (1, 2, 3) for side in ("hybrid", "raft")]
        res = s.summarize(self.write(rows))
        self.assertEqual(self.verdicts(res)["rate_attain"], "FAIL")
        self.assertEqual(res["verdict"], "FAIL")


class Cpu(Run):
    def cell_rows(self, hybrid, raft, cell="rf3-oncommit-tput"):
        rows = []
        for rnd in (1, 2, 3):
            for side, per in (("hybrid", hybrid), ("raft", raft)):
                rows.append((rnd, cell, side, "ok", {**self.ingest(1000, 500), **self.cpu(per)}))
        return rows

    def test_cpu_per_kmsg_sums_every_broker(self):
        d = self.cpu(per_kmsg=30.0)
        self.assertAlmostEqual(s.cpu_ms_per_kmsg(d), 30.0, places=2)
        # 3 brokers x 1000 ticks at 100/s = 30 CPU s over 1e6 messages = 30 ms per 1000.
        d = {"acked_records": 1_000_000,
             "broker_cpu": {"hz": 100, "boxes": {"1": [5, 1005], "2": [0, 1000], "3": [7, 1007]}}}
        self.assertAlmostEqual(s.cpu_ms_per_kmsg(d), 30.0)

    def test_the_message_count_is_what_the_generator_counted(self):
        boxes = {"1": [0, 100]}
        for key in ("acked_records", "received", "replayed", "consumed_records"):
            d = {key: 10_000, "broker_cpu": {"hz": 100, "boxes": boxes}}
            self.assertAlmostEqual(s.cpu_ms_per_kmsg(d), 100.0, msg=key)

    def test_a_missing_or_backwards_sample_is_no_value(self):
        good = {"acked_records": 1000, "broker_cpu": {"hz": 100, "boxes": {"1": [0, 10]}}}
        self.assertIsNotNone(s.cpu_ms_per_kmsg(good))
        for bad in ({"acked_records": 1000},
                    {"broker_cpu": good["broker_cpu"]},
                    {"acked_records": 1000, "broker_cpu": {"hz": 100, "boxes": {"1": [10, 0]}}},
                    {"acked_records": 1000, "broker_cpu": {"hz": 100, "boxes": {"1": [0, None]}}},
                    {"acked_records": 1000, "broker_cpu": {"hz": 100, "boxes": {"1": [0, 10], "2": [None, 4]}}}):
            self.assertIsNone(s.cpu_ms_per_kmsg(bad), bad)

    def test_raft_within_10_percent_of_hybrid_passes(self):
        res = s.summarize(self.write(self.cell_rows(100.0, 109.0)))
        self.assertEqual(res["verdict"], "PASS")

    def test_raft_over_10_percent_more_cpu_fails_like_a_throughput_miss(self):
        res = s.summarize(self.write(self.cell_rows(100.0, 112.0)))
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(self.verdicts(res)["cpu_kmsg"], "FAIL")
        # Everything else matched, so only CPU failed.
        others = {k: v for k, v in self.verdicts(res).items() if k != "cpu_kmsg"}
        self.assertEqual(set(others.values()), {"PASS"})

    def test_noise_does_not_widen_the_cpu_tolerance(self):
        rows = []
        for rnd, (h, r) in enumerate([(100, 100), (60, 140), (140, 60)], 1):
            rows.append((rnd, "rf3-oncommit-tput", "hybrid", "ok", {**self.ingest(1000, 500), **self.cpu(h)}))
            rows.append((rnd, "rf3-oncommit-tput", "raft", "ok", {**self.ingest(1000, 500), **self.cpu(r * 1.15)}))
        res = s.summarize(self.write(rows))
        self.assertEqual(self.verdicts(res)["cpu_kmsg"], "FAIL")

    def test_less_cpu_on_raft_passes(self):
        self.assertEqual(s.summarize(self.write(self.cell_rows(100.0, 60.0)))["verdict"], "PASS")

    def test_a_cell_with_no_cpu_sample_fails(self):
        rows = []
        for rnd in (1, 2, 3):
            rows.append((rnd, "rf3-oncommit-tput", "hybrid", "ok", self.ingest(1000, 500)))
            doc = {**self.ingest(1000, 500), "broker_cpu": None}
            rows.append((rnd, "rf3-oncommit-tput", "raft", "ok", doc))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "FAIL")

    def test_the_report_lists_cpu_per_cell_with_the_ratio(self):
        md = s.markdown(s.summarize(self.write(self.cell_rows(100.0, 105.0))))
        self.assertIn("Broker CPU per 1000 messages (ms, all brokers", md)
        self.assertIn("| rf3-oncommit-tput | 100 | 105 | 1.050 | PASS |", md)

    def test_cpu_files_are_read_from_the_cell_directory(self):
        d = tempfile.mkdtemp()
        for box, (before, after) in {"1": (100, 400), "2": (50, 250)}.items():
            for phase, ticks in (("before", before), ("after", after)):
                with open(os.path.join(d, "cpu-%s-box%s.txt" % (phase, box)), "w") as f:
                    f.write("ticks=%d hz=100\n" % ticks)
        self.assertEqual(s.read_cpu(d), {"hz": 100, "boxes": {"1": [100, 400], "2": [50, 250]}})
        # A box whose after-sample failed is kept as missing, so the cell has no value.
        os.remove(os.path.join(d, "cpu-after-box2.txt"))
        got = s.read_cpu(d)
        self.assertEqual(got["boxes"]["2"], [50, None])
        self.assertIsNone(s.cpu_ms_per_kmsg({"acked_records": 1000, "broker_cpu": got}))
        self.assertIsNone(s.read_cpu(tempfile.mkdtemp()))

    def test_load_run_attaches_the_cpu_files_to_the_cell(self):
        rows = [(1, "rf3-mqtt", side, "ok", self.ingest(1000, 500)) for side in ("hybrid", "raft")]
        run = self.write(rows)
        for side, per in (("hybrid", 4000), ("raft", 4200)):
            cell = os.path.join(run, "cells", "r1-rf3-mqtt-%s" % side)
            with open(os.path.join(cell, "loadgen.json")) as f:
                doc = json.load(f)
            doc.pop("broker_cpu"); doc.pop("acked_records")
            doc["acked_records"] = 100_000
            with open(os.path.join(cell, "loadgen.json"), "w") as f:
                json.dump(doc, f)
            for phase, ticks in (("before", 10), ("after", 10 + per)):
                with open(os.path.join(cell, "cpu-%s-box1.txt" % phase), "w") as f:
                    f.write("ticks=%d hz=100\n" % ticks)
        res = s.summarize(run)
        cpu = [m for m in res["cells"][0]["metrics"] if m["id"] == "cpu_kmsg"][0]
        self.assertAlmostEqual(cpu["hybrid"]["mean"], 400.0)
        self.assertAlmostEqual(cpu["raft"]["mean"], 420.0)
        self.assertEqual(cpu["verdict"], "PASS")


class TokenHandoffTest(unittest.TestCase):
    def test_a_token_is_never_copied_onto_the_box_it_comes_from(self):
        # A self-copy truncates client.jwt, and the loadgen then connects with no token.
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "run.sh")) as f:
            lines = [l for l in f if 'put "$dir/tokens/client.jwt"' in l]
        self.assertTrue(lines)
        for l in lines:
            self.assertIn('[ "$lg" = "$CP_BOX" ] ||', l)


if __name__ == "__main__":
    unittest.main()
