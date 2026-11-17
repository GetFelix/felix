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

    def test_missing_current_side_fails(self):
        self.assertEqual(s.compare([], [1, 2], True)["verdict"], "FAIL")

    def test_missing_reference_side_is_not_a_verdict(self):
        self.assertEqual(s.compare([1, 2], [], True)["verdict"], "N/A")
        self.assertEqual(s.compare([], [], True)["verdict"], "N/A")
        self.assertEqual(s.compare_exact([0], [], lambda v: v == 0, "= 0")["verdict"], "N/A")
        self.assertEqual(s.compare_exact([], [0], lambda v: v == 0, "= 0")["verdict"], "FAIL")


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


class Baseline(Run):
    CELL = "rf3-oncommit-tput"

    def rows(self, hybrid=1000, raft=1000, base=1000, base_status="ok", rounds=3, p99=500, cell=None):
        cell = cell or self.CELL
        out = []
        for rnd in range(1, rounds + 1):
            out.append((rnd, cell, "hybrid", "ok", self.ingest(hybrid, p99)))
            out.append((rnd, cell, "raft", "ok", self.ingest(raft, p99)))
            out.append((rnd, cell, "baseline", base_status,
                        self.ingest(base, p99) if base_status == "ok" else None))
        return out

    def test_no_baseline_side_leaves_the_gate_off(self):
        res = s.summarize(self.write([r for r in self.rows() if r[2] != "baseline"]))
        self.assertNotIn("baseline_verdict", res)
        self.assertNotIn("Versus", s.markdown(res))

    def test_all_three_equal_pass(self):
        res = s.summarize(self.write(self.rows()))
        self.assertEqual(res["verdict"], "PASS")
        self.assertEqual(res["baseline_verdict"], "PASS")
        self.assertEqual(res["cells"][0]["baseline_verdict"], "PASS")
        self.assertIn("Versus", s.markdown(res))

    def test_raft_slower_than_baseline_fails_only_raft(self):
        # Raft equals hybrid, so the Raft-vs-hybrid gate passes, and both are 30% under the baseline.
        res = s.summarize(self.write(self.rows(hybrid=1000, raft=1000, base=1400)))
        self.assertEqual(res["verdict"], "PASS")
        self.assertEqual(res["baseline_verdict"], "FAIL")
        self.assertIn("raft msg_s", res["cells"][0]["baseline_failures"])
        self.assertIn("hybrid msg_s", res["cells"][0]["baseline_failures"])

    def test_hybrid_regression_alone_is_named(self):
        # Raft beats a hybrid that has itself regressed: Raft vs hybrid passes, hybrid vs baseline fails.
        res = s.summarize(self.write(self.rows(hybrid=700, raft=1000, base=1000)))
        self.assertEqual(res["verdict"], "PASS")
        fails = res["cells"][0]["baseline_failures"]
        self.assertIn("hybrid msg_s", fails)
        self.assertNotIn("raft msg_s", fails)

    def test_faster_than_baseline_passes_and_noise_is_the_same_rule(self):
        self.assertEqual(s.summarize(self.write(self.rows(base=500)))["baseline_verdict"], "PASS")
        # 2% under the baseline is inside the noise floor.
        self.assertEqual(s.summarize(self.write(self.rows(hybrid=980, raft=980)))["baseline_verdict"], "PASS")

    def test_latency_regression_against_baseline_fails(self):
        rows = self.rows(p99=500)
        rows = [(r, c, sd, st, self.ingest(1000, 800) if sd == "raft" else d) for r, c, sd, st, d in rows]
        res = s.summarize(self.write(rows))
        self.assertEqual(res["baseline_verdict"], "FAIL")
        self.assertTrue(any(f.startswith("raft ack_p") for f in res["cells"][0]["baseline_failures"]))

    def test_na_cell_is_left_out_and_not_a_failure(self):
        rows = self.rows()
        for rnd in (1, 2):
            rows.append((rnd, "rf3-mqtt", "hybrid", "ok", self.ingest(1000, 500)))
            rows.append((rnd, "rf3-mqtt", "raft", "ok", self.ingest(1000, 500)))
            rows.append((rnd, "rf3-mqtt", "baseline", "na", None))
        res = s.summarize(self.write(rows))
        mqtt = [c for c in res["cells"] if c["id"] == "rf3-mqtt"][0]
        self.assertEqual(mqtt["baseline_verdict"], "N/A")
        self.assertEqual(mqtt["verdict"], "PASS")
        self.assertEqual(res["baseline_verdict"], "PASS")
        self.assertIn("baseline n/a", " ".join(mqtt["notes"]))
        self.assertNotIn("baseline not measured", " ".join(mqtt["notes"]))

    def test_every_cell_na_judges_nothing(self):
        res = s.summarize(self.write(self.rows(base_status="na")))
        self.assertEqual(res["baseline_verdict"], "FAIL")

    def test_a_baseline_cell_that_failed_is_not_n_a(self):
        res = s.summarize(self.write(self.rows(base_status="loadgen-failed")))
        self.assertEqual(res["cells"][0]["baseline_verdict"], "FAIL")
        self.assertEqual(res["verdict"], "PASS")
        self.assertEqual(res["baseline_verdict"], "FAIL")

    def test_baseline_fail_makes_strict_exit_nonzero(self):
        run = self.write(self.rows(base=1500))
        self.assertEqual(s.main(["summarize.py", run, "--strict"]), 1)
        self.assertEqual(s.main(["summarize.py", run]), 0)
        self.assertTrue(os.path.exists(os.path.join(run, "results.md")))

    def test_a_side_absent_from_the_run_is_not_a_failure(self):
        res = s.summarize(self.write([r for r in self.rows(hybrid=1000, base=1000) if r[2] != "raft"]))
        self.assertEqual(res["verdict"], "N/A")
        self.assertEqual(res["baseline_verdict"], "PASS")
        self.assertEqual(res["cells"][0]["baseline_failures"], [])
        md = s.markdown(res)
        self.assertIn("not in this run", md)
        self.assertIn("1.000", md)
        self.assertEqual(s.main(["summarize.py", self.write([r for r in self.rows() if r[2] != "raft"]), "--strict"]), 0)

    def test_hybrid_regression_still_fails_without_a_raft_side(self):
        res = s.summarize(self.write([r for r in self.rows(hybrid=700, base=1000) if r[2] != "raft"]))
        self.assertEqual(res["baseline_verdict"], "FAIL")
        self.assertIn("hybrid msg_s", res["cells"][0]["baseline_failures"])

    def test_raft_from_another_run_is_judged_against_the_baseline(self):
        a = self.write([r for r in self.rows(hybrid=1000, base=1000) if r[2] != "raft"])
        b = self.write([r for r in self.rows(raft=600) if r[2] == "raft"])
        res = s.summarize(a, raft_from=b)
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(res["baseline_verdict"], "FAIL")
        self.assertEqual(res["cells"][0]["baseline_failures"], ["raft msg_s"])

    def test_failed_baseline_does_not_hide_a_raft_vs_hybrid_failure(self):
        res = s.summarize(self.write(self.rows(hybrid=1000, raft=600, base=1000)))
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(res["baseline_verdict"], "FAIL")


class MissingData(Baseline):
    def no_cpu(self, doc):
        return {**doc, "broker_cpu": None}

    def test_a_baseline_without_cpu_files_is_not_a_failure(self):
        rows = []
        for rnd in (1, 2, 3):
            for side in ("hybrid", "raft"):
                rows.append((rnd, self.CELL, side, "ok", self.ingest(1000, 500)))
            rows.append((rnd, self.CELL, "baseline", "ok", self.no_cpu(self.ingest(1000, 500))))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["baseline_verdict"], "PASS")
        self.assertEqual(res["cells"][0]["baseline_failures"], [])

    def test_a_cell_with_no_cpu_on_either_engine_is_judged_on_the_rest(self):
        rows = []
        for rnd in (1, 2, 3):
            for side in ("hybrid", "raft"):
                rows.append((rnd, self.CELL, side, "ok", self.no_cpu(self.ingest(1000, 500))))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["verdict"], "PASS")

    def test_a_cell_where_nothing_was_judged_is_n_a(self):
        rows = []
        for rnd in (1, 2, 3):
            for side in ("hybrid", "raft"):
                rows.append((rnd, self.CELL, side, "ok", self.no_cpu({})))
        res = s.summarize(self.write(rows))
        self.assertEqual(res["cells"][0]["verdict"], "N/A")
        self.assertEqual(res["verdict"], "N/A")

    def test_current_run_without_cpu_against_a_baseline_with_it_fails(self):
        rows = []
        for rnd in (1, 2, 3):
            for side in ("hybrid", "raft"):
                rows.append((rnd, self.CELL, side, "ok", self.no_cpu(self.ingest(1000, 500))))
            rows.append((rnd, self.CELL, "baseline", "ok", self.ingest(1000, 500)))
        res = s.summarize(self.write(rows))
        self.assertIn("hybrid cpu_kmsg", res["cells"][0]["baseline_failures"])


class ExactRulesAgainstBaseline(Run):
    CELL = "rf3-follower-tail"

    def rows(self, hybrid_gaps, raft_gaps, base_gaps, with_raft=True):
        out = []
        for rnd in (1, 2, 3):
            out.append((rnd, self.CELL, "hybrid", "ok", self.tail(20000, 800, 2500, gaps=hybrid_gaps)))
            if with_raft:
                out.append((rnd, self.CELL, "raft", "ok", self.tail(20000, 800, 2500, gaps=raft_gaps)))
            out.append((rnd, self.CELL, "baseline", "ok", self.tail(20000, 800, 2500, gaps=base_gaps)))
        return out

    def tail(self, msg_s, p50, p99, gaps=0, held=20000):
        return {"delivered_throughput_msg_s": msg_s, "gaps": gaps,
                "publish_throughput_msg_s": held, "publish_target_msg_s": 20000,
                "delivery_latency_us": {"p50": p50, "p99": p99}}

    def test_gaps_equal_to_the_baseline_still_fail_without_a_raft_side(self):
        res = s.summarize(self.write(self.rows(4, 0, 4, with_raft=False)))
        self.assertEqual(res["cells"][0]["baseline_failures"], ["hybrid gaps"])

    def test_gaps_are_judged_against_the_baseline_when_both_engines_ran(self):
        res = s.summarize(self.write(self.rows(0, 4, 0)))
        self.assertEqual(res["cells"][0]["baseline_failures"], ["raft gaps"])
        self.assertEqual(res["baseline_verdict"], "FAIL")


class RaftFrom(Baseline):
    def test_only_cells_in_both_runs_are_judged(self):
        a = self.write(self.rows(cell="rf3-oncommit-tput") + self.rows(cell="rf3-mqtt"))
        b = self.write([r for r in self.rows(cell="rf3-oncommit-tput") if r[2] == "raft"])
        res = s.summarize(a, raft_from=b)
        by = {c["id"]: c for c in res["cells"]}
        self.assertEqual(by["rf3-oncommit-tput"]["verdict"], "PASS")
        self.assertEqual(by["rf3-mqtt"]["verdict"], "N/A")
        self.assertEqual(by["rf3-mqtt"]["baseline_verdict"], "N/A")
        self.assertIn("not in both runs", " ".join(by["rf3-mqtt"]["notes"]))
        self.assertEqual(res["verdict"], "PASS")

    def test_differing_round_counts_warn_and_both_envs_print(self):
        a = self.write(self.rows(rounds=3))
        b = self.write([r for r in self.rows(rounds=2) if r[2] == "raft"])
        res = s.summarize(a, raft_from=b)
        self.assertEqual(len(res["warnings"]), 1)
        md = s.markdown(res)
        self.assertIn("WARNING: round counts differ", md)
        self.assertIn("raft_from.rev abc", md)
        self.assertIn("rev abc", md)


class Cli(Baseline):
    def test_flags_parse_in_any_position(self):
        a = self.write([r for r in self.rows() if r[2] != "raft"])
        b = self.write([r for r in self.rows(raft=600) if r[2] == "raft"])
        self.assertEqual(s.main(["summarize.py", "--raft-from", b, a, "--strict"]), 1)
        with self.assertRaises(SystemExit):
            s.main(["summarize.py"])


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
