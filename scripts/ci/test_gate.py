import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import gate_perf  # noqa: E402
import gate_report  # noqa: E402


def cells(base, head, key="msg_s"):
    rows = []
    for side, values in (("baseline", base), ("head", head)):
        for i, value in enumerate(values):
            rows.append({"side": side, "cell": "c", "round": i, key: value})
    out = {}
    for row in rows:
        out.setdefault(row["cell"], {}).setdefault(row["side"], []).append(row)
    return out


class PerfRules(unittest.TestCase):
    def test_throughput_within_ten_percent_passes(self):
        self.assertTrue(gate_perf.judge(cells([100, 100], [91, 95]))[0]["ok"])

    def test_throughput_down_eleven_percent_fails(self):
        self.assertFalse(gate_perf.judge(cells([100, 100], [89, 89]))[0]["ok"])

    def test_p99_up_twenty_percent_passes_and_more_fails(self):
        self.assertTrue(gate_perf.judge(cells([100], [120], "p99_us"))[1]["ok"])
        self.assertFalse(gate_perf.judge(cells([100], [121], "p99_us"))[1]["ok"])

    def test_missing_side_fails(self):
        self.assertFalse(gate_perf.judge({"c": {"head": [{"msg_s": 1}]}})[0]["ok"])

    def test_expected_cell_with_no_rows_fails(self):
        data = cells([100, 100], [100, 100])
        self.assertTrue(gate_perf.judge(data, expected=["c"])[0]["ok"])
        verdicts = gate_perf.judge(data, expected=["c", "gone"])
        self.assertEqual([v["ok"] for v in verdicts if v["cell"] == "gone"], [False, False])

    def test_too_few_rounds_fails(self):
        data = cells([100, 100], [100, 100])
        self.assertTrue(gate_perf.judge(data, min_rounds=2)[0]["ok"])
        self.assertFalse(gate_perf.judge(data, min_rounds=3)[0]["ok"])

    def test_a_round_one_side_lost_is_dropped_for_both(self):
        # Head lost round 0, the slow baseline round. Pairing drops it for both,
        # so the medians compare rounds 1 and 2 only.
        data = {"c": {"baseline": [{"round": 0, "msg_s": 1000}, {"round": 1, "msg_s": 100},
                                   {"round": 2, "msg_s": 100}],
                      "head": [{"round": 1, "msg_s": 100}, {"round": 2, "msg_s": 100}]}}
        verdict = gate_perf.judge(data, min_rounds=2)[0]
        self.assertEqual((verdict["rounds"], verdict["baseline"], verdict["ok"]), (2, 100, True))
        self.assertFalse(gate_perf.judge(data, min_rounds=3)[0]["ok"])

    def test_failover_no_worse(self):
        data = {"failover": {"baseline": [{"outage_max_ms": 2000}], "head": [{"outage_max_ms": 2001}]}}
        self.assertFalse(gate_perf.judge(data)[0]["ok"])
        self.assertTrue(gate_perf.judge(data, failover_slack=0.01)[0]["ok"])

    def test_failover_variants_are_judged_on_the_outage_alone(self):
        data = {"failover-with_election": {"baseline": [{"outage_max_ms": 10}], "head": [{"outage_max_ms": 9}]}}
        verdicts = gate_perf.judge(data)
        self.assertEqual([v["metric"] for v in verdicts], ["outage_max_ms"])
        self.assertTrue(verdicts[0]["ok"])

    def test_failover_line_parses_every_unit(self):
        line = "failover heal: outage: 9 of 9 served, p50 Some(1.2s) p99 Some(2s) max Some(2.5s); 0 silent"
        self.assertEqual(gate_perf.failover_max_ms(line), 2500.0)
        self.assertEqual(gate_perf.failover_max_ms("failover heal: outage: 1 of 1 served, max Some(850ms);"), 850.0)
        self.assertIsNone(gate_perf.failover_max_ms("failover heal: outage: 0 of 1 served, max None"))


class Report(unittest.TestCase):
    def part(self, root, name, engine, status):
        d = pathlib.Path(root) / f"gate-{name}-{engine}"
        d.mkdir()
        (d / "result.json").write_text(json.dumps(
            {"part": name, "engine": engine, "status": status, "summary": "s"}))

    def verdict(self, statuses):
        with tempfile.TemporaryDirectory() as root:
            for name, status in statuses.items():
                self.part(root, name, "hybrid", status)
            return gate_report.overall(gate_report.read_parts(root, "hybrid"))

    def test_all_pass(self):
        self.assertEqual(self.verdict({n: "pass" for n, _ in gate_report.PARTS}), "PASS")

    def test_one_failure_fails(self):
        s = {n: "pass" for n, _ in gate_report.PARTS}
        s["perf"] = "fail"
        self.assertEqual(self.verdict(s), "FAIL")

    def test_not_applicable_part_is_ignored(self):
        s = {n: "pass" for n, _ in gate_report.PARTS}
        s["groups"] = "n/a"
        self.assertEqual(self.verdict(s), "PASS")

    def test_a_missing_part_is_incomplete(self):
        self.assertEqual(self.verdict({"history": "pass"}), "INCOMPLETE")

    def test_a_priced_part_never_changes_the_verdict(self):
        s = {n: "pass" for n, _ in gate_report.PARTS}
        for status in ("fail", "pass"):
            s["liveness"] = status
            self.assertEqual(self.verdict(s), "PASS")

    def test_unavailable_engine_is_not_available_not_pass(self):
        self.assertEqual(self.verdict({n: "unavailable" for n, _ in gate_report.PARTS}), "NOT AVAILABLE")


if __name__ == "__main__":
    unittest.main()
