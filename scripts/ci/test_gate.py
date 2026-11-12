import contextlib
import io
import json
import math
import pathlib
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import gate_perf  # noqa: E402
import gate_report  # noqa: E402


def cells(base, head, key="msg_s", cell="c", aa=None):
    """Rows for one cell: round i has baseline base[i] and head head[i]."""
    out = {cell: {"baseline": [], "head": []}}
    for side, values in (("baseline", base), ("head", head), ("baseline_aa", aa or [])):
        for i, value in enumerate(values):
            out[cell].setdefault(side, []).append({"round": i, key: value})
    return out


def ratios(n, ratio, key="msg_s", **kw):
    """n rounds with the baseline at 100 and the head at 100 * ratio."""
    return cells([100.0] * n, [100.0 * r for r in ([ratio] * n if not isinstance(ratio, list) else ratio)], key, **kw)


def one(data, **kw):
    """The verdict of the first metric the data carries."""
    return [v for v in gate_perf.judge(data, **kw) if v["rounds"] or v["metric"] in
            {m for sides in data.values() for rows in sides.values() for r in rows for m in r}][0]


def p99(data, **kw):
    return [v for v in gate_perf.judge(data, **kw) if v["metric"] == "p99_us"][0]


class PairedRule(unittest.TestCase):
    def test_equal_candidate_passes(self):
        for n in (6, 12, 24):
            self.assertEqual(one(ratios(n, 1.0))["status"], "pass", n)

    def test_three_percent_regression_fails(self):
        for n in (6, 12, 24):
            self.assertEqual(one(ratios(n, 0.96))["status"], "fail", n)

    def test_exactly_at_the_bound_is_not_a_pass(self):
        self.assertNotEqual(one(ratios(6, 0.97))["status"], "pass")

    def test_throughput_straddling_the_bound_is_undecided_then_fails_at_24(self):
        spread = [0.95, 0.96, 0.98, 0.99, 1.0, 1.01]
        self.assertEqual(one(ratios(6, spread))["status"], "undecided")
        self.assertFalse(one(ratios(6, spread))["ok"])
        wide = spread * 4
        v = one(ratios(24, wide))
        self.assertEqual(v["status"], "fail")
        self.assertIn("not measured", v["reason"])

    def test_interval_ends_are_the_order_statistics(self):
        data = ratios(12, [1.0 + i / 100 for i in range(12)])
        v = one(data)
        self.assertEqual((round(v["lo"], 2), round(v["hi"], 2)), (1.01, 1.10))
        v = one(ratios(24, [1.0 + i / 100 for i in range(24)]))
        self.assertEqual((round(v["lo"], 2), round(v["hi"], 2)), (1.05, 1.18))
        v = one(ratios(6, [1.0 + i / 100 for i in range(6)]))
        self.assertEqual((round(v["lo"], 2), round(v["hi"], 2)), (1.00, 1.05))

    def test_coverage_of_the_intervals(self):
        self.assertAlmostEqual(gate_perf.coverage(6, 1), 0.96875)
        self.assertGreater(gate_perf.coverage(12, 2), 0.99)
        self.assertGreater(gate_perf.coverage(24, 6), 0.99)

    def test_p99_up_eleven_percent_fails_and_nine_passes(self):
        self.assertEqual(p99(ratios(6, 1.11, "p99_us"))["status"], "fail")
        self.assertEqual(p99(ratios(6, 1.09, "p99_us"))["status"], "pass")
        self.assertNotEqual(p99(ratios(6, 1.10, "p99_us"))["status"], "pass")

    def test_p99_is_judged_on_the_upper_end(self):
        # One noisy round pushes the upper end over 1.10: not a pass.
        v = p99(ratios(6, [1.0, 1.0, 1.0, 1.0, 1.0, 1.2], "p99_us"))
        self.assertEqual(v["status"], "undecided")

    def test_missing_side_fails(self):
        self.assertFalse(one({"c": {"head": [{"round": 0, "msg_s": 1}]}})["ok"])

    def test_missing_round_fails_even_when_the_rest_passes(self):
        data = ratios(6, 1.0)
        data["c"]["head"].pop()
        v = one(data)
        self.assertEqual(v["status"], "fail")
        self.assertIn("partial", v["reason"])

    def test_planned_rounds_make_a_short_run_fail(self):
        v = one(ratios(6, 1.0), rounds=12)
        self.assertEqual(v["status"], "fail")
        self.assertFalse(v["ok"])

    def test_a_round_off_the_ladder_cannot_decide(self):
        self.assertFalse(one(ratios(5, 1.0))["ok"])
        self.assertFalse(one(ratios(8, 1.0))["ok"])

    def test_expected_cell_with_no_rows_fails(self):
        data = ratios(6, 1.0)
        self.assertTrue(gate_perf.judge(data, expected=["c"])[0]["ok"])
        verdicts = gate_perf.judge(data, expected=["c", "gone"])
        self.assertEqual([v["ok"] for v in verdicts if v["cell"] == "gone"], [False, False])

    def test_exact_false_pass_rates_at_the_bound(self):
        # A candidate whose true median is the bound: each round lands above it
        # with probability 1/2. Count the patterns that pass, exactly.
        def passes(n):
            total = 0
            for below in range(n + 1):
                pattern = [0.96] * below + [0.98] * (n - below)
                if one(ratios(n, pattern))["ok"]:
                    total += math.comb(n, below)
            return total / 2**n

        self.assertEqual(passes(6), 1 / 64)
        self.assertLess(passes(12), 1 / 300)
        self.assertLess(passes(24), 1 / 300)
        self.assertEqual(passes(12), 13 / 4096)

    def test_a_p99_at_the_bound_has_the_same_false_pass_rate(self):
        def passes(n):
            total = 0
            for above in range(n + 1):
                pattern = [1.12] * above + [1.08] * (n - above)
                if p99(ratios(n, pattern, "p99_us"))["ok"]:
                    total += math.comb(n, above)
            return total / 2**n

        self.assertEqual(passes(6), 1 / 64)
        self.assertLess(passes(24), 1 / 300)

    def test_a_worse_candidate_passes_less_often(self):
        # 1.5% under the bound at six rounds: only an all-above pattern passes.
        self.assertEqual(one(ratios(6, [0.99] * 5 + [0.96]))["status"], "undecided")

    def test_p50_is_turned_over(self):
        # Half the latency is a gain; 3.2% more latency is under 0.97 once turned over.
        self.assertEqual(one(ratios(6, 0.5, "p50_us", cell="c-b1"))["status"], "pass")
        self.assertEqual(one(ratios(6, 1.032, "p50_us", cell="c-b1"))["status"], "fail")

    def test_batch_one_cells_judge_latency_only(self):
        data = ratios(6, 1.0, "p50_us", cell="rf1-1k-b1")
        for side, rows in data["rf1-1k-b1"].items():
            for r in rows:
                r["p99_us"] = 100.0
        self.assertEqual([v["metric"] for v in gate_perf.judge(data)], ["p50_us", "p99_us"])
        self.assertEqual([v["metric"] for v in gate_perf.judge(ratios(6, 1.0))], ["msg_s", "p99_us"])

    def test_failover_uses_0_95_turned_over(self):
        data = ratios(6, 1.0, "outage_max_ms", cell="failover-x")
        self.assertEqual(one(data)["status"], "pass")
        # 6% slower: 1 / 1.06 = 0.943 is under 0.95.
        self.assertEqual(one(ratios(6, 1.06, "outage_max_ms", cell="failover-x"))["status"], "fail")
        # 4% slower is above 0.95 once turned over.
        self.assertEqual(one(ratios(6, 1.04, "outage_max_ms", cell="failover-x"))["status"], "pass")
        self.assertEqual(one(ratios(6, 1.06, "pause_max_ms", cell="move-x"))["status"], "fail")

    def test_failover_is_paired_in_engine_mode_too(self):
        data = ratios(6, 1.06, "outage_max_ms", cell="failover-x")
        self.assertEqual(one(data, mode="engine")["status"], "fail")

    def test_aa_half_width_over_the_margin_voids_the_cell(self):
        steady = [100.0] * 6
        noisy = [100.0, 100.0, 100.0, 100.0, 90.0, 110.0]
        self.assertEqual(one(ratios(6, 1.0, aa=steady))["status"], "pass")
        v = one(ratios(6, 1.0, aa=noisy))
        self.assertEqual(v["status"], "void")
        self.assertFalse(v["ok"])
        self.assertAlmostEqual(v["aa_half_width"], 0.10)

    def test_aa_margins_per_bound(self):
        # Half width 0.04: over the 3% margin of a 0.97 bound, under p99's 10%.
        aa = [100.0, 100.0, 100.0, 100.0, 96.0, 104.0]
        self.assertEqual(one(ratios(6, 1.0, aa=aa))["status"], "void")
        self.assertEqual(one(ratios(6, 1.0, "p99_us", aa=aa))["status"], "pass")
        # 0.95 bound: a 5% margin.
        self.assertEqual(one(ratios(6, 1.0, "outage_max_ms", cell="failover-x", aa=aa))["status"], "pass")

    def test_aa_is_not_a_band_for_the_candidate(self):
        # The A/A interval not containing 1.0 is fine as long as it is narrow.
        aa = [101.0] * 6
        self.assertEqual(one(ratios(6, 1.0, aa=aa))["status"], "pass")

    def test_required_aa_that_is_missing_voids(self):
        self.assertEqual(one(ratios(6, 1.0), require_aa=True)["status"], "void")

    def test_a_failing_candidate_stays_a_fail_without_aa(self):
        self.assertEqual(one(ratios(6, 0.9), require_aa=True)["status"], "fail")

    def test_main_exit_codes(self):
        def run(data):
            with tempfile.TemporaryDirectory() as d:
                path = pathlib.Path(d) / "r.jsonl"
                with open(path, "w") as f:
                    for cell, sides in data.items():
                        for side, rows in sides.items():
                            for r in rows:
                                f.write(json.dumps({"side": side, "cell": cell, "p99_us": 100.0, **r}) + "\n")
                with contextlib.redirect_stdout(io.StringIO()):
                    return gate_perf.main(["compare", str(path)])

        self.assertEqual(run(ratios(6, 1.0)), 0)
        self.assertEqual(run(ratios(6, 0.9)), 1)
        self.assertEqual(run(ratios(6, [0.95, 0.96, 0.98, 0.99, 1.0, 1.01])), 2)

    def test_failover_line_parses_every_unit(self):
        line = "failover heal: outage: 9 of 9 served, p50 Some(1.2s) p99 Some(2s) max Some(2.5s); 0 silent"
        self.assertEqual(gate_perf.failover_max_ms(line), 2500.0)
        self.assertEqual(gate_perf.failover_max_ms("failover heal: outage: 1 of 1 served, max Some(850ms);"), 850.0)
        self.assertIsNone(gate_perf.failover_max_ms("failover heal: outage: 0 of 1 served, max None"))


class Workflow(unittest.TestCase):
    def test_head_runs_in_the_middle_slot_every_round(self):
        text = (pathlib.Path(__file__).parents[2] / ".github/workflows/gate-perf.yml").read_text()
        orders = re.findall(r'sides="([a-z_ ]+)"', text)
        self.assertEqual(len(orders), 2)
        for order in orders:
            self.assertEqual(order.split()[1], "head", order)
        self.assertEqual({o.split()[0] for o in orders}, {"baseline", "baseline_aa"})

    def test_a_non_pass_verdict_fails_the_part(self):
        text = (pathlib.Path(__file__).parents[2] / ".github/workflows/gate-perf.yml").read_text()
        self.assertIn('if .status == "pass" then "pass" else "fail" end', text)


class EngineRule(unittest.TestCase):
    def test_throughput_within_ten_percent_passes(self):
        self.assertTrue(one(cells([100, 100], [91, 95]), mode="engine")["ok"])

    def test_throughput_down_eleven_percent_fails(self):
        self.assertFalse(one(cells([100, 100], [89, 89]), mode="engine")["ok"])

    def test_p99_up_twenty_percent_passes_and_more_fails(self):
        self.assertTrue(p99(cells([100], [120], "p99_us"), mode="engine")["ok"])
        self.assertFalse(p99(cells([100], [121], "p99_us"), mode="engine")["ok"])

    def test_p50_is_judged_at_0_90_turned_over(self):
        data = cells([100, 100], [105, 105], "p50_us", cell="c-b1")
        self.assertTrue(one(data, mode="engine")["ok"])
        self.assertFalse(one(cells([100, 100], [115, 115], "p50_us", cell="c-b1"), mode="engine")["ok"])

    def test_too_few_rounds_fails(self):
        data = cells([100, 100], [100, 100])
        self.assertTrue(one(data, mode="engine", min_rounds=2)["ok"])
        self.assertFalse(one(data, mode="engine", min_rounds=3)["ok"])


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

    def test_an_undecided_part_is_a_failure(self):
        s = {n: "pass" for n, _ in gate_report.PARTS}
        s["perf"] = "undecided"
        self.assertEqual(self.verdict(s), "FAIL")

    def test_unavailable_engine_is_not_available_not_pass(self):
        self.assertEqual(self.verdict({n: "unavailable" for n, _ in gate_report.PARTS}), "NOT AVAILABLE")


if __name__ == "__main__":
    unittest.main()
