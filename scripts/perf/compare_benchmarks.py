#!/usr/bin/env python3
"""Compare two latency_demo JSONL result sets (baseline vs candidate) and flag
statistically significant regressions.

Designed for a back-to-back same-runner comparison: run the baseline (e.g. the
PR's merge-base) and the candidate (e.g. the PR branch) on the *same* CI
runner instance in the same job, each producing their own JSONL via
run_latency_matrix.py, then compare those two files. This controls for
ambient runner noise far better than comparing against a historical stored
baseline captured on a different runner instance at a different time.

Statistics: Welch's two-sample t-test per metric per (profile, fanout, batch,
payload, preset, binary) group, since trial counts and variances can differ
between the two sides. A change is only flagged as a regression if BOTH:
  - it's statistically significant (p < --alpha, default 0.05), AND
  - it exceeds a minimum practical-effect floor (--min-effect-pct, default
    3%), so a technically-significant-but-trivial wobble isn't reported as a
    regression just because both sides happened to have very low variance.

A change in the worse direction of at least --inconclusive-pct (default 10%)
that is not significant is reported as inconclusive, not as passing: with a
handful of trials and a wide spread, the t-test cannot tell a doubling from
noise, and a green mark would claim it can.

This intentionally has no third-party dependency (no scipy) so it runs on a
bare `python3` in CI without an extra install step.
"""
import argparse
import json
import math
import sys
from pathlib import Path

# Metrics where a HIGHER value in the candidate is worse (regressions).
LATENCY_METRICS = ["p50_us", "p99_us", "p999_us"]
# Metrics where a LOWER value in the candidate is worse (regressions).
THROUGHPUT_METRICS = ["throughput", "delivered_throughput"]


def load_runs(path: Path):
    runs = []
    if not path.exists():
        return runs
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            runs.append(json.loads(line))
    return runs


def group_key(run: dict):
    workload = run.get("workload") or {}
    return (
        run.get("broker_profile"),
        workload.get("fanout"),
        workload.get("batch"),
        workload.get("payload_bytes"),
        run.get("preset"),
        workload.get("binary"),
    )


def successful_values(runs: list, metric: str):
    values = []
    for run in runs:
        if run.get("parse_error") or run.get("exit_code") not in (0, None):
            continue
        metrics = run.get("metrics") or {}
        value = metrics.get(metric)
        if value is not None:
            values.append(float(value))
    return values


def mean(values):
    return sum(values) / len(values)


def variance(values, mean_value):
    if len(values) < 2:
        return 0.0
    return sum((v - mean_value) ** 2 for v in values) / (len(values) - 1)


def median(values):
    values = sorted(values)
    n = len(values)
    mid = n // 2
    if n % 2 == 1:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


# --- Welch's t-test, implemented without scipy -----------------------------
#
# p-value comes from the two-sided Student's t distribution CDF, computed via
# the regularized incomplete beta function (Numerical Recipes' `betai`/`betacf`
# continued-fraction algorithm -- a standard, widely-implemented routine, not
# something novel). Verified below against known reference t-table values.


def _betacf(a, b, x, max_iter=200, eps=3e-12):
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < 1e-300:
        d = 1e-300
    d = 1.0 / d
    h = d
    for m in range(1, max_iter + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-300:
            d = 1e-300
        c = 1.0 + aa / c
        if abs(c) < 1e-300:
            c = 1e-300
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-300:
            d = 1e-300
        c = 1.0 + aa / c
        if abs(c) < 1e-300:
            c = 1e-300
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return h


def _betai(a, b, x):
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    ln_beta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    front = math.exp(ln_beta + a * math.log(x) + b * math.log(1.0 - x))
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def t_dist_two_sided_pvalue(t: float, df: float) -> float:
    """P(|T| > |t|) for a Student's t distribution with `df` degrees of freedom."""
    if df <= 0:
        return 1.0
    x = df / (df + t * t)
    return _betai(df / 2.0, 0.5, x)


def welch_t_test(a: list, b: list):
    """Returns (t, df, p_value) for two independent samples. None if either
    sample has fewer than 2 points (can't estimate variance)."""
    if len(a) < 2 or len(b) < 2:
        return None
    mean_a, mean_b = mean(a), mean(b)
    var_a, var_b = variance(a, mean_a), variance(b, mean_b)
    se_a, se_b = var_a / len(a), var_b / len(b)
    se_sum = se_a + se_b
    if se_sum <= 0:
        # Both samples are (numerically) constant. Treat any difference as
        # maximally significant; identical constants are "no difference".
        return (0.0, len(a) + len(b) - 2, 0.0 if mean_a == mean_b else 1e-12)
    t = (mean_a - mean_b) / math.sqrt(se_sum)
    df = se_sum**2 / (
        (se_a**2) / (len(a) - 1) + (se_b**2) / (len(b) - 1)
    )
    p = t_dist_two_sided_pvalue(t, df)
    return (t, df, p)


def _self_check():
    """Sanity-check t_dist_two_sided_pvalue against known reference values
    before trusting it for real comparisons."""
    # For df=10, t=2.228 is the two-sided 5% critical value (p ~= 0.05).
    p = t_dist_two_sided_pvalue(2.228, 10)
    assert abs(p - 0.05) < 0.002, f"t-dist self-check failed: p={p}"
    # For df=30, t=2.750 is the two-sided 1% critical value (p ~= 0.01).
    p = t_dist_two_sided_pvalue(2.750, 30)
    assert abs(p - 0.01) < 0.002, f"t-dist self-check failed: p={p}"
    # t=0 must always give p=1 (no evidence of any difference).
    p = t_dist_two_sided_pvalue(0.0, 20)
    assert abs(p - 1.0) < 1e-9, f"t-dist self-check failed: p={p}"


def coefficient_of_variation(values):
    if len(values) < 2:
        return None
    m = mean(values)
    return math.sqrt(variance(values, m)) / m * 100.0 if m else None


def compare_group(key, baseline_runs, candidate_runs, alpha, min_effect_pct, inconclusive_pct):
    rows = []
    for metric in LATENCY_METRICS + THROUGHPUT_METRICS:
        base_vals = successful_values(baseline_runs, metric)
        cand_vals = successful_values(candidate_runs, metric)
        if not base_vals or not cand_vals:
            rows.append(
                {
                    "key": key,
                    "metric": metric,
                    "status": "insufficient_data",
                    "baseline_n": len(base_vals),
                    "candidate_n": len(cand_vals),
                }
            )
            continue

        base_median = median(base_vals)
        cand_median = median(cand_vals)
        pct_change = (
            ((cand_median - base_median) / base_median) * 100.0
            if base_median
            else 0.0
        )

        higher_is_worse = metric in LATENCY_METRICS
        worse_direction = pct_change > 0 if higher_is_worse else pct_change < 0

        test = welch_t_test(base_vals, cand_vals)
        p_value = test[2] if test else None

        is_regression = (
            worse_direction
            and p_value is not None
            and p_value < alpha
            and abs(pct_change) >= min_effect_pct
        )
        is_inconclusive = (
            not is_regression and worse_direction and abs(pct_change) >= inconclusive_pct
        )
        if is_regression:
            status = "regression"
        elif is_inconclusive:
            status = "inconclusive"
        else:
            status = "ok"

        rows.append(
            {
                "key": key,
                "metric": metric,
                "status": status,
                "baseline_n": len(base_vals),
                "candidate_n": len(cand_vals),
                "baseline_median": base_median,
                "candidate_median": cand_median,
                "pct_change": pct_change,
                "p_value": p_value,
                "baseline_cv_pct": coefficient_of_variation(base_vals),
                "candidate_cv_pct": coefficient_of_variation(cand_vals),
            }
        )
    return rows


def format_key(key):
    profile, fanout, batch, payload, preset, binary = key
    return f"{profile}/{preset} fanout={fanout} batch={batch} payload={payload}B"


def _p(value):
    return "-" if value is None else f"{value:.4f}"


def _cv(value):
    return "-" if value is None else f"{value:.0f}%"


def render_markdown(all_rows, alpha, min_effect_pct, inconclusive_pct):
    regressions = [r for r in all_rows if r["status"] == "regression"]
    inconclusive = [r for r in all_rows if r["status"] == "inconclusive"]
    insufficient = [r for r in all_rows if r["status"] == "insufficient_data"]

    lines = []
    if regressions:
        lines.append(
            f"### :warning: {len(regressions)} potential performance regression(s) detected\n"
        )
        if inconclusive:
            lines.append(f"_Also {len(inconclusive)} inconclusive large change(s), marked below._\n")
    elif inconclusive:
        lines.append(
            f"### :grey_question: {len(inconclusive)} large change(s) in the worse direction "
            "these trials can neither confirm nor rule out\n"
        )
    else:
        lines.append("### :white_check_mark: No statistically significant performance regressions\n")
    lines.append(
        f"_Welch's t-test, p < {alpha}, minimum practical effect {min_effect_pct}%. "
        f"A worse change of {inconclusive_pct}% or more that is not significant is "
        "inconclusive: rerun the cell with more trials before trusting it. "
        "Baseline and candidate were built and benchmarked back-to-back on the same "
        "runner instance in this job to control for ambient noise. This check is "
        "advisory only and does not block merging._\n"
    )
    lines.append(
        "| Config | Metric | Baseline (median) | Candidate (median) | Change | p-value | CV base/cand | |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    for row in all_rows:
        cfg = format_key(row["key"])
        if row["status"] == "insufficient_data":
            lines.append(
                f"| {cfg} | {row['metric']} | - | - | - | - | - | :grey_question: insufficient data "
                f"(n={row['baseline_n']}/{row['candidate_n']}) |"
            )
            continue
        marker = {
            "regression": ":warning:",
            "inconclusive": ":grey_question: inconclusive",
        }.get(row["status"], ":white_check_mark:")
        unit = "us" if row["metric"] in LATENCY_METRICS else "msg/s"
        lines.append(
            f"| {cfg} | {row['metric']} | {row['baseline_median']:.1f} {unit} | "
            f"{row['candidate_median']:.1f} {unit} | {row['pct_change']:+.1f}% | "
            f"{_p(row['p_value'])} | {_cv(row['baseline_cv_pct'])}/{_cv(row['candidate_cv_pct'])} | {marker} |"
        )

    if insufficient:
        lines.append(
            f"\n_{len(insufficient)} metric(s) had too few successful trials on one "
            "or both sides to compare (see job logs for individual trial failures)._"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Compare two latency_demo JSONL result sets for regressions."
    )
    parser.add_argument("--baseline", required=True, help="Path to baseline JSONL")
    parser.add_argument("--candidate", required=True, help="Path to candidate JSONL")
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--min-effect-pct", type=float, default=3.0)
    parser.add_argument("--inconclusive-pct", type=float, default=10.0)
    parser.add_argument("--output-markdown", default=None)
    parser.add_argument("--output-json", default=None)
    args = parser.parse_args()

    _self_check()

    baseline_runs = load_runs(Path(args.baseline))
    candidate_runs = load_runs(Path(args.candidate))
    if not baseline_runs:
        raise SystemExit(f"No baseline runs found in {args.baseline}")
    if not candidate_runs:
        raise SystemExit(f"No candidate runs found in {args.candidate}")

    baseline_groups = {}
    for run in baseline_runs:
        baseline_groups.setdefault(group_key(run), []).append(run)
    candidate_groups = {}
    for run in candidate_runs:
        candidate_groups.setdefault(group_key(run), []).append(run)

    all_keys = sorted(
        set(baseline_groups) | set(candidate_groups), key=lambda k: [str(x) for x in k]
    )

    all_rows = []
    for key in all_keys:
        all_rows.extend(
            compare_group(
                key,
                baseline_groups.get(key, []),
                candidate_groups.get(key, []),
                args.alpha,
                args.min_effect_pct,
                args.inconclusive_pct,
            )
        )

    markdown = render_markdown(all_rows, args.alpha, args.min_effect_pct, args.inconclusive_pct)
    print(markdown)

    if args.output_markdown:
        Path(args.output_markdown).write_text(markdown, encoding="utf-8")
    if args.output_json:
        Path(args.output_json).write_text(json.dumps(all_rows, indent=2), encoding="utf-8")

    regressions = [r for r in all_rows if r["status"] == "regression"]
    # Advisory only: never fail the process itself. The workflow decides what
    # to do with the markdown/JSON output (e.g. post a PR comment).
    if regressions:
        print(
            f"\n{len(regressions)} regression(s) flagged (advisory, not failing).",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
