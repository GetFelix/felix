#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import math
import textwrap
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except ImportError:
    # Charting is the last step of `task perf:latency-matrix`, after a benchmark
    # run that takes many minutes. A bare ImportError here reads as "the run
    # failed" when the measurements are already safely on disk, so say what is
    # missing, what survived, and how to finish.
    raise SystemExit(
        "matplotlib is not installed, so charts were not rendered.\n"
        "\n"
        "The benchmark data is already written and is NOT lost:\n"
        "  data/raw/latency_demo_runs.jsonl      (raw runs)\n"
        "  data/derived/latency_demo_agg.csv     (aggregated)\n"
        "\n"
        "Install the charting dependencies, then re-render without re-running\n"
        "the benchmark:\n"
        "  task perf:deps      # creates .venv-perf (system Python is usually\n"
        "                      # externally managed -- PEP 668 -- so a bare\n"
        "                      # pip install into it fails)\n"
        "  task perf:charts    # re-renders from data/derived"
    ) from None

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "scripts" / "perf" / "presets.yml"
AGG_CSV = ROOT / "data" / "derived" / "latency_demo_agg.csv"
CHARTS_DIR = ROOT / "charts" / "latency_demo"


def load_config(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore

            return yaml.safe_load(text)
        except ImportError as exc:
            raise SystemExit(
                "Failed to parse presets.yml as JSON and PyYAML is not installed. "
                "Install PyYAML or keep presets.yml JSON-compatible."
            ) from exc


def read_csv(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def to_int(value):
    if value is None or value == "":
        return None
    return int(value)


def to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def cast_rows(rows):
    casted = []
    for row in rows:
        row = dict(row)
        for key in ("fanout", "batch", "payload_bytes", "trial_count", "failure_count", "warmup", "total"):
            row[key] = to_int(row.get(key))
        for key in (
            "throughput_median",
            "throughput_p10",
            "throughput_p90",
            "effective_throughput_median",
            "delivered_throughput_median",
            "p50_ms_median",
            "p99_ms_median",
            "p999_ms_median",
        ):
            row[key] = to_float(row.get(key))
        row["binary"] = True if row.get("binary") == "True" else False
        casted.append(row)
    return casted


def unique_or_mixed(values):
    values = {v for v in values if v is not None}
    if len(values) == 1:
        return values.pop()
    if not values:
        return None
    return "mixed"


def percentile(values, pct):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return float(values[f])
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return float(d0 + d1)


def chart_group(
    rows,
    metric_key,
    ylabel,
    title_prefix,
    output_path,
    presets,
    payloads,
    footer,
    cap_percentile=None,
    scale=None,
    log_y=False,
):
    payload_positions = list(range(len(payloads)))
    group_width = 0.82
    bar_width = group_width / max(1, len(presets))
    offsets = [
        (i - (len(presets) - 1) / 2) * bar_width
        for i in range(len(presets))
    ]

    raw_values = [
        r.get(metric_key)
        for r in rows
        if r.get(metric_key) is not None
    ]
    cap_value = None
    if cap_percentile is not None and raw_values:
        cap_value = percentile(raw_values, cap_percentile)

    # A figure where every bar is absent is worse than no figure: it looks like
    # a measurement of zero, or like the chart is broken. Refuse to emit one.
    if not raw_values:
        print(f"  skipping {output_path.name}: no data for {metric_key}")
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    all_values: list = []

    for idx, preset in enumerate(presets):
        heights = []
        for payload in payloads:
            match = next(
                (
                    r
                    for r in rows
                    if r["preset"] == preset and r["payload_bytes"] == payload
                ),
                None,
            )
            heights.append(match.get(metric_key) if match else None)
        values = []
        clipped = []
        for value in heights:
            if value is None:
                # NaN, never 0.0. A zero bar is a *measurement*: on a throughput
                # chart it reads as total failure and on a latency chart as
                # perfect. Matplotlib draws NaN as nothing, which is the honest
                # rendering of "this configuration was never run".
                values.append(float("nan"))
                clipped.append(False)
            elif cap_value is not None and value > cap_value:
                values.append(cap_value / scale if scale else cap_value)
                clipped.append(True)
            else:
                values.append(value / scale if scale else value)
                clipped.append(False)
        all_values.extend(v for v in values if not math.isnan(v))
        positions = [p + offsets[idx] for p in payload_positions]
        bars = ax.bar(positions, values, width=bar_width * 0.95, label=preset)

        # A clipped bar is drawn at the cap, which is visually identical to one
        # that legitimately equals it. Mark them and print the real value, or
        # the clipping hides exactly the tail a p99 chart exists to show.
        for bar, was_clipped, raw in zip(bars, clipped, heights):
            if was_clipped:
                bar.set_hatch("///")
                bar.set_edgecolor("black")
                ax.annotate(
                    f"{raw / scale:.3g}\u2191" if scale else f"{raw:.3g}\u2191",
                    (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    ha="center",
                    va="bottom",
                    fontsize=6,
                    rotation=90,
                )
        # Absent configurations get a visible marker at the baseline, so a gap
        # cannot be mistaken for a bar too small to see.
        for position, value in zip(positions, values):
            if math.isnan(value):
                ax.annotate(
                    "no data",
                    (position, 0),
                    ha="center",
                    va="bottom",
                    fontsize=6,
                    rotation=90,
                    color="0.5",
                )

    # Message rate spans orders of magnitude across payload sizes (a 0 B run
    # does millions/s, a 16 KiB run tens of thousands), so on a linear axis the
    # large-payload bars round to nothing and read as "no throughput" when they
    # are in fact the highest *byte* rates on the chart. Log scale keeps every
    # bar legible; the MB/s companion chart is what to read for actual capacity.
    if log_y and all_values and min(all_values) > 0:
        ax.set_yscale("log")

    ax.set_xticks(payload_positions)
    ax.set_xticklabels([str(p) for p in payloads])
    ax.set_xlabel("payload (bytes)")
    ax.set_ylabel(ylabel)
    title = title_prefix
    if cap_value is not None:
        title = f"{title} (clipped p{int(cap_percentile * 100)})"
    # Wrapped rather than truncated: these titles carry the caveats that keep a
    # chart from being misread, and a caveat cut off at the figure edge is worse
    # than no caveat at all.
    ax.set_title("\n".join(textwrap.wrap(title, width=95)), fontsize=9)
    ax.legend(ncol=3, fontsize=8)
    fig.text(0.99, 0.01, footer, ha="right", va="bottom", fontsize=7)
    fig.tight_layout(rect=[0, 0.02, 1, 1])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path.with_suffix(".png"), dpi=140)
    fig.savefig(output_path.with_suffix(".svg"))
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Generate clustered charts from latency_demo_agg.csv.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--input", default=str(AGG_CSV))
    parser.add_argument("--cap-percentile", type=float, default=None)
    args = parser.parse_args()

    config = load_config(Path(args.config))
    presets = config.get("preset_order") or list(config.get("presets", {}).keys())
    payloads = config.get("workload", {}).get("payload_bytes", [0, 256, 1024, 4096])
    cap_percentile = args.cap_percentile
    if cap_percentile is None:
        cap_percentile = config.get("chart_outlier_cap_percentile")

    rows = cast_rows(read_csv(Path(args.input)))

    # One chart is one build. The aggregator already refuses to median across
    # commits and hosts, but grouping charts only by workload shape put those
    # separate rows back on the same axes -- bars from different builds side by
    # side, distinguishable only by a footer reading "git mixed". Comparing
    # commits is a legitimate thing to want, but it needs a chart that says so,
    # not one that looks like a single measurement.
    groups = {}
    for row in rows:
        key = (
            row["broker_profile"],
            row["fanout"],
            row["batch"],
            row["binary"],
            row.get("git_sha"),
            row.get("hostname"),
        )
        groups.setdefault(key, []).append(row)

    date_str = dt.datetime.now(dt.UTC).date().isoformat()

    for key, group_rows in groups.items():
        profile, fanout, batch, binary, key_git_sha, key_hostname = key
        warmup = unique_or_mixed([r.get("warmup") for r in group_rows])
        total = unique_or_mixed([r.get("total") for r in group_rows])
        if total == "mixed" and isinstance(batch, int) and batch > 1:
            # Throughput runs scale message count per payload so run volume
            # exceeds the QUIC send window (see run_latency_matrix.py).
            total = "volume-scaled"
        trials = unique_or_mixed([r.get("trial_count") for r in group_rows])
        git_sha = key_git_sha

        title_prefix = (
            f"latency_demo {profile} fanout={fanout} batch={batch} "
            f"binary={binary} warmup={warmup} total={total} trials={trials}"
        )
        # A batched run measures throughput, not request latency: a batch of 64
        # waits for the batch to fill. Reading its p99 as Felix's latency is the
        # single easiest way to conclude performance is terrible from a chart
        # that is working correctly, so the chart says so itself.
        latency_caveat = ""
        if isinstance(batch, int) and batch > 1:
            latency_caveat = (
                f" - THROUGHPUT PROFILE (batch={batch}): includes batch fill and"
                " queueing delay. Not a request-latency measurement; use batch=1"
                " with per-message acks for that."
            )
        dirty = any(r.get("git_dirty") in (True, "True") for r in group_rows)
        footer = (
            f"git {(git_sha or 'unknown')[:12]}{' (DIRTY)' if dirty else ''}"
            f" | {key_hostname or 'unknown host'} | {date_str}"
        )

        base_dir = CHARTS_DIR / profile
        # `binary` is part of the grouping key, so it must be part of the file
        # name too. Without it a JSON run and a binary run of the same
        # fanout/batch write to the same path and the last one silently wins.
        encoding = "binary" if binary else "json"
        # The commit is in the path too, so two builds cannot overwrite each
        # other's charts and it is obvious which build a file describes.
        build = (key_git_sha or "unknown")[:8]
        base_name = base_dir / f"f{fanout}_b{batch}_{encoding}_{build}"

        chart_group(
            group_rows,
            "p50_ms_median",
            "p50 latency (ms)",
            title_prefix + latency_caveat,
            base_name.with_name(base_name.name + "_p50"),
            presets,
            payloads,
            footer,
            cap_percentile,
        )
        chart_group(
            group_rows,
            "p99_ms_median",
            "p99 latency (ms)",
            title_prefix + latency_caveat,
            base_name.with_name(base_name.name + "_p99"),
            presets,
            payloads,
            footer,
            cap_percentile,
        )
        # Two different quantities, charted separately and named explicitly.
        # `throughput` counts messages a publisher sent; `delivered_throughput`
        # counts subscriber deliveries, so at fanout N they differ by N. Charting
        # the publish rate under a bare "throughput" label made fanout-10 look
        # 10x worse than it is -- the headline fanout numbers are delivered.
        for metric, label, suffix in (
            ("throughput_median", "publish throughput", "_publish_throughput"),
            (
                "delivered_throughput_median",
                "delivered throughput",
                "_delivered_throughput",
            ),
        ):
            values = [
                r.get(metric) for r in group_rows if r.get(metric) is not None
            ]
            if not values:
                continue
            scale = None
            ylabel = f"{label} (msg/s)"
            if max(values) >= 1_000_000:
                scale = 1_000_000.0
                ylabel = f"{label} (M msg/s)"
            chart_group(
                group_rows,
                metric,
                ylabel,
                f"{title_prefix} - {label}",
                base_name.with_name(base_name.name + suffix),
                presets,
                payloads,
                footer,
                cap_percentile,
                scale,
                log_y=True,
            )

        # Byte rate, the only cross-payload-comparable throughput number.
        # Message rate necessarily falls as payloads grow, so a msg/s chart
        # makes large payloads look like a regression when they are usually
        # moving the most data. 0 B carries no payload bytes and is excluded
        # rather than drawn as a zero bar.
        byte_payloads = [p for p in payloads if p > 0]
        if byte_payloads:
            for row in group_rows:
                delivered = row.get("delivered_throughput_median")
                payload = row.get("payload_bytes")
                if delivered is not None and payload:
                    row["delivered_mb_per_s"] = delivered * payload / 1e6
            chart_group(
                group_rows,
                "delivered_mb_per_s",
                "delivered payload (MB/s)",
                f"{title_prefix} - delivered payload byte rate",
                base_name.with_name(base_name.name + "_delivered_mb_per_s"),
                presets,
                byte_payloads,
                footer,
                cap_percentile,
            )


if __name__ == "__main__":
    main()
