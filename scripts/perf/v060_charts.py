#!/usr/bin/env python3
"""Charts for the v0.6.0 performance review, built from the raw Azure cells.

    python3 scripts/perf/v060_charts.py \
        [--sessions scripts/perf/azure/sessions] \
        [--out docs-site/public/charts/perf-v060]

Every number is recomputed from the cell directories (meta.env, the broker's
1 Hz series, the generators' run output, the folded profiles) with the same
code the harness uses: `summarize.cell_row` for steady state and
`fold_categories.classify` for profile categories. Nothing is read from a
hand-edited table. Besides the SVGs it writes `data.csv`, one row per cell and
metric plotted, so a figure in the doc can be traced to its cell.

Needs matplotlib (scripts/perf/requirements.txt).
"""

import argparse
import collections
import csv
import gzip
import re
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "azure"))

import fold_categories  # noqa: E402
import summarize  # noqa: E402

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Reference categorical palette (light), fixed order. Validated as a set.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
GRAY = "#8a8985"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 10,
    "axes.edgecolor": GRID,
    "axes.labelcolor": INK_2,
    "axes.titlecolor": INK,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
    "xtick.color": INK_2,
    "ytick.color": INK_2,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "axes.axisbelow": True,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "legend.frameon": False,
    "svg.fonttype": "none",
})

A = "v060-a2-results"


class Cells:
    """Steady-state rows for a session's cells, keyed by directory name."""

    def __init__(self, sessions, session):
        self.root = sessions / session / "cells"
        self.session = session
        self._rows = {}

    def trials(self, prefix):
        """Rows for every trial whose directory starts with `prefix-t<N>`."""
        pat = re.compile(re.escape(prefix) + r"-t\d+(-|$)")
        out = []
        for d in sorted(self.root.iterdir()):
            if d.is_dir() and pat.match(d.name) and (d / "done").exists():
                if d.name not in self._rows:
                    self._rows[d.name] = summarize.cell_row(d)
                out.append(self._rows[d.name])
        if not out:
            sys.exit(f"no finished cells for {prefix!r} in {self.root}")
        return out

    def one(self, name):
        d = self.root / name
        if not d.is_dir():
            matches = [p for p in self.root.iterdir() if p.name.startswith(name + "-") or p.name == name]
            if len(matches) != 1:
                sys.exit(f"cell {name!r}: {len(matches)} matches in {self.root}")
            d = matches[0]
        if d.name not in self._rows:
            self._rows[d.name] = summarize.cell_row(d)
        return self._rows[d.name]


class Trace:
    """Collects (chart, label, cell, metric, value) rows for data.csv."""

    def __init__(self):
        self.rows = []

    def add(self, chart, label, row, metric, value):
        self.rows.append((chart, label, row["cell"], metric, f"{value:.3f}"))
        return value

    def write(self, path):
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["chart", "label", "cell", "metric", "value"])
            w.writerows(self.rows)


def mb_per_core(row):
    return row["ss_ingress_mb_s"] / row["ss_cores"]


def fio_mb_s(sessions, session, test):
    kv = summarize.kv_lines(sessions / session / "system" / "felixperf-broker-0.fio.txt")
    return float(kv[f"fio.{test}.mb_s"])


PREVIEW = None


def save(fig, out, name):
    fig.savefig(out / f"{name}.svg", bbox_inches="tight")
    if PREVIEW:
        fig.savefig(PREVIEW / f"{name}.png", bbox_inches="tight", dpi=110)
    plt.close(fig)
    print(f"wrote {out / name}.svg")


# --- chart 1: listeners -------------------------------------------------------


def chart_listeners(cells, sessions, out, trace):
    """Ingress vs listener count, in memory and durable, at both MTUs."""
    series = [
        ("In memory, MTU 1500, before #905", SERIES[0], "o", [
            (1, "l557-l1-io0-inmem"), (2, "l557-l2-io0-inmem"), (4, "l557-l4-io0-inmem")]),
        ("In memory, MTU 3900, with #905", SERIES[1], "o", [
            (1, "e8-mtu3900-l1"), (4, "best-inmem-l4"), (8, "best-inmem-l8")]),
        ("Durable on_commit, MTU 1500, before #905", SERIES[2], "s", [
            (1, "l557-l1-io0-dur"), (2, "l557-l2-io0-dur"), (4, "l557-l4-io0-dur")]),
        ("Durable on_commit, MTU 3900, with #905", SERIES[3], "s", [
            (1, "best-dur-l1"), (2, "best-dur-l2"), (4, "best-dur-l4"), (8, "best-dur-l8")]),
    ]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5, 4.2))
    for label, color, marker, points in series:
        xs, ys, cs = [], [], []
        for n, prefix in points:
            rows = cells.trials(prefix)
            key = "ss_append_mb_s" if "dur" in prefix else "ss_ingress_mb_s"
            vals = [trace.add("listeners", label, r, key, r[key]) for r in rows]
            cores = [trace.add("listeners", label, r, "ss_cores", r["ss_cores"]) for r in rows]
            xs.append(n)
            ys.append(statistics.mean(vals))
            cs.append(statistics.mean(cores))
            ax1.scatter([n] * len(vals), vals, s=10, color=color, alpha=0.5, linewidths=0)
        ax1.plot(xs, ys, color=color, lw=2, marker=marker, ms=7, label=label,
                 markeredgecolor=SURFACE, markeredgewidth=1.5)
        ax2.plot(xs, cs, color=color, lw=2, marker=marker, ms=7, label=label,
                 markeredgecolor=SURFACE, markeredgewidth=1.5)
    fio = fio_mb_s(sessions, A, "seq256k-j4")
    ax1.axhline(fio, color=GRAY, lw=1.2, ls="--")
    ax1.text(4.3, fio + 60, f"fio 256 KiB + fdatasync,\n4 jobs: {fio:.0f} MB/s", color=INK_2, fontsize=8, va="bottom")
    ax2.axhline(8, color=GRAY, lw=1.2, ls="--")
    ax2.text(0.85, 8.1, "8 vCPU", color=INK_2, fontsize=8, va="bottom")
    for ax in (ax1, ax2):
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
        ax.set_xlim(0.8, 10)
        ax.set_xlabel("client listeners (FELIX_QUIC_LISTENERS)")
    ax1.set_ylabel("MB/s (in memory: ingress; durable: append)")
    ax1.set_ylim(0, 4500)
    ax1.set_title("Throughput, one 8 vCPU broker")
    ax2.set_ylabel("broker process cores")
    ax2.set_ylim(0, 9)
    ax2.set_title("Broker CPU at that throughput")
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=2, bbox_to_anchor=(0.5, -0.1), fontsize=9)
    fig.tight_layout()
    save(fig, out, "listeners")


# --- chart 2: experiments -----------------------------------------------------


def chart_experiments(cells, out, trace):
    """MB/s per broker core for each A/B on the 4-listener in-memory cell."""
    arms = [
        ("before #905", "e1-base", SERIES[0]),
        ("#905", "e1-801", SERIES[1]),
        ("#905 + client\nACK threshold 64", "e2-ackelicit64", SERIES[1]),
        ("#905 control\n(for mimalloc)", "e7-801", SERIES[1]),
        ("#905 +\nmimalloc", "e7-mimalloc", GRAY),
        ("#905 +\nMTU 3900", "e8-mtu3900", SERIES[3]),
        ("#905 + MTU 3900\n+ ACK 64", "best-inmem-l4", SERIES[3]),
    ]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6.2), sharex=True)
    for i, (label, prefix, color) in enumerate(arms):
        rows = cells.trials(prefix)
        tput = [trace.add("experiments", label.replace("\n", " "), r, "ss_ingress_mb_s", r["ss_ingress_mb_s"]) for r in rows]
        per = [trace.add("experiments", label.replace("\n", " "), r, "mb_s_per_core", mb_per_core(r)) for r in rows]
        for ax, vals in ((ax1, tput), (ax2, per)):
            m = statistics.mean(vals)
            ax.bar(i, m, width=0.62, color=color, edgecolor=SURFACE, linewidth=2)
            ax.scatter([i] * len(vals), vals, s=12, color=INK, zorder=3, linewidths=0)
            ax.text(i, m * 1.02 + (40 if ax is ax1 else 6), f"{m:.0f}", ha="center", va="bottom", fontsize=8.5, color=INK)
    ax1.set_ylabel("ingress MB/s")
    ax1.set_ylim(0, 4800)
    ax1.set_title("4 listeners, in memory, 4 KiB x 64, fire-and-forget (bars: mean, dots: trials)")
    ax2.set_ylabel("MB/s per broker core")
    ax2.set_ylim(0, 640)
    ax2.set_xticks(range(len(arms)), [a[0] for a in arms], fontsize=8.5)
    fig.tight_layout()
    save(fig, out, "experiments")


# --- chart 3: CPU cost model --------------------------------------------------

GROUPS = [
    ("kernel: NIC rx softirq", ["kernel: softirq / NIC rx"]),
    ("kernel: UDP recv syscall (copy)", ["kernel: UDP recv syscall"]),
    ("kernel: UDP send, other", ["kernel: UDP send syscall", "kernel: other", "storage I/O (kernel)"]),
    ("QUIC crypto (AES-GCM)", ["QUIC crypto (AEAD)"]),
    ("quinn-proto packets", ["quinn-proto packet processing"]),
    ("quinn drivers / UDP", ["quinn drivers / udp"]),
    ("broker publish path", ["broker publish / scheduler", "felix-wire decode/encode", "storage (user space)"]),
    ("memcpy / malloc", ["memcpy / alloc", "libc, unsymbolized (mostly memcpy/malloc)"]),
    ("tokio, tracing, other", ["tokio runtime / park", "tracing / metrics", "other"]),
]
GROUP_COLORS = [SERIES[0], SERIES[6], GRAY, SERIES[1], SERIES[2], SERIES[3], SERIES[4], SERIES[7], "#c3c2b7"]


def profile_shares(folded):
    totals = collections.Counter()
    with gzip.open(folded, "rt", errors="replace") as fh:
        for line in fh:
            stack, _, count = line.rstrip("\n").rpartition(" ")
            try:
                n = int(float(count))
            except ValueError:
                continue
            frames = stack.split(";")
            if frames and not ("::" in frames[0] or frames[0].endswith("_[k]")) \
                    and fold_categories.THREAD_FRAME.match(frames[0]) and len(frames) > 1:
                frames = frames[1:]
            totals[fold_categories.classify(frames)] += n
    total = sum(totals.values())
    return {k: v / total for k, v in totals.items()}


def chart_cost_model(cells, out, trace):
    """Broker cores per GB/s of ingress, by category, from the four profiles."""
    profiles = [
        ("before #905, MTU 1500\n4 listeners", "prof-l4-inmem"),
        ("#905, MTU 1500\n4 listeners", "prof-801-l4"),
        ("#905, MTU 3900, ACK 64\n4 listeners", "prof-best-l4"),
        ("#905, MTU 3900, ACK 64\n8 listeners", "prof-best-l8"),
    ]
    fig, ax = plt.subplots(figsize=(10, 3.9))
    for i, (label, name) in enumerate(profiles):
        row = cells.one(name)
        cdir = cells.root / row["cell"]
        shares = profile_shares(cdir / "felixperf-broker-0.folded.gz")
        per_gb = row["ss_cores"] / (row["ss_ingress_mb_s"] / 1000)
        trace.add("cost-model", label.replace("\n", " "), row, "cores_per_gb_s", per_gb)
        left = 0.0
        for (gname, cats), color in zip(GROUPS, GROUP_COLORS):
            w = sum(shares.get(c, 0.0) for c in cats) * per_gb
            trace.add("cost-model", f"{label.replace(chr(10), ' ')}: {gname}", row, "cores_per_gb_s", w)
            ax.barh(i, w, left=left, height=0.6, color=color, edgecolor=SURFACE, linewidth=2,
                    label=gname if i == 0 else None)
            left += w
        ax.text(left + 0.05, i, f"{per_gb:.2f}", va="center", fontsize=9, color=INK)
    ax.set_yticks(range(len(profiles)), [p[0] for p in profiles], fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xlabel("broker cores per GB/s of ingress (lower is cheaper)")
    ax.set_xlim(0, 4.5)
    ax.grid(axis="y", visible=False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=3, fontsize=8.5)
    ax.set_title("Where the broker's CPU goes, per GB/s (perf profiles, 4 KiB x 64, in memory)")
    fig.tight_layout()
    save(fig, out, "cost-model")


# --- chart 4: per-record cost -------------------------------------------------


def chart_per_record(cells, out, trace):
    """Broker core-microseconds per record, batched vs unbatched."""
    arms = [
        ("in memory, 4 KiB, batch 64", "ab-felix-inmem-b64-p4096-k48", SERIES[0]),
        ("in memory, 256 B, batch 64", "ab-felix-inmem-b64-p256-k48", SERIES[0]),
        ("in memory, 4 KiB, batch 1", "ab-felix-inmem-b1-p4096-k48", SERIES[0]),
        ("in memory, 256 B, batch 1", "ab-felix-inmem-b1-p256-k48", SERIES[0]),
        ("periodic, 4 KiB, batch 64", "ab-felix-per-b64-p4096-k48", SERIES[2]),
        ("periodic, 256 B, batch 64", "ab-felix-per-b64-p256-k48", SERIES[2]),
        ("on_commit, 4 KiB, batch 64", "ab-felix-dur-b64-p4096-k48", SERIES[3]),
        ("on_commit, 256 B, batch 64", "ab-felix-dur-b64-p256-k48", SERIES[3]),
        ("on_commit, 4 KiB, batch 1", "ab-felix-dur-b1-p4096-k48", SERIES[3]),
        ("on_commit, 256 B, batch 1", "ab-felix-dur-b1-p256-k48", SERIES[3]),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    for i, (label, prefix, color) in enumerate(arms):
        rows = cells.trials(prefix)
        vals = []
        for r in rows:
            rec_s = r["ss_publish_mb_s"] * 1e6 / int(r["payload_bytes"])
            trace.add("per-record", label, r, "records_per_s", rec_s)
            vals.append(trace.add("per-record", label, r, "core_us_per_record", r["ss_cores"] / rec_s * 1e6))
        m = statistics.mean(vals)
        ax.barh(i, m, height=0.62, color=color, edgecolor=SURFACE, linewidth=2)
        ax.text(m * 1.08, i, f"{m:.1f} µs", va="center", fontsize=8.5, color=INK)
    ax.set_yticks(range(len(arms)), [a[0] for a in arms], fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.set_xlim(0.3, 300)
    ax.set_xlabel("broker core-µs per record (log scale)")
    ax.grid(axis="y", visible=False)
    ax.set_title("Broker CPU per record (4 listeners, MTU 3900, 64 batches in flight)")
    fig.tight_layout()
    save(fig, out, "per-record")


# --- chart 5: Felix and NATS JetStream pairs ----------------------------------

# Each pair ran on the same broker VM and generators, interleaved, with 48 keys
# for Felix and 48 streams for NATS. Labels say what both sides guarantee.
NATS_PAIRS = [
    ("fsync before ack, batch 64, 4 KiB\n(NATS: atomic batch)", "dur-a64-p4096"),
    ("fsync before ack, batch 64, 256 B\n(NATS: atomic batch)", "dur-a64-p256"),
    ("fsync before ack, batch 1, 4 KiB", "dur-b1-p4096"),
    ("fsync before ack, batch 1, 256 B", "dur-b1-p256"),
    ("Felix periodic / NATS default sync,\nbatch 64, 4 KiB", "per-b64-p4096"),
    ("Felix periodic / NATS default sync,\nbatch 64, 256 B", "per-b64-p256"),
    ("in memory, batch 64, 4 KiB", "inmem-b64-p4096"),
    ("in memory, batch 64, 256 B", "inmem-b64-p256"),
    ("in memory, batch 1, 4 KiB", "inmem-b1-p4096"),
    ("in memory, batch 1, 256 B", "inmem-b1-p256"),
    ("no ack (core NATS), 4 KiB", "ff-p4096"),
    ("no ack (core NATS), 256 B", "ff-p256"),
]


def records_per_s(cells, row):
    """Steady-state records/s, counted the same way for both systems.

    Durable and JetStream cells divide the steady append rate by the cell's
    mean stored record size, so per-record overhead (Felix's header, NATS's
    subject and metadata) is not counted as throughput. An in-memory Felix cell
    appends nothing, so its records come from the payload bytes published; a
    core NATS cell from the bytes and requests the server received."""
    cdir = cells.root / row["cell"]
    keys = ("m.append_bytes", "m.append_records")
    rate = row.get("ss_append_mb_s")
    if not rate:
        if not str(row.get("ref", "")).startswith("nats"):
            return row["ss_publish_mb_s"] * 1e6 / int(row["payload_bytes"])
        rate, keys = row["ss_publish_mb_s"], ("m.publish_bytes", "m.publish_requests")
    nbytes = nrecs = 0.0
    for before in cdir.glob("felixperf-broker-*.before.txt"):
        after = before.with_name(before.name.replace(".before.", ".after."))
        b, a = summarize.kv_lines(before), summarize.kv_lines(after)
        nbytes += summarize.delta(b, a, keys[0]) or 0.0
        nrecs += summarize.delta(b, a, keys[1]) or 0.0
    if not nbytes or not nrecs:
        sys.exit(f"{row['cell']}: no {keys} counters")
    return rate * 1e6 / (nbytes / nrecs)


def chart_nats_pairs(cells, out, trace):
    """Records/s and broker core-µs per record, Felix against NATS JetStream."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 7.4), sharey=True)
    colors = {"Felix": SERIES[0], "NATS": SERIES[1]}
    h = 0.38
    for i, (label, pair) in enumerate(NATS_PAIRS):
        flat = label.replace("\n", " ")
        for j, (system, prefix) in enumerate((("Felix", f"ab-felix-{pair}-k48"), ("NATS", f"ab-nats-{pair}-s48"))):
            rows = cells.trials(prefix)
            recs = [trace.add("nats-pairs", f"{system}: {flat}", r, "records_per_s", records_per_s(cells, r)) for r in rows]
            cost = [trace.add("nats-pairs", f"{system}: {flat}", r, "core_us_per_record", r["ss_cores"] / n * 1e6)
                    for r, n in zip(rows, recs)]
            y = i + (j - 0.5) * h
            for ax, vals, fmt_ in ((ax1, recs, "{:,.0f}"), (ax2, cost, "{:.1f} µs")):
                m = statistics.mean(vals)
                ax.barh(y, m, height=h, color=colors[system], edgecolor=SURFACE, linewidth=1,
                        label=system if i == 0 and ax is ax1 else None)
                if len(vals) > 1:
                    ax.scatter(vals, [y] * len(vals), s=9, color=INK, zorder=3, linewidths=0)
                ax.text(m * 1.12, y, fmt_.format(m), va="center", fontsize=7.5, color=INK)
    ax1.set_yticks(range(len(NATS_PAIRS)), [p[0] for p in NATS_PAIRS], fontsize=8)
    ax1.invert_yaxis()
    for ax in (ax1, ax2):
        ax.set_xscale("log")
        ax.grid(axis="y", visible=False)
    ax1.set_xlim(2e4, 6e7)
    ax1.set_xlabel("records/s, steady state from server counters (log scale)")
    ax1.set_title("Throughput")
    ax2.set_xlim(0.3, 300)
    ax2.set_xlabel("server core-µs per record (log scale, lower is cheaper)")
    ax2.set_title("Server CPU per record")
    fig.legend(*ax1.get_legend_handles_labels(), loc="lower center", ncol=2, bbox_to_anchor=(0.5, -0.03))
    fig.suptitle("Felix and NATS JetStream 2.15.0 on the same 8 vCPU NVMe broker, MTU 3900, TLS (dots: trials)",
                 fontsize=11, fontweight="bold", color=INK)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    save(fig, out, "nats-pairs")


# --- chart 6: one message in flight, before and after #927 ---------------------


def latency_us(cells, row, kind):
    lj = summarize.loadgen_json(cells.root / row["cell"] / "felixperf-loadgen.run.txt")
    return lj[f"{kind}_latency_us"]


def chart_latency_927(cells, out, trace):
    """p50 and p99 ack and delivery latency at 256 B, one publish in flight."""
    groups = [
        ("in memory\n(NATS: file stream, default sync)", [
            ("Felix before #927", ["ab927-base-lat-inmem-r1", "ab927-base-lat-inmem-r2"], SERIES[3]),
            ("Felix after #927", ["ab927-pr927-lat-inmem-r1", "ab927-pr927-lat-inmem-r2"], SERIES[0]),
            ("NATS JetStream", ["nats-lat-periodic-p256-t1"], SERIES[1]),
        ]),
        ("fsync before ack\n(Felix on_commit, NATS sync always)", [
            ("Felix before #927", ["ab927-base-lat-oncommit-r1", "ab927-base-lat-oncommit-r2"], SERIES[3]),
            ("Felix after #927", ["ab927-pr927-lat-oncommit-r1", "ab927-pr927-lat-oncommit-r2"], SERIES[0]),
            ("NATS JetStream", ["nats-lat-oncommit-p256-t1"], SERIES[1]),
        ]),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.4), sharey=True)
    w = 0.26
    for ax, kind, title in ((axes[0], "ack", "publish to ack"), (axes[1], "delivery", "publish to subscriber")):
        for gi, (glabel, arms) in enumerate(groups):
            for ai, (alabel, names, color) in enumerate(arms):
                rows = [cells.one(n) for n in names]
                p50 = [trace.add("latency-927", f"{glabel.splitlines()[0]}: {alabel} {kind} p50", r, "us",
                                 latency_us(cells, r, kind)["p50"]) for r in rows]
                p99 = [trace.add("latency-927", f"{glabel.splitlines()[0]}: {alabel} {kind} p99", r, "us",
                                 latency_us(cells, r, kind)["p99"]) for r in rows]
                x = gi + (ai - 1) * w
                m = statistics.mean(p50)
                ax.bar(x, m, width=w, color=color, edgecolor=SURFACE, linewidth=1.5,
                       label=alabel if gi == 0 and ax is axes[0] else None)
                ax.scatter([x] * len(p99), p99, marker="_", s=120, color=INK, zorder=3, linewidths=1.6,
                           label="p99" if gi == 0 and ai == 0 and ax is axes[0] else None)
                ax.text(x, m / 2, f"{m:.0f}", ha="center", va="center", fontsize=8, color=SURFACE, fontweight="bold")
        ax.set_xticks(range(len(groups)), [g[0] for g in groups], fontsize=8.5)
        ax.set_title(title)
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("µs (bars: p50, ticks: p99)")
    axes[0].set_ylim(0, 1600)
    fig.legend(*axes[0].get_legend_handles_labels(), loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.06))
    fig.suptitle("256 B, one publish in flight, same broker VM, MTU 3900", fontsize=11, fontweight="bold", color=INK)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, out, "latency-927")


def main():
    root = HERE.parent.parent
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sessions", type=Path, default=HERE / "azure" / "sessions")
    ap.add_argument("--out", type=Path, default=root / "docs-site" / "public" / "charts" / "perf-v060")
    ap.add_argument("--preview", type=Path, help="also write PNGs here, for looking at the charts")
    args = ap.parse_args()
    global PREVIEW
    PREVIEW = args.preview
    args.out.mkdir(parents=True, exist_ok=True)
    cells = Cells(args.sessions, A)
    trace = Trace()
    chart_listeners(cells, args.sessions, args.out, trace)
    chart_experiments(cells, args.out, trace)
    chart_cost_model(cells, args.out, trace)
    chart_per_record(cells, args.out, trace)
    chart_nats_pairs(cells, args.out, trace)
    chart_latency_927(cells, args.out, trace)
    trace.write(args.out / "data.csv")
    print(f"wrote {args.out / 'data.csv'}")


if __name__ == "__main__":
    main()
