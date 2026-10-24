#!/usr/bin/env python3
"""Charts for the v0.6.0 performance write-up, straight from cell directories.

    v060_charts.py --single <dir> [--rf3 <dir>] [--release <dir>] [--rf3-release <dir>]
                   [--out docs-site/public/charts/v060]

Reads the same cells as v060_tables.py and writes one SVG per chart. Every chart
makes one point, which its title states. Bars are the median of the finished
trials and whiskers span min to max where there are two or more.

--single       the single-broker campaign (campaign-final.sh), including the NATS
               pairs (ab-*) and, once run, the NATS latency pairs
               (felix-lat-* / nats-lat-*).
--rf3          the RF=3 campaign before the fleet features were finalized
               (campaign-rf3.sh: rf3-q-*, rf3-lat-*).
--release      the release A/B: a results directory whose cells use the main
               campaign's names (m-mem-p4096-b64-ff-spread, m-mem-p4096-b64-ack-spread,
               defaults-mem-p4096-b64-ff) on the release build.
--rf3-release  the RF=3 campaign on a new cluster with every feature finalized,
               same cell names as --rf3.
--site         write latency in milliseconds only, for getfelix.dev. The default
               follows the write-up: microseconds below 1 ms, milliseconds above.

A chart whose data is missing is skipped with a note on stderr, so re-running
the script once a pending run lands fills it in. Needs matplotlib
(scripts/perf/requirements.txt).
"""

import argparse
import re
import statistics as st
import sys
from pathlib import Path

import matplotlib

matplotlib.use("svg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import summarize  # noqa: E402
from v060_tables import latencies, shed_pct  # noqa: E402

TRIAL = re.compile(r"^(?P<group>.+)-t(?P<t>\d+)$")

# Felix is the brand cyan-teal; NATS and "before" are a neutral grey. The
# background is transparent and every colour is a mid-tone chosen to keep at
# least 3:1 (marks) and 4:1 (text) against both white and the dark themes'
# near-black, so one SVG reads in either theme.
FELIX = "#0f92b3"
FELIX_DARK = "#0b748e"
DURABLE = "#8f71d6"
DURABLE_LIGHT = "#a085e0"
DURABLE_DARK = "#8164c4"
NEUTRAL = "#8a9099"
TEXT = "#767e89"

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Inter", "DejaVu Sans"],
    "font.size": 10,
    "text.color": TEXT,
    "axes.labelcolor": TEXT,
    "axes.edgecolor": TEXT,
    "axes.titlecolor": TEXT,
    "axes.titlesize": 11.5,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.color": TEXT,
    "grid.alpha": 0.18,
    "xtick.color": TEXT,
    "ytick.color": TEXT,
    "legend.frameon": False,
    "figure.facecolor": "none",
    "axes.facecolor": "none",
    "savefig.facecolor": "none",
    "savefig.transparent": True,
    "svg.fonttype": "none",
})


def load(root):
    """group -> list of finished trials, each a dict of the rates and latencies used here."""
    groups = {}
    if not root:
        return groups
    cells = Path(root) / "cells"
    for d in sorted(cells.glob("*")) if cells.is_dir() else []:
        if not d.is_dir():
            continue
        m = TRIAL.match(d.name)
        group = m.group("group") if m else d.name
        r = summarize.cell_row(d) or {}
        if not r.get("done") or r.get("legacy"):
            continue
        ack, dl = latencies(d)
        groups.setdefault(group, []).append({
            "payload": r.get("ss_publish_mb_s"),
            "stored": r.get("ss_payload_mb_s"),
            "append": r.get("ss_append_mb_s"),
            "cores": r.get("ss_cores"),
            "shed": shed_pct(d),
            "ack": ack,
            "del": dl,
        })
    return groups


def vals(groups, group, key, sub=None):
    out = []
    for t in groups.get(group, []):
        v = t.get(key)
        if sub is not None:
            v = v.get(sub) if v else None
        if isinstance(v, (int, float)):
            out.append(float(v))
    return out


def stat(vs):
    """(median, low, high) or None."""
    if not vs:
        return None
    return st.median(vs), min(vs), max(vs)


def err(stats):
    """matplotlib yerr from a list of (median, low, high)."""
    return [[s[0] - s[1] for s in stats], [s[2] - s[0] for s in stats]]


def mbs(x, _pos=None):
    if x == 0:
        return "0"
    return f"{x / 1000:g} GB/s" if x >= 1000 else f"{x:g} MB/s"


def rate(x):
    """A bar label: MB/s below 1 GB/s, else GB/s to one decimal, as in the prose."""
    return f"{x / 1000:.1f} GB/s" if x >= 1000 else f"{x:.0f} MB/s"


def times(r):
    return f"{r:.1f}\u00d7" if r >= 10 else f"{r:.2f}\u00d7"


SITE = False


def lat(ms, _pos=None):
    """A latency given in ms: the site uses ms throughout, the write-up uses µs below 1 ms."""
    if ms == 0:
        return "0"
    if SITE or ms >= 1:
        return f"{ms:.3g} ms"
    return f"{ms * 1000:.3g} \u00b5s"


PREVIEW = None


def save(fig, out, name, done):
    fig.tight_layout()
    path = out / f"{name}.svg"
    fig.savefig(path, metadata={"Date": None})
    if PREVIEW:
        fig.savefig(PREVIEW / f"{name}.png", dpi=110, facecolor="white")
        fig.savefig(PREVIEW / f"{name}-dark.png", dpi=110, facecolor="#0d1218")
    plt.close(fig)
    done.append(path)


def skip(name, why):
    print(f"skipped {name}: {why}", file=sys.stderr)


def chart_gate(g, out, done):
    shapes = [
        ("mem-p256-b1-ack-spread", "In-memory 256 B, batch 1, acked"),
        ("mem-p256-b1-ack-plain", "  same, plain keys"),
        ("mem-p256-b64-ack-spread", "In-memory 256 B, batch 64, acked"),
        ("mem-p256-b64-ack-plain", "  same, plain keys"),
        ("mem-p4096-b64-ff-spread", "In-memory 4 KiB, batch 64, fire-and-forget"),
        ("dur-p4096-b64-ack-spread", "Durable 4 KiB, batch 64, acked (append)"),
    ]
    rows = []
    for s, label in shapes:
        key = "append" if s.startswith("dur") else "payload"
        old, new = stat(vals(g, "s0-p9-" + s, key)), stat(vals(g, "s0-new-" + s, key))
        if old and new:
            rows.append((label, old, new, len(vals(g, "s0-new-" + s, key))))
    if not rows:
        return skip("gate", "no s0-p9/s0-new cells")
    fig, ax = plt.subplots(figsize=(9, 3.8))
    y = list(range(len(rows)))[::-1]
    rel = [r[2][0] / r[1][0] * 100 for r in rows]
    lo = [r[2][1] / r[1][0] * 100 for r in rows]
    hi = [r[2][2] / r[1][0] * 100 for r in rows]
    ax.barh(y, rel, color=FELIX, height=0.6, xerr=[[a - b for a, b in zip(rel, lo)],
                                                   [b - a for a, b in zip(rel, hi)]],
            error_kw={"ecolor": TEXT, "capsize": 3, "lw": 1})
    ax.axvline(100, color=NEUTRAL, lw=1.5, ls="--")
    ax.text(100.5, -0.75, "preview.9 = 100%", color=TEXT, va="center", fontsize=8.5)
    for yi, r, v in zip(y, rows, rel):
        ax.text(max(v, 100) + 2, yi, f"{v - 100:+.1f}%   {r[1][0]:.0f} → {r[2][0]:.0f} MB/s",
                va="center", fontsize=9)
    ax.set_yticks(y, [r[0] for r in rows])
    ax.set_xlim(80, 165)
    ax.set_ylim(-1.1, len(rows) - 0.5)
    ax.set_xlabel("Throughput, % of v0.6.0-preview.9 (2 trials each, whiskers min to max)")
    ax.grid(axis="y", visible=False)
    ax.set_title("This build matches or beats preview.9 on every gate shape")
    save(fig, out, "gate", done)


def chart_throughput(g, out, done):
    sizes = [(256, "256 B"), (4096, "4 KiB"), (65536, "64 KiB")]
    series = [
        ("mem", 1, "In-memory, batch 1", FELIX, None),
        ("mem", 64, "In-memory, batch 64", FELIX_DARK, None),
        ("dur", 1, "Durable, batch 1", DURABLE_LIGHT, "//"),
        ("dur", 64, "Durable, batch 64", DURABLE_DARK, "//"),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.4))
    width = 0.2
    # Each size group is centred on the shapes it has, so a shape that was not
    # measured leaves no gap.
    have = {(i, j): stat(vals(g, f"m-{kind}-p{p}-b{b}-ack-spread", "payload"))
            for i, (kind, b, _, _, _) in enumerate(series) for j, (p, _) in enumerate(sizes)}
    slot = {}
    for j in range(len(sizes)):
        present = [i for i in range(len(series)) if have[(i, j)]]
        for k, i in enumerate(present):
            slot[(i, j)] = j + (k - (len(present) - 1) / 2) * width
    any_bar = False
    for i, (kind, b, label, color, hatch) in enumerate(series):
        xs = [slot[(i, j)] for j in range(len(sizes)) if have[(i, j)]]
        ss = [have[(i, j)] for j in range(len(sizes)) if have[(i, j)]]
        if not ss:
            continue
        any_bar = True
        ax.bar(xs, [s[0] for s in ss], width, label=label, color=color, hatch=hatch,
               edgecolor="white" if hatch else None, linewidth=0,
               yerr=err(ss), error_kw={"ecolor": TEXT, "capsize": 2, "lw": 1})
        for x, s in zip(xs, ss):
            ax.text(x, s[2] * 1.08, rate(s[0]), ha="center", fontsize=7.5)
    if not any_bar:
        plt.close(fig)
        return skip("throughput", "no m-* cells")
    # In the legend rather than on the line, where any label would cross a bar.
    disk = ax.axhline(1414, color=TEXT, lw=1, ls=":", label="Disk, fsync per write (fio)")
    ax.set_yscale("log")
    ax.set_ylim(30, 60000)
    ax.yaxis.set_major_formatter(FuncFormatter(mbs))
    ax.set_xticks(range(len(sizes)), [s[1] for s in sizes])
    ax.set_xlabel("Payload per message. Acked, 64 requests in flight per publisher, 64 publishers")
    ax.set_ylabel("Payload accepted (log scale)")
    ax.grid(axis="x", visible=False)
    handles, labels = ax.get_legend_handles_labels()
    order = [i for i, h in enumerate(handles) if h is not disk] + [handles.index(disk)]
    ax.legend([handles[i] for i in order], [labels[i] for i in order], ncol=5, loc="upper center",
              fontsize=8, handlelength=1.6, columnspacing=1.2)
    ax.set_title("Batching lifts 256 B throughput 9 to 22\u00d7; durable batches run at the disk's rate")
    save(fig, out, "throughput", done)


def chart_lossless(g, rel, out, done):
    arms = [
        ("m-mem-p4096-b64-ff-spread", "Fire-and-forget,\nwaits for room", FELIX, None),
        ("m-mem-p4096-b64-ack-spread", "Acked,\n64 in flight", FELIX_DARK, None),
        ("defaults-mem-p4096-b64-ff", "Shipped defaults,\nsheds when full", NEUTRAL, "xx"),
    ]
    labels, stats, colors, hatches, notes, rels = [], [], [], [], [], []
    for group, label, color, hatch in arms:
        s = stat(vals(g, group, "payload"))
        if not s:
            continue
        shed = stat(vals(g, group, "shed"))
        n = len(vals(g, group, "payload"))
        note = f"{n} trials"
        if group.startswith("defaults") and shed:
            note += f"\nshed {min(vals(g, group, 'shed')):.1f}\u2013{max(vals(g, group, 'shed')):.1f}%"
        else:
            note += "\nnothing shed"
        labels.append(label)
        stats.append(s)
        colors.append(color)
        hatches.append(hatch)
        notes.append(note)
        rels.append(stat(vals(rel, group, "payload")))
    if not stats:
        return skip("lossless", "no 4 KiB batch 64 cells")
    have_rel = any(rels)
    fig, ax = plt.subplots(figsize=(9, 3.8))
    w = 0.38 if have_rel else 0.6
    x = list(range(len(stats)))
    off = -w / 2 if have_rel else 0
    for xi, s, c, h in zip(x, stats, colors, hatches):
        ax.bar(xi + off, s[0], w, color=c, hatch=h, edgecolor="white" if h else None, linewidth=0,
               yerr=[[s[0] - s[1]], [s[2] - s[0]]], error_kw={"ecolor": TEXT, "capsize": 3, "lw": 1},
               label=None)
    for xi, s, note in zip(x, stats, notes):
        ax.text(xi + off, s[2] + 80, f"{rate(s[0])}\n{note}", ha="center", va="bottom", fontsize=8)
    if have_rel:
        for xi, r in zip(x, rels):
            if r:
                ax.bar(xi + w / 2, r[0], w, color="none", edgecolor=FELIX, linewidth=1.5,
                       yerr=[[r[0] - r[1]], [r[2] - r[0]]], error_kw={"ecolor": TEXT, "capsize": 3})
                ax.text(xi + w / 2, r[2] + 80, f"{rate(r[0])}\nrelease", ha="center",
                        va="bottom", fontsize=8)
    ax.set_xticks(x, labels)
    ax.set_ylim(0, max(s[2] for s in stats) * 1.35)
    ax.yaxis.set_major_formatter(FuncFormatter(mbs))
    ax.set_ylabel("Payload accepted, in-memory 4 KiB, batch 64")
    ax.grid(axis="x", visible=False)
    ax.set_title("Acked publishing is the fastest lossless mode on this build" if not have_rel else
                 "Lossless modes before and after the read-loop fix")
    save(fig, out, "lossless", done)


def chart_regimes(g, out, done):
    """p50 ack against total requests outstanding, with Little's law's prediction."""
    lines = [
        ("In-memory 4 KiB", "l1-inmem-p4096", "mem", 4096, FELIX, "-"),
        ("In-memory 256 B", "l1-inmem-p256", "mem", 256, FELIX, "--"),
        ("Durable 4 KiB, fsync before ack", "l1-oncommit-p4096", "dur", 4096, DURABLE, "-"),
        ("Durable 256 B, fsync before ack", "l1-oncommit-p256", "dur", 256, DURABLE, "--"),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    plotted = False
    for label, l1, kind, p, color, ls in lines:
        pts = []
        s = stat(vals(g, l1, "ack", "p50"))
        if s:
            pts.append((1, s, None))
        for inflight, group in ((1, f"lc-{kind}-p{p}-f1"), (16, f"lc-{kind}-p{p}-f16"),
                                (64, f"m-{kind}-p{p}-b1-ack-spread")):
            s = stat(vals(g, group, "ack", "p50"))
            mb = stat(vals(g, group, "payload"))
            if s and mb:
                outstanding = 64 * inflight
                little = outstanding / (mb[0] * 1e6 / p) * 1e6
                pts.append((outstanding, s, little))
        if len(pts) < 2:
            continue
        plotted = True
        xs = [q[0] for q in pts]
        ys = [q[1][0] / 1000 for q in pts]
        ax.errorbar(xs, ys, yerr=[[(q[1][0] - q[1][1]) / 1000 for q in pts],
                                  [(q[1][2] - q[1][0]) / 1000 for q in pts]],
                    color=color, ls=ls, marker="o", ms=4, lw=1.6, capsize=2, label=label)
        lx = [q[0] for q in pts if q[2]]
        ly = [q[2] / 1000 for q in pts if q[2]]
        ax.scatter(lx, ly, marker="x", s=28, color=TEXT, zorder=3, linewidths=1)
    if not plotted:
        plt.close(fig)
        return skip("regimes", "no l1/lc cells")
    ax.scatter([], [], marker="x", color=TEXT, label="Little's law: outstanding / completions per s")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([1, 64, 1024, 4096], ["1\none publisher,\none in flight", "64\n64 publishers \u00d7 1",
                                        "1,024\n64 \u00d7 16", "4,096\n64 \u00d7 64"])
    ax.set_ylim(0.1, 100)
    ax.yaxis.set_major_formatter(FuncFormatter(lat))
    ax.axvspan(0.7, 1.6, color=FELIX, alpha=0.08, lw=0)
    ax.text(0.75, 60, "service\ntime", fontsize=9, va="top")
    ax.text(5000, 0.25, "queueing:\nthe wait is set by\nhow much is in flight", fontsize=9, ha="right")
    ax.set_xlim(0.7, 7000)
    ax.set_xlabel("Acked publishes outstanding at once, batch 1 (log scale)")
    ax.set_ylabel("Ack latency, p50 (log scale)")
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(0.14, 1.0))
    ax.set_title(f"Unloaded, a publish takes under {lat(0.4)}; under load, it waits in line")
    save(fig, out, "latency-regimes", done)


def chart_percentiles(g, out, done):
    rows = [
        ("l1-oncommit-p256", "Durable\non_commit\n256 B"),
        ("l1-oncommit-p4096", "Durable\non_commit\n4 KiB"),
        ("l1-periodic-p256", "Durable\nperiodic\n256 B"),
        ("l1-periodic-p4096", "Durable\nperiodic\n4 KiB"),
        ("l1-inmem-p256", "In-memory\n256 B"),
        ("l1-inmem-p4096", "In-memory\n4 KiB"),
    ]
    rows = [r for r in rows if vals(g, r[0], "ack", "p50")]
    if not rows:
        return skip("percentiles", "no l1 cells")
    fig, ax = plt.subplots(figsize=(9, 4))
    w = 0.26
    # A darker teal for p999 would vanish on a dark page, so p999 is hatched instead.
    for i, (pct, color, hatch) in enumerate((("p50", FELIX, None), ("p99", FELIX_DARK, None),
                                             ("p999", FELIX_DARK, "////"))):
        ss = [stat([v / 1000 for v in vals(g, r[0], "ack", pct)]) for r in rows]
        xs = [j + (i - 1) * w for j in range(len(rows))]
        ax.bar(xs, [s[0] for s in ss], w, color=color, label=pct, yerr=err(ss), hatch=hatch,
               edgecolor="white" if hatch else None, linewidth=0,
               error_kw={"ecolor": TEXT, "capsize": 2, "lw": 1})
        if pct == "p50":
            for x, s in zip(xs, ss):
                ax.text(x, s[2] + 0.01, f"{s[0] * 1000:.0f}", ha="center", fontsize=7)
    n = len(vals(g, rows[0][0], "ack", "p50"))
    ax.set_xticks(range(len(rows)), [r[1] for r in rows], fontsize=8.5)
    ax.yaxis.set_major_formatter(FuncFormatter(lat))
    ax.set_ylabel(f"Ack latency, one publish in flight\n({n} trials, whiskers min to max)")
    ax.grid(axis="x", visible=False)
    ax.legend(ncol=3, fontsize=9, loc="upper right")
    ax.set_title(f"An fsynced publish takes about {lat(0.31)}, and p999 stays within 1.5\u00d7 of p50")
    save(fig, out, "latency-percentiles", done)


NATS_PAIRS = [
    ("dur-b1", 256, "Durable, one at a time, 256 B", "group commit"),
    ("dur-b1", 4096, "Durable, one at a time, 4 KiB", "group commit"),
    ("dur-a64", 256, "Durable, 64 per request, 256 B", "NATS 1 batch in flight"),
    ("dur-a64", 4096, "Durable, 64 per request, 4 KiB", "NATS 1 batch in flight"),
    ("per-b64", 256, "Periodic fsync, 64 per request, 256 B", "NATS syncs every 2 min"),
    ("per-b64", 4096, "Periodic fsync, 64 per request, 4 KiB", "NATS syncs every 2 min"),
    ("inmem-b1", 256, "In memory, 256 B", "Felix keeps a replay ring only"),
    ("inmem-b1", 4096, "In memory, 4 KiB", "Felix keeps a replay ring only"),
    ("ff", 256, "Fire and forget, 256 B", "core NATS, no subscriber"),
    ("ff", 4096, "Fire and forget, 4 KiB", "core NATS, no subscriber"),
]


def stored(g, group):
    """Payload at the server: records stored x payload, else the accepted rate (in-memory Felix)."""
    vs = vals(g, group, "stored")
    return vs if len(vs) == len(g.get(group, [])) and vs else vals(g, group, "payload")


def chart_nats(g, out, done):
    rows = []
    for pair, p, label, caveat in NATS_PAIRS:
        f = stored(g, f"ab-felix-{pair}-p{p}-k48")
        n = stored(g, f"ab-nats-{pair}-p{p}-s48")
        if f and n:
            ratio = st.mean(f) / st.mean(n)
            rows.append((label, caveat, ratio, min(f) / max(n), max(f) / min(n)))
    if not rows:
        return skip("nats", "no ab-* cells")
    fig, ax = plt.subplots(figsize=(9, 5.2))
    y = list(range(len(rows)))[::-1]
    for yi, (label, caveat, r, lo, hi) in zip(y, rows):
        color = FELIX if r >= 1 else NEUTRAL
        ax.barh(yi, (r - 1) if r >= 1 else (1 - r), left=1 if r >= 1 else r, height=0.62, color=color)
        ax.plot([lo, hi], [yi, yi], color=TEXT, lw=1)
        txt = f"Felix {times(r)}" if r >= 1 else f"NATS {times(1 / r)}"
        edge = max(r, hi) if r >= 1 else min(r, lo)
        ax.text(edge * 1.05 if r >= 1 else edge / 1.05, yi, txt, va="center",
                ha="left" if r >= 1 else "right", fontsize=8.5)
    ax.set_yticks(y, [f"{label}\n{caveat}" for label, caveat, *_ in rows], fontsize=8)
    ax.axvline(1, color=TEXT, lw=1)
    ax.set_xscale("log")
    ax.set_xlim(0.4, 30)
    ax.set_xticks([0.5, 1, 2, 5, 10, 20], ["0.5\u00d7", "1\u00d7", "2\u00d7", "5\u00d7", "10\u00d7", "20\u00d7"])
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="y", visible=False)
    ax.text(1.04, len(rows) - 0.45, "Felix faster \u2192", fontsize=9)
    ax.text(0.96, len(rows) - 0.45, "\u2190 NATS faster", fontsize=9, ha="right")
    ax.set_ylim(-0.6, len(rows) - 0.1)
    ax.set_xlabel("Felix rate / NATS rate, payload at the server, same VMs\n(log scale; bar: mean of the trials, line: min to max)")
    ax.set_title("Felix leads every acked and durable pair; core NATS leads fire-and-forget")
    save(fig, out, "nats", done)


def chart_nats_latency(g, out, done):
    pairs = [("oncommit", "Durable, fsync before ack"), ("periodic", "Durable, periodic fsync"),
             ("inmem", "In memory"), ("core", "Fire and forget")]
    rows = []
    for pr, label in pairs:
        for p, ps in ((256, "256 B"), (4096, "4 KiB")):
            f = stat([v / 1000 for v in vals(g, f"felix-lat-{pr}-p{p}", "ack", "p50")]
                     or [v / 1000 for v in vals(g, f"felix-lat-{pr}-p{p}", "del", "p50")])
            n = stat([v / 1000 for v in vals(g, f"nats-lat-{pr}-p{p}", "ack", "p50")]
                     or [v / 1000 for v in vals(g, f"nats-lat-{pr}-p{p}", "del", "p50")])
            if f and n:
                rows.append((f"{label}, {ps}", f, n))
    if not rows:
        return skip("nats-latency", "no felix-lat-*/nats-lat-* cells yet")
    fig, ax = plt.subplots(figsize=(8, 4))
    y = list(range(len(rows)))[::-1]
    h = 0.38
    ax.barh([v + h / 2 for v in y], [r[1][0] for r in rows], h, color=FELIX, label="Felix",
            xerr=[[r[1][0] - r[1][1] for r in rows], [r[1][2] - r[1][0] for r in rows]],
            error_kw={"ecolor": TEXT, "capsize": 2})
    ax.barh([v - h / 2 for v in y], [r[2][0] for r in rows], h, color=NEUTRAL, label="NATS",
            xerr=[[r[2][0] - r[2][1] for r in rows], [r[2][2] - r[2][0] for r in rows]],
            error_kw={"ecolor": TEXT, "capsize": 2})
    ax.set_yticks(y, [r[0] for r in rows], fontsize=8.5)
    ax.xaxis.set_major_formatter(FuncFormatter(lat))
    ax.set_xlabel("p50 latency, one publish in flight (whiskers min to max over trials)")
    ax.legend(fontsize=9)
    ax.grid(axis="y", visible=False)
    ax.set_title("One publish at a time: Felix against NATS JetStream")
    save(fig, out, "nats-latency", done)


def chart_rf3_throughput(g, rf3, rel, out, done):
    shapes = [(256, 1, "256 B, batch 1"), (256, 64, "256 B, batch 64"),
              (4096, 1, "4 KiB, batch 1"), (4096, 64, "4 KiB, batch 64")]
    arms = [("Single broker, 8 vCPU, 1 copy", g, "m-dur-p{p}-b{b}-ack-spread", NEUTRAL, None),
            ("3 brokers, 4 vCPU each, 3 copies, before finalizing", rf3, "rf3-q-dur-p{p}-b{b}-ack",
             FELIX, None),
            ("3 brokers, release, features finalized", rel, "rf3-q-dur-p{p}-b{b}-ack", FELIX_DARK, None)]
    arms = [a for a in arms if any(vals(a[1], a[2].format(p=p, b=b), "payload") for p, b, _ in shapes)]
    if len(arms) < 2:
        return skip("rf3-throughput", "no RF=3 cells")
    fig, ax = plt.subplots(figsize=(9, 4))
    w = 0.8 / len(arms)
    for i, (label, src, pat, color, _h) in enumerate(arms):
        xs, ss = [], []
        for j, (p, b, _) in enumerate(shapes):
            s = stat(vals(src, pat.format(p=p, b=b), "payload"))
            if s:
                xs.append(j + (i - (len(arms) - 1) / 2) * w)
                ss.append(s)
        ax.bar(xs, [s[0] for s in ss], w, color=color, label=label, yerr=err(ss),
               error_kw={"ecolor": TEXT, "capsize": 2, "lw": 1})
        for x, s in zip(xs, ss):
            ax.text(x, s[2] + 20, rate(s[0]), ha="center", fontsize=8)
    ax.set_xticks(range(len(shapes)), [s[2] for s in shapes])
    ax.yaxis.set_major_formatter(FuncFormatter(mbs))
    ax.set_ylabel("Durable payload, acked after fsync\n(2 trials, whiskers min to max)")
    ax.grid(axis="x", visible=False)
    ax.legend(fontsize=8.5, loc="upper left")
    ax.set_title("Three copies on smaller brokers keep pace until their disks are the limit")
    save(fig, out, "rf3-throughput", done)


def chart_rf3_latency(g, rf3, rel, out, done):
    arms = [("Single broker, on_commit", g, "l1-oncommit-p{p}", NEUTRAL),
            ("RF=3 Quorum, before finalizing", rf3, "rf3-lat-dur-p{p}", FELIX),
            ("RF=3 Quorum, release, finalized", rel, "rf3-lat-dur-p{p}", FELIX_DARK)]
    arms = [a for a in arms if any(vals(a[1], a[2].format(p=p), "ack", "p50") for p in (256, 4096))]
    if len(arms) < 2:
        return skip("rf3-latency", "no RF=3 latency cells")
    fig, ax = plt.subplots(figsize=(9, 3.6))
    w = 0.8 / len(arms)
    for i, (label, src, pat, color) in enumerate(arms):
        xs, ss, ns = [], [], []
        for j, p in enumerate((256, 4096)):
            vs = [v / 1000 for v in vals(src, pat.format(p=p), "ack", "p50")]
            if vs:
                xs.append(j + (i - (len(arms) - 1) / 2) * w)
                ss.append(stat(vs))
                ns.append(len(vs))
        ax.bar(xs, [s[0] for s in ss], w, color=color, label=label, yerr=err(ss),
               error_kw={"ecolor": TEXT, "capsize": 2})
        for x, s, n in zip(xs, ss, ns):
            ax.text(x, s[2] + 0.05, f"{lat(s[0])}\n{n} trial{'s' if n > 1 else ''}", ha="center",
                    fontsize=8)
    ax.set_xticks([0, 1], ["256 B", "4 KiB"])
    ax.set_ylim(0, 5.6)
    ax.yaxis.set_major_formatter(FuncFormatter(lat))
    ax.set_ylabel("Durable ack p50,\none publish in flight")
    ax.grid(axis="x", visible=False)
    ax.legend(fontsize=8.5, loc="upper left")
    ax.set_title("Before finalizing, a replicated ack waits about 3.5 ms")
    save(fig, out, "rf3-latency", done)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--single", required=True)
    ap.add_argument("--rf3")
    ap.add_argument("--release")
    ap.add_argument("--rf3-release")
    ap.add_argument("--out", default="docs-site/public/charts/v060")
    ap.add_argument("--preview", help="also write PNGs on white and on near-black here, for review")
    ap.add_argument("--site", action="store_true", help="latency in ms only, for getfelix.dev")
    a = ap.parse_args()
    global PREVIEW, SITE
    SITE = a.site
    PREVIEW = Path(a.preview) if a.preview else None
    if PREVIEW:
        PREVIEW.mkdir(parents=True, exist_ok=True)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    g, rf3 = load(a.single), load(a.rf3)
    rel, rf3_rel = load(a.release), load(a.rf3_release)
    done = []
    chart_gate(g, out, done)
    chart_throughput(g, out, done)
    chart_lossless(g, rel, out, done)
    chart_regimes(g, out, done)
    chart_percentiles(g, out, done)
    chart_nats(g, out, done)
    chart_nats_latency(g, out, done)
    chart_rf3_throughput(g, rf3, rf3_rel, out, done)
    chart_rf3_latency(g, rf3, rf3_rel, out, done)
    for p in done:
        print(p)


if __name__ == "__main__":
    main()
