#!/usr/bin/env python3
"""Reduce a session's cell directories to one row per cell and one per group.

    summarize.py <results-dir>
    summarize.py --cell <cell-dir>     one console line for one cell

Reads <results-dir>/cells/*/ as written by cells.sh and writes:

  cells.csv   one row per trial: build, knobs, broker-side throughput and
              storage counters, client-side numbers, CPU, UDP errors,
              per-listener datagram share
  summary.md  trials of a cell grouped (name minus -tN): mean and spread

The headline throughput is steady state, from each broker's 1 Hz counter
series (<broker>.series.tsv): the window when every generator was running
(gen.start/gen.end in its run output), less its first and last 10%, cut into
one-second steps summed over brokers, and the median step reported. Summing
per-generator averages, or dividing a before/after delta by the cell's
length, both count the ramp and the tail where some generators have stopped,
and misread an unfair split. Cells without a series are marked legacy and
keep only the older numbers, which are also kept beside the new ones.

The loadgen's throughput is an enqueue rate for fire-and-forget publishes.
Standard library only, so it runs anywhere the drivers do.
"""

import csv
import json
import re
import statistics
import sys
from pathlib import Path

BASE_KNOBS = {
    "FELIX_QUIC_LISTENERS": "listeners",
    "FELIX_IO_RUNTIME_THREADS": "io_threads",
    "FELIX_DURABLE_FSYNC_MODE": "fsync",
    "FELIX_ACK_ON_COMMIT": "ack_on_commit",
    "FELIX_STORAGE_IO_URING": "io_uring",
    "FELIX_PUB_INGRESS_WAIT": "ingress_wait",
}
CLIENT_PORTS = range(5000, 5064)
TRIM = 0.10


def read_series(path):
    """(hz, rows): rows are dicts of floats keyed by the header's columns.

    A NATS series (stats=nats) carries the server's received bytes in the
    publish column; it becomes the row's in_bytes, the ingress figure, since
    NATS clients use TCP and the per-port UDP counters stay at zero."""
    hz, cols, rows, nats = 100.0, None, [], False
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("#"):
            m = re.search(r"\bhz=(\d+)", line)
            if m:
                hz = float(m.group(1))
            nats = nats or "stats=nats" in line
            continue
        parts = line.split("\t")
        if cols is None:
            cols = parts
            continue
        if len(parts) != len(cols):
            continue
        try:
            row = {c: float(v) for c, v in zip(cols, parts)}
        except ValueError:
            continue
        if nats:
            row["in_bytes"] = row.get("publish_bytes", 0.0)
        rows.append(row)
    return hz, rows


def at(rows, key, x):
    """A counter linearly interpolated at time x; None outside the series."""
    if not rows or x < rows[0]["t"] or x > rows[-1]["t"]:
        return None
    lo, hi = 0, len(rows) - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if rows[mid]["t"] <= x:
            lo = mid
        else:
            hi = mid
    a, b = rows[lo], rows[hi]
    if b["t"] == a["t"]:
        return a.get(key)
    va, vb = a.get(key), b.get(key)
    if va is None or vb is None:
        return None
    return va + (vb - va) * (x - a["t"]) / (b["t"] - a["t"])


def moving_window(series):
    """First to last sample where append or ingress moved on any broker."""
    lo = hi = None
    for _, rows in series.values():
        for prev, cur in zip(rows, rows[1:]):
            if cur["append_bytes"] > prev["append_bytes"] or ingress(cur) > ingress(prev):
                lo = prev["t"] if lo is None else min(lo, prev["t"])
                hi = cur["t"] if hi is None else max(hi, cur["t"])
    return lo, hi


def ingress(row):
    if "in_bytes" in row:
        return row["in_bytes"]
    return sum(v for k, v in row.items() if (m := re.match(r"^port\.(\d+)\.bytes$", k)) and int(m.group(1)) in CLIENT_PORTS)


def steady_state(series, gen_times):
    """Median one-second aggregate rates over the trimmed all-generators window.

    series: broker -> (hz, rows). gen_times: [(start, end)] per generator."""
    out = {"ss_window": None}
    if gen_times:
        w0, w1 = max(s for s, _ in gen_times), min(e for _, e in gen_times)
        out["ss_window"] = "gens"
    else:
        w0, w1 = moving_window(series)
        out["ss_window"] = "counters" if w0 is not None else None
    if w0 is None or w1 is None or w1 <= w0:
        out["ss_window"] = "no-overlap" if gen_times else None
        return out
    a, b = w0 + TRIM * (w1 - w0), w1 - TRIM * (w1 - w0)
    grid = [a + i for i in range(int(b - a) + 1)] if b - a >= 1 else [a, b]
    if len(grid) < 2:
        grid = [a, b]
    steps = {k: [] for k in ("append", "publish", "ingress", "cores", "datagrams", "drops")}
    for g0, g1 in zip(grid, grid[1:]):
        dt = g1 - g0
        acc = dict.fromkeys(steps, 0.0)
        ok = True
        for hz, rows in series.values():
            for name, key in (("append", "append_bytes"), ("publish", "publish_bytes"), ("datagrams", "udp_in"),
                              ("drops", "udp_rcvbuf_errors"), ("cores", "proc_ticks")):
                v0, v1 = at(rows, key, g0), at(rows, key, g1)
                if v0 is None or v1 is None:
                    ok = False
                    break
                d = max(v1 - v0, 0.0)
                acc[name] += d / hz if name == "cores" else d
            if not ok:
                break
            i0 = ingress_at(rows, g0)
            i1 = ingress_at(rows, g1)
            acc["ingress"] += max(i1 - i0, 0.0)
        if ok:
            for k in steps:
                steps[k].append(acc[k] / dt)
    n = len(steps["append"])
    out["ss_secs"] = b - a
    out["ss_steps"] = n
    out["ss_all_gens_secs"] = w1 - w0
    if not n:
        return out
    med = statistics.median
    out["ss_append_mb_s"] = med(steps["append"]) / 1e6
    out["ss_publish_mb_s"] = med(steps["publish"]) / 1e6
    out["ss_ingress_mb_s"] = med(steps["ingress"]) / 1e6
    out["ss_datagrams_s"] = med(steps["datagrams"])
    out["ss_cores"] = med(steps["cores"])
    # Drops come in bursts; a median would read zero. Mean over the window.
    out["ss_drops_s"] = statistics.mean(steps["drops"])
    return out


def ingress_at(rows, x):
    if "in_bytes" in rows[0]:
        return at(rows, "in_bytes", x) or 0.0
    keys = [k for k in rows[0] if (m := re.match(r"^port\.(\d+)\.bytes$", k)) and int(m.group(1)) in CLIENT_PORTS]
    return sum(at(rows, k, x) or 0.0 for k in keys)


def kv_lines(path):
    out = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if "=" not in line or line.startswith(("!!", ">>")):
            continue
        key, value = line.split("=", 1)
        if " " in key:
            continue
        out[key] = value
    return out


def num(d, key):
    try:
        return float(d[key])
    except (KeyError, ValueError):
        return None


def delta(before, after, key):
    a, b = num(after, key), num(before, key)
    if a is None or b is None:
        return None
    # A counter that went backwards means the broker restarted mid-cell.
    return a - b if a >= b else None


def loadgen_json(path):
    """The instrument's result line: felix-loadgen's LOADGEN_JSON, or the
    NATS_BENCH_JSON that nats-agent reduces `nats bench` CSVs to, which uses
    the same key names (throughput_mb_s, throughput_msg_s, ack_latency_us)."""
    for line in reversed(path.read_text(errors="replace").splitlines()):
        for tag in ("LOADGEN_JSON ", "NATS_BENCH_JSON "):
            if line.startswith(tag):
                try:
                    return json.loads(line[len(tag):])
                except json.JSONDecodeError:
                    return None
    return None


def parse_args(args):
    toks = args.split()
    flags = {}
    for i, t in enumerate(toks):
        if t.startswith("--"):
            nxt = toks[i + 1] if i + 1 < len(toks) and not toks[i + 1].startswith("--") else "1"
            flags[t[2:]] = nxt
    return flags


def fsum(values):
    vals = [v for v in values if v is not None]
    return sum(vals) if vals else None


def fmax(values):
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


def cell_row(cdir):
    meta = kv_lines(cdir / "meta.env")
    if not meta:
        return None
    row = {"cell": cdir.name, "done": (cdir / "done").exists()}
    m = re.match(r"^(.*)-t(\d+)$", cdir.name)
    row["group"], row["trial"] = (m.group(1), int(m.group(2))) if m else (cdir.name, 1)
    row["ref"] = meta.get("ref", "")
    flags = parse_args(meta.get("args", ""))
    gens = meta.get("generators", "").split()
    row["generators"] = len(gens)
    for f in ("scenario", "stream", "payload-bytes", "batch", "in-flight", "concurrency", "keys", "fanout"):
        row[f.replace("-", "_")] = flags.get(f, "")
    row["loadgen_env"] = meta.get("loadgen_env", "").strip()
    overrides = {k[len("override."):]: v for k, v in meta.items() if k.startswith("override.")}
    for env, col in BASE_KNOBS.items():
        row[col] = overrides.get(env, "")
    row["other_overrides"] = " ".join(f"{k}={v}" for k, v in sorted(overrides.items()) if k not in BASE_KNOBS)

    brokers = sorted({p.name.split(".")[0] for p in cdir.glob("felixperf-broker-*.before.txt")})
    shas, appends, peaks, gbytes, syncs, sdur_s, sdur_c, bsum, bcnt = [], [], [], [], [], [], [], [], []
    fails, quorum, fwd, busy, cores, si, rcvbuf, inerr, active = [], [], [], [], [], [], [], [], []
    ports = {}
    listen = []
    for b in brokers:
        before = kv_lines(cdir / f"{b}.before.txt")
        after = kv_lines(cdir / f"{b}.after.txt")
        ref = before.get("bin.broker.ref", "")
        shas.append(ref.split()[1][:12] if len(ref.split()) > 1 else "")
        listen.append(before.get("listen.ports", ""))
        appends.append(num(after, "s.append_mb_s"))
        peaks.append(num(after, "s.append_peak_mb_s"))
        d = delta(before, after, "m.append_bytes")
        gbytes.append(d / 1e9 if d is not None else None)
        syncs.append(delta(before, after, "m.sync_total"))
        sdur_s.append(delta(before, after, "m.sync_dur_sum"))
        sdur_c.append(delta(before, after, "m.sync_dur_count"))
        bsum.append(delta(before, after, "m.sync_batch_sum"))
        bcnt.append(delta(before, after, "m.sync_batch_count"))
        fails.append(delta(before, after, "m.sync_failures"))
        quorum.append(delta(before, after, "m.quorum_failed"))
        fwd.append(delta(before, after, "m.forwarded"))
        active.append(num(after, "m.append_series_active"))
        busy.append(num(after, "s.cpu_busy"))
        cores.append(num(after, "s.proc_cores"))
        si.append(num(after, "s.cpu_si"))
        rcvbuf.append(delta(before, after, "udp.RcvbufErrors"))
        inerr.append(delta(before, after, "udp.InErrors"))
        for key in after:
            mp = re.match(r"^port\.(\d+)\.pkts$", key)
            if mp and int(mp.group(1)) in CLIENT_PORTS:
                d = delta(before, after, key)
                if d:
                    ports[int(mp.group(1))] = ports.get(int(mp.group(1)), 0) + d
    row["snap_append_mb_s"] = snap_rate(cdir, brokers)
    row["build_sha"] = ",".join(sorted(set(s for s in shas if s)))
    row["listen_ports"] = "|".join(listen)
    row["broker_append_mb_s"] = fsum(appends)
    row["broker_append_peak_mb_s"] = fsum(peaks)
    row["broker_append_gb"] = fsum(gbytes)
    row["active_logs"] = fsum(active)
    row["syncs"] = fsum(syncs)
    sc = fsum(sdur_c)
    row["sync_mean_ms"] = (fsum(sdur_s) / sc * 1e3) if sc else None
    bc = fsum(bcnt)
    row["sync_fanin"] = (fsum(bsum) / bc) if bc else None
    row["sync_failures"] = fsum(fails)
    row["quorum_failed"] = fsum(quorum)
    row["forwarded"] = fsum(fwd)
    row["broker_cpu_busy_max"] = fmax(busy)
    row["broker_proc_cores"] = fsum(cores)
    row["broker_cpu_si_max"] = fmax(si)
    row["udp_rcvbuf_errors"] = fsum(rcvbuf)
    row["udp_in_errors"] = fsum(inerr)
    total = sum(ports.values())
    row["port_share"] = " ".join(f"{p}:{100 * c / total:.0f}%" for p, c in sorted(ports.items())) if total else ""
    row["port_imbalance"] = (max(ports.values()) / min(ports.values())) if len(ports) > 1 else None

    client_mb, client_msg, p50, p99, dp50, lcpu = [], [], None, None, None, []
    gp50 = gp99 = None
    gen_times, gen_rates, pub_secs = [], [], []
    for g in gens:
        run = cdir / f"{g}.run.txt"
        j = loadgen_json(run) if run.exists() else None
        lcpu.append(num(kv_lines(cdir / f"{g}.after.txt"), "s.cpu_busy"))
        rk = kv_lines(run)
        gs, ge = num(rk, "gen.start"), num(rk, "gen.end")
        if gs is not None and ge is not None:
            gen_times.append((gs, ge))
        if not j:
            continue
        # pubsub is sized by record count, not time: how long it actually
        # published, so a run that is mostly drain is not read as steady.
        if j.get("scenario") == "pubsub" and j.get("publish_throughput_msg_s"):
            n = (j.get("warmup") or 0) + (j.get("total") or 0)
            pub_secs.append(n / j["publish_throughput_msg_s"])
        gen_rates.append((g, j.get("throughput_mb_s")))
        mb = j.get("throughput_mb_s")
        if mb is None and j.get("publish_throughput_msg_s") and j.get("payload_bytes"):
            mb = j["publish_throughput_msg_s"] * j["payload_bytes"] / 1e6
        client_mb.append(mb)
        client_msg.append(j.get("throughput_msg_s", j.get("publish_throughput_msg_s")))
        if p50 is None:
            lat = (j.get("ack_latency_us") or j.get("batch_ack_latency_us")
                   or (j.get("put") or {}).get("latency_us"))
            if lat:
                p50, p99 = lat.get("p50"), lat.get("p99")
            gl = (j.get("get") or {}).get("latency_us")
            if gl:
                gp50, gp99 = gl.get("p50"), gl.get("p99")
            dl = j.get("delivery_latency_us")
            if dl:
                dp50 = dl.get("p50")
    row["client_mb_s"] = fsum(client_mb)
    row["client_msg_s"] = fsum(client_msg)
    row["lat_p50_us"], row["lat_p99_us"], row["delivery_p50_us"] = p50, p99, dp50
    row["get_p50_us"], row["get_p99_us"] = gp50, gp99
    row["loadgen_cpu_busy_max"] = fmax(lcpu)
    rates = [r for _, r in gen_rates if r]
    row["gen_mb_s"] = " ".join(f"{g.replace('felixperf-', '')}:{r:.0f}" for g, r in gen_rates if r is not None)
    row["gen_fairness"] = max(rates) / min(rates) if len(rates) > 1 else None
    row["gen_start_skew_s"] = (max(s for s, _ in gen_times) - min(s for s, _ in gen_times)) if gen_times else None
    row["gen_end_skew_s"] = (max(e for _, e in gen_times) - min(e for _, e in gen_times)) if gen_times else None
    series = {}
    for b in brokers:
        p = cdir / f"{b}.series.tsv"
        if p.exists():
            hz, rows = read_series(p)
            if len(rows) >= 2:
                series[b] = (hz, rows)
    row["legacy"] = not series or len(series) < len(brokers)
    ss = {} if row["legacy"] else steady_state(series, gen_times if len(gen_times) == len(gens) else [])
    run_secs = max((e - st for st, e in gen_times), default=0)
    if pub_secs and run_secs and max(pub_secs) < 0.5 * run_secs:
        # The median second would be drain, reading as ~0. Quote the client.
        ss = {"ss_window": f"publish {max(pub_secs):.0f}s of {run_secs:.0f}s run; use client rates"}
    for k in SS_COLS:
        row[k] = ss.get(k)
    row["ss_records_s"], row["ss_payload_mb_s"] = records_rate(cdir, brokers, row)
    row["results"] = sum(1 for g in gens if (cdir / f"{g}.run.txt").exists() and loadgen_json(cdir / f"{g}.run.txt"))
    return row


def records_rate(cdir, brokers, row):
    """Steady-state records/s and payload MB/s: the steady append rate over
    the cell's mean appended record size (snapshot deltas of bytes over
    records), times the payload. Puts Felix and NATS on the same footing,
    since their stored records carry different per-record overhead. A NATS
    core cell appends nothing; its records come from what the server received."""
    db = dr = 0.0
    rate = row.get("ss_append_mb_s")
    keys = ("m.append_bytes", "m.append_records")
    if not rate and row.get("scenario", "").startswith("nats-core"):
        rate, keys = row.get("ss_ingress_mb_s"), ("m.publish_bytes", "m.publish_requests")
    for b in brokers:
        before = kv_lines(cdir / f"{b}.before.txt")
        after = kv_lines(cdir / f"{b}.after.txt")
        x, y = delta(before, after, keys[0]), delta(before, after, keys[1])
        if x is None or y is None:
            return None, None
        db += x
        dr += y
    try:
        payload = float(row.get("payload_bytes") or 0)
    except ValueError:
        payload = 0.0
    if not rate or not dr or not db:
        return None, None
    recs = rate * 1e6 / (db / dr)
    return recs, (recs * payload / 1e6 if payload else None)


SS_COLS = ("ss_window", "ss_all_gens_secs", "ss_secs", "ss_steps", "ss_append_mb_s", "ss_ingress_mb_s",
           "ss_publish_mb_s", "ss_datagrams_s", "ss_drops_s", "ss_cores")


def snap_rate(cdir, brokers):
    """Append MB/s from the before/after snapshots over the whole cell, the
    older and coarser measure, kept for comparison."""
    total, secs = 0.0, []
    for b in brokers:
        before = kv_lines(cdir / f"{b}.before.txt")
        after = kv_lines(cdir / f"{b}.after.txt")
        d = delta(before, after, "m.append_bytes")
        t0, t1 = num(before, "t"), num(after, "t")
        if d is None or t0 is None or t1 is None or t1 <= t0:
            return None
        total += d
        secs.append(t1 - t0)
    return total / max(secs) / 1e6 if secs else None


def cell_console_line(cdir):
    r = cell_row(cdir)
    if not r:
        return ""
    old = f"sampler append {fmt(r['broker_append_mb_s'])}, snapshots {fmt(r['snap_append_mb_s'])}, client sum {fmt(r['client_mb_s'])} MB/s"
    fair = f"; gens {r['gen_mb_s']} (max/min {fmt(r['gen_fairness'], 2)})" if r["gen_mb_s"] else ""
    if r["legacy"] or r["ss_append_mb_s"] is None:
        why = "legacy, no series" if r["legacy"] else f"no steady window ({r['ss_window']})"
        if not r["legacy"] and r["client_msg_s"]:
            why += f": client {fmt(r['client_msg_s'], 0)} msg/s"
        return f"{why}: {old}{fair}"
    return (
        f"steady append {fmt(r['ss_append_mb_s'])} MB/s ({fmt(r['ss_records_s'], 0)} records/s, "
        f"payload {fmt(r['ss_payload_mb_s'])} MB/s), ingress {fmt(r['ss_ingress_mb_s'])} MB/s, "
        f"drops {fmt(r['ss_drops_s'], 0)}/s, cores {fmt(r['ss_cores'], 2)} "
        f"over {fmt(r['ss_secs'], 0)}s of {fmt(r['ss_all_gens_secs'], 0)}s all-gens ({r['ss_window']}){fair}; "
        f"old: {old}"
    )


def fmt(v, digits=1):
    if v is None or v == "":
        return "-"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def spread(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None
    mean = statistics.mean(vals)
    return mean, (100 * (max(vals) - min(vals)) / mean if mean and len(vals) > 1 else None)


def main():
    if len(sys.argv) > 2 and sys.argv[1] == "--cell":
        line = cell_console_line(Path(sys.argv[2]))
        if line:
            print(line)
        return
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    rows = [r for r in (cell_row(d) for d in sorted((root / "cells").glob("*")) if d.is_dir()) if r]
    if not rows:
        print(f"no cells under {root}/cells")
        return
    cols = list(rows[0].keys())
    with open(root / "cells.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt(v, 3) if isinstance(v, float) else v for k, v in r.items()})

    groups = {}
    for r in rows:
        groups.setdefault(r["group"], []).append(r)
    lines = [f"# {root.name}", ""]
    sj = root / "session.json"
    if sj.exists():
        s = json.loads(sj.read_text())
        lines += [
            f"Session `{s.get('session')}`, tier {s.get('tier')}, {s.get('location')}; "
            f"brokers {s.get('broker_vm_size')} x{len((s.get('broker_ips') or '').split(','))}, "
            f"generators {s.get('loadgens')}; builds: {', '.join(s.get('builds') or []) or s.get('release_url') or s.get('release_tag')}; "
            f"loadgen {s.get('loadgen_spec')}; RTT ms {s.get('rtt_ms')}.",
            "",
        ]
    fio = []
    for p in sorted((root / "system").glob("*.fio.txt")):
        kv = kv_lines(p)
        for k, v in kv.items():
            if k.startswith("fio."):
                fio.append((p.name.split(".")[0], k[4:], v))
    if fio:
        lines += ["## Device baseline (fio, buffered write + fdatasync)", "", "| host | test | value |", "|---|---|---|"]
        lines += [f"| {h} | {k} | {v} |" for h, k, v in fio]
        lines.append("")
    lines += [
        "## Cells",
        "",
        "Append and ingress MB/s are steady state: the median one-second rate, summed over brokers, "
        "over the window every generator was running less its first and last 10%. Append counts "
        "record bytes (header included); ingress counts UDP bytes (IP/UDP headers included) on the "
        "client listener ports. Drops/s is UDP RcvbufErrors averaged over that window; cores is "
        "broker process CPU. Records/s and payload MB/s divide the steady append rate by the mean "
        "appended record size and multiply by the payload, so per-record overhead (Felix's record header, "
        "NATS's subject and metadata) does not count as throughput; compare systems on these. Fair is the fastest generator's MB/s over the slowest's. The old "
        "columns are the sampler's moving-counter append rate and the sum of client averages. "
        "`legacy` marks cells recorded without a series; `client only` marks pubsub cells that published for under "
        "half the run, where the client rate is the number. Spread is (max-min)/mean over trials.",
        "",
        "| cell | n | ref | knobs | append MB/s (spread) | records/s | payload MB/s | ingress MB/s | drops/s | cores | fair | old append / client MB/s | p50 / p99 us | sync ms | fan-in | broker CPU % | gen CPU % | rcvbuf err | ports |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for g, rs in groups.items():
        ok = [r for r in rs if r["done"]]
        use = ok or rs
        bm, _ = spread([r["broker_append_mb_s"] for r in use])
        legacy = all(r["legacy"] or r["ss_append_mb_s"] is None for r in use)
        client_only = legacy and all(str(r["ss_window"] or "").startswith("publish ") for r in use)
        sa, ss = spread([r["ss_append_mb_s"] for r in use])
        rr, _ = spread([r["ss_records_s"] for r in use])
        rp, _ = spread([r["ss_payload_mb_s"] for r in use])
        si, _ = spread([r["ss_ingress_mb_s"] for r in use])
        sd, _ = spread([r["ss_drops_s"] for r in use])
        sc, _ = spread([r["ss_cores"] for r in use])
        sf, _ = spread([r["gen_fairness"] for r in use])
        cm, _ = spread([r["client_mb_s"] for r in use])
        p50, _ = spread([r["lat_p50_us"] for r in use])
        p99, _ = spread([r["lat_p99_us"] for r in use])
        sm, _ = spread([r["sync_mean_ms"] for r in use])
        fi, _ = spread([r["sync_fanin"] for r in use])
        cpu, _ = spread([r["broker_cpu_busy_max"] for r in use])
        lcpu, _ = spread([r["loadgen_cpu_busy_max"] for r in use])
        rb = fsum([r["udp_rcvbuf_errors"] for r in use])
        r0 = use[0]
        knobs = " ".join(
            f"{c}={r0[c]}" for c in ("listeners", "io_threads", "fsync", "ack_on_commit", "io_uring", "ingress_wait") if r0[c]
        )
        if r0["other_overrides"]:
            knobs += " " + r0["other_overrides"]
        if r0["loadgen_env"]:
            knobs += " client:" + r0["loadgen_env"]
        lines.append(
            f"| {g} | {len(ok)}/{len(rs)} | {r0['ref']} {r0['build_sha']} | {knobs} | "
            f"{('client only' if client_only else 'legacy') if legacy else f'{fmt(sa)} ({fmt(ss, 0)}%)'} | {fmt(rr, 0)} | {fmt(rp)} | {fmt(si)} | {fmt(sd, 0)} | {fmt(sc, 2)} | {fmt(sf, 2)} | "
            f"{fmt(bm)} / {fmt(cm)} | {fmt(p50, 0)} / {fmt(p99, 0)} | {fmt(sm, 2)} | {fmt(fi)} | "
            f"{fmt(cpu, 0)} | {fmt(lcpu, 0)} | {fmt(rb, 0)} | {r0['port_share']} |"
        )
    lines.append("")
    (root / "summary.md").write_text("\n".join(lines))
    print(f">> {root / 'cells.csv'} ({len(rows)} rows), {root / 'summary.md'} ({len(groups)} groups)")


if __name__ == "__main__":
    main()
