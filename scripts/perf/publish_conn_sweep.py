#!/usr/bin/env python3
import argparse
import re
import statistics
import subprocess
from dataclasses import dataclass

RESULT_RE = re.compile(r"Results \(publish n = \d+, sampled \d+, received \d+, dropped \d+\).*")
P50_RE = re.compile(r"\s+p50\s+=\s+([0-9.]+)\s+(us|ms)")
P99_RE = re.compile(r"\s+p99\s+=\s+([0-9.]+)\s+(us|ms)")
P999_RE = re.compile(r"\s+p999\s+=\s+([0-9.]+)\s+(us|ms)")
THR_RE = re.compile(r"\s+throughput =\s+([0-9.]+)\s+msg/s")


@dataclass
class Sample:
    throughput: float
    p50_ms: float
    p99_ms: float
    p999_ms: float


def to_ms(value: float, unit: str) -> float:
    return value / 1000.0 if unit == "us" else value


def run_once(payload: int, fanout: int, batch: int, pub_conns: int, pub_streams: int, warmup: int, total: int) -> Sample:
    cmd = [
        "cargo", "run", "--release", "--bin", "latency-demo",
        "--features", "demo,telemetry,perf_debug", "--",
        "--warmup", str(warmup),
        "--total", str(total),
        "--payload", str(payload),
        "--fanout", str(fanout),
        "--batch", str(batch),
        "--pub-conns", str(pub_conns),
        "--pub-streams-per-conn", str(pub_streams),
        "--pub-stream-count", "1",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    lines = proc.stdout.splitlines()

    p50 = p99 = p999 = thr = None
    in_result = False
    for line in lines:
        if RESULT_RE.search(line):
            in_result = True
            continue
        if not in_result:
            continue
        if p50 is None:
            m = P50_RE.match(line)
            if m:
                p50 = to_ms(float(m.group(1)), m.group(2))
                continue
        if p99 is None:
            m = P99_RE.match(line)
            if m:
                p99 = to_ms(float(m.group(1)), m.group(2))
                continue
        if p999 is None:
            m = P999_RE.match(line)
            if m:
                p999 = to_ms(float(m.group(1)), m.group(2))
                continue
        if thr is None:
            m = THR_RE.match(line)
            if m:
                thr = float(m.group(1))
                continue
        if p50 is not None and p99 is not None and p999 is not None and thr is not None:
            break

    if None in (p50, p99, p999, thr):
        raise RuntimeError("failed to parse latency-demo output")
    return Sample(throughput=thr, p50_ms=p50, p99_ms=p99, p999_ms=p999)


def summarize(samples: list[Sample]) -> dict[str, float]:
    thr = [s.throughput for s in samples]
    p50 = [s.p50_ms for s in samples]
    p99 = [s.p99_ms for s in samples]
    p999 = [s.p999_ms for s in samples]
    return {
        "thr_mean": statistics.fmean(thr),
        "thr_stdev": statistics.pstdev(thr) if len(thr) > 1 else 0.0,
        "p50_median": statistics.median(p50),
        "p99_median": statistics.median(p99),
        "p999_median": statistics.median(p999),
        "p999_stdev": statistics.pstdev(p999) if len(p999) > 1 else 0.0,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Sweep publisher conn/stream scaling")
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--payload", type=int, default=256)
    ap.add_argument("--fanout", type=int, default=1)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--warmup", type=int, default=200)
    ap.add_argument("--total", type=int, default=5000)
    ap.add_argument("--pub-conns", default="1,2,4,8")
    ap.add_argument("--pub-streams", default="1,2")
    args = ap.parse_args()

    conns = [int(x) for x in args.pub_conns.split(",") if x]
    streams = [int(x) for x in args.pub_streams.split(",") if x]

    print(
        f"Sweep payload={args.payload} fanout={args.fanout} batch={args.batch} "
        f"warmup={args.warmup} total={args.total} trials={args.trials}"
    )
    print("pub_conns,pub_streams,thr_mean,thr_stdev,p50_median_ms,p99_median_ms,p999_median_ms,p999_stdev")

    for c in conns:
        for s in streams:
            samples: list[Sample] = []
            for _ in range(args.trials):
                samples.append(
                    run_once(
                        payload=args.payload,
                        fanout=args.fanout,
                        batch=args.batch,
                        pub_conns=c,
                        pub_streams=s,
                        warmup=args.warmup,
                        total=args.total,
                    )
                )
            stats = summarize(samples)
            print(
                f"{c},{s},{stats['thr_mean']:.2f},{stats['thr_stdev']:.2f},"
                f"{stats['p50_median']:.3f},{stats['p99_median']:.3f},"
                f"{stats['p999_median']:.3f},{stats['p999_stdev']:.3f}"
            )


if __name__ == "__main__":
    main()
