# Broker CPU at the durable ceiling — v0.4.0, single L8as_v4

Sampled from `/proc/stat` at 1 Hz on the broker, around one isolated
`ingest --stream perf-durable --payload-bytes 4096 --batch 64 --concurrency 24`
case (947.2 MB/s, 231246 msg/s, 0 publish retries). Only samples above 15% busy
are counted, so the numbers describe the case and not the idle either side of it.

| | busy | us | sy | si | wa |
|---|---|---|---|---|---|
| v0.4.0 (this run, 947 MB/s) | **50-51%** | 19-21 | 19-20 | 9-10 | 2 |
| v0.3.1 nvme2 (977 MB/s) | **~48%** | 18 | 20 | 9 | — |

Fourteen of the sixteen in-case samples read 50% or 51%.

## What it means

**#511 did not move the ceiling.** It replaced `Notify::notify_waiters()` with a
per-offset `oneshot`, removing N wake-ups per commit — real, and sound — but the
plateau, the decline past c8, and the CPU breakdown are all unchanged from
v0.3.1. Same throughput within run-to-run variance, same CPU to within a point,
same user/system/softirq split.

The wall is still there and it is still not CPU (half the cores idle) and still
not disk (iowait 2%). Something in the commit path **serializes**. Removing the
wake-up storm made each release cheaper without widening the path.

## Run-to-run variance

The same c24 case measured 922.0 MB/s in the ramp and 947.2 MB/s standalone —
~2.7%. Treat differences below ~3% on this rig as noise.

## Caveat

One broker, one load generator. The generator plateaus near ~1.0 GB/s on the
in-memory stream, so a broker ceiling above that could not be seen here. The
durable curve declining *below* the in-memory plateau is what argues the
constraint is broker-side, together with the CPU profile matching nvme2 — where
adding a 2nd and 3rd generator did not go faster and hit `publish queue full`.
