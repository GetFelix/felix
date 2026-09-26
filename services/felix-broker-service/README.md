# broker service

Executable entrypoint for running a Felix broker node.

Responsibilities:
- Bootstraps the broker runtime
- Hosts the data plane for pub/sub and cache
- Runs under Tokio with structured logging

See `README.md` for the system overview.

> **The numbers below are stale.** They predate transport-layer tuning
> (path MTU discovery, congestion window, socket buffers), shared-frame
> fanout (event batches are now encoded once per publish regardless of
> subscriber count, not once per subscriber), dense stream-handle routing,
> and the opt-in `core_shards` thread-per-core mode — all of which
> materially changed these results (the same `--binary --fanout 10 --batch
> 64 --payload 4096` command cited below now measures ~560-590K delivered
> msg/s, not ~70k). For current, methodology-documented numbers see
> [Benchmarks](https://gabloe.github.io/felix/features/benchmarks/).
> The profile *names and env vars* below are still valid starting points;
> only the specific latency/throughput figures and charts need
> regenerating via `task perf:latency-matrix` (see below).

## Performance profiles

These profiles tune the event delivery path for different latency/throughput trade-offs.
They are calibrated for localhost tests with fanout up to 10, batch=64, payload up to 4KB.

Balanced (default / recommended starting profile)
- `FELIX_PUB_CONN_POOL=4`
- `FELIX_PUB_STREAMS_PER_CONN=2`
- `FELIX_EVENT_CONN_POOL=8`
- `FELIX_EVENT_CONN_RECV_WINDOW=268435456` (256 MiB)
- `FELIX_EVENT_STREAM_RECV_WINDOW=67108864` (64 MiB)
- `FELIX_EVENT_SEND_WINDOW=268435456` (256 MiB)
- `FELIX_EVENT_BATCH_MAX_DELAY_US=250`
- `FELIX_PUBLISH_CHUNK_BYTES=16384`
- `FELIX_CACHE_CONN_POOL=8`
- `FELIX_CACHE_STREAMS_PER_CONN=4`
- `FELIX_DISABLE_TIMINGS=0`

High-memory / burst-tolerant (memory-heavy)
- `FELIX_PUB_CONN_POOL=4`
- `FELIX_PUB_STREAMS_PER_CONN=2`
- `FELIX_EVENT_CONN_POOL=8`
- `FELIX_EVENT_CONN_RECV_WINDOW=536870912` (512 MiB)
- `FELIX_EVENT_STREAM_RECV_WINDOW=134217728` (128 MiB)
- `FELIX_EVENT_SEND_WINDOW=536870912` (512 MiB)
- `FELIX_EVENT_BATCH_MAX_DELAY_US=250`
- `FELIX_PUBLISH_CHUNK_BYTES=32768`
- `FELIX_CACHE_CONN_POOL=8`
- `FELIX_CACHE_STREAMS_PER_CONN=4`
- `FELIX_DISABLE_TIMINGS=1`

Notes
- These window values are per connection; pool sizes multiply memory usage.
- The balanced profile matches the built-in defaults; override only if you need different trade-offs.
- Flow-control credit is not the same as committed RSS, but large windows can increase memory pressure under bursty workloads.
- Connection windows apply per connection; stream recv windows apply per stream; send windows are per connection but can be pressured by many streams.
- Cache stream pooling can multiply stream recv credit in the worst case: stream_recv_window × streams_per_conn × conn_pool.
- The high-memory profile is not guaranteed to win on localhost; it trades memory for headroom under load.
- The high-memory profile currently only changes event-delivery envs; cache remains on the balanced defaults unless overridden.
- It primarily helps when subscribers fall behind briefly and need to absorb bursts without triggering flow-control stalls.
- Hot-case reference (localhost, binary mode, `FELIX_DISABLE_TIMINGS=1`; median of 5 runs):
```
FELIX_EVENT_CONN_POOL=8
FELIX_EVENT_CONN_RECV_WINDOW=268435456
FELIX_EVENT_STREAM_RECV_WINDOW=67108864
FELIX_EVENT_SEND_WINDOW=268435456
FELIX_EVENT_BATCH_MAX_DELAY_US=250
FELIX_PUB_CONN_POOL=4
FELIX_PUB_STREAMS_PER_CONN=2
FELIX_PUBLISH_CHUNK_BYTES=16384
FELIX_DISABLE_TIMINGS=1
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- --binary --fanout 10 --batch 64 --payload 4096 --total 5000 --warmup 200
```
  Median shape: p50 ~43 ms, ~70k msg/s effective throughput.

## Benchmark matrix (localhost)

Binary framing enabled; batch size varies.
Balanced vs high-memory profiles, `--all --binary`, timings disabled.
All charts are binary-only medians across 5 runs per profile.
This benchmark matrix measures pub/sub event delivery (streaming), not cache.
f1 = fanout 1, f10 = fanout 10.

Reproducible latency-demo benchmarks
- Matrix config lives in `scripts/perf/presets.yml`.
- Run the full pipeline:
```bash
python3 scripts/perf/run_latency_matrix.py --trials 5
python3 scripts/perf/normalize_and_aggregate.py
python3 scripts/perf/make_charts.py
python3 scripts/perf/render_markdown_snippets.py
```
- Or use the Taskfile target:
```bash
task perf:latency-matrix
```

Run it
```bash
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- --all --binary
```

Charts are clipped at p95 to keep outliers from dominating the axes.

Batch=1 (low batching): balanced

fanout=1
![](/docs/assets/latency_demo/balanced/f1_b1_p50.png)
![](/docs/assets/latency_demo/balanced/f1_b1_p99.png)
![](/docs/assets/latency_demo/balanced/f1_b1_throughput.png)

fanout=10
![](/docs/assets/latency_demo/balanced/f10_b1_p50.png)
![](/docs/assets/latency_demo/balanced/f10_b1_p99.png)
![](/docs/assets/latency_demo/balanced/f10_b1_throughput.png)

Batch=1 (low batching): high-memory

fanout=1
![](/docs/assets/latency_demo/high_memory/f1_b1_p50.png)
![](/docs/assets/latency_demo/high_memory/f1_b1_p99.png)
![](/docs/assets/latency_demo/high_memory/f1_b1_throughput.png)

fanout=10
![](/docs/assets/latency_demo/high_memory/f10_b1_p50.png)
![](/docs/assets/latency_demo/high_memory/f10_b1_p99.png)
![](/docs/assets/latency_demo/high_memory/f10_b1_throughput.png)

Batch=64 (high batching): balanced

fanout=1
![](/docs/assets/latency_demo/balanced/f1_b64_p50.png)
![](/docs/assets/latency_demo/balanced/f1_b64_p99.png)
![](/docs/assets/latency_demo/balanced/f1_b64_throughput.png)

fanout=10
![](/docs/assets/latency_demo/balanced/f10_b64_p50.png)
![](/docs/assets/latency_demo/balanced/f10_b64_p99.png)
![](/docs/assets/latency_demo/balanced/f10_b64_throughput.png)

Batch=64 (high batching): high-memory

fanout=1
![](/docs/assets/latency_demo/high_memory/f1_b64_p50.png)
![](/docs/assets/latency_demo/high_memory/f1_b64_p99.png)
![](/docs/assets/latency_demo/high_memory/f1_b64_throughput.png)

fanout=10
![](/docs/assets/latency_demo/high_memory/f10_b64_p50.png)
![](/docs/assets/latency_demo/high_memory/f10_b64_p99.png)
![](/docs/assets/latency_demo/high_memory/f10_b64_throughput.png)

## Cache benchmark (localhost)

Cache benchmarks use the cache stream pool and run with concurrency=32.
All charts are medians across 5 runs (release build, timings disabled).
These numbers assume `FELIX_CACHE_CONN_POOL=8` and `FELIX_CACHE_STREAMS_PER_CONN=4` to avoid connection-level HOL at concurrency=32.
With stream pooling enabled (streams_per_conn=4), 256B get_hit p99 improved from ~450us to ~360us in the localhost conc=32 benchmark.

```
FELIX_CACHE_CONN_POOL=8
FELIX_CACHE_STREAMS_PER_CONN=4
FELIX_CACHE_BENCH_CONCURRENCY=32
FELIX_CACHE_BENCH_KEYS=1024
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

Cache put: p50 latency
```mermaid
xychart-beta
    title "Cache put p50 latency (us)"
    x-axis ["0B","64B","256B","1024B","4096B"]
    y-axis "p50 (us)" 0 --> 300
    bar [158.46,159.46,179.83,193.50,260.50]
```

Cache get_hit: p50 latency
```mermaid
xychart-beta
    title "Cache get_hit p50 latency (us)"
    x-axis ["0B","64B","256B","1024B","4096B"]
    y-axis "p50 (us)" 0 --> 260
    bar [164.38,172.54,177.38,210.00,238.29]
```

Cache get_miss: p50 latency
```mermaid
xychart-beta
    title "Cache get_miss p50 latency (us)"
    x-axis ["0B","64B","256B","1024B","4096B"]
    y-axis "p50 (us)" 0 --> 200
    bar [162.50,163.33,165.83,164.12,165.29]
```

Cache put: throughput
```mermaid
xychart-beta
    title "Cache put throughput (k ops/s)"
    x-axis ["0B","64B","256B","1024B","4096B"]
    y-axis "k ops/s" 0 --> 200
    bar [184.2,174.5,155.1,126.6,77.7]
```

Cache get_hit: throughput
```mermaid
xychart-beta
    title "Cache get_hit throughput (k ops/s)"
    x-axis ["0B","64B","256B","1024B","4096B"]
    y-axis "k ops/s" 0 --> 200
    bar [177.5,173.0,166.0,141.2,125.3]
```

Cache get_miss: throughput
```mermaid
xychart-beta
    title "Cache get_miss throughput (k ops/s)"
    x-axis ["0B","64B","256B","1024B","4096B"]
    y-axis "k ops/s" 0 --> 200
    bar [180.8,180.3,178.4,179.6,179.1]
```
