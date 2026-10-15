---
title: "Performance Tuning"
---

Felix trades between latency, throughput, and memory with explicit knobs.
This page explains which knob moves which needle, gives three starting-point
profiles, and points at the measured numbers. None of this has run in
production. The configurations are benchmark-tested starting points, and
your own measurements should override them.

## Understanding Felix Performance

These factors decide Felix performance, and they interact:

1. **Network transport**: QUIC connection and stream configuration
2. **Batching**: Message aggregation at publish and delivery stages
3. **Parallelism**: Connection pools and worker threads
4. **Buffering**: Queue depths and flow control windows
5. **Encoding**: Binary wire framing efficiency
6. **Outbound lanes**: Subscriber writer lane count and lane sharding policy

## Performance Profiles

Felix has three preset profiles to start from. They are the same pipeline with
one setting changed: how long a message may wait for others to batch with
before it is sent.

```mermaid
flowchart LR
    subgraph lat["Latency-optimized"]
        direction TB
        L1(["message"]) l1@--> L2["send immediately<br/><small>batch ≈ 1, shallow queues,<br/>block instead of drop</small>"]
        L2 l2@--> L3(["lowest p99<br/><small>fewest messages per syscall</small>"])
    end

    subgraph bal["Balanced (default)"]
        direction TB
        B1(["message"]) b1@--> B2["brief coalescing window<br/><small>moderate batching + pools</small>"]
        B2 b2@--> B3(["sub-ms latency at<br/>useful throughput"])
    end

    subgraph thr["Throughput-optimized"]
        direction TB
        T1(["message"]) t1@--> T2["fill the batch<br/><small>deep queues, large windows,<br/>lossless pacing</small>"]
        T2 t2@--> T3(["most bytes per second<br/><small>latency includes batch fill</small>"])
    end

    l1@{ animation: fast }
    l2@{ animation: fast }
    b1@{ animate: true }
    b2@{ animate: true }
    t1@{ animation: slow }
    t2@{ animation: slow }

    classDef step fill:#e8f0fe,stroke:#4a6fa5,color:#1a2b40
    classDef ok fill:#e9f5ec,stroke:#4a8a5e,color:#16301f
    classDef warm fill:#fdf0e3,stroke:#b07d3a,color:#3d2a12
    class L2,B2,T2 step
    class L1,B1,T1 ok
    class L3,B3 ok
    class T3 warm
```

:::note[These are trade-offs]
None of these is "faster" in the abstract. Batching raises bytes per second and
raises per-message latency at the same time, because a batched message waits for
the batch. That is why the [benchmark harness](/features/benchmarks/)
reports batch-1 and batch-64 as separate profiles and never compares their
latency percentiles to each other.
:::

### Balanced Profile (Default)

General-purpose settings for mixed workloads:

**Broker configuration**:

```yaml
# Connection pools
pub_conn_pool: 4
pub_streams_per_conn: 2
event_conn_pool: 8
cache_conn_pool: 8
cache_streams_per_conn: 4

# QUIC flow control
event_conn_recv_window: 268435456      # 256 MiB
event_stream_recv_window: 67108864     # 64 MiB
event_send_window: 268435456           # 256 MiB

# Batching
event_batch_max_events: 64
event_batch_max_bytes: 65536
event_batch_max_delay_us: 250
fanout_batch_size: 64

# Queue depths (defaults favor bounded latency + visible overload over deep buffering)
pub_queue_depth: 64
pub_inflight_bytes: 67108864           # 64 MiB shared in-flight publish byte budget
subscriber_queue_capacity: 512
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 64
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto

publish_chunk_bytes: 16384
```

**Expected performance**: see [Benchmarks](/features/benchmarks/) for current, measured
numbers across payload/fanout shapes. The harness runs against this profile. Numbers here are intentionally not duplicated to avoid
drift; the benchmarks page is regenerated from `latency-demo` and is the
source of truth.

**Best for**:
- Mixed pub/sub, cache and consumer-group workloads
- Moderate fanout (1-20 subscribers)
- General application development
- Starting point for tuning

### Latency-Optimized Profile

Minimize tail latency at the cost of throughput:

**Broker configuration**:

```yaml
# Smaller pools
pub_conn_pool: 2
pub_streams_per_conn: 1
event_conn_pool: 4

# Smaller windows
event_conn_recv_window: 67108864       # 64 MiB
event_stream_recv_window: 16777216     # 16 MiB
event_send_window: 67108864            # 64 MiB

# Minimal batching
event_batch_max_events: 8
event_batch_max_bytes: 32768
event_batch_max_delay_us: 100
fanout_batch_size: 8

# Fast acknowledgements
ack_on_commit: true

# Shallow queues, blocking backpressure instead of drops, single writer per
# connection for stable per-message ordering
pub_queue_depth: 32
subscriber_queue_capacity: 64
subscriber_queue_policy: block
subscriber_writer_lanes: 2
subscriber_lane_queue_depth: 32
subscriber_lane_queue_policy: block
subscriber_single_writer_per_conn: true
subscriber_flush_max_items: 1
subscriber_flush_max_delay_us: 0
subscriber_lane_shard: auto
```

**Expected performance**: the latency-focused profile in
[Benchmarks](/features/benchmarks/) (batch = 1, per-message acked) measures this
shape directly: sub-millisecond p999 at fanout 1-10 on the reference
hardware there.

**Best for**:
- Real-time interactive applications
- Trading systems, gaming
- Sensor data with immediate processing
- Low fanout (1-5 subscribers)

### Throughput-Optimized Profile

Maximize throughput and burst tolerance:

**Broker configuration**:

```yaml
# Large pools
pub_conn_pool: 8
pub_streams_per_conn: 4
event_conn_pool: 16
cache_conn_pool: 16
cache_streams_per_conn: 8

# Large windows
event_conn_recv_window: 536870912      # 512 MiB
event_stream_recv_window: 134217728    # 128 MiB
event_send_window: 536870912           # 512 MiB

# Aggressive batching
event_batch_max_events: 256
event_batch_max_bytes: 1048576
event_batch_max_delay_us: 2000
fanout_batch_size: 256

# Async acknowledgements
ack_on_commit: false

# Deep queues, lossless end-to-end backpressure (paces the publisher to the
# pipeline's sustainable rate instead of shedding), thread-per-core stream
# ownership for multi-stream workloads
pub_queue_depth: 256
pub_inflight_bytes: 268435456          # 256 MiB
pub_ingress_wait: true
subscriber_queue_capacity: 4096
subscriber_queue_policy: block
subscriber_writer_lanes: 8
subscriber_lane_queue_depth: 1024
subscriber_lane_queue_policy: block
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
core_shards: 4                         # tune to (physical cores - 2); 0 = off

publish_chunk_bytes: 32768
```

**Expected performance**: the throughput-focused profile in
[Benchmarks](/features/benchmarks/) (batch = 64, lossless, zero drops)
measures this shape directly. Message rate falls and byte rate rises as
payloads grow, so read the byte rate when comparing payload sizes.
`core_shards` may help multi-stream workloads, but its published gains predate
the transport scheduling work and need re-validation.

:::note[Lossless pacing is an explicit trade-off]
`block` queues + `pub_ingress_wait: true` mean producers slow down
instead of anything being dropped. Use it for pipelines that can't tolerate
loss, and for benchmarking sustainable throughput.
Production defaults favor shedding (`drop_new`) so overload stays
visible and bounded. See
[Benchmarks: Saturation behavior](/features/benchmarks/#saturation-behavior).
:::
**Best for**:
- High-throughput data pipelines
- Log aggregation, metrics collection
- High fanout (20-100+ subscribers)
- Batch processing workflows

## Configuration Parameters

### Pub/Sub Parameters

#### Connection Pooling

```yaml
event_conn_pool: 8              # QUIC connections for events
pub_conn_pool: 4                # QUIC connections for publishing
pub_streams_per_conn: 2         # Publish streams per connection
```

**Tuning guidance**:

| Workload | event_conn_pool | pub_conn_pool | streams_per_conn |
|----------|----------------|---------------|------------------|
| Light | 2-4 | 2 | 1-2 |
| Medium | 4-8 | 2-4 | 2 |
| Heavy | 8-16 | 4-8 | 2-4 |
| Very heavy | 16-32 | 8-16 | 4-8 |

:::caution[Worker Sizing]
Set `pub_workers_per_conn` ≤ `pub_streams_per_conn`. Excess workers create contention without benefit.
:::
#### Flow Control Windows

```yaml
event_conn_recv_window: 268435456      # Per-connection receive window
event_stream_recv_window: 67108864     # Per-stream receive window
event_send_window: 268435456           # Per-connection send window
```

**Memory impact calculation**:

```
Worst-case memory = (conn_window × conn_pool) + 
                    (stream_window × avg_streams × conn_pool)
```

Example:
- `conn_pool=8`, `conn_window=256MB`, `stream_window=64MB`, `avg_streams=10`
- Memory ≈ (256MB × 8) + (64MB × 10 × 8) = 2GB + 5.1GB = **7.1GB**

**Tuning guidance**:

- **Low latency, limited bursts**: Use smaller windows (64-128 MiB)
- **High throughput, bursty**: Use larger windows (256-512 MiB)
- **Memory constrained**: Reduce pool size before reducing windows

These are the client's windows. On the broker side, `pub_conn_recv_window` and
`pub_stream_recv_window` bound how much unread publish data one client
connection can leave in the broker. They default to `pub_conn_inflight_bytes`
(16 MiB), so backpressure reaches a publisher soon after ingress fills.

#### Batching Parameters

```yaml
event_batch_max_events: 64             # Max events per batch
event_batch_max_bytes: 262144          # Max batch size (256 KB)
event_batch_max_delay_us: 250          # Max batching delay under load (250 µs)
fanout_batch_size: 64                  # Fanout batch size
```

**Batch triggers**: Event batch is sent when **any** condition is met. The
delay applies only under load: a batch that finds nothing else queued is sent
at once.

**Trade-off analysis**:

| Parameter | ↑ Increase Effect | ↓ Decrease Effect |
|-----------|------------------|------------------|
| `max_events` | Higher throughput, higher latency | Lower latency, lower throughput |
| `max_delay_us` | Higher throughput, higher latency under load | Lower latency under load, lower throughput |
| `max_bytes` | Fewer frames, more efficiency | More frames, less efficiency |
| `fanout_batch_size` | Better fanout efficiency | Lower fanout latency |

**Recommended settings by workload**:

| Workload | `event_batch_max_events` | `event_batch_max_delay_us` |
|----------|--------------------------|----------------------------|
| Latency first | 4 | 50 |
| Low latency | 8 | 100 |
| Balanced (default) | 64 | 250 |
| High throughput | 128 | 1000 |
| Maximum throughput | 256 | 2000 |

For example, the high-throughput profile as a config file:

```yaml
event_batch_max_events: 128
event_batch_max_delay_us: 1000
```

#### Queue Depths and Byte Budgets

```yaml
pub_queue_depth: 64                    # Publish pipeline queue (items)
pub_inflight_bytes: 67108864           # Shared in-flight publish byte budget (bytes, not items)
subscriber_queue_capacity: 512         # Per-subscriber broker-core queue
subscriber_lane_queue_depth: 64        # Per-lane outbound writer queue
pub_workers_per_conn: 4                # Publish workers per connection (ignored when core_shards > 0)
```

**Design intent**: defaults are deliberately shallow. `pub_queue_depth` and
the lane queues bound how much can queue *before* backpressure or shedding
kicks in. The goal is throughput that plateaus with bounded latency, and
overload that shows up in drops and counters. A deep buffer hides the backlog
until the process runs out of memory or tail latency grows without bound. `pub_inflight_bytes`
is a second, independent budget on *bytes* rather than item count, so a few
large batches can't blow past the ingress memory budget even with a small
`pub_queue_depth`.

- **Shallower** (production default direction): lower memory, backpressure/drops surface sooner, bounded tail latency.
- **Deeper** (opt-in, throughput profile): higher burst tolerance and memory, and only safe paired with `subscriber_queue_policy: block` + `pub_ingress_wait: true` (lossless pacing). Without those, deep queues only delay the drops.

**Memory per queue**:

```
Queue memory ≈ queue_depth × avg_message_size

Example (default subscriber_queue_capacity=512): 512 × 4KB = 2MB per subscriber queue
With 100 subscribers: 100 × 2MB = 200MB
```

#### Outbound Writer Lanes and Sharding

Writer lanes parallelize outbound subscriber writes while preserving per-subscriber ordering.

```yaml
subscriber_writer_lanes: 4
max_subscriber_writer_lanes: 8
subscriber_lane_queue_depth: 64
subscriber_lane_shard: auto  # auto | subscriber_id_hash | connection_id_hash | round_robin_pin
```

:::note[What lanes parallelize changed]
Event batches are now encoded once per publish and the encoded `Bytes`
handle is shared across every subscriber of a stream (see
[Wire Protocol: Shared Binary EventBatch](/architecture/wire-protocol/#shared-binary-eventbatch-encoding)).
Lanes no longer parallelize *encoding* cost. They parallelize the QUIC
*write* syscalls across subscribers. Older lane-count sweep numbers from
before this change are not representative of current behavior and have
been removed; see [Benchmarks](/features/benchmarks/) for current measurements.
:::
Start here:
1. `subscriber_lane_shard: auto`
2. `subscriber_writer_lanes: 4`
3. Increase to `8` only if throughput is still lane-bound
4. Avoid assuming larger lane counts always help; watch p99/p999
5. For multi-stream workloads, also evaluate `core_shards` (thread-per-core
   stream ownership). See [Benchmarks](/features/benchmarks/), which showed larger
   gains there than lane count alone.

### Cache Parameters

```yaml
cache_conn_pool: 8                     # QUIC connections for cache
cache_streams_per_conn: 4              # Streams per connection
cache_conn_recv_window: 268435456      # 256 MiB per connection
cache_stream_recv_window: 67108864     # 64 MiB per stream
```

**Concurrency calculation**:

```
Max concurrent cache ops = cache_conn_pool × cache_streams_per_conn
```

**Recommended by workload**:

| Workload | conn_pool | streams_per_conn | Max Concurrency |
|----------|-----------|------------------|-----------------|
| Low | 4 | 2 | 8 |
| Medium | 8 | 4 | 32 |
| High | 16 | 8 | 128 |
| Very high | 32 | 16 | 512 |

### Event Frame Encoding

Subscription event delivery uses binary `EventBatch` framing by default.

## Benchmark Results

### Pub/Sub Latency and Throughput

See **[Benchmarks](/features/benchmarks/)** for current, methodology-documented
results: latency and throughput profiles across payload sizes and fanout,
the transport levers behind them (MTU/GSO, congestion window, socket
buffers), the `core_shards` thread-per-core lever, and how to regenerate the
numbers yourself with `latency-demo`. That page is generated from the same
harness referenced throughout this guide and is kept current; numbers are
intentionally not duplicated here to avoid the two pages drifting apart.

### Cache Performance (Localhost)

**Configuration**: 8 connections, 4 streams/conn, concurrency=32

| Operation | Payload | p50 | p99 | Throughput |
|-----------|---------|-----|-----|------------|
| put | 0 B | 158 µs | 350 µs | 184k ops/sec |
| put | 256 B | 179 µs | 380 µs | 155k ops/sec |
| put | 4 KB | 260 µs | 480 µs | 78k ops/sec |
| get (hit) | 256 B | 177 µs | 360 µs | 166k ops/sec |
| get (miss) | - | 165 µs | 340 µs | 179k ops/sec |

:::note
These cache numbers predate the transport-layer tuning (MTU discovery,
congestion window, socket buffers) documented in
[Benchmarks](/features/benchmarks/). The cache path uses the same QUIC
transport and likely benefits similarly, but hasn't been re-measured
since. Treat as directional until re-run with `cache-demo`.
:::
## Profiling and Diagnostics

### Telemetry Feature

Enable detailed performance telemetry:

```toml
[dependencies]
felix-client = { version = "0.1", features = ["telemetry"] }
felix-broker = { version = "0.1", features = ["telemetry"] }
```

```yaml
# Broker config
disable_timings: false                 # Enable timing measurements
```

**Metrics collected**:

- Per-operation latency histograms (publish, subscribe, cache)
- Frame counters (publish frames, event frames, cache frames)
- Queue depth samples
- Flow control events

**Overhead**: 5-15% throughput reduction in high-load scenarios.

:::caution[Production Use]
Disable telemetry in production for maximum throughput. Enable only for profiling and debugging specific issues.
:::
### Performance Debugging

**High publish latency**:

1. Check `pub_queue_depth` and `pub_inflight_bytes` - is the queue or byte budget filling up?
2. Check `pub_workers_per_conn` (or `core_shards` if enabled) - enough parallelism?
3. Check broker CPU usage - saturated?
4. Enable telemetry - where is time spent?
5. Check `felix_broker_ingress_dropped_total` / `felix_broker_ingress_rejected_total` - is ingress shedding under `pub_ingress_wait: false`?

**High subscribe latency**:

1. Check `subscriber_queue_capacity`, `subscriber_queue_policy`, and lane drop counters - subscribers falling behind?
2. Check `event_batch_max_delay_us` - batches waiting too long under load?
3. Check QUIC flow control - windows exhausted?
4. Check subscriber processing time - bottleneck in application?
5. Check path MTU discovery (`FELIX_MTU_UPPER_BOUND`) - see [Benchmarks](/features/benchmarks/) for why this matters more than it looks.

**Low throughput**:

1. Increase `event_batch_max_events` - more aggressive batching
2. Increase connection pools - more parallelism
3. Confirm binary `EventBatch` decoding path in subscribers
4. Check network bandwidth - saturated?
5. Increase `pub_workers_per_conn` - more publish parallelism

**High memory usage**:

1. Reduce flow control windows
2. Reduce queue depths
3. Reduce connection pool sizes
4. Check for slow subscribers - filling buffers?

## Production Recommendations

### Sizing Guidelines

:::note[These bands are illustrative, not measured]
Single-broker measurements in [Benchmarks](/features/benchmarks/)
comfortably exceed the "large deployment" band below on one dev machine. Use
these YAML shapes as starting points for connection/queue sizing, not as a
throughput ceiling; run your own workload through `latency-demo` before sizing
hardware.
:::
**Small deployment**:

```yaml
pub_conn_pool: 2
event_conn_pool: 4
cache_conn_pool: 4
event_batch_max_events: 32
pub_queue_depth: 32
subscriber_queue_capacity: 64
subscriber_writer_lanes: 2
```

**Expected resources**: 2 CPU cores, 2-4 GB RAM

**Medium deployment**:

```yaml
pub_conn_pool: 4
event_conn_pool: 8
cache_conn_pool: 8
event_batch_max_events: 64
pub_queue_depth: 64
subscriber_queue_capacity: 512
subscriber_writer_lanes: 4
```

**Expected resources**: 4-8 CPU cores, 4-8 GB RAM

**Large deployment** (multi-stream, high fanout):

```yaml
pub_conn_pool: 8
event_conn_pool: 16
cache_conn_pool: 16
event_batch_max_events: 128
pub_queue_depth: 256
pub_inflight_bytes: 268435456
subscriber_queue_capacity: 4096
subscriber_writer_lanes: 8
core_shards: 4   # tune to (physical cores - 2)
```

**Expected resources**: 16-32 CPU cores, 16-32 GB RAM

### Tuning Workflow

1. **Start with balanced profile**: Use defaults
2. **Measure baseline**: Run realistic workload, measure latency/throughput
3. **Identify bottleneck**: CPU? Memory? Network? Queue depths?
4. **Tune one parameter**: Change single parameter
5. **Re-measure**: Verify improvement
6. **Iterate**: Repeat until requirements met

:::tip[Measure before and after]
Tuning without measuring tends to make things worse. Benchmark before and after each change.
:::
### Monitoring in Production

**Key metrics to track**:

- Publish rate and latency (p50, p99, p999)
- Subscribe rate and latency
- Queue depths (publish, event)
- Lane queue pressure (per-lane enqueue/drop/highwater)
- Connection count
- CPU and memory usage
- Network bandwidth
- Dropped event count
- Slow subscriber count

Alert against your own measured baseline (say, p99 above twice it) rather
than absolute numbers, because useful thresholds depend on the workload.

## Hardware Recommendations

### CPU

- **Minimum**: 2 cores
- **Recommended**: 4-8 cores for medium workloads
- **High performance**: 16-32 cores for high throughput

Felix is CPU-bound for:
- QUIC encryption (TLS 1.3 AEAD, always on)
- Wire encoding/decoding: binary by default for unacknowledged publishes
  and always for event delivery; JSON only for acked publishes and explicit
  `publish_json`/`publish_batch_json` calls
- Fanout: encoding happens once per publish batch and the encoded frame is
  shared across subscribers (not re-encoded per subscriber), so this scales
  with publish rate rather than `publish rate × fanout`

### Memory

- **Minimum**: 2 GB
- **Recommended**: 4-8 GB for medium workloads
- **High performance**: 16-32 GB for high throughput with large queues

Memory usage scales with:
- Connection pool sizes × flow control windows
- Queue depths × subscriber count
- Cache size

### Network

- **Minimum**: 1 Gbps
- **Recommended**: 10 Gbps for high throughput
- **Ideal**: 25+ Gbps for very high throughput

QUIC benefits from:
- Low latency networks (< 1 ms RTT)
- High bandwidth
- Low packet loss (< 0.1%)

### Disk

- **Ephemeral streams**: not used, so there is no disk I/O on the hot path
- **Durable streams**: NVMe SSD strongly recommended. Under `fsync_mode =
  on_commit` each commit costs one device flush (~4ms on a typical NVMe), which
  group commit amortises across concurrent publishers; under `periodic` the
  flush is off the append path entirely. Measured figures and a regression
  budget are in
  [storage-performance.md](https://github.com/GetFelix/felix/blob/main/docs/storage-performance.md).

## Summary

Start from the balanced profile, run a realistic workload, and change one
knob at a time off a measurement. Queue depths tell you where pressure is;
batching buys throughput at the price of per-message latency; pools buy
isolation at the price of memory. Leave telemetry off in production, keep
2–3× headroom above expected load, and write down what you changed and what
it measured.
