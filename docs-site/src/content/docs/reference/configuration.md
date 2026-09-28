---
title: "Configuration Reference"
---

Complete reference for all Felix broker configuration options.

## Configuration Methods

Felix supports three configuration methods, applied in order (later sources override earlier):

1. **Built-in defaults**: Sensible defaults for development
2. **Environment variables**: `FELIX_*` variables for quick overrides
3. **YAML config file**: Structured configuration for production

### Precedence Example

```bash
# Default: quic_bind = 0.0.0.0:5000
# Environment: FELIX_QUIC_BIND=127.0.0.1:5001
# YAML: quic_bind: "0.0.0.0:6000"
# Result: 0.0.0.0:6000 (YAML wins)
```

## Configuration Structure

### YAML Config File

**Location priority:**

1. `$FELIX_BROKER_CONFIG` (explicit path)
2. `/usr/local/felix/config.yml` (default, optional)

**Example:**

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
controlplane_url: "http://controlplane:8443"
controlplane_sync_interval_ms: 2000
ack_on_commit: false
max_frame_bytes: 16777216
publish_queue_wait_timeout_ms: 2000
ack_wait_timeout_ms: 2000
disable_timings: false
control_stream_drain_timeout_ms: 50
cache_conn_recv_window: 268435456
cache_stream_recv_window: 67108864
cache_send_window: 268435456
event_batch_max_events: 64
event_batch_max_bytes: 65536
event_batch_max_delay_us: 250
fanout_batch_size: 64
pub_workers_per_conn: 4
pub_queue_depth: 64
pub_inflight_bytes: 67108864
pub_conn_inflight_bytes: 16777216
subscriber_queue_capacity: 512
max_subscriptions_per_conn: 4096
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 64
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
```

## Network Configuration

### `quic_bind`

**Description**: QUIC listener bind address and port.

**Type**: `SocketAddr` (IP:Port)

**Default**: `0.0.0.0:5000`

**Environment**: `FELIX_QUIC_BIND`

**Example**:
```yaml
quic_bind: "0.0.0.0:5000"
```

**Notes**:
- UDP port for QUIC transport
- Use `0.0.0.0` to listen on all interfaces
- Use `127.0.0.1` for localhost only

### `metrics_bind`

**Description**: HTTP metrics and health endpoint bind address.

**Type**: `SocketAddr` (IP:Port)

**Default**: `0.0.0.0:8080`

**Environment**: `FELIX_BROKER_METRICS_BIND`

**Example**:
```yaml
metrics_bind: "0.0.0.0:8080"
```

**Endpoints**:
- `/healthz`: Health check
- `/metrics`: Prometheus metrics (if enabled)

## Control Plane Configuration

### `controlplane_url`

**Description**: Optional control plane base URL for metadata sync.

**Type**: `String` (URL)

**Default**: `None`

**Environment**: `FELIX_CONTROLPLANE_URL`

**Example**:
```yaml
controlplane_url: "http://felix-controlplane:8443"
```

**Notes**:
- Optional for single-node deployments
- Required for multi-node clusters
- Should include scheme (`http://` or `https://`)

### `controlplane_sync_interval_ms`

**Description**: Interval for polling control plane changes.

**Type**: `u64` (milliseconds)

**Default**: `2000`

**Environment**: `FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`

**Example**:
```yaml
controlplane_sync_interval_ms: 2000
```

**Recommendations**:
- **Fast changes**: `500-1000ms`
- **Normal operation**: `2000-5000ms`
- **Stable clusters**: `5000-10000ms`

## Publishing Configuration

### `ack_on_commit`

**Description**: Send acknowledgements after message commit.

**Type**: `bool`

**Default**: `false`

**Environment**: `FELIX_ACK_ON_COMMIT` (`1`, `true`, `yes` = enabled)

**Example**:
```yaml
ack_on_commit: true
```

**Trade-offs**:
- **`false`**: Lower latency. The ack is sent when the publish is queued, before
  it is written, so an acknowledged record can still be lost: if the broker
  crashes before the write, or if a pause outlasts the broker's cluster lease
  (the write is then refused, since another broker may lead the shard). Near
  the end of the lease a publish waits for its write anyway, so a lapse is
  reported rather than lost. Losses after an ack are counted in
  `felix_broker_acked_publishes_dropped_total`.
- **`true`**: Higher latency. The ack means the write happened, under the
  stream's fsync policy. `Quorum` streams and forwarded or idempotent publishes
  always wait for the write, whatever this says.

### `max_frame_bytes`

**Description**: Maximum frame size accepted on QUIC streams.

**Type**: `usize` (bytes)

**Default**: `16777216` (16 MiB)

**Environment**: `FELIX_MAX_FRAME_BYTES`

**Example**:
```yaml
max_frame_bytes: 16777216
```

**Notes**:
- Limits individual message size
- Buffers grow as payload bytes arrive, so a header alone does not reserve this much
- Must match client expectations

### `preauth_max_frame_bytes`

**Description**: Maximum frame size on a stream that has not yet authenticated.

**Type**: `usize` (bytes)

**Default**: `65536` (64 KiB)

**Environment**: `FELIX_PREAUTH_MAX_FRAME_BYTES`

### `preauth_max_streams_per_conn`

**Description**: Unauthenticated streams one connection may have reading at once; more wait.

**Type**: `usize`

**Default**: `16`

**Environment**: `FELIX_PREAUTH_MAX_STREAMS_PER_CONN`

### `auth_timeout_ms`

**Description**: How long a client connection has to authenticate a stream before it is
closed. `0` disables the deadline.

**Type**: `u64` (milliseconds)

**Default**: `10000`

**Environment**: `FELIX_AUTH_TIMEOUT_MS`

### `max_client_connections`

**Description**: Client QUIC connections held at once across all client listeners. Attempts
past it are refused before the handshake.

**Type**: `usize`

**Default**: `8192`

**Environment**: `FELIX_MAX_CLIENT_CONNECTIONS`

### `publish_queue_wait_timeout_ms`

**Description**: How long a commit-acked publish waits for room in the publish
queue (and the byte budget) before it is answered busy.

**Type**: `u64` (milliseconds)

**Default**: `2000`

**Environment**: `FELIX_PUBLISH_QUEUE_WAIT_MS`

**Example**:
```yaml
publish_queue_wait_timeout_ms: 2000
```

**Behavior**:
- A commit-acked publish waits for room if the queue is full, then is answered
  `overloaded` (`publish_queue_full`, retryable)
- Returns error after timeout
- Prevents unbounded memory growth

### `ack_wait_timeout_ms`

**Description**: Maximum time to wait for ack-on-commit completion.

**Type**: `u64` (milliseconds)

**Default**: `2000`

**Environment**: `FELIX_ACK_WAIT_TIMEOUT_MS`

**Example**:
```yaml
ack_wait_timeout_ms: 2000
```

**Notes**:
- Only applies when `ack_on_commit: true`
- Publisher receives error if exceeded

## Event Delivery Configuration

### `event_batch_max_events`

**Description**: Maximum events per batched subscription frame.

**Type**: `usize` (count)

**Default**: `64`

**Environment**: `FELIX_EVENT_BATCH_MAX_EVENTS`

**Example**:
```yaml
event_batch_max_events: 64
```

**Tuning**:
- **Low latency**: `1-16`
- **Balanced**: `32-64`
- **High throughput**: `128-256`

### `event_batch_max_bytes`

**Description**: Maximum bytes per batched subscription frame.

**Type**: `usize` (bytes)

**Default**: `65536` (64 KiB)

**Environment**: `FELIX_EVENT_BATCH_MAX_BYTES`

**Example**:
```yaml
event_batch_max_bytes: 65536
```

**Notes**:
- Whichever limit hits first triggers batch send
- Consider payload size when tuning

### `event_batch_max_delay_us`

**Description**: Maximum delay before flushing a subscription batch.

**Type**: `u64` (microseconds)

**Default**: `250`

**Environment**: `FELIX_EVENT_BATCH_MAX_DELAY_US`

**Example**:
```yaml
event_batch_max_delay_us: 250
```

**Tuning**:
- **Ultra-low latency**: `50-100us`
- **Balanced**: `250-500us`
- **High throughput**: `1000-5000us`

:::caution[Latency Impact]
Lower values reduce latency but may decrease throughput. Higher values improve batching efficiency but increase tail latency.
:::
### `fanout_batch_size`

**Description**: Publish-side fanout batching hint and subscribe delivery batch cap.

**Type**: `usize` (count)

**Default**: `64`

**Environment**: `FELIX_FANOUT_BATCH`

**Example**:
```yaml
fanout_batch_size: 64
```

**Recommendations**:
- **Low fanout (1-10)**: `16-32`
- **Medium fanout (10-100)**: `64-128`
- **High fanout (100+)**: `128-256`

### Event Frame Encoding

Subscription event delivery uses binary `EventBatch` frames.

### Outbound Writer Lanes

Broker outbound subscribe delivery uses lane-sharded writer tasks to reduce contention under
high fanout / large payload workloads.

#### `subscriber_queue_capacity`

**Description**: Per-subscriber queue capacity in broker core (drop-on-full boundary).

**Type**: `usize` (count)

**Default**: `512`

**Environment**: `FELIX_SUBSCRIBER_QUEUE_CAPACITY` (alias: `FELIX_SUB_QUEUE_CAPACITY`)

```yaml
subscriber_queue_capacity: 512
```

#### `max_subscriptions_per_conn`

**Description**: Max concurrent subscriptions a single QUIC connection may hold. `subscriber_queue_capacity` bounds the size of *one* subscription's buffer; this bounds *how many* subscriptions one connection can open in total, which is otherwise unbounded — a connection issuing unlimited `Subscribe` requests could grow broker memory without limit.

**Type**: `usize` (count)

**Default**: `4096`

**Environment**: `FELIX_MAX_SUBSCRIPTIONS_PER_CONN`

```yaml
max_subscriptions_per_conn: 4096
```

#### `subscriber_queue_policy`

**Description**: Backpressure policy when a subscriber's broker-core queue (`subscriber_queue_capacity`) is full — this is the fanout enqueue path, upstream of the writer lanes below.

**Type**: `enum` (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

**Environment**: `FELIX_SUB_QUEUE_POLICY`

```yaml
subscriber_queue_policy: drop_new
```

**Tuning**:
- `drop_new` (default): sheds the newest event when the queue is full — bounds latency, overload becomes visible as drops (`felix_subscribe_dropped_total`).
- `block`: publish waits for queue space — strongest delivery guarantee, but a single slow subscriber can add latency to publishers. Used by the benchmark harness's lossless throughput mode alongside `pub_ingress_wait`.
- `drop_old`: currently emulated with `drop_new` semantics; tracked separately in metrics.

#### `subscriber_writer_lanes`

**Description**: Requested number of outbound writer lanes.

**Type**: `usize` (count)

**Default**: `4`

**Environment**: `FELIX_SUB_WRITER_LANES`

```yaml
subscriber_writer_lanes: 4
```

#### `subscriber_lane_queue_depth`

**Description**: Bounded command queue depth per writer lane.

**Type**: `usize` (count)

**Default**: `64`

**Environment**: `FELIX_SUB_LANE_QUEUE_DEPTH` (alias: `FELIX_SUB_QUEUE_BOUND`)

```yaml
subscriber_lane_queue_depth: 64
```

#### `subscriber_lane_queue_policy`

**Description**: Backpressure policy for the writer-lane command queue (downstream of `subscriber_queue_policy`; gates the actual QUIC write).

**Type**: `enum` (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

**Environment**: `FELIX_SUB_QUEUE_MODE` (alias: `FELIX_SUB_LANE_QUEUE_POLICY`)

```yaml
subscriber_lane_queue_policy: drop_new
```

Same semantics as `subscriber_queue_policy`, applied one stage later in the pipeline. Control commands (subscriber register/unregister) always use blocking send regardless of this setting.

#### `max_subscriber_writer_lanes`

**Description**: Safety clamp for `subscriber_writer_lanes` to avoid oversubscription regressions.

**Type**: `usize` (count)

**Default**: `8`

**Environment**: `FELIX_MAX_SUB_WRITER_LANES`

```yaml
max_subscriber_writer_lanes: 8
```

#### `subscriber_lane_shard`

**Description**: Lane assignment policy for subscriber outbound writes.

**Type**: `enum` (`auto`, `subscriber_id_hash`, `connection_id_hash`, `round_robin_pin`)

**Default**: `auto`

**Environment**: `FELIX_SUB_LANE_SHARD`

```yaml
subscriber_lane_shard: auto
```

Policy guidance:
- `auto`: Prefer connection-aware routing when connection id is available; fallback to subscriber id.
- `subscriber_id_hash`: Good general distribution independent of connection topology.
- `connection_id_hash`: Useful when many subscribers share connections and connection-local contention dominates.
- `round_robin_pin`: Pins lane at subscribe-time; preserves ordering, but can underperform in skewed workloads.

#### `subscriber_single_writer_per_conn`

**Description**: If true, route all subscribers on the same QUIC connection to one writer lane (serializes writes per connection instead of spreading them across lanes).

**Type**: `bool`

**Default**: `false`

**Environment**: `FELIX_SUB_SINGLE_WRITER_PER_CONN` (`1`, `true`, `yes` = enabled)

```yaml
subscriber_single_writer_per_conn: false
```

**Tuning**: The latency-focused benchmark profile (batch = 1) enables this for stable per-message ordering; the throughput profile leaves it off to use parallel lanes.

#### `subscriber_flush_max_items`

**Description**: Maximum queued lane commands drained per flush before a write is issued.

**Type**: `usize` (count)

**Default**: `16`

**Environment**: `FELIX_SUB_FLUSH_MAX_ITEMS`

```yaml
subscriber_flush_max_items: 16
```

#### `subscriber_flush_max_delay_us`

**Description**: Maximum time spent waiting to fill a lane flush buffer before writing what's accumulated.

**Type**: `u64` (microseconds)

**Default**: `50`

**Environment**: `FELIX_SUB_FLUSH_MAX_DELAY_US`

```yaml
subscriber_flush_max_delay_us: 50
```

#### `subscriber_max_bytes_per_write`

**Description**: Upper bound on coalesced bytes per QUIC write call to a subscriber stream.

**Type**: `usize` (bytes)

**Default**: `65536` (64 KiB)

**Environment**: `FELIX_SUB_MAX_BYTES_PER_WRITE`

```yaml
subscriber_max_bytes_per_write: 65536
```

### Delivery Stream Topology

#### `sub_streams_per_conn`

**Description**: Number of delivery streams to use per connection in hashed-pool mode (`sub_stream_mode: hashed_pool`).

**Type**: `usize` (count)

**Default**: `4`

**Environment**: `FELIX_SUB_STREAMS_PER_CONN`

```yaml
sub_streams_per_conn: 4
```

#### `sub_stream_mode`

**Description**: Strategy for mapping subscribers to event streams.

**Type**: `enum` (`per_subscriber`, `hashed_pool`)

**Default**: `per_subscriber`

**Environment**: `FELIX_SUB_STREAM_MODE`

```yaml
sub_stream_mode: per_subscriber
```

**Notes**: `hashed_pool` is not yet enabled — the broker currently falls back to `per_subscriber` and logs a debug warning if `hashed_pool` is requested.

## Cache Configuration

### `cache_conn_recv_window`

**Description**: Cache connection flow-control receive window.

**Type**: `u64` (bytes)

**Default**: `268435456` (256 MiB)

**Environment**: `FELIX_CACHE_CONN_RECV_WINDOW`

**Example**:
```yaml
cache_conn_recv_window: 268435456
```

**Notes**:
- Per-connection receive credit
- Multiplied by connection pool size
- Affects burst tolerance

### `cache_stream_recv_window`

**Description**: Cache stream flow-control receive window.

**Type**: `u64` (bytes)

**Default**: `67108864` (64 MiB)

**Environment**: `FELIX_CACHE_STREAM_RECV_WINDOW`

**Example**:
```yaml
cache_stream_recv_window: 67108864
```

**Notes**:
- Per-stream receive credit
- Multiplied by streams per connection
- Total credit = `stream_window × streams_per_conn × conn_pool`

### `cache_send_window`

**Description**: Cache connection send window.

**Type**: `u64` (bytes)

**Default**: `268435456` (256 MiB)

**Environment**: `FELIX_CACHE_SEND_WINDOW`

**Example**:
```yaml
cache_send_window: 268435456
```

**Notes**:
- Per-connection send credit
- Affects concurrent request throughput

## Worker and Queue Configuration

### `pub_workers_per_conn`

**Description**: Executors of the broker's process-wide publish scheduler (the
name predates it): how many shards' ordered publish steps may run at once.

**Type**: `usize` (count)

**Default**: `4`

**Environment**: `FELIX_BROKER_PUB_WORKERS_PER_CONN`

**Example**:
```yaml
pub_workers_per_conn: 4
```

**Notes**:
- Each shard is an ordered lane that runs one publish at a time, so more
  executors help only when publishes spread over several shards.
- A device flush, a forward to another broker and a quorum wait do not hold an
  executor, so a slow shard or peer does not use one up.
- With `core_shards` set, each core shard gets this many executors.

### `pub_queue_depth`

**Description**: Publish jobs each tenant is guaranteed room for in the
publish queue. The queue holds `pub_queue_depth × pub_workers_per_conn` jobs.

**Type**: `usize` (count)

**Default**: `64`

**Environment**: `FELIX_BROKER_PUB_QUEUE_DEPTH`

**Example**:
```yaml
pub_queue_depth: 64
```

**Tuning**:
- Tenants are served by deficit round robin, weighted by bytes; a tenant past
  its share may borrow idle room but never the last `pub_queue_depth` slots.
- A publish that finds no room is answered `overloaded`
  (`detail.reason = "publish_queue_full"`, retryable) if it asked for an ack,
  and shed otherwise; both are counted in `felix_tenant_publish_queue_full_total`.
- Larger values allow more buffering under burst but increase saturation latency
- Consider with `publish_queue_wait_timeout_ms`

### `pub_inflight_bytes`

**Description**: Shared in-flight publish byte budget across all publish workers (process-wide, not per-connection).

**Type**: `usize` (bytes)

**Default**: `67108864` (64 MiB)

**Environment**: `FELIX_BROKER_PUBLISH_INFLIGHT_BYTES`

**Example**:
```yaml
pub_inflight_bytes: 67108864
```

**Tuning**:
- `pub_queue_depth` bounds the number of queued *jobs*, but a job's payload can be as large as `max_frame_bytes`; `pub_inflight_bytes` bounds actual queued-or-processing *bytes* regardless of item count.
- The budget is acquired before a job is handed to a worker queue and released only once the job finishes processing, so it reflects real resident memory, not just admission-time bytes.
- Should be set well above `max_frame_bytes` — a job larger than the remaining budget waits (and can time out under `EnqueuePolicy::Wait`) rather than being admitted.
- Lower this to shrink worst-case ingress memory under large-payload workloads; raise it to allow more large batches in flight concurrently.

### `pub_conn_inflight_bytes`

**Description**: Per-connection share of `pub_inflight_bytes`. `pub_inflight_bytes` is intentionally process-wide (see its description above), which on its own means nothing stops one connection from occupying the entire shared budget. `pub_conn_inflight_bytes` closes that gap: it's a second, independent byte-budget gate sized per connection, checked before the shared budget on every publish admission.

**Type**: `usize` (bytes)

**Default**: `16777216` (16 MiB)

**Environment**: `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES`

**Example**:
```yaml
pub_conn_inflight_bytes: 16777216
```

**Tuning**:
- Must be smaller than `pub_inflight_bytes` to have any effect; setting it equal to or above `pub_inflight_bytes` means a single connection can once again claim the whole shared budget.
- Roughly `pub_inflight_bytes / N` for the expected number of concurrently active connections gives each a fair share while still allowing the shared budget to absorb bursts from fewer connections.

### `publish_window`

**Description**: The most acknowledged publishes one connection may have unanswered when its client pipelines them. Granted to a client that offers `FEATURE_PUBLISH_PIPELINE`, in `AuthOk.publish_window`. The broker answers that client's publishes on each stream in the order they were sent, and stops reading the connection's publishes while this many are outstanding, so the client is slowed by QUIC flow control instead of being refused.

**Type**: `u32`

**Default**: `256`

**Environment**: `FELIX_BROKER_PUBLISH_WINDOW`

**Example**:
```yaml
publish_window: 256
```

**Tuning**:
- `0` turns pipelining off; every client gets completion-order acks and no window.
- An idempotent producer keeps at most 64 batches in flight whatever this says, because a leader remembers 64 sequences per producer and a re-send has to find its batch remembered.

### `pub_ingress_wait`

**Description**: When true, un-acked (fire-and-forget) publishes wait — bounded by `publish_queue_wait_timeout_ms` — for ingress capacity instead of being shed when the publish queue or byte budget is full.

**Type**: `bool`

**Default**: `false`

**Environment**: `FELIX_PUB_INGRESS_WAIT`

**Example**:
```yaml
pub_ingress_wait: true
```

**Tuning**:
- Off (default): overload sheds fire-and-forget publishes visibly (`felix_broker_ingress_dropped_total`) and keeps latency bounded.
- On: backpressure propagates through QUIC flow control to the publisher — nothing is shed, producers slow down. Use for lossless pipelines and sustainable-throughput benchmarking.

### `core_shards`

**Description**: Number of core-pinned shard executors owning stream work (thread-per-core). Each stream's handle id deterministically selects an owning shard; that shard runs the stream's publish worker and its subscriptions' lane feeders on one dedicated single-threaded runtime (pinned to a CPU core on Linux). Publish append, fanout enqueue, and subscriber dequeue all stay core-local.

**Type**: `usize` (count; `0` = disabled)

**Default**: `0`

**Environment**: `FELIX_CORE_SHARDS`

**Example**:
```yaml
core_shards: 4
```

**Tuning**:
- When enabled, the publish queue is split per shard: each shard has its own queue and `pub_workers_per_conn` executors on its core, and a stream's publishes run on the shard that owns it.
- Benefits scale with stream count: workloads spread across many streams gain parallel, contention-free per-core pipelines (measured +34% delivered throughput at 4 streams × 4 shards, unpinned). Single-stream workloads serialize on one shard by design — neutral to mildly positive.
- Core pinning requires Linux (`sched_setaffinity`); elsewhere shards still get dedicated threads, preserving the single-writer ownership model without hard affinity.
- Reasonable starting point: number of physical cores minus 2 (leaving headroom for QUIC I/O on the main runtime).

## Performance Configuration

### `disable_timings`

**Description**: Disable per-stage timing collection for lower overhead.

**Type**: `bool`

**Default**: `false`

**Environment**: `FELIX_DISABLE_TIMINGS` (`1`, `true`, `yes` = disabled)

**Example**:
```yaml
disable_timings: true
```

**Trade-offs**:
- **`false`**: Detailed latency metrics, slight overhead
- **`true`**: Maximum performance, no per-stage timings

**Recommendations**:
- Development: `false` (debug performance)
- Production low-load: `false` (observability)
- Production high-load: `true` (reduce overhead)

### `control_stream_drain_timeout_ms`

**Description**: Maximum time to wait for control-stream writer to drain.

**Type**: `u64` (milliseconds)

**Default**: `50`

**Environment**: `FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS`

**Example**:
```yaml
control_stream_drain_timeout_ms: 50
```

**Notes**:
- Affects graceful connection shutdown
- Balance between responsiveness and reliability

## Client-Side Configuration

While this reference covers broker configuration, clients also have tunable parameters:

### Event Connection Pool

**Environment**: `FELIX_EVENT_CONN_POOL`

**Default**: `8`

**Description**: Number of QUIC connections in the event pool.

### Cluster Client Connections

**Environment**: `FELIX_CLUSTER_CONN_POOL`, `FELIX_CLUSTER_STREAMS_PER_CONN`

**Default**: `8`, `1024`

**Description**: A `ClusterClient` holds one connection per broker and multiplexes every stream on it. The first is the most connections it will open to one broker; the second is how many streams a connection carries before another opens. The connection pool sizes here apply to a `Client` built with `Client::connect`.

### Cache Connection Pool

**Environment**: `FELIX_CACHE_CONN_POOL`

**Default**: `8`

**Description**: Number of QUIC connections for cache operations.

### Cache Streams Per Connection

**Environment**: `FELIX_CACHE_STREAMS_PER_CONN`

**Default**: `4`

**Description**: Concurrent cache streams per connection.

### Publish Chunk Bytes

**Environment**: `FELIX_PUBLISH_CHUNK_BYTES`

**Default**: `16384` (16 KiB)

**Description**: Chunk size for publishing large messages.

## Configuration Validation

Felix validates configuration at startup:

```bash
# Test configuration
cargo run --release -p felix-broker-service -- --dry-run

# Explicit config file
FELIX_BROKER_CONFIG=/path/to/config.yml cargo run --release -p felix-broker-service
```

**Common validation errors**:

- Invalid socket address format
- Negative or zero values where positive required
- Conflicting settings

## Performance Profiles

### Low Latency (p50-optimized)

```yaml
event_batch_max_events: 1
event_batch_max_delay_us: 50
fanout_batch_size: 16
subscriber_writer_lanes: 2
subscriber_lane_shard: auto
subscriber_queue_capacity: 64
disable_timings: true
```

### Balanced (recommended)

```yaml
event_batch_max_events: 64
event_batch_max_bytes: 65536
event_batch_max_delay_us: 250
fanout_batch_size: 64
subscriber_writer_lanes: 4
subscriber_lane_shard: auto
subscriber_queue_capacity: 512
disable_timings: false
cache_conn_recv_window: 268435456
```

### High Throughput (batch-optimized)

```yaml
event_batch_max_events: 256
event_batch_max_bytes: 1048576
event_batch_max_delay_us: 1000
fanout_batch_size: 128
subscriber_writer_lanes: 8
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
subscriber_queue_capacity: 4096
pub_ingress_wait: true
core_shards: 4
disable_timings: true
```

### High Memory (burst-tolerant)

```yaml
cache_conn_recv_window: 536870912
cache_stream_recv_window: 134217728
cache_send_window: 536870912
subscriber_queue_capacity: 2048
subscriber_lane_queue_depth: 16384
pub_queue_depth: 2048
```

## Configuration Examples

### Development

```yaml
quic_bind: "127.0.0.1:5000"
metrics_bind: "127.0.0.1:8080"
disable_timings: false
event_batch_max_events: 32
```

### Production Single-Node

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
ack_on_commit: true
disable_timings: true
event_batch_max_events: 64
event_batch_max_delay_us: 250
subscriber_writer_lanes: 4
subscriber_lane_shard: auto
cache_conn_recv_window: 268435456
```

### Production Cluster

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
controlplane_url: "http://felix-controlplane:8443"
controlplane_sync_interval_ms: 2000
ack_on_commit: true
disable_timings: true
event_batch_max_events: 128
event_batch_max_bytes: 262144
fanout_batch_size: 128
subscriber_writer_lanes: 4
subscriber_lane_shard: auto
```

## Next Steps

- **Environment variables reference**: [Environment Variables](/felix/reference/environment-variables/)
- **Troubleshooting issues**: [Troubleshooting Guide](/felix/reference/troubleshooting/)
- **Performance tuning**: [Performance Guide](/felix/features/performance/)
