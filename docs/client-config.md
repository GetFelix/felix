# Client Config

This document describes the `felix-client` configuration options and how they are loaded.

Transport-level tuning (QUIC I/O runtimes, ACK frequency, MTU discovery, UDP
buffers) is process-wide and env-driven rather than part of `ClientConfig`;
see the "QUIC Transport Tuning" section of
[Environment Variables](../docs-site/src/content/docs/reference/environment-variables.md).
The performance-critical defaults are already on; no tuning is required.

**Running against a cluster** (seed brokers, discovery, failover, retries and
redirects) is [Running a client against a cluster](multi-node-client.md). None
of it is configured here: it is chosen by using `ClusterClient` rather than
`Client`, and by the `ReconnectPolicy` handed to it.

## Loading Order

`ClientConfig::from_env_or_yaml(quinn, config_path)` uses:
- Optimized defaults first
- Environment variables next
- YAML overrides last (if provided by `config_path` or `FELIX_CLIENT_CONFIG`)

If a YAML path is provided and cannot be read or parsed, it returns an error.

## Config File Path

Set `FELIX_CLIENT_CONFIG` or pass a path into `ClientConfig::from_env_or_yaml(...)`.

Example:
```rust
let quinn = QuinnClientConfig::with_root_certificates(roots)?;
let cfg = ClientConfig::from_env_or_yaml(quinn, Some("client.yml"))?;
```

## Options

### Publish Parallelism

- `publish_conn_pool` (env: `FELIX_PUB_CONN_POOL`)
  - Number of QUIC connections for publish lanes.
- `publish_streams_per_conn` (env: `FELIX_PUB_STREAMS_PER_CONN`)
  - Number of bidirectional publish streams per connection.
- `publish_chunk_bytes` (env: `FELIX_PUBLISH_CHUNK_BYTES`)
  - Chunk size for large publish writes.
- `publish_queue_depth` (env: `FELIX_PUBLISH_QUEUE_DEPTH`)
  - Bounded request queue per publish worker. Default: `64`.
- `publish_inflight_bytes` (env: `FELIX_PUBLISH_INFLIGHT_BYTES`)
  - Shared byte budget across all publish workers. Default: `4194304` (4 MiB).
  - Publishers wait for budget before enqueueing. A single publish larger than the
    budget waits until nothing else is in flight and then takes all of it, so it
    goes out alone; the frame cap (`max_frame_bytes`) is what bounds its size.
  - An acked publish holds its budget until the broker's ack arrives (acked
    publishes are pipelined on the stream), so the budget also caps acked
    data in flight as well as data queued for writing.
- `publish_sharding` (env: `FELIX_PUB_SHARDING`)
  - Sharding mode across publish streams.
  - Values: `rr` or `hash_stream`.
- `publish_shard_streams` (env: `FELIX_PUB_SHARD_STREAMS`)
  - Most publish streams a `ClusterClient` opens per broker for one shard
    each, beside the pool. Default: `16`. `0` turns them off.
  - Each `ClusterClient` publish goes on its shard's own stream, so a shard
    stalled on the broker holds up only itself. Opened on the shard's first
    publish and kept. Shards past the cap use the pool. Only with
    `hash_stream`. A plain `Client` ignores it: it does not know a stream's
    width, so all its publishes to a stream share one pooled stream.

### Cache Parallelism

- `cache_conn_pool` (env: `FELIX_CACHE_CONN_POOL`)
  - QUIC connection pool size for cache operations.
- `cache_streams_per_conn` (env: `FELIX_CACHE_STREAMS_PER_CONN`)
  - Streams per cache connection.

### Event (Subscribe) Parallelism

- `event_conn_pool` (env: `FELIX_EVENT_CONN_POOL`)
  - QUIC connection pool size for subscription event streams.
- `client_sub_queue_capacity` (env: `FELIX_CLIENT_SUB_QUEUE_CAPACITY`)
  - Per-subscription delivery queue depth. Default: `256`.
- `client_sub_queue_policy` (env: `FELIX_CLIENT_SUB_QUEUE_POLICY`)
  - Behavior when the delivery queue is full: `block`, `drop_new`, or `drop_old`. `drop_old`
    behaves as `drop_new`: the arriving event is dropped, not the oldest.
  - Default: `drop_new`, which bounds latency and exposes overload through dropped-event telemetry.
  - Applies to live records only. History replayed by `subscribe_from` (records below the
    subscription's `live_offset`) always waits for room, so a replay is never dropped.

### QUIC Flow Control Windows

- `event_conn_recv_window` (env: `FELIX_EVENT_CONN_RECV_WINDOW`)
- `event_stream_recv_window` (env: `FELIX_EVENT_STREAM_RECV_WINDOW`)
- `event_send_window` (env: `FELIX_EVENT_SEND_WINDOW`)
- `cache_conn_recv_window` (env: `FELIX_CACHE_CONN_RECV_WINDOW`)
- `cache_stream_recv_window` (env: `FELIX_CACHE_STREAM_RECV_WINDOW`)
- `cache_send_window` (env: `FELIX_CACHE_SEND_WINDOW`)

All window values are raw bytes.

### Safety / Limits

- `event_router_max_pending` (env: `FELIX_EVENT_ROUTER_MAX_PENDING`)
  - Max pending subscription registrations/streams held by the event router.
- `max_frame_bytes` (env: `FELIX_MAX_FRAME_BYTES`)
  - Hard cap on any single frame length.

### Cluster Client Connections

A `ClusterClient` holds one connection per broker and multiplexes every kind of
stream on it, so the `*_conn_pool` sizes above do not decide how many
connections it opens. They still decide how many publish and cache *streams*
it runs: `publish_conn_pool * publish_streams_per_conn` publish writers and
`cache_conn_pool * cache_streams_per_conn` cache workers, all placed on the
shared connection. See `docs/multi-node-client.md` ("Connections").

- `cluster_conn_pool` (env: `FELIX_CLUSTER_CONN_POOL`)
  - The most connections a cluster client opens to one broker. It starts with
    one and adds another only when the existing ones are saturated. Default: `8`.
- `cluster_streams_per_conn` (env: `FELIX_CLUSTER_STREAMS_PER_CONN`)
  - Streams one of those connections carries before another is opened beside
    it. The broker's QUIC stream credit is the other trigger, whichever comes
    first. Default: `1024`.

### Bench / Telemetry

- `bench_embed_ts` (env: `FELIX_BENCH_EMBED_TS`)
  - Enables embedding publish timestamps into payloads for end-to-end latency histograms.

## YAML Example (Optimized Defaults)

```yaml
publish_conn_pool: 4
publish_streams_per_conn: 2
publish_chunk_bytes: 16384
publish_queue_depth: 64
publish_inflight_bytes: 4194304
publish_sharding: "hash_stream"
cache_conn_pool: 8
cache_streams_per_conn: 4
event_conn_pool: 8
cluster_conn_pool: 8
cluster_streams_per_conn: 1024
client_sub_queue_capacity: 256
client_sub_queue_policy: "drop_new"
event_conn_recv_window: 268435456
event_stream_recv_window: 67108864
event_send_window: 268435456
cache_conn_recv_window: 268435456
cache_stream_recv_window: 67108864
cache_send_window: 268435456
event_router_max_pending: 16384
max_frame_bytes: 16777216
bench_embed_ts: false
```

## Notes

- Client throughput is gated by publish parallelism. A single connection with one stream will bottleneck regardless of broker tuning.
- The shared in-flight byte budget prevents additional publish streams from multiplying hidden backlog.
- `publish_sharding=hash_stream` preserves per-stream ordering while spreading across workers; `rr` is best for uniform, high-concurrency loads.
- The `hash_stream` seed is per client, so separate clients publishing the same stream land on different publish connections, and so on different broker listeners when the broker binds several (`FELIX_QUIC_LISTENERS`). Within one client a stream always uses one connection.
