# Broker Config Examples

This document shows sample `BrokerConfig` YAML files for common performance goals.

The broker attempts to load a YAML file from:
- `FELIX_BROKER_CONFIG` if set
- otherwise `/usr/local/felix/config.yml`

All fields are optional. Omitted values fall back to defaults. Every field also
has an environment variable override — see
[Environment Variables](../docs-site/src/content/docs/reference/environment-variables.md)
for the full `FELIX_*` mapping, including legacy aliases.

Defaults favor bounded latency and visible overload over unbounded buffering:
subscriber queues default to `drop_new` (shed and count, rather than block or
grow), and ingress sheds fire-and-forget publishes under sustained overload
unless `pub_ingress_wait` is enabled.

## General Purpose

Balanced settings for mixed workloads and moderate fanout — these are the
built-in defaults, shown explicitly.

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
controlplane_url: null
controlplane_sync_interval_ms: 2000
ack_on_commit: false
max_frame_bytes: 16777216
preauth_max_frame_bytes: 65536
preauth_max_streams_per_conn: 16
auth_timeout_ms: 10000
max_client_connections: 8192
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
pub_ingress_wait: false
core_shards: 0
subscriber_queue_capacity: 512
max_subscriptions_per_conn: 4096
subscriber_queue_policy: drop_new
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 64
subscriber_lane_queue_policy: drop_new
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
subscriber_single_writer_per_conn: false
subscriber_flush_max_items: 16
subscriber_flush_max_delay_us: 50
subscriber_max_bytes_per_write: 65536
sub_streams_per_conn: 4
sub_stream_mode: per_subscriber
```

## Latency Optimized

Targets low tail latency for small payloads and limited batching. Matches the
`latency-demo` harness's latency profile (batch = 1): blocking queues so no
event is silently dropped, immediate flush, single writer per connection for
stable per-message ordering.

```yaml
ack_on_commit: true # NOTE: serializes commit/ack; reduces pipeline parallelism
publish_queue_wait_timeout_ms: 1000
ack_wait_timeout_ms: 1000
control_stream_drain_timeout_ms: 25
event_batch_max_events: 8
event_batch_max_delay_us: 100
fanout_batch_size: 8
pub_workers_per_conn: 2
pub_queue_depth: 32
subscriber_queue_capacity: 64
subscriber_queue_policy: block
subscriber_writer_lanes: 2
subscriber_lane_queue_depth: 32
subscriber_lane_queue_policy: block
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
subscriber_single_writer_per_conn: true
subscriber_flush_max_items: 1
subscriber_flush_max_delay_us: 0
subscriber_max_bytes_per_write: 65536
```

## Throughput Optimized

Targets max lossless publish/delivery throughput with higher batching.
Matches the `latency-demo` harness's throughput profile (batch > 1):
blocking queues plus `pub_ingress_wait` so the publisher is paced to the
pipeline's sustainable rate instead of shedding — every message delivered,
`delivery drops 0`.

```yaml
ack_on_commit: false
publish_queue_wait_timeout_ms: 60000
ack_wait_timeout_ms: 3000
control_stream_drain_timeout_ms: 100
event_batch_max_events: 256
event_batch_max_bytes: 1048576
event_batch_max_delay_us: 2000
fanout_batch_size: 256
pub_workers_per_conn: 8
pub_queue_depth: 256
pub_inflight_bytes: 268435456
pub_ingress_wait: true
subscriber_queue_capacity: 4096
subscriber_queue_policy: block
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 1024
subscriber_lane_queue_policy: block
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto
subscriber_single_writer_per_conn: false
subscriber_flush_max_items: 16
subscriber_flush_max_delay_us: 50
subscriber_max_bytes_per_write: 65536
```

!!! note
    `publish_queue_wait_timeout_ms` bounds *acked* publishes only. With
    `pub_ingress_wait: true`, un-acked publishes use unbounded, cancellable
    backpressure instead: there is no ack channel on which a timeout could be
    reported, so a bounded wait there could only end in a silent drop. Such a
    publish waits for capacity and is abandoned only if the connection is torn
    down.

!!! note
    Lossless pacing (`block` queues + `pub_ingress_wait: true`) is a
    deliberate opt-in for pipelines that cannot tolerate drops, or for
    benchmarking sustainable throughput. Production defaults favor shedding
    (`drop_new`) so overload stays visible and bounded instead of building
    unbounded backlog under a slow subscriber. See
    [Benchmarks](../docs-site/src/content/docs/features/benchmarks.md#saturation-behavior).

## Multi-Core Scaling

`core_shards` runs each stream's publish worker and subscription lane feeders
on a dedicated, core-pinned (Linux) single-threaded runtime, selected
deterministically by stream handle id. This keeps a stream's fanout enqueue
and dequeue on one core instead of bouncing across tokio's work-stealing
pool. Off by default (`0`); benefits scale with stream count and are
neutral-to-positive for single-stream workloads.

```yaml
core_shards: 4 # e.g. physical cores - 2, leaving headroom for QUIC I/O
```

See [Benchmarks](../docs-site/src/content/docs/features/benchmarks.md) for measured
results.

## Client-Side Parallelism (Important)

Broker throughput and fanout scalability depend on client publish parallelism. For
high throughput or fanout workloads:

- Use multiple QUIC connections
- Use multiple publish streams per connection
- Use round-robin (RR) or hash-based sharding across streams

A single connection with a single publish stream will bottleneck regardless of broker tuning.

## Worker Sizing (Important)

> IMPORTANT: `pub_workers_per_conn` should not exceed the number of active publish streams.
> Excess workers increase contention and can worsen tail latency.

> When `core_shards > 0`, the shard count replaces `pub_workers_per_conn` as
> the publish worker count (one worker per shard, so each stream has a single
> owning core). `pub_workers_per_conn` is ignored in that mode.

## TLS

TLS is configured through the environment only, not the YAML file: the paths
are deployment wiring (a mounted Secret), not tuning.

### Client TLS

The QUIC listeners, and the Kafka listener when `FELIX_KAFKA_TLS` is on, serve
one certificate:

| Variable | Effect |
| --- | --- |
| `FELIX_TLS_CERT`, `FELIX_TLS_KEY` | PEM chain (leaf first) and key to serve. Both or neither. Re-read every 30s; a renewal over the same paths reaches the next handshake, and open connections keep theirs. |
| `FELIX_TLS_CLIENT_CA` | Clients must present a certificate chaining to this bundle. Needs the two above. Read once. |
| `FELIX_TLS_REQUIRE_CERT=true` | Refuse to start without `FELIX_TLS_CERT`. Set it in production. |
| `FELIX_TLS_REQUIRE_ALPN=true` | Refuse QUIC clients that offer no ALPN. By default they are served; clients offering `felix/1` get it, and a client offering only other protocols is refused. The shipped clients offer `felix/1` only when configured to (see [ALPN](protocol.md#alpn)), so turn that on in every client first. |
| `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true` | A client presenting a certificate must use a token whose `sub` the certificate was issued to: a URI SAN `felix:principal:<sub>` (exact match), or a DNS or IP SAN the `sub` matches. Control-plane tokens carry a 64-hex principal id, too long for a DNS label, so their certificates need the URI SAN. Needs `FELIX_TLS_CLIENT_CA`. QUIC listeners only; with the Kafka listener on, startup warns that it does not bind (its SASL path has no access to the peer certificate). |
| `FELIX_TLS_CERT_EXPORT` | Write the generated development certificate out for clients to trust. Refused together with `FELIX_TLS_CERT`. |

Without `FELIX_TLS_CERT` the broker generates a self-signed certificate for
`localhost` at every start and logs a warning: clients can encrypt to it but
cannot tell which broker they reached. The certificate you configure must
carry every name clients dial, typically the load-balanced Service name and
each broker's `FELIX_CLIENT_ADVERTISE_ADDR` host (a wildcard over the headless
Service covers the per-broker names in Kubernetes). Clients verify it with the
issuing CA and that server name: `ClusterClient::connect(seeds, server_name, ..)`
in Rust, `ca_file=`/`server_name=` in Python and `caFile`/`serverName` in
TypeScript.

### Peer mTLS

A broker with `FELIX_NODE_ID` binds the internal listener, and refuses to
start unless `FELIX_INTERNAL_TLS_CERT`, `_KEY` and `_CA` are set or
`FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`. The rule is keyed on cluster
membership, not replication factor: replication factors arrive from the
control plane after the listener is up, and even an RF=1 cluster forwards
writes over that port. See [internal-protocol.md](internal-protocol.md).

### Control-plane CA

With an `https://` `FELIX_CONTROLPLANE_URL`, the broker verifies the control
plane against the public roots plus `FELIX_CONTROLPLANE_CA` when set. Setting
the CA with an `http://` URL fails startup.

## Connection Limits and Tenant Quotas

These are environment-only (no YAML keys), and every one is per broker. The
full table, and what a client sees when a limit refuses it, is in the
[environment variable reference](../docs-site/src/content/docs/reference/environment-variables.md#connection-limits-and-tenant-quotas).

```bash
# One host may hold at most 512 client connections (the default).
FELIX_MAX_CONNECTIONS_PER_IP=512
# Every tenant: 50 MiB/s and 20k messages/s, with a one-second burst.
FELIX_TENANT_PUBLISH_BYTES_PER_SEC=52428800
FELIX_TENANT_PUBLISH_MSGS_PER_SEC=20000
FELIX_TENANT_PUBLISH_BURST_MS=1000
# Except these two: `acme` gets more bytes, `batch` is unlimited.
FELIX_TENANT_PUBLISH_QUOTAS=acme:209715200:20000,batch:0:0
```

- The per-address cap exists so one host cannot hold every connection a broker
  can serve. Set it high enough for the NAT gateways and connection pools in
  front of you; a client that pools connections uses several per broker.
- A tenant's quota covers its QUIC and Kafka publishes together. It is checked
  before any shared byte budget is taken, so a tenant over its quota holds none
  of the ingress memory other tenants need.
- Per-tenant traffic is in `felix_tenant_published_*`, `felix_tenant_delivered_*`
  and `felix_tenant_publish_throttled_total`, labelled by tenant up to
  `FELIX_TENANT_METRICS_MAX` distinct tenants.

## Notes

- All byte values are raw bytes; use powers of two for MiB values (e.g., 1048576 = 1 MiB).
- Tune `pub_workers_per_conn` and `pub_queue_depth` together; deep queues trade latency for throughput.
- Increasing `pub_workers_per_conn` only helps if publish load is spread across multiple streams or
  connections. Oversubscribing workers relative to streams can degrade performance.
- `pub_inflight_bytes` bounds actual queued-or-processing publish *bytes*, independent of
  `pub_queue_depth`'s item count — a handful of large batches can't blow past the ingress
  memory budget even with a small queue depth.
- `pub_conn_inflight_bytes` is a per-connection share of `pub_inflight_bytes`: it bounds how
  much of the shared budget a single connection can occupy, so one connection publishing large
  batches can't starve every other connection's admission. Must be smaller than
  `pub_inflight_bytes` to have any effect; a value equal to or larger than it degenerates to
  "no per-connection cap."
- The pre-auth limits bound what a client costs before it proves who it is. Until a stream's
  `Auth` succeeds its frames are capped at `preauth_max_frame_bytes` (only an `Auth` has to
  fit), and at most `preauth_max_streams_per_conn` unauthenticated streams per connection are
  read at once; more wait their turn. A connection that has authenticated no stream within
  `auth_timeout_ms` is closed with application code `1` (`0` disables the deadline).
  `max_client_connections` caps client QUIC connections across all client listeners; attempts
  past it are refused before the handshake. Frame buffers grow as payload bytes arrive, so a
  frame header alone never reserves `max_frame_bytes`.
- `max_subscriptions_per_conn` bounds how many concurrent subscriptions a single QUIC connection
  may hold, independent of `subscriber_queue_capacity`. It protects broker memory from a
  connection that opens unbounded subscriptions rather than bounding any one subscription's
  buffer size.
- `subscriber_queue_capacity` and `subscriber_queue_policy` control broker-core per-subscriber
  buffering and drop behavior (the fanout enqueue path); `subscriber_lane_queue_depth` and
  `subscriber_lane_queue_policy` control the writer-lane stage one hop later (the actual QUIC
  write). Both default to `drop_new`.
- `subscriber_writer_lanes` and `subscriber_lane_shard` control outbound event write parallelism.
- `subscriber_lane_shard: auto` is the default and is usually the best starting point.
- Lanes often help high fanout + large payload workloads, but gains can plateau; do not assume
  that more than 8 lanes will improve performance.
- Event delivery uses binary `EventBatch` (or shared `EventBatch` — see
  [Wire Protocol](../docs-site/src/content/docs/architecture/wire-protocol.md)) frames. Unacknowledged
  client publishes are binary-encoded by default; acked publishes currently use the JSON
  control encoding (`Publisher::publish_json`/`publish_batch_json` select JSON explicitly).
- Queue depths directly impact memory usage. Large queue depths combined with large batch sizes and
  high fanout can significantly increase resident memory usage.
