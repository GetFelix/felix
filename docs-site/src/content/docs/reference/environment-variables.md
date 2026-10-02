---
title: "Environment Variables Reference"
---

Complete reference for all Felix environment variables, organized by category.

## Overview

Felix uses environment variables prefixed with `FELIX_` for configuration. These variables provide quick overrides without modifying config files.

**Priority**: Environment variables override built-in defaults. A YAML config file (`FELIX_BROKER_CONFIG` or `FELIX_CONTROLPLANE_CONFIG`) overrides both.

## Network and Binding

### `FELIX_QUIC_BIND`

**Description**: QUIC listener bind address and port (UDP).

**Type**: `SocketAddr` format

**Default**: `0.0.0.0:5000`

**Example**:
```bash
export FELIX_QUIC_BIND="0.0.0.0:5000"
export FELIX_QUIC_BIND="127.0.0.1:5001"  # Localhost only
export FELIX_QUIC_BIND="10.0.1.5:5000"   # Specific interface
```

**Notes**:
- Must be a valid IP:Port combination
- UDP port for QUIC transport
- Use `0.0.0.0` to bind all interfaces
- With `FELIX_QUIC_LISTENERS` above 1, this is the **first** port of a
  consecutive run

### `FELIX_QUIC_LISTENERS`

**Description**: How many client-facing QUIC listeners to bind, on consecutive
ports starting at `FELIX_QUIC_BIND`.

**Type**: Integer (at least 1)

**Default**: derived from the cores the broker may use, as
`max(1, min(cores / 2, 4))`: `1` up to 3 cores, `2` at 4 or 5, `4` from 8.
On Linux the core count respects cgroup CPU limits. Unset, the count is also
shortened so the range stops before `FELIX_INTERNAL_BIND` and port 65535, so a
cluster member on the default ports (client `5000`, internal `5001`) keeps one
listener.

**Example**:
```bash
export FELIX_QUIC_BIND="0.0.0.0:5000"
export FELIX_QUIC_LISTENERS=4          # binds 5000, 5001, 5002, 5003
```

**Notes**:
- **Why it exists**: one UDP socket is one QUIC endpoint, and that endpoint's
  driver is a single task that reads every inbound datagram and routes it by
  connection id. It cannot use more than one core, and it is the per-broker
  throughput ceiling, measured at ~88% of one core while the rest of the
  machine idled. Separate ports are separate sockets, which are separate
  drivers.
- The broker advertises the port set during authentication, and a client
  spreads its connection pools across it. A client that predates this ignores
  the advertisement and keeps using the single address it dialled.
- **Why the default stops at half the cores**: on an 8-vCPU Azure broker,
  4 listeners measured 4099 MB/s and 8 measured 3437 MB/s. Past half the
  cores, listeners contend with the broker's own threads.
- Every port in the range must be open in firewalls, security groups and
  service definitions, not just `FELIX_QUIC_BIND`. Clients dial the advertised
  ports on the host they connected to, so a container that publishes only
  `5000`, or remaps it to another host port, needs `FELIX_QUIC_LISTENERS=1`.
- An explicit `FELIX_INTERNAL_BIND` inside an explicit range fails startup,
  since peer traffic and client traffic must not share a listener.
- An explicit count whose range would run past port 65535 also fails startup,
  rather than binding fewer listeners than asked for.
- Adding brokers remains the horizontal lever. This raises what one broker can
  do before you need another.
- Do not pin [`FELIX_IO_RUNTIME_THREADS`](#felix_io_runtime_threads) below one
  runtime per listener plus one. It is derived from this setting, and a pool
  too small puts every listener's driver back on a single thread. Startup
  refuses that combination.

### `FELIX_TLS_CERT`

**Description**: PEM certificate chain (leaf first) the client-facing QUIC
listeners, and the Kafka listener when it serves TLS, present to clients.

**Type**: File path

**Default**: unset. The broker generates a self-signed `localhost`
certificate at startup, for development only

**Example**:
```bash
export FELIX_TLS_CERT="/etc/felix/tls/tls.crt"
export FELIX_TLS_KEY="/etc/felix/tls/tls.key"
```

**Notes**:
- Set with [`FELIX_TLS_KEY`](#felix_tls_key). One without the other fails
  startup.
- Re-read with the key every 30s. A renewal written over the same paths (a
  cert-manager Secret volume, for instance) is used by the next handshake
  without a restart. Connections already up keep theirs.
- The certificate must carry every name clients dial: the load-balanced
  Service name and each broker's advertised name.
- Unreadable files, or a key that does not match, fail startup.

### `FELIX_TLS_KEY`

**Description**: PEM private key for [`FELIX_TLS_CERT`](#felix_tls_cert).

**Type**: File path

**Default**: unset

### `FELIX_TLS_CLIENT_CA`

**Description**: PEM CA bundle client certificates must chain to. When set,
every client of the QUIC and Kafka listeners must present a certificate
issued by it, or the handshake is refused.

**Type**: File path

**Default**: unset. Clients are not asked for a certificate

**Notes**:
- Needs `FELIX_TLS_CERT` and `FELIX_TLS_KEY`. Set alone, it fails startup.
- Read once at startup: trust roots change at a restart.
- Bearer tokens are still required. This adds a transport factor, it does not
  replace authentication.
- The Rust client presents a certificate through its `quinn::ClientConfig`.
  The Python and TypeScript bindings do not offer client certificates yet.

### `FELIX_TLS_REQUIRE_ALPN`

**Description**: Refuse QUIC clients that do not negotiate the `felix/1` ALPN.

**Type**: Boolean (`true`/`false`)

**Default**: `false`

**Notes**:
- The client listeners always select `felix/1` for a client that offers it and
  refuse a client that offers only other protocols. A client offering no ALPN
  (every client built before it existed) is still accepted unless this is set.
- The shipped clients offer `felix/1` only when asked, because a broker older
  than ALPN support refuses a client that offers it: `offer_alpn=True` in
  Python, `offerAlpn` in TypeScript, `felix_client::quic_client_config(roots,
  true)` in Rust. Turn it on in every client before setting this.

### `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT`

**Description**: When a client presents a certificate, require that
certificate to be issued to its token's subject (`sub`).

**Type**: Boolean (`true`/`false`)

**Default**: `false`

**Notes**:
- A certificate binds `sub` if its subject alternative names include the URI
  `felix:principal:<sub>`, with `sub` verbatim (exact, case-sensitive match),
  or a DNS or IP name that `sub` matches the way a server name would.
- Control-plane tokens carry a 64-hex principal id, longer than a DNS label
  may be, so issue those clients' certificates with the URI SAN.
- Needs `FELIX_TLS_CLIENT_CA`. Set without it, startup fails.
- Applies to the QUIC client listeners, and to the Kafka listener when it
  serves TLS: SASL/PLAIN checks the token against the certificate the client
  presented in the TLS handshake. Over `SASL_PLAINTEXT` there is no certificate
  to bind to.

### `FELIX_TLS_REQUIRE_CERT`

**Description**: Refuse to start on the generated development certificate.

**Type**: Boolean (`true`/`false`)

**Default**: `false`

**Notes**:
- Set it in production. Without `FELIX_TLS_CERT` a broker serves a
  self-signed certificate that changes on every start and that clients cannot
  verify, and says so in a startup warning. This turns the warning into a
  refusal.

### `FELIX_TLS_CERT_EXPORT`

**Description**: Write the broker's generated self-signed certificate to this
path (PEM) at startup, so clients can trust it explicitly.

**Type**: File path

**Default**: unset. No certificate is written

**Example**:
```bash
export FELIX_TLS_CERT_EXPORT="/tmp/felix-dev-ca.pem"
```

**Notes**:
- Development only. The broker generates a self-signed certificate at startup.
  Without exporting it, the only way for a non-Rust client to connect is to
  skip verification entirely.
- Point a client at the file: the Python client takes `ca_file=`, and other
  clients take whatever their TLS stack calls a CA bundle.
- Startup fails if the file cannot be written. A deployment that asked for
  the export has clients configured to read it, and coming up without it turns
  into connection failures far from their cause.
- Not a substitute for real certificates. See
  [`FELIX_TLS_CERT`](#felix_tls_cert). Setting both fails startup: a
  configured certificate is trusted through the CA that issued it.

### `FELIX_BROKER_METRICS_BIND`

**Description**: HTTP metrics and health endpoint bind address.

**Type**: `SocketAddr` format

**Default**: `0.0.0.0:8080`

**Example**:
```bash
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8080"
```

**Exposed endpoints** (no authentication):
- `/live`: always `ok` while the process runs
- `/ready`: `ok`, or 503 `draining` once shutdown starts
- `/metrics`: Prometheus metrics
- `/replication/halted`: replicas that stopped replicating, as JSON
- `/backup/offsets`: the committed offset of every log, for backups

## Control Plane

### `FELIX_CONTROLPLANE_URL`

**Description**: Control plane base URL for metadata synchronization.

**Required**: the broker exits at startup with `FELIX_CONTROLPLANE_URL must be
set for auth` when it is unset, single-node deployments included.

**Type**: String (URL)

**Default**: None (required)

**Example**:
```bash
export FELIX_CONTROLPLANE_URL="http://felix-controlplane:8443"
export FELIX_CONTROLPLANE_URL="https://cp.example.com:8443"
```

**Usage**:
- Also where `felix-controlplane admin` sends its requests, unless `--url` is given (default `http://127.0.0.1:8443`)
- Include scheme (`http://` or `https://`). Over `http://`, node credentials,
  token exchange and tenant JWKS cross the network in clear text. Use
  `https://` anywhere the network is not trusted.

### `FELIX_CONTROLPLANE_CA`

**Description**: PEM CA bundle trusted, in addition to the system trust store, when
`FELIX_CONTROLPLANE_URL` is `https://`. Read by brokers and by
`felix-controlplane admin`.

**Type**: File path

**Default**: unset (system trust store only)

**Notes**:
- Set it when the control plane's certificate comes from a private CA
  (cert-manager, an internal PKI).
- Startup fails if the bundle cannot be read or is empty, and if it is set
  while `FELIX_CONTROLPLANE_URL` is `http://`.

### `FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`

**Description**: Control plane polling interval in milliseconds. Paces the
node-catalog refresh and the broker's background passes. Shard assignment
changes are long-polled and arrive as they are written, and each one also
refreshes the node catalog. Against a control
plane that does not support long-polling, this is also the assignment poll
interval.

**Type**: Unsigned integer

**Default**: `2000`

**Example**:
```bash
export FELIX_CONTROLPLANE_SYNC_INTERVAL_MS="2000"
export FELIX_CONTROLPLANE_SYNC_INTERVAL_MS="500"   # Fast polling
export FELIX_CONTROLPLANE_SYNC_INTERVAL_MS="10000" # Slow polling
```

## Publishing Configuration

### `FELIX_ACK_ON_COMMIT`

**Description**: Enable publish acknowledgements after commit.

**Type**: Boolean

**Default**: `false`

**Accepted values**: `1`, `true`, `yes` (case-insensitive) = enabled

**Example**:
```bash
export FELIX_ACK_ON_COMMIT="true"
export FELIX_ACK_ON_COMMIT="1"
export FELIX_ACK_ON_COMMIT="yes"
```

**Trade-off**:
- `false`: Fire-and-forget, lower latency
- `true`: Explicit acks, higher latency guarantee

### `FELIX_MAX_FRAME_BYTES`

**Description**: Maximum frame size accepted on QUIC streams.

**Type**: Positive integer (bytes)

**Default**: `16777216` (16 MiB)

**Example**:
```bash
export FELIX_MAX_FRAME_BYTES="16777216"    # 16 MiB
export FELIX_MAX_FRAME_BYTES="33554432"    # 32 MiB
export FELIX_MAX_FRAME_BYTES="8388608"     # 8 MiB
```

**Notes**:
- Value of `0` uses default
- Affects max message size

### `FELIX_PREAUTH_MAX_FRAME_BYTES`

**Description**: Maximum frame size a stream may send before it has authenticated. Only an
`Auth` has to fit. A larger frame on an unauthenticated stream ends the stream.

**Type**: Positive integer (bytes)

**Default**: `65536` (64 KiB). Never more than `FELIX_MAX_FRAME_BYTES`.

**Example**:
```bash
export FELIX_PREAUTH_MAX_FRAME_BYTES="65536"
```

### `FELIX_PREAUTH_MAX_STREAMS_PER_CONN`

**Description**: Unauthenticated streams one connection may have reading at once. Further
streams wait until one authenticates or ends. They are not refused.

**Type**: Positive integer

**Default**: `16`

### `FELIX_AUTH_TIMEOUT_MS`

**Description**: How long a client connection has to authenticate a stream. A connection with
no authenticated stream by then is closed with QUIC application code `1` and reason
`authentication timeout`.

**Type**: Non-negative integer (milliseconds)

**Default**: `10000`. `0` disables the deadline.

**Notes**:
- Clients from this release authenticate every pooled connection when they connect. An older
  client that holds an event connection idle until its first subscribe will see that
  connection closed. Raise the timeout or set `0` while such clients remain.

### `FELIX_MAX_CLIENT_CONNECTIONS`

**Description**: Client QUIC connections the broker holds at once, across all client
listeners. Attempts past the cap are refused before the handshake (`CONNECTION_REFUSED`) and
counted in `felix_quic_connections_refused_total{reason="limit"}`.

**Type**: Positive integer

**Default**: `8192`
- Must align with client configuration

### `FELIX_PUBLISH_QUEUE_WAIT_MS`

**Description**: How long a commit-acked publish waits for room in the broker's
publish queue (and its byte budget, together) before it is answered busy.

**Type**: Positive integer (milliseconds)

**Default**: `2000`

**Example**:
```bash
export FELIX_PUBLISH_QUEUE_WAIT_MS="2000"
export FELIX_PUBLISH_QUEUE_WAIT_MS="5000"  # More patient
export FELIX_PUBLISH_QUEUE_WAIT_MS="500"   # Fail fast
```

**Behavior**:
- A commit-acked publish that finds the queue full waits up to this long
- It is then answered `overloaded` (`detail.reason = "publish_queue_full"`),
  which is retryable: nothing was queued
- An enqueue-acked publish is answered busy at once, without waiting
- Backpressure mechanism

### `FELIX_ACK_WAIT_TIMEOUT_MS`

**Description**: How long the broker waits to answer an acknowledged publish
whose answer depends on a commit: an ack-on-commit publish, a `Quorum` publish,
a forwarded publish or an idempotent one. The value used is the larger of this
and `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` + 500 ms. The publisher gets an error when
it is exceeded.

**Type**: Positive integer (milliseconds)

**Default**: `2000`

**Example**:
```bash
export FELIX_ACK_WAIT_TIMEOUT_MS="2000"
```

## Event Batching and Delivery

### `FELIX_EVENT_BATCH_MAX_EVENTS`

**Description**: Maximum events per subscription batch frame.

**Type**: Positive integer (count)

**Default**: `64`

**Example**:
```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="64"
export FELIX_EVENT_BATCH_MAX_EVENTS="1"    # No batching
export FELIX_EVENT_BATCH_MAX_EVENTS="256"  # Large batches
```

**Tuning**:
- Small values (1-16): Low latency
- Medium values (32-64): Balanced
- Large values (128-256): High throughput

### `FELIX_EVENT_BATCH_MAX_BYTES`

**Description**: Maximum bytes per subscription batch frame.

**Type**: Positive integer (bytes)

**Default**: `65536` (64 KiB)

**Example**:
```bash
export FELIX_EVENT_BATCH_MAX_BYTES="65536"    # 64 KiB (default)
export FELIX_EVENT_BATCH_MAX_BYTES="524288"   # 512 KiB
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"  # 1 MiB
```

**Notes**:
- Batch sent when event count OR byte limit reached
- Adjust based on typical message size

### `FELIX_EVENT_BATCH_MAX_DELAY_US`

**Description**: The most a subscription batch waits for more events under
load (microseconds). A batch takes whatever events are already queued for the subscriber and
flushes at once, so an event that arrives alone is sent without waiting. Only
when the previous batch found events queued behind its first (events arriving
faster than the broker drains them) does the next batch wait, up to this long
from its first event, for more to fill it. Tokio timers have 1 ms resolution,
so a non-zero delay below 1000 µs waits until the next millisecond tick; `0`
never waits on a timer.

**Type**: Unsigned integer

**Default**: `250`

**Example**:
```bash
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_EVENT_BATCH_MAX_DELAY_US="0"     # Never wait; batch only what is queued
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"  # Bigger batches under load
```

**Tuning**:
- It does not affect an idle or lightly loaded subscriber, which is sent each
  event as it arrives
- Lower: lower delivery latency under load, more frames
- Higher: fewer, fuller frames under load, higher latency under load

### `FELIX_FANOUT_BATCH`

**Description**: Subscribers to process in parallel during fanout.

**Type**: Positive integer (count)

**Default**: `64`

**Example**:
```bash
export FELIX_FANOUT_BATCH="64"
export FELIX_FANOUT_BATCH="128"  # High fanout
export FELIX_FANOUT_BATCH="16"   # Low fanout
```

**Recommendations**:
- Match to typical subscriber count
- Higher values for high-fanout streams
- Lower values reduce concurrency overhead

### Event Frame Encoding

Subscription event delivery uses binary `EventBatch` frames by default.

### `FELIX_SUBSCRIBER_QUEUE_CAPACITY`

**Description**: Per-subscriber queue capacity in broker core.

**Type**: Positive integer (count)

**Default**: `512`

```bash
export FELIX_SUBSCRIBER_QUEUE_CAPACITY="512"
# Alias (same behavior):
export FELIX_SUB_QUEUE_CAPACITY="512"
```

### `FELIX_MAX_SUBSCRIPTIONS_PER_CONN`

**Description**: Max concurrent subscriptions a single QUIC connection may hold. Independent of `FELIX_SUBSCRIBER_QUEUE_CAPACITY` (which bounds one subscription's buffer size). This bounds how many subscriptions a connection can open in total, protecting broker memory from a connection that opens unbounded subscriptions.

**Type**: Positive integer (count)

**Default**: `4096`

```bash
export FELIX_MAX_SUBSCRIPTIONS_PER_CONN="4096"
```

### `FELIX_SUB_QUEUE_POLICY`

**Description**: Backpressure policy when broker subscriber queues are full.

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

```bash
export FELIX_SUB_QUEUE_POLICY="drop_new"
```

**Policy notes**:
- `block`: await queue space (strongest delivery guarantee, may reduce publish throughput).
- `drop_new`: drop incoming item when queue is full.
- `drop_old`: currently emulated with `drop_new` semantics and tracked separately.

### `FELIX_SUB_SINGLE_WRITER_PER_CONN`

**Description**: Keep all subscribers on the same QUIC connection on one writer lane.

**Type**: Boolean (`1|true|yes` to enable)

**Default**: `false`

```bash
export FELIX_SUB_SINGLE_WRITER_PER_CONN="true"
```

### `FELIX_SUB_WRITER_LANES`

**Description**: Requested outbound subscriber writer lanes.

**Type**: Positive integer (count)

**Default**: `4`

```bash
export FELIX_SUB_WRITER_LANES="4"
# Alias (checked first, same behavior):
export FELIX_SUB_EGRESS_LANES="4"
```

### `FELIX_SUB_LANE_QUEUE_DEPTH`

**Description**: Queue depth per outbound writer lane.

**Type**: Positive integer (count)

**Default**: `64`

```bash
export FELIX_SUB_LANE_QUEUE_DEPTH="64"
# Alias (checked first, same behavior):
export FELIX_SUB_QUEUE_BOUND="64"
```

### `FELIX_SUB_QUEUE_MODE`

**Description**: Backpressure policy for the writer-lane command queue (downstream of `FELIX_SUB_QUEUE_POLICY`, which gates the earlier broker-core fanout enqueue).

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

```bash
export FELIX_SUB_QUEUE_MODE="drop_new"
# Alias (same behavior):
export FELIX_SUB_LANE_QUEUE_POLICY="drop_new"
```

### `FELIX_MAX_SUB_WRITER_LANES`

**Description**: Safety clamp for writer lanes.

**Type**: Positive integer (count)

**Default**: `8`

```bash
export FELIX_MAX_SUB_WRITER_LANES="8"
```

### `FELIX_SUB_LANE_SHARD`

**Description**: Outbound lane sharding policy.

**Type**: Enum (`auto`, `subscriber_id_hash`, `connection_id_hash`, `round_robin_pin`)

**Default**: `auto`

```bash
export FELIX_SUB_LANE_SHARD="auto"
```

**Policy notes**:
- `auto`: prefers connection-aware routing when connection id is known.
- `subscriber_id_hash`: stable by subscriber id.
- `connection_id_hash`: stable by connection id.
- `round_robin_pin`: pinned RR assignment per subscriber.

### `FELIX_SUB_FLUSH_MAX_ITEMS`

**Description**: Maximum queued lane commands drained per flush before a write is issued.

**Type**: Positive integer (count)

**Default**: `16`

```bash
export FELIX_SUB_FLUSH_MAX_ITEMS="16"
```

### `FELIX_SUB_FLUSH_MAX_DELAY_US`

**Description**: Maximum time spent waiting to fill a lane flush buffer before writing what's accumulated.

**Type**: Unsigned integer (microseconds)

**Default**: `50`

```bash
export FELIX_SUB_FLUSH_MAX_DELAY_US="50"
```

### `FELIX_SUB_MAX_BYTES_PER_WRITE`

**Description**: Upper bound on coalesced bytes per QUIC write call to a subscriber stream.

**Type**: Positive integer (bytes)

**Default**: `65536` (64 KiB)

```bash
export FELIX_SUB_MAX_BYTES_PER_WRITE="65536"
```

### `FELIX_SUB_STREAMS_PER_CONN`

**Description**: Number of delivery streams per connection in hashed-pool mode.

**Type**: Positive integer (count)

**Default**: `4`

```bash
export FELIX_SUB_STREAMS_PER_CONN="4"
```

### `FELIX_SUB_STREAM_MODE`

**Description**: Strategy for mapping subscribers to event streams. `hashed_pool` is not enabled. The broker falls back to `per_subscriber` and logs a debug warning if requested.

**Type**: Enum (`per_subscriber`, `hashed_pool`)

**Default**: `per_subscriber`

```bash
export FELIX_SUB_STREAM_MODE="per_subscriber"
```

## Cache Configuration

### `FELIX_CACHE_CONN_POOL`

**Description**: Number of QUIC connections in cache pool (client-side).

**Type**: Positive integer (count)

**Default**: `8`

**Example**:
```bash
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_CONN_POOL="16"  # High concurrency
export FELIX_CACHE_CONN_POOL="4"   # Low concurrency
```

**Notes**:
- Client-side setting
- Affects concurrent request capacity
- Each connection can have multiple streams

### `FELIX_CACHE_STREAMS_PER_CONN`

**Description**: Cache request streams per connection (client-side).

**Type**: Positive integer (count)

**Default**: `4`

**Example**:
```bash
export FELIX_CACHE_STREAMS_PER_CONN="4"
export FELIX_CACHE_STREAMS_PER_CONN="8"   # More parallelism
export FELIX_CACHE_STREAMS_PER_CONN="2"   # Less overhead
```

**Tuning**:
- Total cache parallelism = `pool × streams_per_conn`
- Higher values for high-concurrency workloads

### `FELIX_CACHE_CONN_RECV_WINDOW`

**Description**: Cache connection flow-control receive window (broker).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_CACHE_CONN_RECV_WINDOW="268435456"    # 256 MiB
export FELIX_CACHE_CONN_RECV_WINDOW="536870912"    # 512 MiB
export FELIX_CACHE_CONN_RECV_WINDOW="134217728"    # 128 MiB
```

**Memory impact**:
- Per-connection credit
- Multiplied by connection pool size
- Affects burst tolerance

### `FELIX_CACHE_STREAM_RECV_WINDOW`

**Description**: Cache stream flow-control receive window (broker).

**Type**: Positive integer (bytes)

**Default**: `67108864` (64 MiB)

**Example**:
```bash
export FELIX_CACHE_STREAM_RECV_WINDOW="67108864"   # 64 MiB
export FELIX_CACHE_STREAM_RECV_WINDOW="134217728"  # 128 MiB
export FELIX_CACHE_STREAM_RECV_WINDOW="33554432"   # 32 MiB
```

**Notes**:
- Per-stream credit
- Total: `stream_window × streams_per_conn × conn_pool`

### `FELIX_CACHE_SEND_WINDOW`

**Description**: Cache connection send window (broker).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_CACHE_SEND_WINDOW="268435456"
```

## Cluster Client Connections

### `FELIX_CLUSTER_CONN_POOL`

**Description**: The most QUIC connections a `ClusterClient` opens to one broker. It starts with one, shared by every kind of traffic and every role the broker plays, and opens more only when those are saturated.

**Type**: Positive integer (count)

**Default**: `8`

**Example**:
```bash
export FELIX_CLUSTER_CONN_POOL="8"
```

### `FELIX_CLUSTER_STREAMS_PER_CONN`

**Description**: Streams one of a `ClusterClient`'s connections carries before another is opened beside it. The broker's QUIC stream credit is the other trigger, whichever comes first.

**Type**: Positive integer (count)

**Default**: `1024`

**Example**:
```bash
export FELIX_CLUSTER_STREAMS_PER_CONN="1024"
```

## Event Connection Pool (Client)

### `FELIX_EVENT_CONN_POOL`

**Description**: Number of QUIC connections for event delivery (client).

**Type**: Positive integer (count)

**Default**: `8`

**Example**:
```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_POOL="4"   # Lower overhead
export FELIX_EVENT_CONN_POOL="16"  # More parallelism
# Alias used by perf scripts:
export FELIX_SUB_CONNS="8"
```

### `FELIX_EVENT_CONN_RECV_WINDOW`

**Description**: Event connection receive window (client).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_EVENT_CONN_RECV_WINDOW="268435456"
```

### `FELIX_EVENT_STREAM_RECV_WINDOW`

**Description**: Event stream receive window (client).

**Type**: Positive integer (bytes)

**Default**: `67108864` (64 MiB)

**Example**:
```bash
export FELIX_EVENT_STREAM_RECV_WINDOW="67108864"
```

### `FELIX_EVENT_SEND_WINDOW`

**Description**: Event connection send window (client).

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_EVENT_SEND_WINDOW="268435456"
```

### `FELIX_CLIENT_SUB_QUEUE_CAPACITY`

**Description**: Bounded queue capacity between client subscription IO and dispatch stages.

**Type**: Positive integer (count)

**Default**: `256`

```bash
export FELIX_CLIENT_SUB_QUEUE_CAPACITY="256"
```

### `FELIX_CLIENT_SUB_QUEUE_POLICY`

**Description**: Client-side backpressure policy for subscription pipeline queues. Applies to live records only: history replayed from an earlier offset (below the subscription's `live_offset`) always waits for room and is never dropped.

**Type**: Enum (`block`, `drop_new`, `drop_old`)

**Default**: `drop_new`

```bash
export FELIX_CLIENT_SUB_QUEUE_POLICY="drop_new"
```

## Publishing Pool (Client)

### `FELIX_PUB_CONN_POOL`

**Description**: Number of publishing QUIC connections (client).

**Type**: Positive integer (count)

**Default**: `4`

**Example**:
```bash
export FELIX_PUB_CONN_POOL="4"
export FELIX_PUB_CONN_POOL="8"  # More publishers
```

### `FELIX_PUB_STREAMS_PER_CONN`

**Description**: Publishing streams per connection (client).

**Type**: Positive integer (count)

**Default**: `2`

**Example**:
```bash
export FELIX_PUB_STREAMS_PER_CONN="2"
export FELIX_PUB_STREAMS_PER_CONN="4"  # More concurrency
```

### `FELIX_PUBLISH_CHUNK_BYTES`

**Description**: Chunk size for publishing large messages (client).

**Type**: Positive integer (bytes)

**Default**: `16384` (16 KiB)

**Example**:
```bash
export FELIX_PUBLISH_CHUNK_BYTES="16384"    # 16 KiB
export FELIX_PUBLISH_CHUNK_BYTES="32768"    # 32 KiB
export FELIX_PUBLISH_CHUNK_BYTES="8192"     # 8 KiB
```

### `FELIX_PUBLISH_QUEUE_DEPTH`

**Description**: Bounded request queue depth per client publish worker.

**Type**: Positive integer (count)

**Default**: `64`

```bash
export FELIX_PUBLISH_QUEUE_DEPTH="64"
```

### `FELIX_PUBLISH_INFLIGHT_BYTES`

**Description**: Shared queued and in-flight publish byte budget across client workers.

**Type**: Positive integer (bytes)

**Default**: `4194304` (4 MiB)

```bash
export FELIX_PUBLISH_INFLIGHT_BYTES="4194304"
```

## Broker Workers and Queues

### `FELIX_BROKER_PUB_WORKERS_PER_CONN`

**Description**: Executors of the broker's publish scheduler: how many shards'
ordered publish steps (claiming offsets, an in-memory append) may run at once.
Despite the name the scheduler is process-wide, not per connection, because
per-connection pools multiplied concurrent callers into shared broker state.
Each shard is an ordered lane that runs one publish at a time, so raising this
lets more different shards run at once. It cannot give one shard more than
one. Waits on anything outside the broker (a device flush, a forward to
another broker, a quorum) do not hold an executor. With `FELIX_CORE_SHARDS`
set, each core shard gets this many executors of its own. Also scales the
queue: see `FELIX_BROKER_PUB_QUEUE_DEPTH`.

**Type**: Positive integer (count)

**Default**: `4`

**Example**:
```bash
export FELIX_BROKER_PUB_WORKERS_PER_CONN="4"
export FELIX_BROKER_PUB_WORKERS_PER_CONN="8"   # High concurrency
export FELIX_BROKER_PUB_WORKERS_PER_CONN="2"   # Lower overhead
```

### `FELIX_BROKER_PUB_FLUSH_CONCURRENCY`

**Description**: Durable publishes one shard may have awaiting their device
flush at once (broker). Offsets are still claimed serially, in arrival order,
so this does not affect the order records land in. It decides how many
flushes group commit gets to coalesce. A shard at the limit waits for a flush
to finish before its next claim, without holding up any other shard. `1`
allows one flush at a time, which caps a shard at roughly one batch per flush.

**Type**: Positive integer (count)

**Default**: `32`

**Example**:
```bash
export FELIX_BROKER_PUB_FLUSH_CONCURRENCY="32"
export FELIX_BROKER_PUB_FLUSH_CONCURRENCY="64"  # Deeper coalescing on fast devices
export FELIX_BROKER_PUB_FLUSH_CONCURRENCY="1"   # One flush at a time
```

### `FELIX_BROKER_PUB_QUEUE_DEPTH`

**Description**: Publish jobs every tenant is guaranteed room for in the
broker's publish queue. The queue holds this many times
`FELIX_BROKER_PUB_WORKERS_PER_CONN` jobs in all (per core shard, with
`FELIX_CORE_SHARDS`). A tenant past its guaranteed share may borrow idle room,
but never the last share's worth, so one tenant flooding the queue is refused
while another still gets in. A publish that finds no room is answered
`overloaded` with `detail.reason = "publish_queue_full"` when it asked for an
ack, and shed otherwise (or waits, with `FELIX_PUB_INGRESS_WAIT`). Either way
it is counted in `felix_tenant_publish_queue_full_total{tenant,action}`.

**Type**: Positive integer (count)

**Default**: `64`

**Example**:
```bash
export FELIX_BROKER_PUB_QUEUE_DEPTH="64"
export FELIX_BROKER_PUB_QUEUE_DEPTH="256"  # More buffering
export FELIX_BROKER_PUB_QUEUE_DEPTH="32"   # Less memory
```

### `FELIX_BROKER_PUBLISH_INFLIGHT_BYTES`

**Description**: Shared in-flight publish byte budget across all publish workers (process-wide). Bounds queued-or-processing bytes independent of `FELIX_BROKER_PUB_QUEUE_DEPTH`'s item count, so a handful of large payloads/batches can't blow past the intended ingress memory budget.

**Type**: Positive integer (bytes)

**Default**: `67108864` (64 MiB)

```bash
export FELIX_BROKER_PUBLISH_INFLIGHT_BYTES="67108864"
```

### `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES`

**Description**: Per-connection share of `FELIX_BROKER_PUBLISH_INFLIGHT_BYTES`. Bounds how much of the shared, process-wide publish byte budget a single connection can occupy at once, so one connection publishing large batches can't starve every other connection's admission into the shared budget.

**Type**: Positive integer (bytes)

**Default**: `16777216` (16 MiB)

```bash
export FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES="16777216"
```

### `FELIX_BROKER_PUBLISH_WINDOW`

**Description**: The most acknowledged publishes one stream may have unanswered when its client pipelines them (`FEATURE_PUBLISH_PIPELINE`). The broker grants this number in `AuthOk.publish_window`, answers a pipelining stream's publishes in the order it sent them, and stops reading that stream's publishes while this many are outstanding on it. Each stream has its own window, so one stuck behind a stalled shard does not stall the others on its connection. `0` turns pipelining off: no client is granted a window, and acks come back in completion order.

**Type**: Non-negative integer

**Default**: `256`

```bash
export FELIX_BROKER_PUBLISH_WINDOW="256"
```

### `FELIX_PUB_INGRESS_WAIT`

**Description**: When enabled, un-acked (fire-and-forget) publishes wait for ingress capacity, bounded by `FELIX_PUBLISH_QUEUE_WAIT_MS`, instead of being shed when the publish queue or byte budget is full. Backpressure then propagates through QUIC flow control to the publisher. Leave off in production for visible shedding under overload. Turn on for lossless pipelines and sustainable-throughput benchmarking.

**Type**: Boolean (`1`, `true`, `yes` = enabled)

**Default**: disabled

```bash
export FELIX_PUB_INGRESS_WAIT="1"
```

### `FELIX_CORE_SHARDS`

**Description**: Number of core-pinned shard executors owning stream work (thread-per-core, shared-nothing). Each stream is owned by one shard: its publish worker and its subscriptions' lane feeders run on that shard's dedicated single-threaded runtime, pinned to a CPU core on Linux. Benefits scale with stream count. Single-stream workloads serialize on one shard by design.

**Type**: Positive integer (count; `0` = disabled)

**Default**: `0`

```bash
export FELIX_CORE_SHARDS="4"
```

## Connection Limits and Tenant Quotas

Every limit here is per broker. A tenant publishing through three brokers gets
three times its quota. Divide by the number of brokers a tenant's clients spread
across when you pick a number. Invalid values fail startup rather than falling
back to "unlimited".

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_MAX_CONNECTIONS_PER_IP` | `512` | Client QUIC connections one source IP may hold across all of this broker's QUIC listeners. The next one is refused before its handshake (the client sees a QUIC connection refusal) and counted in `felix_quic_connections_refused_total{reason="per_ip_limit"}`. Applies alongside the broker-wide `FELIX_MAX_CLIENT_CONNECTIONS`. IPv4-mapped IPv6 addresses count as the IPv4 address. `0` is unlimited. |
| `FELIX_TENANT_PUBLISH_BYTES_PER_SEC` | `0` | Payload bytes per second every tenant may publish, unless `FELIX_TENANT_PUBLISH_QUOTAS` names it. `0` is unlimited. |
| `FELIX_TENANT_PUBLISH_MSGS_PER_SEC` | `0` | Messages per second every tenant may publish, the same way. Either rate refuses on its own. |
| `FELIX_TENANT_PUBLISH_QUOTAS` | unset | Per-tenant rates that replace the defaults: comma-separated `tenant:bytes_per_sec:msgs_per_sec`, `0` meaning unlimited, e.g. `acme:10485760:5000,beta:0:100`. A tenant named twice fails startup. |
| `FELIX_TENANT_PUBLISH_BURST_MS` | `1000` | How far a tenant may run ahead of its rate, as time at that rate. A publish is admitted whenever the tenant is not already over, and takes its whole cost, so one batch larger than the burst still goes through and the tenant then waits it out. The overdraft is capped at five seconds of rate. |
| `FELIX_TENANT_METRICS_MAX` | `100` | Tenants that get their own `tenant` label on the `felix_tenant_*` metrics. Later tenants are counted together under `tenant="_overflow"`, and `felix_tenant_metrics_overflow_total` counts those recordings. |

What a publish over its tenant's quota gets, by path:

- **Acknowledged QUIC publish**: refused before anything is queued, as
  `overloaded` with `detail.reason = "tenant_quota"` and `detail.retry_after_ms`
  set to when the tenant is back under. A client that did not negotiate error
  codes sees the same text as any other overload.
- **Fire-and-forget QUIC publish**: shed like any other overload, unless
  `FELIX_PUB_INGRESS_WAIT` is on. Then it waits for the quota, which slows the
  publisher through QUIC flow control instead of dropping.
- **Kafka produce**: written and answered with `throttle_time_ms`. The
  connection reads nothing more until that time has passed, as a Kafka broker
  enforces its own quotas.

Every case is counted in `felix_tenant_publish_throttled_total{tenant,action}`.

## QUIC Transport Tuning

Process-wide levers read by every Felix QUIC endpoint (broker, client, demos). See [Benchmarks](/felix/features/benchmarks/) for measured impact.

### `FELIX_MTU_UPPER_BOUND`

**Description**: Upper bound for QUIC path-MTU discovery. Discovery converges to the real path MTU at or below it, so on a 1500-byte network the bound never binds.

Raising it above 6,553 on Linux will stall delivery. Linux UDP GSO packs a whole `sendmsg` batch into one IP datagram, so `MTU × segments` must stay under 65,535, and quinn batches up to 10. Above that the kernel rejects every batch with `EMSGSIZE`, which quinn does not recognise as a GSO failure (it falls back only on `EIO`/`EINVAL`). So the transmit is dropped after quinn has counted it as sent, and the stall is permanent rather than degrading. Treat 6,553 as a hard limit, not a tuning choice.

The default is `4096` rather than 6,553 because quinn's batch size is private to it, and 6,553 breaks the moment it rises. 4,096 also measured fastest on Linux. macOS has no GSO and no such limit.

**Type**: Positive integer (bytes, clamped to 1200–65527)

**Default**: `4096` (`16384` on macOS)

```bash
export FELIX_MTU_UPPER_BOUND="4096"
export FELIX_MTU_UPPER_BOUND="16384"  # macOS, or any path with no GSO
```

### `FELIX_INITIAL_MTU`

**Description**: Starting datagram size before path-MTU discovery completes. The RFC-safe default works everywhere. Raising it on known-good paths (jumbo-frame LAN) skips the discovery ramp. Connections to a loopback peer automatically start at the loopback MTU and guarantee it, which makes the path immune to spurious black-hole collapse (see `FELIX_MTU_BLACK_HOLE_COOLDOWN_MS`) and, because the guarantee also freezes the discovery bound, removes probe traffic entirely.

The guaranteed size is 16,336 bytes on macOS and 4,096 elsewhere (both capped by `FELIX_MTU_UPPER_BOUND`). The cap is lower off macOS because Linux UDP GSO packs a whole `sendmsg` batch into a single IP datagram, so `MTU × segments` must stay under 65,535. Quinn batches up to 10, putting the real ceiling at 6,553 bytes. Above it the kernel rejects every batch and delivery stalls outright, measured as a total stall at both 8,192 and 16,336. macOS has no GSO (one syscall per datagram) and no such limit. 4,096 also measured fastest on Linux. See the [performance case study](/felix/features/performance-case-study/).

The loopback path also requires the socket's granted UDP buffers to reach ~1 MiB. That threshold is a proxy for "this host has been tuned", not a burst-headroom calculation. Hosts where Linux silently clamps `SO_RCVBUF` to a stock `net.core.rmem_max` (~208 KB) keep the RFC-safe default. Raise `rmem_max`/`wmem_max` to enable it. Setting `FELIX_INITIAL_MTU` explicitly disables the loopback special case and applies to every path.

**Type**: Positive integer (bytes, clamped to 1200–65527)

**Default**: `1200` (loopback peers: `16336` on macOS, `4096` elsewhere)

```bash
export FELIX_INITIAL_MTU="1200"
```

### `FELIX_MTU_BLACK_HOLE_COOLDOWN_MS`

**Description**: How long a connection waits after an MTU black-hole verdict before re-probing for a larger MTU. Quinn's black-hole detector cannot distinguish a path that silently drops large packets from a congestive loss burst that happened to contain only full-MTU packets (which is what overflowing the peer's UDP socket buffer looks like at high rate). A false verdict collapses the path MTU to the initial value, multiplying datagram and syscall counts per byte by ~13× on a 16 KiB-MTU path, and quinn's stock 60-second cooldown then pins that state. Felix shortens the cooldown so a spurious collapse re-probes at the connection's next idle gap. Quinn only sends recovery probes when the connection has nothing else to transmit, so a sender with a continuous backlog cannot recover until its load has a gap regardless of this setting. That is why loopback connections start at full MTU instead (see `FELIX_INITIAL_MTU`).

**Type**: Positive integer (milliseconds, minimum 100)

**Default**: `2000`

```bash
export FELIX_MTU_BLACK_HOLE_COOLDOWN_MS="2000"
```

### `FELIX_INITIAL_CWND`

**Description**: Optional initial congestion window override in bytes. By default Felix keeps Quinn's RFC 9002 behavior, including raising the minimum window to two datagrams after path-MTU discovery. Increase only on trusted low-loss paths where a larger initial burst is acceptable.

**Type**: Positive integer (bytes)

**Default**: Quinn's RFC 9002 default

```bash
export FELIX_INITIAL_CWND="1048576"
```

### `FELIX_UDP_SEND_BUFFER` / `FELIX_UDP_RECV_BUFFER`

**Description**: Requested UDP socket buffer sizes (SO_SNDBUF / SO_RCVBUF). Applied best-effort: halved until the OS accepts. Kernel-level datagram drops surface as QUIC retransmits and tail-latency spikes, so large buffers matter at high message rates.

**Type**: Positive integer (bytes)

**Default**: `8388608` (8 MiB)

```bash
export FELIX_UDP_SEND_BUFFER="8388608"
export FELIX_UDP_RECV_BUFFER="8388608"
```

### `FELIX_MAX_UDP_PAYLOAD`

**Description**: Largest UDP datagram the endpoint accepts (receive side). Must be at least the peer's discovered MTU or large datagrams are rejected.

**Type**: Positive integer (bytes, clamped to 1200–65527)

**Default**: `65527`

```bash
export FELIX_MAX_UDP_PAYLOAD="65527"
```

### `FELIX_IO_RUNTIME_THREADS`

**Description**: Size of the dedicated QUIC I/O runtime pool. Quinn's driver tasks (endpoint receive loop, per-connection transmit/ACK loops) do a bounded slice of work per poll and reschedule themselves, so their scheduler re-poll latency is the transport's throughput ceiling. Felix therefore runs them on a pool of single-threaded runtimes isolated from application tasks, assigned by role: server endpoints spread across every runtime but the last, client endpoints share the last. `0` disables the isolation and runs drivers on the application runtime. A larger pool cannot make a single endpoint faster (an endpoint's driver is one task on one runtime), and it splits endpoints that talk to each other onto separate threads, which measured 5–6× slower.

:::caution[A macOS optimization, off by default elsewhere]
The ceiling this pool removes is specific to macOS. On Linux the same
benchmark already sustains ~643 MB/s (628 K msg/s × 1 KiB) without it,
more than macOS reaches even with the pool, and isolating the drivers there
only adds a cross-thread hop per datagram. Measured on Linux: p50 latency
86 µs → 152 µs and fanout-10 throughput 1.48 M → 1.09 M msg/s, consistent
across pool sizes 1/2/4/8 and with pump colocation on or off. Non-macOS
platforms therefore default to `0`. Set the variable explicitly to
experiment.
:::

**Type**: Non-negative integer

**Default**: derived on macOS as one runtime per server endpoint plus one for
clients, so a broker with one client listener gets `2`, and a cluster member
with four client listeners gets `6` (four client listeners, the internal
listener, and the client runtime). `0` elsewhere. It follows the listener
count whether that was set or derived, so the two defaults never conflict.

```bash
export FELIX_IO_RUNTIME_THREADS="2"
export FELIX_IO_RUNTIME_THREADS="0"   # disable driver isolation
```

:::danger[Do not set this below one runtime per listener]
The pool is derived from the listener count for a reason. Server endpoints
are assigned `sequence % (pool_len - 1)`, so a pool of 2 gives every one of
them runtime `0`: every listener's driver on a single thread, which is exactly
the single feeder [`FELIX_QUIC_LISTENERS`](#felix_quic_listeners) exists to
escape.

Each setting is harmless alone. `FELIX_IO_RUNTIME_THREADS=1` isolates a driver.
`FELIX_QUIC_LISTENERS=4` asks for four. Together they collapse the four onto
one, and the only symptom is throughput that does not improve, which reads as
the extra listeners being pointless rather than as a misconfiguration.

The broker refuses to start on that combination rather than let you find out
from a benchmark:

```
FELIX_IO_RUNTIME_THREADS (2) is too small for FELIX_QUIC_LISTENERS (4): this
broker binds 4 server endpoints and every driver past the first would share a
thread with another, which is the single feeder several listeners exist to
escape. Use 5, or 0 to put drivers on the app runtime
```

Leave it unset unless you are experimenting. The derived value is correct by
construction.
:::

:::caution[One client process cannot measure a broker's ceiling on macOS]
Every client endpoint in a process shares the pool's last runtime, which is
one thread. That is deliberate: a client's publish and event endpoints carry
two halves of one request/response flow, and splitting them costs two
cross-thread wakes per message (measured ~123 K msg/s co-located against
~21 K spread). The consequence is that a single process on macOS cannot send
faster than one thread can drive, however many clients, sockets or publisher
tasks it creates. A sweep of listener counts or broker settings from one
generator process comes back flat whether or not the change works.

To measure transport scaling, generate load from several processes (on
Linux, from separate machines), as `scripts/perf/azure` does. Linux defaults
this pool to `0`, so the cap does not apply there.
:::

### `FELIX_ACK_ELICITING_THRESHOLD`

**Description**: How many ack-eliciting packets a peer may receive before it must send an ACK (QUIC ACK-frequency extension, between quinn peers). The RFC default of every other packet costs a reverse-path datagram, plus its wakeup chain, per ~2 datagrams of data. The higher default trades a little loss-detection latency (bounded by the 2 ms `max_ack_delay` Felix also negotiates) for measurably less per-byte wakeup traffic (~+15% throughput on loopback).

**Type**: Positive integer (packets)

**Default**: `20`

```bash
export FELIX_ACK_ELICITING_THRESHOLD="20"
export FELIX_ACK_ELICITING_THRESHOLD="1"   # RFC-like cadence
```

### `FELIX_ACK_FREQ_DISABLE`

**Description**: Set to any value to skip negotiating the ACK-frequency extension entirely, restoring stock quinn ACK behavior (25 ms max ack delay, ACK every other packet).

**Type**: Presence toggle

**Default**: unset (extension negotiated)

```bash
export FELIX_ACK_FREQ_DISABLE="1"
```

### `FELIX_CONN_STATS_MS`

**Description**: Log live `quinn::ConnectionStats` (path MTU, cwnd, rtt, loss, congestion events, blocked-frame counters, UDP datagram/byte/io counts) for every connection on this interval, on both the broker and the client. The client-side log is the only place the publish path's sender-side congestion state is visible. Diagnostic, and off unless set.

**Type**: Positive integer (milliseconds)

**Default**: unset (disabled)

```bash
export FELIX_CONN_STATS_MS="1000"
```

## Performance and Monitoring

### `FELIX_DISABLE_TIMINGS`

**Description**: Disable per-stage timing collection.

**Type**: Boolean

**Default**: `false`

**Accepted values**: `1`, `true`, `yes` = disabled

**Example**:
```bash
export FELIX_DISABLE_TIMINGS="false"  # Enable timings
export FELIX_DISABLE_TIMINGS="true"   # Disable for performance
export FELIX_DISABLE_TIMINGS="1"
```

**Trade-off**:
- `false`: Detailed metrics, slight overhead
- `true`: Maximum performance, no timing data

### `FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS`

**Description**: Timeout for control stream drain (broker).

**Type**: Positive integer (milliseconds)

**Default**: `50`

**Example**:
```bash
export FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS="50"
export FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS="100"  # More graceful
export FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS="20"   # Faster shutdown
```

### `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS`

**Description**: Total budget for draining in-flight work after SIGTERM or SIGINT,
before remaining tasks are force-cancelled. Applies to both the broker and the
control plane. This is a single budget shared by every subsystem, not a per-subsystem
timeout, so total shutdown time stays bounded by this value.

**Type**: Positive integer (milliseconds)

**Default**: `25000`

**Example**:
```bash
export FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS="25000"
export FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS="55000"  # With terminationGracePeriodSeconds: 60
export FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS="5000"   # Fast rollouts, short-lived requests
```

**Note**: Keep this below the platform's kill deadline (Kubernetes'
`terminationGracePeriodSeconds`, default `30`) so the drain finishes and logs its
outcome before SIGKILL. See [Graceful Shutdown](/felix/deployment/graceful-shutdown/).

### `FELIX_SHUTDOWN_PREDRAIN_MS`

**Description**: How long the control plane or broker keeps serving after it starts
reporting unready, before it stops accepting connections. Readiness-first shutdown only helps
if something has time to act on it: a load balancer learns an instance is draining by
polling, so closing the listener the moment readiness flips leaves requests still
being routed to a socket that is gone.

**Applies to**: Control plane and broker.

**Type**: Non-negative integer (milliseconds); `0` skips the wait.

**Default**: `5000` on the control plane and `0` on the broker, whose Helm chart covers
propagation with a preStop sleep instead (waiting twice only shortens the drain)

**Example**:
```bash
export FELIX_SHUTDOWN_PREDRAIN_MS="5000"
export FELIX_SHUTDOWN_PREDRAIN_MS="15000" # readinessProbe periodSeconds 5 x failureThreshold 3
export FELIX_SHUTDOWN_PREDRAIN_MS="0"     # single instance, nothing routing to it
```

**Note**: Size it above the prober's `periodSeconds` multiplied by its
`failureThreshold`, so the load balancer has actually removed this instance before the
listener closes. It is spent *inside* the platform's kill deadline, so
`terminationGracePeriodSeconds` must cover this plus
`FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS`. A second SIGTERM ends the wait early.

### `FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS`

**Description**: How long a clustered broker spends handing its shards to other
brokers after SIGTERM, before it closes its listener and drains. With readiness
already off, it asks the control plane to drain it and keeps serving until it leads
no shard, so each shard is moved rather than failed over and a rolling restart
refuses no publish. Skipped when the broker leads nothing, when no other broker can
take its shards, or when the control plane does not answer within 5 s. A second
SIGTERM ends the wait. Shards still led when it expires fail over as they would
without it.

**Applies to**: Broker, when it is a cluster member (`FELIX_CONTROLPLANE_URL` and
membership configured).

**Type**: Non-negative integer (milliseconds); `0` turns the handoff off.

**Default**: `30000`

**Example**:
```bash
export FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS="30000"
export FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS="120000"  # Brokers leading many shards
export FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS="0"       # Fail over instead, e.g. a single broker
```

**Note**: Spent before `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS`, and inside the platform's
kill deadline, so `terminationGracePeriodSeconds` must cover both. Moves are paced by
`FELIX_SHARD_MOVES_MAX_CONCURRENT` and `FELIX_SHARD_MOVES_MAX_PER_NODE` like any
other. See [Graceful Shutdown](/felix/deployment/graceful-shutdown/#handing-shards-off).

### `FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS`

**Description**: Inbound peer connections this broker holds at once, across all
peers. Over the limit, a connection is refused rather than queued. A peer told
no can back off, where one left waiting cannot tell a busy broker from a stuck
one.

**Type**: Integer

**Default**: `512`

### `FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE`

**Description**: Inbound peer connections from any one address. The total alone
does not stop one peer consuming the whole allowance, which is the case that
matters: a peer looping on a reconnect bug starves the rest of the cluster
before anyone notices.

Startup refuses a value above
`FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS`, since the per-source limit would then
never be the one that applies.

**Type**: Integer

**Default**: `16`

## Configuration File

### `FELIX_BROKER_CONFIG`

**Description**: Path to YAML configuration file.

**Type**: String (file path)

**Default**: `/usr/local/felix/config.yml` (optional)

**Example**:
```bash
export FELIX_BROKER_CONFIG="/etc/felix/broker.yml"
export FELIX_BROKER_CONFIG="/tmp/felix-dev.yml"
```

**Behavior**:
- If set and file missing: **error**
- If not set and default missing: **continue with defaults**
- **A key the broker does not know is an error**, naming the key. A setting
  that looks like it is in effect and is not is worse than a refusal, so a
  misspelled key stops startup rather than silently leaving the default in
  place. The same is true of the control plane's config file, nested sections
  included.

## Seeing what is in effect

```bash
felix-broker --print-config
```

Prints the configuration the broker would run with (defaults, the config file,
and the environment already folded together) as YAML, and exits. Nothing is
bound, so it is safe to run on a node that is already serving.

It doubles as a pre-flight check. The config is loaded exactly as startup
loads it, so a file that will not parse, or a key the broker does not know,
fails here with the same message it would have produced on the node, before a
rollout rather than during one.

The node credential is shown as `<redacted>`, or `<unset>` when there is none:
this output is meant to be pasted into an issue, and whether a token is set is
exactly what someone debugging a registration failure needs to see. Warnings
about unrecognised variables go to stderr, so `--print-config > current.yml`
gives a clean document and still shows them.

## Settings that are wrong together

Each variable validates its own value where it is parsed. Some pairs are each
fine alone and wrong in combination, and those are refused at startup too,
because the failure they produce otherwise is behaviour nobody configured, with
no error to explain it:

| Refused when | Why |
|---|---|
| `event_batch_max_bytes` > `max_frame_bytes` | The broker would send subscribers frames larger than it will itself accept, and a client applying the same limit drops them |
| `pub_conn_inflight_bytes` > `pub_inflight_bytes` | The per-connection limit could never be the one that applies, so one connection may take the whole broker-wide allowance |
| `cache_stream_recv_window` > `cache_conn_recv_window` | A single stream can never reach its own window, because the connection's runs out first |
| `FELIX_INTERNAL_BIND` shares a port with `FELIX_QUIC_BIND` | The internal and client-facing listeners must be separate |

Equal is allowed everywhere: these bound each other and do not have to differ.

`felix-broker --print-config` runs the same checks without starting anything,
so a bad combination fails before a rollout rather than on the node.

## Typos in variable names

A misspelled variable cannot be refused the same way, because the process cannot
tell a typo from a variable meant for something else sharing the container. So
both binaries warn instead, at startup, naming every `FELIX_*` variable that is
set and that nothing reads:

```text
WARN FELIX_METRICS_BIND is set and nothing reads it — did you mean one of
     FELIX_BROKER_METRICS_BIND, FELIX_CONTROLPLANE_METRICS_BIND? Those
     settings are using their defaults
```

The suggestion looks for a missing segment first and a misspelling second, so
a plausible-but-wrong shorter name (the mistake someone makes without noticing)
is matched to the real one. Nothing close enough means no suggestion rather
than the nearest arbitrary name.

## Logging

### `RUST_LOG`

**Description**: Rust logging filter (not Felix-specific but commonly used).

**Type**: String (filter expression)

**Default**: Varies by build

**Example**:
```bash
export RUST_LOG="info"
export RUST_LOG="debug"
export RUST_LOG="felix_broker=debug,felix_wire=trace"
export RUST_LOG="warn"
```

**Levels**: `error`, `warn`, `info`, `debug`, `trace`

## Performance Profiles

### Balanced Profile

```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_RECV_WINDOW="268435456"
export FELIX_EVENT_STREAM_RECV_WINDOW="67108864"
export FELIX_EVENT_SEND_WINDOW="268435456"
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_STREAMS_PER_CONN="4"
export FELIX_DISABLE_TIMINGS="0"
```

### High Memory Profile

```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_RECV_WINDOW="536870912"
export FELIX_EVENT_STREAM_RECV_WINDOW="134217728"
export FELIX_EVENT_SEND_WINDOW="536870912"
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_STREAMS_PER_CONN="4"
export FELIX_DISABLE_TIMINGS="1"
```

### Low Latency Profile

```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="1"
export FELIX_EVENT_BATCH_MAX_DELAY_US="50"
export FELIX_FANOUT_BATCH="16"
export FELIX_DISABLE_TIMINGS="1"
```

### High Throughput Profile

```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="256"
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"
export FELIX_FANOUT_BATCH="128"
export FELIX_DISABLE_TIMINGS="1"
```

## Durable Storage Configuration

Durable stream storage is opt-in. With `FELIX_DURABLE_STORAGE_DIR` unset the
broker is in-memory only, and any stream the control plane marks `durable: true`
is **rejected at registration** rather than silently downgraded to a guarantee
the broker cannot keep.

See [Durable Storage](/felix/architecture/durable-storage/) for what each policy
guarantees and what it costs.

### `FELIX_DURABLE_STORAGE_DIR`

**Description**: Root directory for durable stream segments. Setting it enables
durable streams. One subdirectory is created per stream shard.

**Type**: Path

**Default**: unset (durable storage disabled)

**Example**:
```bash
export FELIX_DURABLE_STORAGE_DIR="/var/lib/felix/streams"
```

### `FELIX_DURABLE_FSYNC_MODE`

**Description**: When written bytes are pushed to the storage device.

**Type**: One of `none`, `periodic`, `on_commit`

**Default**: `periodic`

| Value | Acknowledged when | Loss window |
| --- | --- | --- |
| `none` | bytes reach the page cache | unbounded. Survives a process crash, not a power loss |
| `periodic` | bytes reach the page cache | one flush interval |
| `on_commit` | bytes reach the device | none |

**Example**:
```bash
export FELIX_DURABLE_FSYNC_MODE="on_commit"
```

**Trade-off**: `on_commit` costs one device flush per commit (~4ms on typical
NVMe), amortised across concurrent publishers by group commit. `periodic` adds
no measurable append latency at all.

### `FELIX_DURABLE_FSYNC_INTERVAL_MS`

**Description**: Flush interval for `FELIX_DURABLE_FSYNC_MODE=periodic`. Bounds
how much acknowledged data a machine crash can lose.

**Type**: Positive integer (milliseconds)

**Default**: `250`

**Example**:
```bash
export FELIX_DURABLE_FSYNC_INTERVAL_MS="100"
```

**Note**: Setting this without setting the mode implies `periodic`. Zero is
rejected at startup. It is a busy loop, not "always sync". Use `on_commit` for
per-commit durability.

### `FELIX_DURABLE_SEGMENT_BYTES`

**Description**: Size at which the active segment rolls over to a new file.

**Type**: Positive integer (bytes)

**Default**: `268435456` (256 MiB)

**Example**:
```bash
export FELIX_DURABLE_SEGMENT_BYTES="67108864"  # 64 MiB
```

**Trade-off**: Smaller segments bound recovery time (only the active segment is
fully scanned at startup) at the cost of more files and more rollovers.

### `FELIX_DURABLE_RETENTION_BYTES`

**Description**: Delete the oldest sealed segments once a stream's log exceeds
this size. Unset means the log grows without bound, which is the default and the
pre-retention behaviour.

**Type**: Positive integer (bytes)

**Default**: unset (no size bound)

**Example**:
```bash
export FELIX_DURABLE_RETENTION_BYTES="10737418240"  # 10 GiB per stream shard
```

**Note**: The active segment is never deleted, so a log settles at roughly this
size and never below `FELIX_DURABLE_SEGMENT_BYTES` regardless of how small this
is set. Records below the retained range report `CursorTooOld` to a resuming
subscriber, naming the oldest offset still available.

### `FELIX_DURABLE_RETENTION_SECONDS`

**Description**: Delete sealed segments whose newest record is older than this.
Combines with `FELIX_DURABLE_RETENTION_BYTES`. Either bound alone is enough to
trigger a deletion.

**Type**: Positive integer (seconds)

**Default**: unset (no age bound)

**Example**:
```bash
export FELIX_DURABLE_RETENTION_SECONDS="604800"  # 7 days
```

**Note**: Age comes from the records' own timestamps rather than file mtime, so
restoring a backup does not reset it. A segment survives until its *newest*
record has expired, so nothing younger than the bound is ever deleted.

### `FELIX_DURABLE_RETENTION_INTERVAL_SECONDS`

**Description**: How often retention is evaluated. Ignored unless a retention
bound is set.

**Type**: Positive integer (seconds)

**Default**: `60`

**Example**:
```bash
export FELIX_DURABLE_RETENTION_INTERVAL_SECONDS="300"
```

**Trade-off**: Retention is bulk file deletion and runs on its own timer so it
never lands on a publish. A longer interval means disk usage overshoots the
bound for longer between passes.

### `FELIX_DURABLE_INDEX_SPACING_BYTES`

**Description**: Bytes of segment data between sparse index entries. A read
binary-searches the index, then scans forward at most one interval.

**Type**: Positive integer (bytes)

**Default**: `4096`

**Example**:
```bash
export FELIX_DURABLE_INDEX_SPACING_BYTES="8192"
```

**Trade-off**: Smaller spacing means faster seeks and larger index files.

### `FELIX_DURABLE_MAX_RECORDS_PER_READ`

**Description**: Ceiling on records returned by a single range read, on top of
the caller's byte budget. Payload bytes alone do not bound a response made of
empty records.

**Type**: Positive integer

**Default**: `10000`

**Example**:
```bash
export FELIX_DURABLE_MAX_RECORDS_PER_READ="5000"
```

### `FELIX_DURABLE_PREALLOCATE`

**Description**: Reserve a segment's blocks when it is created, keeping block
allocation off the append path.

**Type**: Boolean

**Default**: `true`

**Example**:
```bash
export FELIX_DURABLE_PREALLOCATE="false"
```

**Note**: Disable on filesystems where reservations are expensive or where thin
provisioning makes them counter-productive.

### `FELIX_STORAGE_IO_URING`

**Description**: Submit device flushes through `io_uring` (`IORING_OP_FSYNC`) on
one process-wide ring, instead of handing each one to the log's own flush
thread. Linux only.

**Type**: Boolean (`1` to enable)

**Default**: `0`

**Example**:
```bash
export FELIX_STORAGE_IO_URING="1"
```

**Note**: A kernel too old for the opcode, or a container that forbids the
syscall, falls back to the flush thread rather than failing. Durability must
not depend on an optimisation being available. The kernel still runs each sync
on a worker thread, so this is not faster than the flush thread for one log.

### `FELIX_STORAGE_COMPACTION_BYTES_PER_SEC`

**Description**: I/O budget for cache and counter compaction, in bytes per
second. Each store (caches, counters) paces its background compaction passes
against its own budget.

**Type**: Integer (bytes per second; `0` for unlimited)

**Default**: `67108864` (64 MiB/s)

**Example**:
```bash
export FELIX_STORAGE_COMPACTION_BYTES_PER_SEC="16777216"
```

**Note**: Compaction never delays a write, so this does not trade latency for
space. It bounds how hard compaction can lean on the device. Too low a budget
on a write-heavy cache lets the log grow further between passes. An
unparseable value falls back to the default with a warning.

### `FELIX_DURABLE_VERIFY_ALL_ON_OPEN`

**Description**: Checksum every record of every segment at startup.

**Type**: Boolean

**Default**: `false`

**Example**:
```bash
export FELIX_DURABLE_VERIFY_ALL_ON_OPEN="true"
```

**Trade-off**: Off by default because startup would otherwise cost one full pass
over all data on disk. The active segment is always fully scanned regardless, and
every read verifies the records it returns, so bit rot in cold data is still
caught, just when it is read rather than at boot.

## Next Steps

- **Full configuration details**: [Configuration Reference](/felix/reference/configuration/)
- **Troubleshooting**: [Troubleshooting Guide](/felix/reference/troubleshooting/)
- **Performance tuning**: [Performance Guide](/felix/features/performance/)

## Reference: every remaining variable

The sections above cover the variables most deployments touch, each with an
example and the reasoning. What follows is the rest, in brief, so the page is
complete. `scripts/check_env_reference.py` fails the build if a variable
exists in the code and is not named here.

Variables used only by benchmarks, demos and the test harness are deliberately
absent. They are listed in that script rather than here.

### Control plane

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_CONTROLPLANE_BIND` | `0.0.0.0:8443` | Address the control-plane API listens on. |
| `FELIX_CONTROLPLANE_TLS_CERT` | unset | PEM certificate chain the API listener serves. Set with the key below, or neither. Without them the API is plain HTTP and startup warns. Re-read every 30s, so a renewal on disk reaches the next handshake. The Raft peer listener is separate and has its own `FELIX_RAFT_TLS_CERT`, `_KEY` and `_CA`. |
| `FELIX_CONTROLPLANE_TLS_KEY` | unset | PEM private key for `FELIX_CONTROLPLANE_TLS_CERT`. |
| `FELIX_CONTROLPLANE_METRICS_BIND` | `0.0.0.0:8080` | Separate address for the metrics endpoint. |
| `FELIX_CONTROLPLANE_CONFIG` | unset | Path to a YAML config file. Values in the file override the environment. |
| `FELIX_CONTROLPLANE_STORAGE_BACKEND` | inferred | `memory`, `postgres` or `raft`. Unset, it is `postgres` when `FELIX_CONTROLPLANE_POSTGRES_URL` is set, `raft` when `FELIX_RAFT_NODE_ID`, `FELIX_RAFT_DATA_DIR` and `FELIX_RAFT_PEERS` are set, and `memory` otherwise. `memory` loses everything on restart. An unrecognised value is treated as unset. |
| `FELIX_CONTROLPLANE_POSTGRES_URL` | unset | Connection string. Required when the backend is `postgres`. |
| `FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS` | `10` | Pool size. Caps concurrent database work. |
| `FELIX_CONTROLPLANE_POSTGRES_CONNECT_TIMEOUT_MS` | `5000` | Bounds establishing a new physical connection. |
| `FELIX_CONTROLPLANE_POSTGRES_ACQUIRE_TIMEOUT_MS` | `5000` | Bounds waiting for a pooled connection before failing fast. |
| `FELIX_CONTROLPLANE_CHANGES_LIMIT` | `1000` | Maximum changes returned by one changefeed page. |
| `FELIX_CONTROLPLANE_CHANGE_RETENTION_MAX_ROWS` | `10000` | Bounds the append-only change tables. Smaller means a watcher can fall behind sooner and need a fresh snapshot. |
| `FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS` | `ES256` | Comma-separated JWS algorithms accepted from an upstream IdP: any of `ES256`, `RS256`, `RS384`, `RS512`, `PS256`, `PS384`, `PS512`. |
| `FELIX_CONTROLPLANE_OIDC_ALLOW_INSECURE_HTTP` | `false` | Allow plain-HTTP IdP discovery and JWKS URLs on any host. Without it only `https` is accepted, plus `http` on a loopback host. For development: a JWKS fetched over plain HTTP can be replaced in transit, and whoever replaces it can mint tokens for the tenant. Also implies `FELIX_CONTROLPLANE_OIDC_ALLOW_PRIVATE_IDP`. |
| `FELIX_CONTROLPLANE_OIDC_ALLOW_PRIVATE_IDP` | `false` | Allow IdP discovery and JWKS URLs that are, or resolve to, private, link-local or unique-local addresses. Without it those are refused (loopback is always allowed), so an issuer config cannot make the control plane fetch from internal services or a cloud metadata endpoint. |
| `FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE` | `false` | Also accept `aud: felix-broker` tokens on the control plane's API, which otherwise takes only `felix-controlplane` ones. For migrating callers. It lets a broker replay a client's token against the API. |
| `FELIX_CONTROLPLANE_LEGACY_UNSCOPED_GROUPS` | `false` | Also link each IdP group under its bare name (`group:{name}`) as well as its issuer-scoped one (`group:{issuer}#{name}`), while groupings are migrated. It lets any IdP the tenant trusts claim any bare group name, so turn it off afterwards. |
| `FELIX_EXCHANGE_TOKEN_TTL_SECONDS` | `900` | Lifetime of a Felix access token minted by the token exchange. The default is short to limit blast radius if a token leaks. Prefer refresh over raising it: a long-running process should refresh rather than hold one long-lived bearer token. |
| `FELIX_REFRESH_TOKEN_TTL_SECONDS` | `2592000` | Lifetime of a refresh token (30 days). This is how a long-running process stays authenticated without standing IdP credentials. Refresh tokens are single-use and rotate on every refresh, so this bounds a stolen and never used token. One that is used produces a replay, which revokes its whole chain immediately. |
| `FELIX_RAFT_NODE_ID` | unset | This instance's id in the metadata Raft group (see [Metadata Raft](/felix/architecture/metadata-raft/)). `FELIX_RAFT_NODE_ID`, `FELIX_RAFT_DATA_DIR` and `FELIX_RAFT_PEERS` together select the raft backend, or startup fails on a partial set. |
| `FELIX_RAFT_DATA_DIR` | unset | Where the Raft log, vote, and snapshots live. Must survive restarts: it is what makes a restart a rejoin rather than a fresh member. |
| `FELIX_RAFT_PEERS` | unset | The initial group as `id=host:port,...` of every member's Raft peer listener (`FELIX_RAFT_BIND_ADDR`). Identical on every member. |
| `FELIX_RAFT_BIND_ADDR` | unset | Required under raft. Where this member serves the Raft RPCs. A listener of its own, never the API port: the `propose` route can replace the whole metadata store. Keep it reachable from the other members only. |
| `FELIX_RAFT_CLUSTER_ID` | unset | Required under raft. Names the group, like etcd's cluster token. Recorded in the data dir on first start. A member refuses to start on a data dir recorded under another id, and peers refuse requests carrying another id. |
| `FELIX_RAFT_PEER_TOKEN` | unset | Required under raft, at least 32 characters, identical on every member. Every Raft request must carry it, and it is compared in constant time. It is the cluster-admin credential: whoever holds it can replace the store, so keep it in a Secret and give it to nothing but the members and the `migrate import` tool. |
| `FELIX_RAFT_INSECURE_PEERS` | `false` | `true` lets a raft member start without `FELIX_RAFT_PEER_TOKEN`. For a throwaway local group only. Anyone who reaches the peer listener can then replace the store. |
| `FELIX_RAFT_INITIAL_CLUSTER_STATE` | `existing` | `new` lets an empty member form a brand-new group when a majority of the configured members is empty. `existing` never forms one, so members that lost their volumes wait instead of starting an empty control plane. Set `new` for the first start of a cluster only. |
| `FELIX_RAFT_TLS_CERT` | unset | PEM certificate this member presents on the peer listener and to its peers. Set with `FELIX_RAFT_TLS_KEY` and `FELIX_RAFT_TLS_CA` to turn on peer mTLS. A partial set fails startup. The certificate must name the host each member reaches it by in `FELIX_RAFT_PEERS`. |
| `FELIX_RAFT_TLS_KEY` | unset | PEM private key for `FELIX_RAFT_TLS_CERT`. |
| `FELIX_RAFT_TLS_CA` | unset | PEM CA bundle. Peers must present a certificate signed by it, and only it is trusted for the servers this member connects to. |
| `FELIX_RAFT_HEARTBEAT_MS` | `150` | Leader heartbeat interval within the metadata group. |
| `FELIX_RAFT_ELECTION_TIMEOUT_MIN_MS` | `600` | Lower edge of the election window. Must exceed the heartbeat. A window at or below it elects against healthy leaders, and startup refuses it. |
| `FELIX_RAFT_ELECTION_TIMEOUT_MAX_MS` | `1200` | Upper edge of the election window. Must exceed the minimum. |
| `FELIX_RAFT_SNAPSHOT_LOGS_SINCE_LAST` | `500` | Snapshot after this many log entries. Metadata state is small, so snapshots are cheap and the log stays short. |
| `FELIX_RAFT_LOGS_KEPT_BEHIND_SNAPSHOT` | `100` | Entries kept behind the snapshot so a briefly-lagging member catches up from the log rather than a snapshot install. |
| `FELIX_RAFT_WRITE_TIMEOUT_MS` | `10000` | Overall budget for one proposal, elections and forwarding included. "No quorum" becomes an error at this bound rather than a hang, and the API answers it `503 unavailable`. |
| `FELIX_READINESS_TIMEOUT_MS` | `2000` | Longest a readiness check may take before it counts as a failure. Keep it below the prober's own timeout so the reason is reported rather than lost. |
| `FELIX_READINESS_CACHE_TTL_MS` | `1000` | How long a readiness answer is reused. Bounds probe cost regardless of how many probers there are, and bounds how long recovery takes to become visible. |
| `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` | `25000` | Budget for draining in-flight requests after SIGTERM before tasks are cancelled. |
| `FELIX_SHUTDOWN_PREDRAIN_MS` | `5000` | How long to keep serving after readiness flips to draining, so load balancers remove this instance before the listener closes. `0` skips it. |
| `FELIX_REGION_ID` | `local` | Region this instance reports. |
| `FELIX_REGION_BRIDGES` | unset | Directional region allowlist as comma-separated `source>dest` pairs, e.g. `eu-west-1>us-east-1`. A two-way bridge is two pairs. On the control plane it says where a stream created with a `region` may have copies: its own region and any it has a bridge to. Unset keeps such a stream in its own region. Streams without a region are placed anywhere either way. A malformed pair fails startup. Set the same value on every broker. |
| `FELIX_BOOTSTRAP_ENABLED` | `false` | Enables the first-run bootstrap endpoints. Leave off once credentials exist. |
| `FELIX_BOOTSTRAP_TOKEN` | unset | Token the bootstrap endpoints require. |
| `FELIX_BOOTSTRAP_TOKEN_PREVIOUS` | unset | The token being rotated out, still accepted alongside the current one so a rotation is a rolling deploy rather than an outage. Requires `FELIX_BOOTSTRAP_TOKEN`. |
| `FELIX_BOOTSTRAP_BIND_ADDR` | `127.0.0.1:9095` | Restricts bootstrap to a separate listener. |
| `FELIX_BOOTSTRAP_TLS_CERT` | unset | PEM certificate chain the bootstrap listener presents. All three TLS variables together, or startup fails. A partial set is a misconfiguration, not "TLS off". |
| `FELIX_BOOTSTRAP_TLS_KEY` | unset | PEM private key for the bootstrap listener's certificate. |
| `FELIX_BOOTSTRAP_TLS_CLIENT_CA` | unset | PEM CA bundle. Only clients presenting a certificate signed by it can complete the TLS handshake with the bootstrap listener. |

### Node identity and membership

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_NODE_ID` | unset | This broker's identity in the cluster. Must be stable across restarts. |
| `FELIX_NODE_ADVERTISE_ADDR` | unset | Address peers should reach this broker on. |
| `FELIX_CLIENT_ADVERTISE_ADDR` | unset | Address *clients* should reach it on, when it differs from the peer address. |
| `FELIX_NODE_ZONE` | unset | Failure domain this broker is in within its region, such as an availability zone or a rack, sent with its registration as the node's `zone`. Placement puts each shard's copies in different zones wherever a broker with room is in one the shard lacks, and moves and drains keep that spread. Unset (or blank) means no zone: the broker is treated as sharing a zone with no other. Takes effect when the broker next registers. |
| `FELIX_KAFKA_LISTEN` | unset | `ip:port` the Kafka-protocol listener binds (Kafka consumers and producers). Unset turns the listener off. See [Kafka compatibility](/felix/features/kafka/). |
| `FELIX_KAFKA_ADVERTISE_ADDR` | `FELIX_KAFKA_LISTEN` | `host:port` Kafka clients are told to connect to for this broker (in Metadata responses). A hostname is fine. Registered as the node's `kafka_addr`, and ignored while `FELIX_KAFKA_LISTEN` is unset. |
| `FELIX_KAFKA_TLS` | `true` | Serve TLS on the Kafka listener with the broker's client certificate, so clients connect with `SASL_SSL`. `false` means `SASL_PLAINTEXT`: tokens cross the network in clear text. |
| `FELIX_KAFKA_ANONYMOUS_TENANT` | unset | Development switch: a Kafka connection that does not authenticate reads and writes every stream of this tenant. Leave unset in production. |
| `FELIX_KAFKA_DEFAULT_NAMESPACE` | unset | Namespace a topic name without a dot is looked up in. Unset means such a topic names nothing. |
| `FELIX_KAFKA_MAX_CONNECTIONS` | `1024` | Kafka connections served at once. The next one is closed on arrival and counted in `felix_kafka_refused_total{reason="connection_limit"}`. |
| `FELIX_KAFKA_MAX_CONNECTIONS_PER_IP` | `128` | Kafka connections one source IP may hold, checked before the total, so one host cannot take every slot. Refusals are counted in `felix_kafka_refused_total{reason="per_ip_limit"}`. `0` is unlimited. |
| `FELIX_KAFKA_AUTH_TIMEOUT_MS` | `10000` | How long a Kafka connection has to finish SASL. Until it does, each request is capped at 64 KiB. A connection past the deadline is closed and counted under `reason="auth_timeout"`, an oversized unauthenticated request under `reason="unauthenticated_frame_size"`. Does not apply with `FELIX_KAFKA_ANONYMOUS_TENANT`, where every connection starts authenticated. |
| `FELIX_REGION_BRIDGES` | unset | The same allowlist as the control plane's. A broker forwards a request to a shard's leader only in its own `FELIX_REGION_ID` or a region it has a bridge to, and refuses the rest as `shard_unavailable` with reason `region_not_routable`. Unset forwards within the broker's own region only. A malformed pair fails startup. |
| `FELIX_NODE_TOKEN` / `FELIX_NODE_TOKEN_FILE` | unset | Access credential this broker presents to the control plane, on every call including the metadata feeds it seeds from (which require `node.view:cluster:*`). Required with `FELIX_NODE_ID`. A standalone broker may omit it, but then its sync is refused and it says so at startup. The file form is re-read every 30s, so whatever mints the credential (a Vault agent, SPIRE, a sidecar) can rotate it without a restart. A replacement that is already expired is declined rather than adopted. A broker joining a cluster refuses to start with an expiring token supplied by value and no refresh file, because nothing could then renew it. |
| `FELIX_NODE_REFRESH_TOKEN_FILE` | unset | Path to this broker's refresh token. With it the broker re-mints its access token before expiry and stays registered indefinitely. It is a path, not a value: refreshing spends the token and mints a replacement, so the broker writes the replacement back here. A restart that presented a spent one would be read as a replay and revoke the whole chain. The path must be writable. Setting `FELIX_NODE_REFRESH_TOKEN` instead fails startup, rather than locking the broker out at its first restart. Either this or `FELIX_NODE_TOKEN_FILE` is required when the credential carries an `exp` and `FELIX_NODE_ID` is set. |
| `FELIX_NODE_HEARTBEAT_INTERVAL_MS` | `5000` | How often a broker reports itself alive. |
| `FELIX_NODE_EXPIRY_TIMEOUT_MS` | `15000` | Silence after which a node is considered gone, plus `FELIX_NODE_REGRANT_MARGIN_MS`. The sweep also waits this long after the control plane starts, becomes Raft leader, or regains its store, so an outage does not expire the fleet. Brokers take 0.75 × this as their lease. Placement will not promote a replica whose last report is older than roughly twice this. |
| `FELIX_NODE_EXPIRY_SWEEP_INTERVAL_MS` | `2000` | How often expiry is evaluated. |
| `FELIX_NODE_REGRANT_MARGIN_MS` | a quarter of `FELIX_NODE_EXPIRY_TIMEOUT_MS` | How much longer than the expiry timeout a silent node stays live before it is marked down and its shards are handed on. Covers clock drift between a broker and the control plane. Startup fails below a quarter of the timeout. The silence has to be seen on the sweeping instance's own monotonic clock too, so a wall-clock step cannot shorten it. |
| `FELIX_SHARD_RECONCILE_INTERVAL_MS` | `5000` | How often placement re-plans. Bounds how quickly a failover happens, and how quickly a shard move advances a step. A pass fails over at most 64 shards to nodes that hold no copy of them. The rest wait for the next pass. |
| `FELIX_SHARD_MOVES_MAX_CONCURRENT` | `1` | Copies in flight across the cluster: shard moves, and followers being replaced on a draining broker. Each is a full copy of a shard's log. `0` holds every move. |
| `FELIX_SHARD_MOVES_MAX_PER_NODE` | unset | Copies in flight into or out of any one broker. Unset or `0` is no limit beyond the cluster-wide one. |
| `FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS` | `1000` | How far behind the leader's tail a move's destination may be when the leader is fenced. The rest is copied before the cut-over, so this bounds the switch-over, not what is lost. A replacement follower takes the place of the one it replaces within the same bound; on a `Quorum` stream it must also hold what a majority of the replica set holds. |
| `FELIX_SHARD_MOVE_TIMEOUT_MS` | `1800000` | A move that has not reached its fence, or a follower replacement that has not caught up, this long after it started is abandoned and its slot goes to the next move. A fenced move is always finished. `0` never gives up. |
| `FELIX_SHARD_RESTORE_AFTER_MS` | `300000` | How long a follower's broker may be down or gone, or its copy reported halted by its leader, before placement replaces the copy on another live broker, bringing the shard back to its replication factor. Keep it above the longest broker restart. `0` never replaces a lost or halted follower; a replica set left short of its factor by a failover is still topped up. |
| `FELIX_CP_URL`, `FELIX_CP_SYNC_INTERVAL_MS` | unset | Short aliases used by the demos and cluster harness. |

### Peer protocol, between brokers

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_INTERNAL_BIND` | `0.0.0.0:5001` | Address for the broker-to-broker QUIC endpoint. Separate from the client one. |
| `FELIX_INTERNAL_TLS_CERT` | unset | PEM certificate chain this broker presents to peers, leaf first. Its DNS name must be the broker's `FELIX_NODE_ID`. Set with the two below, or none of the three. |
| `FELIX_INTERNAL_TLS_KEY` | unset | PEM private key for that certificate. Re-read with the certificate every 30s, so a renewal on disk is picked up by the next handshake without a restart. |
| `FELIX_INTERNAL_TLS_CA` | unset | PEM bundle every peer's certificate must chain to. With all three set, every peer connection is mutually authenticated and the certificate's name is checked against the node id in both directions. Without them the peer link is encrypted but unauthenticated, and a broker with `FELIX_NODE_ID` refuses to start unless `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`. |
| `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED` | `false` | Let a cluster member start without peer mTLS. Anything that can reach `FELIX_INTERNAL_BIND` is then treated as a broker and can forward writes and rebuild replicas, so set it only when a network policy keeps that port reachable from brokers alone. |
| `FELIX_INTERNAL_CONNS_PER_PEER` | `1` | Connections held to each peer. |
| `FELIX_INTERNAL_STREAMS_PER_CONN` | `4` | Multiplexed streams per peer connection, so one large forwarded batch does not block smaller requests. |
| `FELIX_INTERNAL_MAX_INFLIGHT` | `1024` | Outstanding requests allowed per peer. |
| `FELIX_INTERNAL_REQUEST_TIMEOUT_MS` | `5000` | Bounds one forwarded request. |
| `FELIX_INTERNAL_HANDSHAKE_TIMEOUT_MS` | `2000` | Bounds dialling a peer that is gone. |
| `FELIX_INTERNAL_FENCE` | `true` | Offer, answer and use the promotion fence (`docs/replication-design.md`, "Fencing a promotion"). `false` makes this broker look to its peers like one that predates the fence: it fences nothing when promoted, and a shard it replicates opens on the lease alone wherever it is promoted. It also stops the broker reporting the `majority_ack` and `lease_free_reads` fleet features, so neither can be finalized while it serves, and is refused at registration once it has been. A switch for backing the fence out, not a tuning knob. |
| `FELIX_INTERNAL_IDLE_TIMEOUT_MS` | `60000` | Idle timeout on a peer connection. |
| `FELIX_INTERNAL_RECONNECT_BASE_MS` | `50` | First reconnect backoff after losing a peer. |
| `FELIX_INTERNAL_RECONNECT_MAX_MS` | `5000` | Backoff ceiling. |
| `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` | `5000` | Longest a `Quorum` publish waits for a majority before failing. |
| `FELIX_QUORUM_READS` | `majority` | How a get or counter get on a replicated `Quorum` cache confirms this broker still leads once the fleet has finalized `lease_free_reads`: `majority`, a round of fences its replicas answer (`docs/replication-design.md`, "Reads without the lease"), or `lease`, the faster path that trusts the lease and is only as safe as the clocks. Before the finalize every read is on the lease whatever this says. Also bounds the round by `FELIX_PUBLISH_QUORUM_TIMEOUT_MS`. |
| `FELIX_REPLICATION_REBUILD_MAX_CONCURRENT` | `1` | Halted followers this broker rebuilds at once, across every shard it leads. `0` rebuilds nothing and leaves every halt to an operator. |
| `FELIX_REPLICATION_REBUILD_BYTES_PER_SEC` | `0` | Bytes per second a rebuilding follower is shipped at. `0` is unlimited. |
| `FELIX_SHARD_MOVE_HOLD_MS` | `2000` | How long a publish to a shard that is moving waits for the move to cut over before it is refused with `shard_unavailable` / `moving`. The wait happens before the publish is accepted, so nothing held is acknowledged. `0` refuses at once. |
| `FELIX_SHARD_MOVE_HOLD_MAX` | `1024` | How many publishes may wait on moving shards at once. Each keeps its payload in memory. Beyond this, a publish to a moving shard is refused at once. |
| `FELIX_SHARD_MOVE_BYTES_PER_SEC` | `0` | Bytes per second this broker ships to move destinations, across every shard it leads. Only a destination the quorum does not need is held to it, so a `Quorum` publish never waits on it. The remainder after the fence is not. `0` is unlimited. |
| `FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS` | `30000` | How long a stopping broker waits for its shards to move to other brokers before it closes its listener and drains. Shards still led when it expires fail over. `0` turns the handoff off. |

### Consumer groups

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_GROUP_VISIBILITY_TIMEOUT_MS` | `30000` | How long a consumer's claim on a record stands. |
| `FELIX_GROUP_MAX_ATTEMPTS` | `5` | Deliveries before a record is dead-lettered. |
| `FELIX_GROUP_MAX_WAIT_MS` | `30000` | Cap on a long-polling client's requested wait. |
| `FELIX_GROUP_MAX_IN_FLIGHT` | `10000` | Most records one group may have handed out and unsettled on a shard. A poll past it answers empty until room frees. |

### Durable storage tuning

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_DURABLE_ROLLOVER_THRESHOLD_PERCENT` | `100` | Percentage of the segment size at which the next segment is prepared in the background. `100` turns background rollover off: it measured worse than rolling inline where fsync reaches the device. |
| `FELIX_DURABLE_MAX_OVERSHOOT_PERCENT` | `100` | With background rollover on, how far past its size a segment may grow while its replacement is prepared. Past it, appends roll inline. It bounds the newest segment, which recovery scans in full. |
| `FELIX_DURABLE_REPAIR_CHECKSUM_TAIL` | `false` | Truncate a complete last record whose checksum fails, instead of refusing to start. A torn write and bit rot on an acknowledged record look the same, so the default refuses. Records cut short by end of file are repaired either way. |

### Client and transport

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_CLIENT_CONFIG` | unset | Path to a client config file. |
| `FELIX_AUTH_TENANT`, `FELIX_AUTH_TOKEN`, `FELIX_TOKEN` | unset | Credentials a client presents. `FELIX_TOKEN` is also the token `felix-controlplane admin` sends, unless `--token` is given. |
| `FELIX_KEEPALIVE_MS` | `2000` | QUIC keep-alive interval. Keep it well under the idle timeout, on both client and broker, or quiet connections are dropped. |
| `FELIX_MAX_IDLE_TIMEOUT_MS` | `6000` | QUIC idle timeout before a connection is dropped. Also how long a broker that died without closing (killed, unplugged) takes to be noticed, and so how long a publish in flight to it waits before a `ClusterClient` retries elsewhere. |
| `FELIX_EVENT_ROUTER_MAX_PENDING` | `16384` | Events buffered by the client's router before it applies backpressure. |
| `FELIX_SUB_DEDICATED_THREAD`, `FELIX_SUB_DEDICATED_QUEUE_CAPACITY` | unset | Give a subscription its own thread and queue. |
| `FELIX_SUB_DELIVERY_SHAPING`, `FELIX_SUB_EGRESS_CONNS` | unset | Delivery shaping and egress fan-out on the broker. |
| `FELIX_PUBLISH_SHARDING` / `FELIX_PUB_SHARDING` | unset | Route publishes to a worker by stream rather than round-robin. |
| `FELIX_PUMP_COLOCATE` | unset | Colocate the delivery pump with the publish worker for a stream. |
| `FELIX_WORKER_THREADS` | unset | Tokio worker threads. Defaults to the core count. |
| `FELIX_TIMING_SAMPLE_EVERY` | unset | Sample rate for timing histograms. |
| `FELIX_SERVICE_INSTANCE_ID` | unset | Instance identity reported in telemetry. |

### felixctl

`felixctl` reads these to override the current context. A flag overrides the
variable, and the variable overrides the context. See
[felixctl](/felix/getting-started/felixctl/).

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELIX_CLI_CONFIG` | `felixctl/config.toml` in the platform config directory | The file holding the contexts. |
| `FELIX_CONTEXT` | the file's current context | Context to use. |
| `FELIX_BROKERS` | unset | Broker addresses, `host:port`, comma-separated. |
| `FELIX_AUTH_TENANT` | unset | Tenant, the same variable the client reads. |
| `FELIX_NAMESPACE` | `default` | Namespace. |
| `FELIX_AUTH_TOKEN`, `FELIX_AUTH_TOKEN_FILE` | unset | Broker token, or a file holding it. |
| `FELIX_CONTROLPLANE_URL` | unset | Control-plane base URL, for the listing commands and shard owners. |
| `FELIX_CONTROLPLANE_TOKEN`, `FELIX_CONTROLPLANE_TOKEN_FILE` | unset | Control-plane token, or a file holding it. Brokers and the control plane accept different tokens. |
| `FELIX_CA_FILE` | platform trust store | PEM bundle broker certificates are checked against. |
| `FELIX_CLIENT_CERT_FILE`, `FELIX_CLIENT_KEY_FILE` | unset | Client certificate and key to present to brokers. |
| `FELIX_CONTROLPLANE_CA` | platform trust store | PEM bundle an `https://` control plane is checked against, as for brokers. |
| `FELIX_SERVER_NAME` | the first broker's host name, or `localhost` | TLS server name sent to brokers. |
