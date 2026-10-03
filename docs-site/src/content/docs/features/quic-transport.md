---
title: "QUIC Transport"
---

Felix clients speak QUIC to the broker. The optional
[Kafka listener](/felix/features/kafka/) is TCP, and so is the control plane's
REST API. Every QUIC connection is encrypted (TLS 1.3 is
part of the protocol, not a layer bolted on top), and many independent streams
multiplex over one connection without blocking each other. This page explains
what that buys Felix, how Felix uses QUIC's streams, and which knobs matter.

## Why QUIC

Two properties do most of the work.

**No head-of-line blocking between streams.** TCP delivers one ordered byte
stream, so when a packet is lost, everything behind it waits, even bytes that
belong to unrelated traffic and have already arrived. QUIC orders each stream
independently: a lost packet stalls only the stream whose data it carried.

![The same lost packet under TCP and under QUIC. Three logical streams share one connection and a packet belonging to stream 2 is lost. Under TCP all three streams stop being delivered until the retransmission arrives, because they share one ordered byte stream. Under QUIC only stream 2 stops, because each stream is ordered on its own.](/felix/diagrams/head-of-line.svg)

Nothing was lost for streams 1 and 3 in either case. Under TCP their bytes had
already arrived and simply could not be handed over, because the transport has
no way to say which bytes belong to which stream. This is why one Felix
connection can carry many
subscriptions without a retransmission on one delaying the others.

**Encryption and the handshake are combined.** A new QUIC connection is ready
in one round trip, versus two or three for TCP plus TLS, and there is no
unencrypted mode to misconfigure. This matters less for Felix than it
sounds, because clients pool and reuse connections, so the hot path never pays
connection setup.

QUIC also gives Felix per-stream *and* per-connection flow control (the
backpressure story below), and connection IDs that survive an IP change.
Felix does not currently do anything special with connection migration.

## How Felix uses streams

**Bidirectional streams** carry request/response traffic:

- The *control stream*: publish, subscribe, and acknowledgements. A client
  authenticates once on the stream, then multiplexes requests down it.
- *Cache streams*: `cache_get`/`cache_put` requests tagged with a
  `request_id`, several in flight per stream, answered on the same stream.
  Reusing streams avoids per-request stream setup, which keeps cache tail
  latency flat under concurrency.

**Unidirectional streams** carry one-way event delivery. After a subscribe,
the broker opens a fresh uni stream to the client and sends events down it,
one stream per subscription, so each subscription gets its own flow-control
window and its own backpressure. A slow subscription fills its own window; it
cannot touch another subscription's.

```mermaid
sequenceDiagram
    participant C as Client
    participant B as Broker

    C->>B: subscribe (on the bidirectional control stream)
    B-->>C: ok
    Note over B: Broker opens a unidirectional stream
    B->>C: event_stream_hello
    loop Event delivery
        B->>C: event_batch
        B->>C: event_batch
    end
```

## Measured behavior

All measured figures live on one page, [Benchmarks](/felix/features/benchmarks/),
so they cannot drift page to page.

:::caution[Connection count is not a throughput multiplier]
Throughput does not scale linearly with connection count. Additional
connections help only until the I/O runtime saturates, because each endpoint's
driver is a single task on a single runtime.

Add connections to isolate workloads and avoid head-of-line blocking, not to
multiply throughput. Measure your own shape with `latency-demo` before sizing.
:::

## Tuning

### Connection pools

The client keeps separate pools for publishing, events, and cache traffic, so
one kind of load cannot starve another of connections:

```rust
let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig {
    event_conn_pool: 8,
    cache_conn_pool: 8,
    publish_conn_pool: 4,
    ..ClientConfig::optimized_defaults(quinn)
};
```

Start with `optimized_defaults` and change pool sizes only off a measurement.
Each connection holds buffers and flow-control state, so pools trade memory
for isolation.

### Flow-control windows

Windows bound how much data may be in flight, per connection and per stream.
Bigger windows favor throughput (more in flight); smaller ones bound memory
and keep latency predictable. They are set as client config fields or
environment variables: `FELIX_EVENT_CONN_RECV_WINDOW`,
`FELIX_EVENT_STREAM_RECV_WINDOW`, `FELIX_EVENT_SEND_WINDOW`, and the
`FELIX_CACHE_*` equivalents. The full list, with defaults, is in the
[environment variable reference](/felix/reference/environment-variables/).

The memory bound is roughly what you would expect: the sum of each
connection's window plus each open stream's window. Size them against the
memory you are willing to spend on in-flight data.

The broker's own receive windows decide how much a client may send that the
broker has not read. Its client listeners use `FELIX_BROKER_PUB_CONN_RECV_WINDOW`
and `FELIX_BROKER_PUB_STREAM_RECV_WINDOW`, which default to the per-connection
publish budget (`FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES`, 16 MiB). When
ingress is full and `FELIX_PUB_INGRESS_WAIT` is on, the broker stops reading
publishes, so a publisher is held back after one more budget's worth instead of
filling hundreds of MiB of broker memory first. Larger windows delay that
backpressure and cost broker memory; smaller ones can cap throughput on a path
with a large bandwidth-delay product.

### Congestion control and ACK cadence

Felix uses quinn's default congestion controller, **CUBIC** (RFC 8312),
loss-based and safe on shared networks. Two Felix-level knobs sit on top:

- `FELIX_INITIAL_CWND`: optional initial-window override for trusted
  low-loss paths (skips the slow-start ramp). Measured workloads did not
  benefit, so the RFC default stands.
- ACK frequency: between quinn peers Felix negotiates the QUIC
  ACK-frequency extension: 2 ms max ACK delay (instead of 25 ms) and an ACK
  at most every 20 ack-eliciting packets (instead of every other). Delayed
  ACKs stall window-limited senders, and every reverse-path ACK costs a
  datagram plus its wakeup chain; the tuned cadence measured ~+15% sustained
  throughput. Override with `FELIX_ACK_ELICITING_THRESHOLD`, or disable the
  extension with `FELIX_ACK_FREQ_DISABLE=1`.

Where the transport runs matters as much as the algorithm: quinn's
driver tasks execute on dedicated single-threaded I/O runtimes
(`FELIX_IO_RUNTIME_THREADS`), isolated from application tasks, because their
scheduler re-poll latency, not congestion control, was the measured
throughput ceiling. See
[Concurrency internals](/felix/development/internals-concurrency/#the-quic-io-runtime).

### Per-connection path stats

Set `FELIX_CONN_STATS_MS` on the broker or in a Rust client's environment to
log path statistics (MTU, cwnd, RTT, loss, flow-control blocking) for healthy
connections on an interval.
This is the data that says whether a throughput problem is transport-side or
above it; it is off by default and costs nothing when unset.

## Troubleshooting

**Connection timeout.** QUIC is UDP; the most common cause is a firewall that
passes TCP and silently drops UDP on the broker port. Check that the UDP port
is open and the broker is bound where you think it is.

**Certificate validation failure** (`UnknownIssuer`). The client verifies the
broker's certificate against the platform trust store by default
(`quinn::ClientConfig::with_platform_verifier()`). A self-signed development
certificate needs its CA added to the client's root store; the demos and the
cluster harness show how.

**A stream blocked on flow control.** The receiver is not draining. For a
subscription, that usually means the application's receive loop is stuck;
for larger burst tolerance, raise the stream window. Non-zero
`stream_data_blocked` counts in the `FELIX_CONN_STATS_MS` output confirm
flow control is the constraint, and zero rules it out.

## What is deliberately not here

Felix does not use QUIC's unreliable datagrams, multipath, or 0-RTT
resumption today, and does not act on connection migration. Nothing in the
design rules them out; they are not built yet.
