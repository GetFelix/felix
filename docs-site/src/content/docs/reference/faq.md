---
title: "Frequently Asked Questions"
---

Answers here are kept consistent with the
[status table](/getting-started/what-felix-is-for/), which is the page
to trust when any two disagree.

## What is Felix?

A distributed data backend that serves streams (pub/sub), work queues
(consumer groups), and a key-value cache, all as readings of one replicated
append-only log, reached over QUIC. The design optimizes for predictable
tail latency, high fanout, and strict slow-consumer isolation. The
[overview](/getting-started/overview/) is the ten-minute version.

## Is Felix production-ready?

No. Felix is in early active development and has not been run in production
by anyone. Quite a lot works: multi-broker clusters, durable replicated
streams, quorum acknowledgement, failover, consumer groups, the log-backed
cache, and OIDC auth with RBAC. It is tested hard, including fault-injection
suites. Releases are tagged (the newest is v0.6.0-preview.3) and publish container images, but
there is no second implementation of anything, and the faults it is proven
against are the ones a single machine can produce.
Use it for prototyping, benchmarking, and contributing.

## How is Felix different from Kafka?

Different centre of gravity. Kafka is a durable log first: everything is
persisted, consumers pull, latency is a throughput trade-off, and the
ecosystem is enormous. Felix is latency-and-fanout first: streams can be
ephemeral (no disk on the hot path), each subscriber is isolated, and the
same log also serves cache and queue semantics so you run one system instead
of three.

Use Kafka when you need long retention, stream processing, or its connector
ecosystem. Felix keeps durable logs and replays by offset, but what a client
can read is bounded by one machine's disk: sealed segments can be copied to a
directory before retention deletes them, and nothing reads them back yet. It is built
for live distribution, not for being your system of record.

Felix does speak part of Kafka's wire protocol, so the two are not an
either-or at the client: see the next question.

## Can I use my Kafka clients with Felix?

Producers, yes. Consumers, if they assign their own partitions. With
`FELIX_KAFKA_LISTEN` set, every broker serves the Kafka protocol and each
durable stream is a topic named `<namespace>.<stream>`, with one partition per
shard and Felix's own offsets. A Kafka producer can write to it with any
compression codec and any `acks`, and an idempotent producer's re-sends are
recognised even after a leader failover. A consumer that calls `assign()` and
keeps its own offsets can read it. This is tested with kcat (librdkafka).

What does not work is anything built on consumer groups or transactions:
`subscribe()` with a `group.id`, committed offsets, `transactional.id`, and so
Kafka Connect, Kafka Streams, ksqlDB, Debezium and MirrorMaker. Those are
refused with an error that says why, rather than left hanging. Record keys and
headers are not stored. The whole picture, with use cases and troubleshooting,
is on [Kafka compatibility](/features/kafka/).

## How is Felix different from Redis?

Redis is a data-structure server with basic pub/sub bolted on. Felix is a
log with a cache reading. If you need sorted sets, Lua, or transactions,
that's Redis. If you need high-fanout delivery with per-subscriber isolation,
a cache whose changes you can watch (with offsets, so reconnects are
gapless), and durable counters, and you'd rather not operate a broker and a
cache separately, that's what Felix is for.

## Why QUIC instead of TCP?

Mostly for one property: streams multiplex over a connection without
head-of-line blocking, so a retransmission for one subscription never stalls
another. Beyond that: TLS 1.3 is part of the protocol (no unencrypted mode
to misconfigure), handshakes are one round trip, and flow control exists per
stream as well as per connection, which is where Felix's backpressure story
starts. The trade-off is real but small: some networks block UDP, and
TCP has better debugging tooling. Details in
[QUIC Transport](/features/quic-transport/).

## How does Felix handle backpressure?

At every level, and always bounded. QUIC has flow-control windows per connection
and per stream. The publish queue is bounded, and its overflow is a visible error
rather than unbounded buffering. Each subscription has a bounded queue whose
overflow policy (drop-new by default) is the isolation mechanism: a slow
subscriber loses its own events instead of slowing anyone else. See
[Publish/Subscribe](/features/pubsub/) for the full story and the
policy trade-off.

## What is ephemeral vs durable storage?

Per stream. An ephemeral stream lives in memory: lowest latency, lost on
restart, right for data whose old values are worthless. A durable stream
(`durable: true`, and the broker must run with `FELIX_DURABLE_STORAGE_DIR`)
writes every record to a segmented, CRC-checked, crash-safe log, and
subscribers can replay from any retained offset. By default a publish to a
`Leader` stream is acknowledged when it is queued, before the write. Set
`FELIX_ACK_ON_COMMIT=true` to acknowledge after it, or use a `Quorum` stream,
which always waits (see [`ack_on_commit`](/reference/configuration/#ack_on_commit)). A stream
marked durable on a broker without a storage dir is rejected, not silently
downgraded.

Retention is available and off by default. Set a stream's `retention`, or
`FELIX_DURABLE_RETENTION_BYTES` / `FELIX_DURABLE_RETENTION_SECONDS` for every
stream that sets none, or a log grows until the disk ends. See
[Durable Storage](/architecture/durable-storage/).

## How does clustering work?

Streams and caches are split into shards. The control plane assigns each
shard a leader (and replicas) by rendezvous hashing, and brokers follow its
assignment feed. One leader accepts a shard's writes. A broker that receives
a request for a shard it doesn't lead forwards it or redirects the client.
Leaders ship log records to followers. This is deliberately not per-shard Raft
(`docs/replication-design.md` records why leases plus log shipping were chosen),
and a lost leader is replaced only by a replica that provably holds the
log. A `Quorum` stream's publishes wait for a majority before acknowledging.

A broker that joins takes shards from any broker leading more than its
share, and a drained broker hands off everything it leads before it is
removed. Both go through a staged handoff so a shard is never served by a
broker that has not seen its log (see
[Adding, draining and removing brokers](/deployment/scaling/)).

Not built: follower reads (every read goes to the leader).

## What about exactly-once?

Not implemented, and not planned as a delivery guarantee. Felix offers
at-most-once (plain subscriptions) and at-least-once (durable streams and
consumer groups). Deduplication has to live in the application anyway,
because only it knows what makes two records "the same". Put it there, keyed on
something the record carries.

## What latency should I expect?

Measured numbers live in one place, [Benchmarks](/features/benchmarks/),
with methodology. The shape of it: single-message publish-and-ack round
trips are low hundreds of microseconds on loopback, cache operations
similar, and batched throughput runs trade per-message latency for rate.
Always benchmark release builds (`--release`). Debug builds are 10–100x
slower and tell you nothing.

## What's the most important tuning knob?

For latency under load, `FELIX_EVENT_BATCH_MAX_DELAY_US`: the longest an
event waits for its batch to fill once events arrive faster than the broker
drains them. An idle subscriber's events are sent without waiting. For
throughput, `FELIX_EVENT_BATCH_MAX_EVENTS` and its byte sibling. For memory,
the flow-control windows (`FELIX_*_RECV_WINDOW`), since window × connections
bounds in-flight data. The
[environment variable reference](/reference/environment-variables/)
has the full list with defaults. Change things off a measurement.

## Can I run Felix in Docker or Kubernetes?

Yes to both. See [Docker Compose](/deployment/docker-compose/) and
[Kubernetes](/deployment/kubernetes/). Each release publishes images to
`ghcr.io/getfelix/felix-broker:<version>` and
`ghcr.io/getfelix/felix-controlplane:<version>` (`ghcr.io/gabloe` for 0.6.0-preview and earlier). The Helm chart is not published,
so install it from `deploy/helm/felix` in the repository. The
broker ships what an orchestrator expects: `/live` and `/ready` that answer
different questions, and a bounded drain on SIGTERM
([graceful shutdown](/deployment/graceful-shutdown/)).

## How do I monitor Felix?

Prometheus metrics on the metrics endpoint (`/metrics`), structured logs via
`RUST_LOG`, and optional OTLP tracing. Which metrics answer which operational
questions is the whole point of the
[observability page](/features/observability/).

## How is Felix secured?

Every QUIC connection uses TLS 1.3; the control-plane API is plain HTTP until
you give it a certificate. The control plane does OIDC token exchange and
issues tenant-scoped EdDSA tokens. The broker enforces RBAC, with delegation
rules that prevent privilege escalation. Broker-to-broker traffic uses mTLS, bound to the
node id, when certificates are configured. Not built: encryption at rest,
end-to-end payload encryption, audit logging. The
[security page](/features/security/) states each plainly.

## Can I grant stream access by IdP group instead of per-user?

Yes. Configure `groups_claim` for the tenant issuer, then bind RBAC roles to
`group:<issuer>#<name>` subjects. During token exchange, Felix maps incoming
group claims to those subjects, scoped by the token's issuer, and evaluates
role permissions:

- grouping: `g, group:https://login.example.com#g1, role:reader, tenant-a`
- policy: `p, role:reader, tenant-a, stream:tenant-a/payments/*, stream.subscribe`

## Will there be clients for other languages?

Rust, Python and TypeScript ship today, the latter two as bindings over the
Rust client rather than reimplementations. Both pass every required scenario in
the client conformance catalogue, and CI is gated on it. Go and C# are not
started; `felix-capi`, the C ABI they will bind to, covers connect, publish
and a polled subscribe so far.
The wire protocol is language-neutral and documented precisely for this
reason (see [Wire Protocol](/architecture/wire-protocol/)), and a
conformance runner exists to check an implementation against it.

## Why won't the broker start? / Why is latency high? / Connection issues?

The [troubleshooting guide](/reference/troubleshooting/) covers these
with commands. The three most common answers: you're running a debug build
(use `--release`), a firewall is dropping UDP on the broker port, or
`FELIX_EVENT_BATCH_MAX_DELAY_US` is set high and your load keeps the
subscriber busy enough that batches wait for it.

## How do I contribute?

Fork, branch, make the change with tests, run `task lint` and `task test`,
open a PR. The [contributing guide](/development/contributing/) has
the details, and [How Felix Works](/development/how-felix-works/) is
the fastest way to build a mental model of the codebase.
