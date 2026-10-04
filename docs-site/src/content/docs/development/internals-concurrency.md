---
title: "Internals: Backpressure & Core Sharding"
---

Felix has six distinct places where a message can be slowed down, queued, or
shed, spread across the publish and subscribe paths covered in
[Internals: The Publish Path](/development/internals-publish/) and
[Internals: Subscribe & Fanout](/development/internals-subscribe/). This page is the
map of all six in one place: what each one guards against, what happens
when it's full, and how they compose into the "throughput plateaus, latency
stays bounded, overload becomes visible" curve the
[Benchmarks](/features/benchmarks/) page measures.

It also covers `core_shards`, the thread-per-core mode that changes *where*
(which OS thread/core) this pipeline runs, without changing any of the
policy logic below.

## The full backpressure chain

In publish → delivery order:

| # | Checkpoint | Bounds | Policy knob | Default |
|---|---|---|---|---|
| 1 | Client `PublishAdmission` | in-flight publish **bytes**, shared across all client workers | none, always waits | `publish_inflight_bytes` = 4 MiB |
| 2 | Client worker mpsc channel | queued publish **items** per stream-worker | none, the channel backpressures | `publish_queue_depth` = 64 |
| 3 | Broker `PublishAdmission` | in-flight publish **bytes**, process-wide | none, always waits (within `EnqueuePolicy::Wait`'s timeout) | `pub_inflight_bytes` = 64 MiB |
| 4 | Broker publish scheduler queue | queued publish **items**, process-wide (or per core shard), shared between tenants by deficit round robin | `EnqueuePolicy`: `Drop` / `Fail` / `Wait` / `Backpressure`; an acked publish that finds no room is answered `overloaded` (`publish_queue_full`) | `pub_queue_depth` × `pub_workers_per_conn` in all, `pub_queue_depth` guaranteed per tenant; `Drop` unless `pub_ingress_wait` (then `Backpressure`) |
| 5 | Broker-core subscriber channel | queued `DeliveryEnvelope`s per subscriber | `subscriber_queue_policy`: `Block` / `DropNew` / `DropOld` | `subscriber_queue_capacity` = 512, `drop_new` |
| 6 | Connection writer, per subscriber | frames queued for one subscription's QUIC stream | `subscriber_lane_queue_policy`: same three | `subscriber_lane_queue_depth` = 64, `drop_new` |

Checkpoints 1-2 are client-side (see
[Internals: The Publish Path](/development/internals-publish/#client-side-publisherpublish)),
3-4 are broker ingest, 5-6 are broker egress (see
[Internals: Subscribe & Fanout](/development/internals-subscribe/)). Below that, QUIC's
own flow control is the final backpressure layer. A subscriber that isn't
reading eventually blocks the connection writer's `send.write_all()`, which
is why checkpoints 5-6 exist at all: without them, one slow subscriber's
QUIC-level backpressure would eventually block fanout to every other
subscriber sharing the broker-core `Arc` snapshot loop.

### Why bytes *and* items, twice

Checkpoints 1/3 (bytes) and 2/4 (items) look redundant but aren't:
`pub_queue_depth` alone bounds how many *jobs* queue, but a job's payload
can be as large as `max_frame_bytes` (16 MiB default), and a handful of large
batches can blow the intended memory budget long before they fill an
item-count queue. `PublishAdmission` (`crates/sdk/felix-client/src/publish/admission.rs`
and `services/felix-broker-service/src/serving/quic/handlers/publish/admission.rs`, two
separate structs with the same design) is a `tokio::sync::Semaphore` sized in bytes rather
than permits-as-items, acquired via `acquire_many_owned(byte_count)`. The
permit is attached to the request/job and released only when it's actually
processed, not when it's merely handed to a channel, so the byte budget
reflects real resident memory, not just admission-time throughput.

### Why two queue-policy checkpoints on egress (5 and 6)

`subscriber_queue_policy` (5) gates the broker-core fanout in
`Broker::complete_publish` (`crates/server/felix-broker/src/broker/publish/completion.rs`),
the first hand-off after a batch is durable and appended to the replay ring. `subscriber_lane_queue_policy` (6) gates the last one, in
the connection writer, where each subscription has its own queue of encoded
frames waiting for its QUIC stream. They're separate because they guard
against different failure modes: (5) is a subscriber whose feeder cannot keep
up, (6) is a subscription whose QUIC stream is flow-controlled because the
client is not reading it.

Between them, the feeder hands frames to a writer lane and the lane hands
them to the connection writer. Both hand-offs are bounded by
`subscriber_lane_queue_depth` and wait when full, under every policy. They
are shared by every subscription on the connection, so shedding there would
drop frames for all of them whenever one publish fans out to more
subscriptions than the queue holds. Waiting there only backs records up into
each subscriber's own queue at checkpoint 5, and the connection writer never
stops taking frames under `drop_new`, so a slow subscription still sheds
alone. Every drop is counted in `felix_sub_queue_dropped_total`.

### `Block` vs `DropNew`/`DropOld`

```rust
pub enum SubQueuePolicy {
    Block,    // sender awaits queue space; nothing is ever shed
    DropNew,  // sender try_sends; on Full, the new item is discarded
    DropOld,  // behaves as DropNew; counted separately in metrics
}
```

:::caution[Lossless needs checkpoint 4 too, not just 5 and 6]
Setting `Block` on the subscriber queue (5), the lane queue (6) *and* the
client's own subscriber channel still does not give you lossless
delivery. Checkpoint 4, the broker's publish ingress queue, defaults to
`Drop`, and unacked publishes have nothing pacing them, so a
burst that outruns the broker core is shed before it ever reaches a
subscriber queue. Measured: a 400-message unacked burst on a 4-CPU host
dropped 77 publishes with every downstream queue set to `Block`.

Lossless mode is `Block` on 5 and 6 plus `pub_ingress_wait: true`
(checkpoint 4 → `Backpressure`). The symptom of getting this wrong is
delivery that appears to stall: the missing events were never published,
so nothing downstream is waiting on anything. `felix_broker_ingress_dropped_total`
is the counter that tells you which it is.
:::

Production defaults are `drop_new` at both checkpoints: overload becomes a
counted, bounded-latency event (`felix_subscribe_dropped_total`,
`felix_sub_queue_dropped_total`) instead of an ever-growing backlog with
unbounded tail latency. The [Benchmarks](/features/benchmarks/) harness
flips both to `Block` (plus `pub_ingress_wait: true` upstream, so the
publisher itself slows down rather than getting shed at checkpoint 4) to
measure *lossless sustainable throughput*. That is a different mode from the
production default, and no more correct. Which one you want depends on what
your application can tolerate: `Block` guarantees delivery
at the cost of publishers slowing down for one bad subscriber. `DropNew`
guarantees publishers never slow down at the cost of that subscriber
missing events.

## Core sharding

**File**: `services/felix-broker-service/src/serving/core_shards.rs`

Everything above describes *what* queues and *what* gets shed. Core
sharding is about *where* the code that does this runs. Normally
tokio's work-stealing scheduler bounces tasks across OS threads/cores
freely, which means a stream's publish worker and its subscribers' feeder
tasks can end up on different cores, turning every hand-off in the chain
above into a cross-core cache-line bounce and a scheduler wakeup.

`core_shards` (off by default, set with `FELIX_CORE_SHARDS` / `core_shards: N`) is a
fixed set of single-threaded tokio runtimes, one per shard, each pinned to
a CPU core on Linux (`sched_setaffinity`), or a dedicated thread with no hard
pinning elsewhere:

```rust
pub struct CoreShards {
    handles: Vec<tokio::runtime::Handle>,
    _shutdown: Vec<oneshot::Sender<()>>,
}

impl CoreShards {
    pub fn shard_for(&self, handle_id: u64) -> usize {
        (handle_id as usize) % self.handles.len()
    }
    pub fn handle_for(&self, handle_id: u64) -> &tokio::runtime::Handle {
        &self.handles[self.shard_for(handle_id)]
    }
}
```

A stream's `StreamHandle::id()` deterministically picks its shard. Two
places use `shard_for`/`handle_for` with that *same* id, so they always
agree on which shard owns a given stream:

- **The publish scheduler** (`handlers/publish/worker.rs:build_tracked_publish_context`,
  `handlers/publish/scheduler.rs`): when `core_shards` is set, the scheduler
  has one queue per shard, and queue `i`'s `pub_workers_per_conn` executors
  are spawned on shard `i`'s runtime via `shards.handle_for(i as u64)`. A
  stream's lane is keyed by `handle.id()` and lives in queue
  `handle.id() % shard_count`, so its publishes run on the core that owns it.
- **Subscription lane feeders** (`subscribe.rs:handle_subscribe_message`, feeding `subscribe/feeder.rs`):
  after registering with the lane manager, the broker resolves the
  subscription's `StreamHandle` and spawns `run_lane_feeder` on
  `shards.handle_for(handle.id())` instead of the default runtime.

The result: a stream's publish-side work and its subscribers'
dequeue/encode execute on one core. QUIC I/O stays off the shards on purpose:
quinn does packetization and TLS in its own driver
tasks regardless of who calls it, and those have their own placement story
(next section). See
[Benchmarks: Core Sharding](/features/benchmarks/) for measured impact
(it scales with stream count, and single-stream workloads are neutral-to-positive
by design, since a single stream only ever has one owning shard either way).

## The QUIC I/O runtime

**File**: `crates/protocol/felix-transport/src/io_runtime.rs`

Quinn's driver tasks (the endpoint receive loop and each connection's
transmit/ACK/timer loop) do a *bounded* slice of work per poll and then
reschedule themselves. That makes their scheduler re-poll latency the
transport's throughput ceiling: wakeups scale with datagram count, so a
shared, loaded runtime turns per-wakeup latency directly into a per-byte
rate cap. Measured on a shared runtime, that cost ~7.5× in sustained
throughput, and it is per-byte rather than per-message, so it looks like a
payload-size problem rather than a scheduling one.

```mermaid
flowchart LR
    subgraph shared["Drivers on the shared application runtime"]
        direction LR
        N1(["datagram"]) s1@--> SD["quinn driver<br/><small>bounded work, then reschedules</small>"]
        SD s2@--> SQ{{"waits behind<br/>~50 app tasks"}}
        SQ s3@--> SP["read/write pump<br/><small>woken on another core</small>"]
        SP s4@--> SO(["~73 MB/s<br/><small>a wakeup chain per datagram</small>"])
    end

    subgraph dedicated["Drivers on dedicated single-threaded runtimes (what Felix does)"]
        direction LR
        N2(["datagram"]) d1@--> DD["quinn driver<br/><small>dedicated thread, re-polls immediately</small>"]
        DD d2@--> DP["colocated pump<br/><small>same-thread task switch</small>"]
        DP d3@--> DO(["~500 MB/s<br/><small>bounded by capacity, not wakeups</small>"])
    end

    s1@{ animation: slow }
    s2@{ animation: slow }
    s3@{ animation: slow }
    s4@{ animation: slow }
    d1@{ animation: fast }
    d2@{ animation: fast }
    d3@{ animation: fast }

    classDef step fill:#e8f0fe,stroke:#4a6fa5,color:#1a2b40
    classDef gate fill:#fdeaea,stroke:#b04a4a,color:#3d1414
    classDef slow fill:#fdf0e3,stroke:#b07d3a,color:#3d2a12
    classDef ok fill:#e9f5ec,stroke:#4a8a5e,color:#16301f
    class SD,SP,DD,DP step
    class SQ gate
    class N1,N2 step
    class SO slow
    class DO ok
```

The two rows animate at different speeds on purpose: the work per hop is the
same in both, and only the waiting differs. Every stage measured in
microseconds while the pipeline ran at a fraction of its capacity, which is
exactly the signature of a latency-bound dependency chain rather than a
saturated resource.

Felix therefore runs quinn drivers on a pool of dedicated *single-threaded*
runtimes, high QoS on macOS, via a custom `quinn::Runtime` implementation.
`FELIX_IO_RUNTIME_THREADS` sizes the pool. On macOS the default is one runtime
per server endpoint plus one for clients. Elsewhere it is `0`, which runs the
drivers on the shared runtime.

Which endpoints share a runtime is load-bearing. Assignment is by role,
not round-robin: server endpoints spread across every runtime but the last,
and client endpoints all share the last one. A server endpoint multiplexes
every connection and drives traffic in both directions, so sharing its
runtime starves it: measured 5–6× slower when a client endpoint landed
next to it. A client's publish and event endpoints carry the two halves of
one request/response flow, so *splitting* them across threads makes every
message pay two cross-thread wakeups. The partition is independent of
creation order, because an order-dependent assignment recreates the slow
topology on every second benchmark case.

Two pump tasks are *colocated* with their connection's drivers through
`QuicConnection::spawn_pump`, because they exchange a wakeup with the
transport per datagram or per write, and a same-thread wakeup is a task
switch rather than a cross-core kernel round trip:

- the client's subscription read task (`felix-client`, `subscription.rs`);
- the broker's per-connection delivery writer (`subscribe/lane.rs`).

The client's *publisher* writer is deliberately not colocated: it spends its
time blocked in `write_all` against a full send window, and parking it on
the I/O thread starves the very drivers it waits on (measured 5× worse).

## Putting it together: what a "saturated" system looks like

With defaults (checkpoints 4-6 shedding, not blocking): as publish rate
exceeds what a subscriber's consumer can drain, checkpoint 5 or 6 starts
shedding for *that subscriber only*. Other subscribers on the stream are
unaffected (checkpoint 5 is per-subscriber, checkpoint 6 is per-lane, and
lane assignment spreads subscribers across lanes). Publish-side throughput
is untouched. The overloaded subscriber sees drops and bounded queue depth
instead of growing latency. That's the "plateau, don't degrade" curve.

With lossless mode (`Block` + `pub_ingress_wait`): the same overload instead
propagates backward through every checkpoint. A slow subscriber blocks its
lane, which blocks its feeder, which blocks broker-core fanout for *every*
subscriber of that stream (checkpoint 5 sends to all subscribers before
returning), which blocks that stream's publishes on their lane, which fills
the broker `PublishAdmission` byte budget, which blocks publishers. This is why
lossless mode is explicitly opt-in: it turns one slow consumer into
backpressure on every producer of that stream.

## If you want to change...

| You want to... | Look at |
|---|---|
| Add a new backpressure checkpoint | Decide which layer it belongs to (ingest vs. broker-core fanout vs. lane). See the table above for precedent |
| Change what happens when a checkpoint is full | `SubQueuePolicy` (`crates/server/felix-broker/src/stream/delivery.rs`) for 5/6, `EnqueuePolicy` (`publish/ack.rs`) for 3/4 |
| Change the byte-budget admission logic | `PublishAdmission`, separately in `crates/sdk/felix-client/src/publish/admission.rs` (client) and `services/felix-broker-service/src/serving/quic/handlers/publish/admission.rs` (broker). They are kept symmetric on purpose, so change both if you change the design |
| Change core-sharding/stream-ownership logic | `services/felix-broker-service/src/serving/core_shards.rs`; the two call sites in `handlers/publish/worker.rs` and `handlers/subscribe.rs` that must agree on `handle_id -> shard` |
| Debug "why is this subscriber not getting messages" | Check `felix_subscribe_dropped_total` / `felix_sub_queue_dropped_total` counters first. If either is nonzero for a stream, you're at checkpoint 5 or 6, not a bug |
