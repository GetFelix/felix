---
title: "An Architecture Tour"
description: "One publish, followed from the client API to bytes on three machines — and the reading order for everything else."
---

This page is for someone who wants to understand Felix well enough to change it.
It follows a single publish from the client's API call to bytes on disk on three
machines, stopping wherever a decision was made that would be surprising if you
met it in the code first.

Read it once end to end. It links out at each step, but the links are for
afterwards — the point here is the shape.

## The one idea

Felix stores everything in **an append-only log, split into shards**. A shard is
owned by one broker and replicated to others. Streams, caches and queues are
three ways of *reading* that log, not three subsystems.

![One append-only log per shard, read three ways: as a stream by offset, as a cache through a key index, and as a queue through a cursor shared by a consumer group.](/felix/diagrams/one-log.svg)

[Projections](/felix/architecture/projections/) animates that same picture —
the three readings advancing over one log at once — and cites the test behind
each claim.

Everything that follows is a consequence. There is one durability path, one
recovery path, one placement rule and one replication path, and each semantic is
a small amount of code on top. When you are deciding where a change belongs, the
question is usually "is this about the log, or about one way of reading it?"

[Projections](/felix/architecture/projections/) is the reference for what each
reading stores and rebuilds, with the test behind every claim.

## The three processes

![Clients connect to any broker over QUIC. Brokers are peers that forward requests for shards they do not own and replicate the ones they lead. A control plane places shards by rendezvous hashing, and brokers watch its assignment feed. Inside a shard, one append-only log is read as a stream by offset and as a cache through a key index.](/felix/diagrams/architecture.svg)

**The client** (`crates/sdk/felix-client`) is a library. It holds pools of QUIC
connections, encodes frames, and knows how to follow a redirect. It never
decides where data lives.

**The broker** (`services/felix-broker-service` + `crates/server/felix-broker`) serves clients over
QUIC and peers over a second QUIC endpoint with its own protocol. It leads some
shards, forwards what it does not lead, and replicates what it leads.

**The control plane** (`services/felix-controlplane-service`) is a stateless REST service over
Postgres. It owns tenants, namespaces, streams, caches, the node catalog, and
**shard assignments**, and it runs placement on a timer.

The division that matters: **the control plane decides ownership, and is never
on the data path.** A publish does not call it. Brokers read its assignment feed
in the background and answer from a routing snapshot they already hold.

## Following one publish

### 1. The client encodes a frame

`Client::publisher()` gives a handle; `publish()` takes a tenant, namespace,
stream, payload and an [`AckMode`](/felix/architecture/wire-protocol/).

The frame is `felix-wire`'s: a header with **flags** that select the payload
layout, then the body. Flags are not a version number. An unknown flag bit is
rejected rather than masked off, because masking one means confidently
misparsing the body.

Separately there are **feature bits**, exchanged in `Auth` and `AuthOk`. Those
say a *request exists* — "this broker serves `cache_delete`" — and never appear
on a frame. They are a different number space for that reason. A client must not
send a featured request to a peer that did not advertise the bit: an
unrecognised message type is fatal to a broker's control loop, so probing costs
the connection rather than returning an error.

> `crates/protocol/felix-wire/src/client/` — `frame.rs`, `flags.rs`, `features.rs`, `message.rs`.

### 2. The broker decodes and routes it to a handler

`services/felix-broker-service/src/serving/quic/` accepts the connection.
`streams/control.rs` is the control-stream loop: it owns auth state and
dispatches every `Message` variant. `handlers/publish.rs` and
`handlers/subscribe.rs` do the per-message work.

### 3. Ownership is resolved — locally

A routing key hashes to a shard. That shard has exactly one owner.

`resolve_route` in `handlers/publish/control.rs` is the single chokepoint every
publish passes through, which is why the ownership gate lives there: nothing
reaches storage without it.

**Resolving an owner is an atomic load, not a network call.** The routing table
is an immutable snapshot swapped in whole (`arc-swap`), so a reader takes a
cheap atomic load and reads a table nobody can mutate underneath it. This is the
hottest question the broker is asked, and it never touches the control plane.

Three outcomes, and no fourth:

- **Local** — this broker leads the shard *and* has opened it. Both are
  required: the cluster saying it is ours is not the same as the log being
  recovered.
- **Forward** — another broker leads it. The publish is sent over the peer
  protocol and the answer relayed back.
- **Refused** — nobody can serve it right now, and the reason says which kind of
  nobody. There is deliberately no "not sure, handle it locally": a broker that
  treats an unknown route as its own is a broker writing a shard it does not own.

> `crates/server/felix-router/src/shard/router.rs`, `services/felix-broker-service/src/shards/routing.rs`.

### 4. Offsets are taken before durability is waited on

This is the ordering most likely to be "fixed" into a bug.

`crates/server/felix-broker/src/broker/publish.rs` calls `begin_append` and *then* `commit`.
The batch claims its place in the stream's order the instant its offsets are
consumed, before anyone waits on the disk. `commit_order.rs` then makes later
publishes wait behind earlier ones — **whether those succeed, fail, or are
cancelled**, because a cancelled publish that released its successors would let
a later record land at an earlier offset.

### 5. Storage appends it

`crates/server/felix-storage/src/disk_log/` — a log-structured segment store, not a
write-ahead log. Four properties carry it, and all four are load-bearing:

- **Records are never rewritten.** Recovery can therefore trust "valid bytes end
  at EOF". Preallocation reserves blocks *without* changing `st_size` for
  exactly this reason.
- **A torn tail is repaired; interior corruption is fatal.** Refusing to start
  beats silently losing acknowledged records.
- **Indexes are derived, never trusted.** A missing, short or stale index is
  rebuilt from the segment it describes — which is why it is safe not to fsync a
  freshly written one.
- **Group commit** is the biggest throughput lever under `FsyncMode::OnCommit`:
  one blocking flush serves many waiters.

> [Durable Storage](/felix/architecture/durable-storage/) and
> [the segment format](/felix/architecture/storage-format/).

### 6. Replication ships it, if the stream asked

For a `Quorum` stream the publish is not acknowledged until a majority of the
shard's replicas — **counting the leader** — hold the record.

The leader ships records at their offsets; a follower checks each batch begins
at its tail, and answers a gap or a divergence explicitly rather than accepting
it. A leader serves only while it holds a **lease** on the shards it leads, so a
broker that has been superseded stops acknowledging rather than finding out
later.

On failover, only a replica that **actually holds the log** is promoted. A shard
whose leader is gone and whose replicas are behind is left unavailable rather
than reopened empty — a silently empty shard *is* the data loss, and nothing
downstream would report it as one.

> `docs/replication-design.md` argues this in full, including why per-shard Raft
> was rejected.

### 7. Fanout happens after durability

`crates/server/felix-broker/src/stream/delivery.rs`. One `DeliveryEnvelope` is shared by every
subscriber and caches its encoded frame, so a publish is encoded once regardless
of fanout.

Subscribers are isolated **by construction**: each has a bounded queue with an
explicit overflow policy, `DropNew` by default. A publisher never blocks on a
slow subscriber.

Because dropping is the default, a subscriber can silently miss records — which
is why delivered events carry log offsets for a durable stream. A jump between
consecutive offsets is a drop, so the loss is at least *detectable*. (A
new leader's generation-start record also takes an offset; the event after
it says so in `skipped_before`, so that jump is not mistaken for one.)

## The orderings that are the design

Several past bugs were "the natural order":

| Do this | Not this | Because |
| --- | --- | --- |
| Register the subscriber, then read history | Read history, then register | A publish landing in between is lost |
| Take offsets, then wait for durability | Wait, then take offsets | Order would depend on disk timing |
| Report the replica set, then release the quorum publish | Release, then report | A leader dying in the gap is replaced by a replica that may not hold the record |
| Record the dead letter, then advance the cursor | Advance, then record | A crash between leaves the record skipped with nothing saying it was tried |

If you find yourself reordering one of these, it is almost certainly a bug.

## Reading order

1. **This page**, for the shape.
2. [What Felix Is For](/felix/getting-started/what-felix-is-for/) — the status
   table. It is kept current per capability and is the page to trust when
   another disagrees.
3. [Projections](/felix/architecture/projections/) — the three readings, with
   the test behind each claim.
4. [Delivery Semantics](/felix/architecture/semantics/) — what is guaranteed,
   and what is not.
5. [Wire Protocol](/felix/architecture/wire-protocol/) — then
   `docs/internal-protocol.md` for the broker-to-broker one.
6. [Durable Storage](/felix/architecture/durable-storage/) and
   [the segment format](/felix/architecture/storage-format/).
7. `docs/replication-design.md` — the argument, not just the mechanism.
8. [How Felix Works](/felix/development/how-felix-works/) — function-by-function
   internals, once the shape above is familiar.

## Where the code is

| Crate | What it owns |
| --- | --- |
| `felix-wire` | Frames, message types, capability negotiation, the internal protocol |
| `felix-transport` | QUIC endpoints and connection setup |
| `felix-router` | Which node serves a shard, and whether it may be reached |
| `felix-storage` | Segments, the disk log, recovery, the log-backed cache |
| `felix-broker` | Streams, delivery, commit ordering, consumer groups |
| `felix-client` | The client library and its connection pools |
| `felix-authz` | Tokens, RBAC, and the actions they gate |
| `services/felix-broker-service` | The broker binary: QUIC handlers, routing, replication, peers |
| `services/felix-controlplane-service` | Metadata, placement, and the REST API |
| `felix-cluster` | A local multi-broker cluster, for integration and failure tests. It injects process, link, clock and fsync faults ([the fault API](https://github.com/gabloe/felix/blob/main/docs/cluster-harness.md#the-fault-api)) and checks histories under them ([the history checker](https://github.com/gabloe/felix/blob/main/docs/history-checker.md)) |

## Before you change something

Three conventions that will otherwise surprise you, all in `CLAUDE.md`:

- **The demo crates are not workspace members.** `task lint` and `task test`
  cannot see them, so "this is unused, delete it" is unreliable. Run
  `task demo:check` after changing a public API.
- **The cluster harness runs a prebuilt binary.** `cargo test -p felix-cluster`
  does not rebuild `felix-broker`, so a broker-side change is not in the binary
  those tests spawn until you build it. This silently invalidates "revert the
  fix and watch the test fail".
- **A regression test that passes without the fix proves nothing.** For
  concurrency and durability work, revert the fix, watch the new test fail, then
  restore it. Several tests in this repo exist because that step caught a test
  that was asserting nothing.
