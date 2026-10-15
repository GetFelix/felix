---
title: "An Architecture Tour"
description: "One publish, followed from the client API to bytes on three machines, plus a reading order for everything else."
---

This page is for someone who wants to understand Felix well enough to change it.
It follows a single publish from the client's API call to bytes on disk on three
machines, stopping wherever a decision was made that would be surprising if you
met it in the code first.

Read it once end to end. It links out at each step, but save the links for
afterwards. The goal here is the overall shape.

## One log, read three ways

Felix stores everything in **an append-only log, split into shards**. A shard is
owned by one broker and replicated to others. Streams, caches and queues are
three ways of *reading* that log, not three subsystems.

![One append-only log per shard, read three ways: as a stream by offset, as a cache through a key index, and as a queue through a cursor shared by a consumer group.](/diagrams/one-log.svg)

[Projections](/architecture/projections/) animates that same picture,
with the three readings advancing over one log at once, and cites the test
behind each claim.

Everything else follows from this. There is one durability path, one
recovery path, one placement rule and one replication path, and each semantic is
a small amount of code on top. When you are deciding where a change belongs, the
question is usually "is this about the log, or about one way of reading it?"

[Projections](/architecture/projections/) is the reference for what each
reading stores and rebuilds, with the test behind every claim.

## The three processes

![Clients connect to any broker over QUIC. Brokers are peers that forward requests for shards they do not own and replicate the ones they lead. A control plane places shards by rendezvous hashing, and brokers watch its assignment feed. Inside a shard, one append-only log is read as a stream by offset and as a cache through a key index.](/diagrams/architecture.svg)

**The client** (`crates/sdk/felix-client`) is a library. It holds pools of QUIC
connections, encodes frames, and knows how to follow a redirect. It never
decides where data lives.

**The broker** (`services/felix-broker-service` + `crates/server/felix-broker`) serves clients over
QUIC and peers over a second QUIC endpoint with its own protocol. It leads some
shards, forwards what it does not lead, and replicates what it leads.

**The control plane** (`services/felix-controlplane-service`) is a REST service whose
metadata lives in an embedded Raft group, in Postgres, or in memory for
development. It owns tenants, namespaces, streams, caches, the node catalog, and
**shard assignments**, and it runs placement on a timer.

**The control plane decides ownership but is never on the data path.** A
publish does not call it. Brokers read its assignment feed
in the background and answer from a routing snapshot they already hold.

## Following one publish

### 1. The client encodes a frame

`Client::publisher()` gives a handle; `publish()` takes a tenant, namespace,
stream, payload and an [`AckMode`](/architecture/wire-protocol/).

The frame is `felix-wire`'s: a header with **flags** that select the payload
layout, then the body. Flags are not a version number. An unknown flag bit is
rejected rather than masked off, because masking one means confidently
misparsing the body.

Separately there are **feature bits**, exchanged in `Auth` and `AuthOk`. Those
say a *request exists* ("this broker serves `cache_delete`") and never appear
on a frame. They are a different number space for that reason. A client must not
send a featured request to a peer that did not advertise the bit. An
unrecognised message type closes the control stream, unless the client offered
`FEATURE_UNSUPPORTED`, in which case the broker answers `unsupported` and keeps
serving.

> `crates/protocol/felix-wire/src/client/`: `frame.rs`, `flags.rs`, `features.rs`, `message.rs`.

### 2. The broker decodes and routes it to a handler

`services/felix-broker-service/src/serving/quic/` accepts the connection.
`streams/control.rs` is the control-stream loop: it owns auth state and
dispatches every `Message` variant. `handlers/publish.rs` and
`handlers/subscribe.rs` do the per-message work.

### 3. Ownership is resolved locally

A routing key hashes to a shard. That shard has exactly one owner.

`resolve_route` in `handlers/publish/route.rs` is the single chokepoint every
publish passes through, which is why the ownership gate lives there: nothing
reaches storage without it.

Resolving an owner is an atomic load rather than a network call. The routing
table is an immutable snapshot swapped in whole (`arc-swap`), so a reader takes
a cheap atomic load and reads a table nobody can mutate underneath it. The
broker answers this question more often than any other, and it never touches
the control plane.

There are exactly three outcomes:

- **Local**: this broker leads the shard *and* has opened it. Both are
  required, because the cluster saying the shard is ours does not mean the log
  has been recovered.
- **Forward**: another broker leads it. The publish is sent over the peer
  protocol and the answer relayed back.
- **Refused**: nobody can serve it right now, and the reason says why. There is
  deliberately no "not sure, handle it locally" case, since a broker that
  treats an unknown route as its own ends up writing a shard it does not own.

> `crates/server/felix-router/src/shard/router.rs`, `services/felix-broker-service/src/shards/routing.rs`.

### 4. Offsets are taken before durability is waited on

This ordering is the one most likely to be "fixed" into a bug.

`crates/server/felix-broker/src/broker/publish.rs` calls `begin_append` and *then* `commit`.
The batch claims its place in the stream's order the instant its offsets are
consumed, before anyone waits on the disk. `CommitSequencer` in
`crates/server/felix-storage/src/commit_order.rs` then makes later
publishes wait behind earlier ones, **whether those succeed, fail, or are
cancelled**, because a cancelled publish that released its successors would let
a later record land at an earlier offset.

### 5. Storage appends it

`crates/server/felix-storage/src/disk_log/` is a log-structured segment store
rather than a write-ahead log. It depends on four properties:

- **Records are never rewritten.** Recovery can therefore trust "valid bytes end
  at EOF". Preallocation reserves blocks *without* changing `st_size` for
  exactly this reason.
- **A torn tail is repaired; interior corruption is fatal.** Refusing to start
  beats silently losing acknowledged records.
- **Indexes are derived, never trusted.** A missing, short or stale index is
  rebuilt from the segment it describes, which is why it is safe not to fsync a
  freshly written one.
- **Group commit** is the biggest throughput lever under `FsyncMode::OnCommit`:
  one blocking flush serves many waiters.

> [Durable Storage](/architecture/durable-storage/) and
> [the segment format](/architecture/storage-format/).

### 6. Replication ships it, if the stream asked

For a `Quorum` stream the publish is not acknowledged until a majority of the
shard's replicas (**counting the leader**) hold the record.

The leader ships records at their offsets; a follower checks each batch begins
at its tail, and answers a gap or a divergence explicitly rather than accepting
it. A leader serves only while it holds a **lease** on the shards it leads, so a
broker that has been superseded stops acknowledging rather than finding out
later. Once the fleet has finalized `majority_ack`, a `Quorum` shard acknowledges
when a majority answers that it holds the write at the leader's generation, and
neither the lease nor the control-plane report is on the write's path. A
superseded leader cannot collect that majority, because its successor fenced the
followers first. With `fenced_caches` finalized too, `Quorum` caches do the same.
`Leader` streams and caches keep the lease.

On failover, only a replica that **actually holds the log** is promoted. A shard
whose leader is gone and whose replicas are behind is left unavailable rather
than reopened empty. A silently empty shard *is* data loss, and nothing
downstream would report it.

> `docs/replication-design.md` argues this in full, including why per-shard Raft
> was rejected.

### 7. Fanout happens after durability

`crates/server/felix-broker/src/stream/delivery.rs`. One `DeliveryEnvelope` is shared by every
subscriber and caches its encoded frame, so a publish is encoded once regardless
of fanout.

Subscribers are isolated: each has a bounded queue with an
explicit overflow policy, `DropNew` by default. A publisher never blocks on a
slow subscriber.

Because dropping is the default, a subscriber can silently miss records. That
is why delivered events carry log offsets for a durable stream. A jump between
consecutive offsets is a drop, so the loss is at least *detectable*. (A
new leader's generation-start record also takes an offset; the event after
it says so in `skipped_before`, so that jump is not mistaken for one.)

## Orderings that matter

Several past bugs came from doing things in the order that seemed natural:

| Do this | Not this | Because |
| --- | --- | --- |
| Register the subscriber, then read history | Read history, then register | A publish landing in between is lost |
| Take offsets, then wait for durability | Wait, then take offsets | Order would depend on disk timing |
| Report the replica set, then release the quorum publish (without `majority_ack`) | Release, then report | A leader dying in the gap is replaced by a replica that may not hold the record |
| Record the dead letter, then advance the cursor | Advance, then record | A crash between leaves the record skipped with nothing saying it was tried |

If you find yourself reordering one of these, it is almost certainly a bug.

## Reading order

1. **This page**, for the shape.
2. [What Felix Is For](/getting-started/what-felix-is-for/), for the
   status table. It is kept current per capability and is the page to trust when
   another disagrees.
3. [Projections](/architecture/projections/): the three readings, with
   the test behind each claim.
4. [Delivery Semantics](/architecture/semantics/): what is guaranteed,
   and what is not.
5. [Wire Protocol](/architecture/wire-protocol/), then
   `docs/internal-protocol.md` for the broker-to-broker one.
6. [Durable Storage](/architecture/durable-storage/) and
   [the segment format](/architecture/storage-format/).
7. `docs/replication-design.md`, which explains the reasoning as well as the
   mechanism.
8. [How Felix Works](/development/how-felix-works/): function-by-function
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
| `felix-replication` | Shipping a shard's log from leader to followers, the peer transport, and when a `Quorum` write is on a majority |
| `services/felix-broker-service` | The broker binary: QUIC handlers, routing, wiring replication into the node |
| `services/felix-controlplane-service` | Metadata, placement, and the REST API |
| `felix-cluster` | A local multi-broker cluster, for integration and failure tests. It injects process, link, clock and fsync faults ([the fault API](https://github.com/GetFelix/felix/blob/main/docs/cluster-harness.md#the-fault-api)) and checks histories under them ([the history checker](https://github.com/GetFelix/felix/blob/main/docs/history-checker.md)) |

## Before you change something

Three things in `CLAUDE.md` that will otherwise surprise you:

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
