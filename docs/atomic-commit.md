# Atomic commits

An atomic commit writes an event and the state updates that go with it as a
single record in one stream shard's log. Every reader sees all of it or none
of it: a subscriber or consumer group gets the event at the commit's offset,
a state read returns the values the commit wrote with that offset as their
version, and no reader, on any broker, before or after a failover, sees one
without the other.

The guarantee holds within one log. Each stream shard is its own log, with
its own leader, generation and committed mark, and a commit never spans two
of them. An operation that would need a second log is refused with a typed
error before anything is sent. Atomicity across logs is a non-goal for now;
see [What atomic does not cover](#what-atomic-does-not-cover).

## The client API

```rust
use felix_client::CommitOp;

let receipt = client
    .commit(
        "acme",
        "orders",
        b"order-42",
        vec![
            CommitOp::publish("order-events", r#"{"type":"placed","id":42}"#),
            CommitOp::put("order-events", "order-42", r#"{"status":"placed"}"#),
            CommitOp::delete("order-events", "cart-42"),
        ],
    )
    .await?;

let state = client
    .state_get("acme", "orders", "order-events", b"order-42", "order-42")
    .await?;
assert_eq!(state.version, Some(receipt.offset));
```

`commit(tenant, namespace, entity_key, ops)` takes:

- exactly one event, as `CommitOp::publish(stream, payload)` or
  `CommitOp::enqueue(queue, payload)`. A queue in Felix is a stream read
  through a consumer group, so the two are the same record; `enqueue` is
  there so the call says what the caller means.
- any number of `CommitOp::put(stream, key, value)` and
  `CommitOp::delete(stream, key)`, applied in order.

Every operation names the same stream. `entity_key` picks the shard the way a
publish's routing key does, so every commit for one entity lands on one shard
and is ordered there.

The client refuses a commit before sending it, with
`felix_client::CommitError`, when:

- an operation names a different stream (`NotOnOwningShard`). A different
  stream is a different log.
- the commit has no event, or more than one (`EventCount`). A commit is one
  record, and a record has one offset.
- the broker did not advertise `FEATURE_ATOMIC_COMMIT` (`Unsupported`).

`Client::commit` talks to one broker and reports a shard led elsewhere as a
`NotLeaderError`. `ClusterClient::commit` follows the redirect to the leader.
A commit is never forwarded between brokers.

`state_get(tenant, namespace, stream, entity_key, key)` answers with the
value, the `version` (the offset of the commit that wrote it) and `as_of`,
the offset of the last commit the answer reflects. Every commit at or below
`as_of` is reflected and no later one is, and the events of all of them can
be read from the stream.

## What atomic covers

A commit is one record. That one fact carries most of the guarantee, because
every part of the system already treats a record as indivisible:

- **Storage.** The record has one header, one checksum and one offset. A torn
  write loses all of it at recovery; there is no torn commit.
- **Replication.** Records are shipped by bytes, and a batch of several
  records can reach a follower in part. A follower promoted with half a batch
  keeps that half. A single record is shipped whole or not at all, so no
  replica ever holds half a commit.
- **Acknowledgement.** The committed mark counts records. A commit is covered
  by the mark or it is not, so a `Quorum` commit is acknowledged once a
  majority holds all of it.
- **Failover.** A promoted leader's log is the log. It holds a commit whole,
  or it does not hold it, and truncation works on record boundaries.

On top of the log, the broker keeps each part visible at the same instant:

- **Stream readers** (subscribers, history reads, consumer groups and the
  Kafka listener) see the commit as its event, at the commit's offset. The
  stored record is unwrapped on the way out, so a reader cannot tell a
  commit's event from a publish.
- **State.** Each stream shard has a keyed state view, projected from its
  commit records. A commit's state updates are applied under the same lock
  that puts its event in the replay ring, so no reader of either sees one
  first.
- **`Quorum` streams.** A commit past the committed mark is held back from
  the ring with its state updates, and both are released together once the
  mark passes it. A state read never reflects a commit a failover could take
  back.

The state view is derived, never stored. It is rebuilt from the log the
first time it is read after a restart, a promotion or a follower catching
up, and a rebuild reads only what the committed mark covers; until the mark
covers the log's tail the read is refused as not ready, and the client
retries.

`docs/formal/FelixAtomicCommit.tla` models the argument: across a failover
and a second promotion, no view on any broker shows part of a commit. Two
variants show each piece is needed. Written as one record per part, the
mark can stop mid-commit after a promotion; applied one part at a time, a
reader lands between parts. TLC finds the partial commit in both.

The history checker's rule 8 tests the same property against a real cluster
under faults: a reader reads a list, then its state, then the list again, and
must find the state at least as new as every commit event it saw first, and
the state's event at the state's version. See `docs/history-checker.md`.

## What atomic does not cover

**Other logs.** Two streams, two shards of one stream, a cache and a stream:
each is its own log, and a commit writes one. Making several logs atomic
needs a distributed transaction (two-phase commit or a coordinator with
commit and abort markers, recovery of in-doubt transactions, and readers that
skip uncommitted records), and it would add latency to every transactional
write. It is not built. A commit that would need it is refused, never split.

**Caches.** The state a commit writes is the stream shard's own state, read
with `state_get`. It is not a Felix cache. A cache is a separate log with
its own shards, so a cache put cannot be part of a commit.

**Retries.** A commit is not idempotent. A client that sends one, loses the
answer and sends it again may write it twice. The idempotent producer's
batch mark and the commit mark are both kinds of record, and a record is one
kind, so a commit cannot yet carry a producer sequence.

**Retention.** State is rebuilt from the retained log. A stream whose
retention trims its commit records loses the state they wrote. Keep entity
streams' retention unbounded, or long enough to cover every live key.

**Ephemeral streams.** A commit needs the shard's log, so an ephemeral stream
refuses one.

## Why the state lives in the stream shard

Streams and caches are separate logs, so something had to change for an
event and a state update to land in one. There were two ways to get there: a
new "entity" shard kind whose log holds stream, cache and queue records, or
routing a stream key, a cache key and a queue for one entity to one shard.

Felix does the second, with the stream shard as the entity's log. A queue is
already a stream read through a consumer group, so an event and an enqueue
are the same record. What was missing was keyed state, and that is now a
projection of the stream shard's own log, the way the cache index is a
projection of a cache's log. No new shard kind exists, so placement, moves,
replacing a lost replica, backup points and the replication protocol all see
an ordinary stream shard. A move copies the log, and the state comes with it
because the state is the log.

## On the wire and on disk

A commit is a `commit` request, answered with `commit_ok` or `error`; a state
read is `state_get`, answered with `state_value`. A client sends either only
to a broker that advertised `FEATURE_ATOMIC_COMMIT`. See
[`protocol.md`](protocol.md).

The record is marked with bit 28 of the record header and lives only in a
segment written at storage format v5, so a build that predates it refuses
the segment instead of misreading it. Replication carries the mark as
`ProducerMark::Commit`. See [`storage-format.md`](storage-format.md).

A cluster member accepts commits only once the fleet has finalized the
`atomic_commit` feature (`POST /v1/fleet/features/atomic_commit/finalize`;
see "Fleet features" in [`control-plane.md`](control-plane.md)), because a replica that predates the record would refuse it and stop
replicating the shard. Until then a commit is refused with an error that
says so. A broker outside a cluster accepts them at once.
