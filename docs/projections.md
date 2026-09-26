# One core log, many semantics

Felix stores everything in one kind of thing: an append-only log of records,
split into shards, each shard owned by one broker and replicated to others.
A *semantic* is a way of reading that log. Streams, caches and queues are three
readings of the same bytes, not three subsystems.

This page says what each one stores, what it keeps in memory, and what it
rebuilds from the log — and cites the test behind every claim it makes. A claim
here without a citation is a claim nothing checks, which is the failure this
page exists to prevent. `scripts/check_doc_evidence.py` verifies that every test
named below still exists.

<p align="center">
  <img src="assets/three-readings.svg" alt="One append-only log read three ways at once: a stream marker advancing over every record, a queue marker trailing because it moves only as records are acknowledged, and one cache marker per key jumping to that key's newest record" width="900">
</p>

The three markers are the whole idea. A stream, a queue and a cache are not
three stores — they are **three ways of pointing into one log**, running at the
same time over the same bytes. Only the cache treats an older record as dead;
compaction reclaims those, while a stream reading the same log still returns
them until retention removes the segment they live in.

## What they share

Every semantic gets the same four things from the log, and none of them
reimplements any of it:

- **Durability.** Records are fsynced according to the stream's policy, and a
  record acknowledged as durable is on disk.
- **Recovery.** A torn tail is repaired at startup; interior corruption refuses
  to start rather than silently losing acknowledged records.
- **Placement and routing.** Every shard of every stream and cache has one
  leader, chosen by rendezvous hashing. A broker that receives a request for a
  shard it does not lead forwards it or refuses.
- **Replication.** The leader ships records at their offsets; a follower checks
  each batch begins at its tail.

> `a_cache_value_survives_the_loss_of_its_owner` — a cache shard is replicated
> by the machinery that replicates a stream, because it is the same log.

## Streams: read the log forward

The plainest reading. A subscriber holds an offset and moves through the log in
order.

- **In the log:** the records, as published.
- **In memory:** per-subscriber queues with an overflow policy, and the offset
  each subscriber has reached.
- **Rebuilt from the log:** nothing needs to be. A subscriber's position is
  supplied by the client, so a broker holds no derived state a restart could
  lose.

Ordering is per shard. Two records in one shard are delivered in the order they
were written; two records in different shards have no order between them,
because they are different logs.

> `a_publish_to_a_non_owner_is_still_forwarded` — a publish entering any broker
> reaches the shard's owner.

## Caches: read the log through a key index

A cache write is an append. A cache read is an index lookup followed by a read
at that offset.

- **In the log:** one record per put and per delete, each carrying the key.
- **In memory:** an index of key to the offset of its latest record, plus the
  bytes each record occupies, so compaction knows when it is worth running.
- **Rebuilt from the log:** the whole index. It is never read from disk, and it
  catches up to the log's tail whenever it is behind — which is what lets a
  follower be promoted and serve records that arrived by replication rather
  than through a put.

Compaction rewrites the live set and drops superseded and expired records. It
**appends the live set at the tail** rather than renumbering from zero, so an
offset names the same record for the life of the shard. The live set is
written to a sibling directory and swapped in with two renames, each followed
by a directory sync; an open that finds the shard directory missing and a
`.retired` copy beside it restores that copy, so a crash between the renames
loses the compaction and nothing else. Counters use the same swap.

> `a_cache_survives_a_restart` — the index is rebuilt from the log.
> `the_index_catches_up_with_records_appended_behind_it` — records that arrive
> without going through a put are still found.
> `compaction_does_not_rewind_the_offset_space` — an offset keeps its meaning.
> `a_deleted_key_stays_deleted_across_a_restart` — a tombstone is a record like
> any other.

## Queues: read the log through a shared cursor

A consumer group is a position in the log shared by its consumers, plus the
bookkeeping for what is currently handed out.

- **In the log:** nothing of the group's own. The records are the stream's.
- **On disk, beside the log:** the group's committed position, and the offsets
  it has given up on. Both are themselves key-to-latest-value projections —
  the same reading the cache is, on their own roots.
- **In memory:** what is handed out and to when, what is owed, and how many
  times each unsettled record has been tried.

The in-memory part is deliberately not durable. A leader that dies loses it and
the group resumes from its committed position, so those records are delivered
again — which is at-least-once, the guarantee a queue offers anyway.

> `a_position_survives_a_restart` — the committed position is durable.
> `an_offset_in_flight_is_not_handed_out_again` — one consumer holds a record at
> a time.
> `a_lapsed_claim_is_handed_out_again` — a consumer that stops answering does
> not hold a record for ever.
> `the_cursor_does_not_advance_over_a_gap` — the position moves only over a
> contiguous run of acknowledgements.
> `a_record_is_given_up_on_after_the_attempt_bound` — one poison record does not
> stop the queue.
> `only_the_shard_owner_serves_a_group` — two brokers cannot both hand out the
> same records.
> `a_group_position_survives_a_leader_failover` — a promoted leader resumes
> where the group had got to.

### What a queue does not promise

**Order.** A shared cursor gives it up the moment two consumers hold adjacent
records, and a redelivery reorders regardless of how many consumers there are.
A queue in Felix preserves the order records were *handed out* in, and nothing
about the order they are finished in.

**A consumer per group.** A group is bound to the shard the caller names, and
nothing assigns shards across a group's consumers. Scaling one past a single
consumer per shard is the application's job today.

**Exactly-once.** A record can be delivered twice: after a claim lapses, after a
leader is lost, or after a redrive. Every consumer has to be able to see the
same record again.

## Counters

The projection model, applied a third time: **a counter is a fold over a log
of signed deltas**, exactly as a group's cursor is a fold over its
acknowledgements and the cache index is a fold over its puts. `counter_add`
appends a delta record; the running sum per key is the derived state, rebuilt
by replaying the log on recovery and never trusted from memory; compaction
collapses the applied deltas into one **checkpoint record** per key — the
fold, restated — appended at the tail so the offset space continues and
replication stays coherent across it. A regression test asserts compaction
changes neither the observable sum nor the log's numbering.

Three decisions, recorded here because #350 asked for them explicitly:

- **Scope.** A counter is addressed `(tenant, namespace, cache, key)` — the
  registered cache scope, the same key-to-shard hash, the same owner and
  forwarding — but lives in a store of its own beside the cache
  (`<root>/counters/`). Beside rather than inside, because a counter record
  is a new durable shape: mixed into the cache's logs it would make every one
  of them unreadable to a build that predates counters, promoted replicas
  included, with the cache's own puts as collateral. In its own store the
  blast radius is the counters. The corollary is that a counter and a cache
  value may share a key and are unrelated.
- **Watch interplay.** A cache watch does not see counter changes — they are
  different logs. A watch over counters (sums, not deltas, by the same
  argument retained delivery makes) is future work, not a silent half-feature
  of the cache watch.
- **Idempotency.** Delivery is **at least once**, honestly: a client that
  retries an add after a lost acknowledgement counts twice, because deltas
  carry no dedupe identity. The failure mode is exactly that — a lost ack
  plus a retry equals a double-count — and an application that cannot
  tolerate it keeps its own idempotency key outside the counter. A dedupe
  window was considered and deferred: it narrows the window without closing
  it, at a cost on every add.

The sum survives what the cursors survive, proven the same way: restart
(rebuilt fold), compaction (checkpoint), and leader failover — the counter
log rides its cache shard's replica set the way group state rides a stream
shard's, shipped by the same driver pass and gating nothing.

> `crates/server/felix-storage/src/counter_log/tests.rs`, including
> `compaction_moves_neither_the_sum_nor_the_offsets`,
> `a_shard_interrupted_mid_compaction_keeps_its_sums` and
> `the_sum_survives_a_restart`;
> `services/felix-broker-service/tests/counters.rs::an_add_answers_with_the_sum_including_it`;
> `crates/testing/felix-cluster/tests/caches/cache_failover.rs::a_counter_survives_the_loss_of_its_owner`.

## Where this stops being true

The claims above are the ones with tests. These are the gaps, listed so nothing
here has to be read as covering them:

- **Retention outranks a group.** A record trimmed before a group reached it is
  skipped, and the group moves past. A retention window shorter than a group is
  allowed to fall behind loses work.
- **A counter update is acknowledged by its leader.** A cache chooses `Leader`
  or `Quorum` for its puts and deletes, as a stream does for publishes, but the
  counter log beside it feeds no quorum mark: on a `Quorum` cache a counter
  update is still acknowledged once durable on the leader, so losing that
  leader between the acknowledgement and the ship loses the update.

What used to be listed here and no longer applies: **dead letters are now
replicated.** The list of offsets a group gave up on is one log per stream
shard — the group folded into the entry key, exactly as the cursors are shaped
— and it ships beside the cursors on the same replica set at the same
generation (`replicate_dead_letter_records` in the internal protocol). A
promoted leader lists what its groups abandoned and an operator's redrive
works there. The per-shard shape is what made this possible: the earlier
layout kept one log per `(stream, group)`, a set the shipping driver cannot
enumerate — groups appear whenever a consumer names one — where a log per
shard is exactly the unit the driver already walks. Entries recorded under
that earlier layout are still read and can still be discarded, but they were
never shipped, so only what is recorded under the per-shard layout survives a
failover.
