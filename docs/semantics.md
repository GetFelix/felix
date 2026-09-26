# Felix semantics

The behavioural contract: what an acknowledgement means, what survives what, and
where each guarantee stops.

**Every normative claim below names the test that holds it.** A guarantee with
no test behind it is an intention, and this document has been wrong before by
describing intentions in the present tense. Where something is not enforced, it
says so rather than omitting it.

Written for the replicated, multi-broker system. It replaces the single-node MVP
contract, which described a system that no longer exists — at-most-once
delivery, no authorization, no durability.

## What an acknowledgement means

An acknowledgement is the only promise Felix makes about a record, and what it
promises depends on two settings that are chosen separately.

### Durability: `FsyncMode`

Where the record is when the broker acknowledges it.

| Mode | An ack means | Lost by |
| --- | --- | --- |
| `None` | The record is in the page cache | Losing the machine |
| `Periodic { interval }` | The record is in the page cache, and will be fsynced within `interval` | Losing the machine within `interval` of the write |
| `OnCommit` | The record is fsynced | Nothing short of the disk failing |

`Periodic` is the default. `OnCommit` is the only setting under which an
acknowledged record survives the loss of the machine, and group commit is what
keeps it affordable: one flush serves every waiter queued behind it.

A crash mid-append leaves a **torn tail**, which recovery repairs by discarding
the incomplete record — it was never acknowledged. Corruption in the *interior*
of a segment is refused instead: the broker will not start. Refusing to start
beats silently losing an acknowledged record.

> Held by `crates/server/felix-storage/src/disk_log/` recovery tests, including
> `a_crash_before_the_header_is_written_leaves_the_log_openable`,
> `a_crash_while_preparing_a_rollover_leaves_the_log_openable`, and
> `a_crash_while_sealing_a_retired_segment_loses_nothing`.

### Consistency: how many brokers hold it

| Level | An ack means | Loss window |
| --- | --- | --- |
| `Leader` | With `ack_on_commit` on, the shard's leader has it durably. Off, the default, the leader has queued it | Everything the leader had not yet shipped, if its storage is lost; with `ack_on_commit` off, also a record still queued when the leader crashes or its lease runs out |
| `Quorum` | A majority of the replica set, leader included, has it durably | None within the replica set |

`Quorum` waits. A publish is not acknowledged until the leader can show a
majority holds the record; if no majority is reachable it is **refused**, with
an error that says this broker cannot vouch for the write rather than one that
claims it failed.

> `a_quorum_publish_without_a_majority_is_refused` — freeze every follower and
> the publish is refused rather than acknowledged.
> `a_frozen_follower_does_not_block_a_quorum` — losing a *minority* does not
> stop it, which is the case `Quorum` exists to tolerate.
> `a_quorum_acknowledged_record_survives_its_leader` — the acknowledged record
> is readable after the acknowledging broker is killed.

That last one holds for the faults the suite injects. A model check of the
promotion protocol finds an interleaving it does not reach — a leader that
acknowledges and then dies before its next report leaves a fresh report naming
a replica without the record, and promotion picks from that report. See
`docs/replication-design.md` under "Who may be promoted", and #527 for the
rule change that closes it.

**With `ack_on_commit` off, a `Leader` ack is sent when the publish is queued,
before it is written.** The record is lost if the leader crashes before the
write, or if its lease lapses while the record waits in the queue. The worker
refuses that write, and has to: another broker may lead the shard by then, and
writing it late is the split brain the lease exists to prevent. To narrow the
window, a publish admitted with less lease left than the publish queue wait
plus the ack wait (capped at half the lease) waits for its write instead of
being acknowledged on enqueue, so a lapse reaches the client as
`shard_unavailable`. A process pause that starts after the ack and outlasts the
lease still loses the record; each such loss is counted in
`felix_broker_acked_publishes_dropped_total` and logged. For an ack that means
the record is on disk, turn `ack_on_commit` on or declare the stream `Quorum`.

> `a_publish_admitted_near_lease_expiry_waits_for_the_write`,
> `a_batch_admitted_near_lease_expiry_waits_for_the_write`,
> `an_acked_publish_the_lease_strands_is_counted`.

**The `Leader` loss window is bounded by replication lag**, exported as
`felix_broker_replication_lag_records`. An operator choosing `Leader` is
choosing that window, and a bound nobody can observe is not a bound.

A majority is of the replica set, leader included: a set of three needs two, a
set of five needs three, and a set of one needs one — which is why
`replication_factor: 1` costs nothing.

## Failover

A shard's leadership is a **time-bounded lease** issued by the control plane,
with the assignment generation as its epoch. Per-shard Raft was considered and
rejected; `docs/replication-design.md` records why.

**Only a replica that holds every record the leader may have acknowledged can
be promoted.** The catch-up bound is zero. A bound above zero would be a bound on
how much a promotion may silently lose, and there is no honest non-zero value
that is not a policy decision.

Under `Leader` that is every record the leader held, since a write is
acknowledged before it ships. Under `Quorum` it is every record up to the
quorum mark: nothing past the mark has been acknowledged, and the mark moves
only once the report naming who holds it has landed. A `Quorum` leader that dies
holding records no follower has yet is therefore still replaced; those records
were never acknowledged, and an idempotent producer sends them again.

> `a_quorum_follower_holding_every_acknowledged_record_can_lead` and
> `a_leader_stream_follower_missing_the_newest_record_cannot_lead`.

> `the_promoted_leader_is_one_of_the_replicas` and
> `an_unreplicated_shard_does_not_fail_over_to_an_empty_broker` — a shard with
> no qualifying replica is left unavailable rather than served empty.
> `failover_completes_within_the_configured_bound`.
> `records_survive_repeated_failovers` — two failovers in a row, not just one.

**A leader that was frozen past its lease and then resumed cannot acknowledge a
write the cluster has lost.** It wakes still believing it leads; the lease and
the generation are what stop it.

> `a_resumed_leader_does_not_acknowledge_writes_the_cluster_loses`.

That holds for acknowledgements sent after it resumes. A `Leader` publish it
acknowledged on enqueue before the freeze, and had not yet written, is lost —
see "Consistency" above.

The lease depends on bounded process suspension, not on synchronised clocks:
each broker measures elapsed time on its own monotonic clock and gives up a
quarter of the lease as margin. See "Where the guarantees stop" below.

## Moves

A shard whose leader is alive is moved rather than failed over: the
destination copies the log, the leader is fenced and reports that its log has
stopped growing, and only then does the destination lead.
`docs/replication-design.md` ("Planned handoff") has the mechanism; this is
what a client sees.

- **Nothing acknowledged is lost, and nothing is stored twice.** Every write
  the old leader accepted lands in its log before it reports drained, and the
  new owner holds it before it leads. A write that finds the fence closed is
  held or refused, never written late.
- **A publish is held, not refused.** One that arrives between the fence and
  the cut-over waits until the broker's routes show the new owner, then goes
  there. It is refused as `shard_unavailable` / `moving`, not written, only
  past `FELIX_SHARD_MOVE_HOLD_MS` or `FELIX_SHARD_MOVE_HOLD_MAX`. Cache and
  counter operations are held and forwarded the same way. Consumer-group
  operations are held and then redirected to the new owner, which
  `ClusterClient`'s group calls (and the Python and TypeScript clients') follow.
- **Subscriptions follow.** The old leader ends each after the writes in
  flight have fanned out, and sends `shard_moved` with the offset to resume
  from. A `ClusterClient` subscription resumes on the new owner at
  `max(last delivered + 1, resume_from)`, so on a durable stream nothing is
  repeated or skipped; an in-memory stream resumes at the tail. A cache watch
  gets the same frame and is reopened by the caller.
- **Group state, counters and producer sequences move with the shard.** The
  drained report waits for the successor to hold the group cursors, dead
  letters and counters, and producer sequences are in the log's records. A
  broker that gets a shard back rebuilds its groups' in-flight state from the
  cursors rather than keeping what it held last time, so nothing finished
  elsewhere is handed out again.

> `continuous_publishing_through_a_move_is_never_refused`,
> `a_subscription_follows_its_shard_to_the_new_owner`,
> `a_moved_shard_keeps_its_group_state_and_counters`,
> `a_shard_moved_away_and_back_hands_out_nothing_already_acked`,
> `a_producer_keeps_its_sequence_across_a_planned_move`,
> `a_durable_publish_claimed_after_the_fence_is_refused`.

Stopping a broker is not a move: the shards it leads fail over. Drain it
first (`POST /v1/nodes/{id}/drain`) to hand them off.

## Routing

A publish and a subscribe make opposite choices, deliberately.

- **A publish to a broker that does not own the shard is forwarded** to the one
  that does, and acknowledged only once the owner has answered.
- **A subscribe to a broker that does not own the shard is redirected**, naming
  the owner. It is never served locally: that would deliver nothing while
  looking exactly like a stream with no traffic.

> `a_publish_to_a_non_owner_is_still_forwarded`,
> `a_subscribe_to_a_non_owner_is_redirected`,
> `a_cluster_client_follows_the_redirect_to_the_owner`.
> `docs/subscribe-routing.md` and `docs/internal-protocol.md` record the two
> decisions.

**A publish may carry a routing key**, and the key decides the shard. A stream
placed with more than one shard therefore spreads across brokers, which is the
mechanism by which one stream scales past a single owner.

> `a_sharded_stream_is_placed_across_brokers`,
> `keys_spread_records_across_shards`,
> `a_keyed_publish_is_forwarded_to_the_shards_owner`.

A publish with **no** key lands on shard 0, which is what every record did
before keys existed and what a single-shard stream does regardless.

> `an_unkeyed_publish_still_lands_on_shard_zero`.

Keyed publishes use the binary encoding, like unkeyed ones. The key rides in the
frame under `FLAG_BINARY_PUBLISH_KEYED` (`0x0040`), negotiated on the handshake;
a broker that predates the bit gets the JSON encoding instead, which costs
throughput rather than correctness.

> `a_keyed_publish_negotiates_the_binary_frame`,
> `an_unacked_keyed_publish_reaches_its_shard`.

## Delivery to subscribers

- **Ordering is per key on a sharded stream, and per stream on a single-shard
  one.** Two records sharing a routing key are ordered with respect to each
  other, because a key always resolves to the same shard and a shard is one log
  on one leader. Two records with different keys may be applied by different
  brokers in either order.

  A stream with one shard keeps total order whatever keys are used, which is
  what makes routing keys safe to add to an existing stream: the guarantee only
  weakens when the shard count does the widening.

  There is no ordering across streams.

  > `one_key_always_lands_on_one_shard`.
- **A slow subscriber is dropped from, not blocked on.** Each subscriber has a
  bounded queue with an explicit overflow policy, `DropNew` by default. A
  publisher never waits for a subscriber.
- **A drop is detectable.** Delivered events on durable streams carry log
  offsets, so a gap in offsets is exactly a drop. This is the only way an
  application can tell, and it is why offsets are on the event rather than
  inferred.
- **A subscription can resume.** `Subscribe` takes `latest`, `earliest`, or an
  offset; stored history joins live delivery with no gap.
- **Replayed history is never dropped.** The overflow policy governs live
  records only. History below the join's `live_offset` is read off disk for
  that subscriber alone, so there is no publisher to protect: it waits for room
  in the client's queue instead, and a slow reader slows the replay rather than
  losing part of it.
- **A subscription reads one shard.** Shards of a stream can have different
  owners and a subscription is bound to one connection, so reading a whole
  multi-shard stream means one subscription per shard. Offsets are per shard,
  so resuming a multi-shard consumer means carrying one offset per shard.

  `ClusterClient::subscribe_sharded` opens one per shard and merges them,
  following each shard's own redirect. It promises **per-shard ordering and
  nothing more**: merging cannot restore an order that never existed. An
  unreachable shard refuses the whole subscription rather than covering three
  shards of four, and a shard whose owner is lost is re-established on its own
  while the others keep delivering.

  > `a_sharded_subscription_receives_every_record`,
  > `an_unreachable_shard_refuses_the_subscription`,
  > `one_shard_failing_over_does_not_stop_the_others`,
  > `a_sharded_subscription_resumes_from_its_per_shard_offsets`.

**Three fields on a stream are declared and not enforced.** The control plane
accepts them and stores them; the broker reads only `durable`, `shards` and
`consistency`.

| Field | What actually decides |
| --- | --- |
| `delivery` (`AtMostOnce` / `AtLeastOnce`) | How a client reads: a plain subscription, or a consumer group |
| `retention` (`max_age_seconds`, `max_size_bytes`) | The broker-wide `FELIX_DURABLE_RETENTION_*` settings |
| `kind` (`Stream` / `Queue` / `Cache`) | Nothing. A queue is a way of *reading* a stream, not a kind of stream |

`kind` is the one most likely to mislead. Creating a stream with `kind: Queue`
does not make it a queue and does not stop it being subscribed to normally —
consumer groups work over any durable stream, and a stream created as `Stream`
serves them just as well. Do not rely on any of the three.

## Clients

A client is told which brokers exist and is redirected to the right one, but the
retry policy is still the application's. `docs/multi-node-client.md` is the
how-to; this is the contract.

- `ClusterClient` takes several broker addresses and rebuilds its connection
  from the rest when the one in use fails.
- It asks a broker which other brokers exist and **adds** them to what it will
  try. The configured seeds are never removed, so a wrong or stale answer cannot
  leave a client with fewer ways in than it started with.
- `publish` reconnects but does **not** resend anything that may have landed.
  Its one re-send is after a cached shard owner answered that it applied
  nothing, and goes to the entry broker. `publish_at_least_once` resends, and
  can therefore duplicate a record whose failure it could not prove was not
  applied. The names are the contract.
- **Retries are bounded by an attempt count and a jittered exponential
  backoff**, and optionally by a deadline across every attempt and sleep. The
  deadline is off by default: one shorter than a single attempt's own timeout
  prevents any retry at all, so a useful value depends on the caller's latency
  budget rather than on a number this library can pick.
- **The backoff is full jitter** — uniform over `[0, ceiling]`, not the ceiling.
  Every client notices a failover at the same moment, and an unjittered backoff
  sends all of them at the freshly promoted broker in step.
- **The broker's retry class decides what happens next.** `fatal` is returned
  at once. `outcome_unknown` is returned by `publish` and never re-sent;
  `publish_at_least_once` and the idempotent producer send it again, the first
  because that is what it promises and the second because its sequence makes
  the re-send land once — on a durable stream, whichever broker leads the
  shard by then. `retry` and `redirect` from a cached owner drop that
  owner and go straight to the entry broker, since a fenced or draining owner
  will not start serving the shard again; from the entry broker they back off.
  `retry_after` backs off for at least as long as the broker asked.

  **"Not found" is retried, for 5 s from the first one.** A broker learns its
  streams from the control plane and opens a shard only once it is given one,
  so a broker promoted a moment ago reports the stream it is about to serve as
  missing. Being named leader and being ready to serve are different moments;
  past a couple of control-plane syncs, the stream really is missing.

  A broker that predates error codes sends prose, and then only a credential
  failure and an offset retention has passed are terminal. Everything else is
  retried, *including errors nobody has classified*, because a wasted attempt
  is a cheaper mistake than a lost operation.

> `a_forbidden_publish_fails_fast_instead_of_retrying`,
> `a_fenced_owner_is_forgotten_and_the_publish_rerouted`,
> `an_outcome_unknown_publish_is_surfaced`,
> `not_found_stops_being_retried_after_its_grace`,
> `a_client_given_one_seed_learns_the_other_brokers`,
> `the_configured_seed_is_never_dropped`,
> `a_client_given_one_seed_survives_losing_it`,
> `a_publisher_survives_losing_its_broker`,
> `records_published_across_a_failover_are_all_readable`.

## Cache

- **Scope:** `(tenant_id, namespace, cache, key)`.
- **Storage:** a cache is a log. A write appends a record; a read consults an
  in-memory index of key → offset and reads the log. See `docs/cache-on-log.md`.
- **Durability:** entries survive a restart when the broker runs with
  `FELIX_DURABLE_STORAGE_DIR`. Without one the cache is in memory and is lost,
  because there is nowhere to write a log.
- **TTL:** lazy on access, against an absolute expiry time, so an expiry that
  passes while the process is down is still an expiry.
- **Reclamation:** compaction rewrites the live set and drops superseded and
  expired records, without ever rewriting a record in place.

> `a_cache_survives_a_restart`, `an_expiry_survives_a_restart`,
> `a_later_write_wins`, `compaction_reclaims_overwritten_records`.

**Cache operations are routed to the key's owner** (#278). A key hashes to a
shard, that shard has exactly one leader, and a broker that receives an
operation for a key it does not own forwards it there rather than serving a
second copy. A value written through any broker is readable through every other,
and two brokers cannot both accept a write for one key.

An operation this broker cannot route is refused, and a read it cannot route is
an error rather than a miss — reporting a miss would let a client conclude a key
does not exist when it does, on the owner.

> `crates/testing/felix-cluster/tests/caches/cache_routing.rs`, including
> `a_value_written_through_one_broker_is_readable_through_every_other` and
> `two_brokers_writing_one_key_do_not_diverge`.

**A cache's shards are replicated.** A cache created with a replication factor
above one has its log shipped to followers exactly as a stream's is, and a value
written before the owning broker dies is readable from the replica promoted in
its place.

> `crates/testing/felix-cluster/tests/caches/cache_failover.rs::a_cache_value_survives_the_loss_of_its_owner`.

**A key or prefix can be watched** (#348). `cache_watch` — negotiated as
`FEATURE_CACHE_WATCH`, and offered only by a log-backed cache — delivers each
applied write in the shard's write order with its log offset: a put with its
value, a delete as a change with none. Resume by offset replays from the log
and joins live delivery with no gap and no duplicate, by the same
register-before-read discipline a stream resume uses. An offset compaction has
collapsed is answered with a marked snapshot of current values, never a silent
gap; a watch that falls behind is ended with `cache_watch_lagged` naming the
first missed offset, because a filtered watch's offsets are sparse and a drop
could not otherwise be seen. TTL expiry appends nothing and so delivers
nothing. See `docs/cache-on-log.md` and `docs/protocol.md`.

> `services/felix-broker-service/tests/cache_watch.rs`, including
> `a_key_watch_sees_its_key_and_no_others`,
> `a_resumed_watch_is_gapless_under_concurrent_writes` and
> `a_watch_from_a_compacted_offset_resnapshots`;
> `crates/server/felix-broker/src/cache/watch/tests.rs::overflow_ends_the_watch_and_names_the_first_missed_offset`.

**A watch can start from current state** (#349). A `retained` watch delivers
each matching key's current value first — at the offset of the write that
produced it — then live changes: MQTT's retained message, and the join
primitive state-sync applications need. `retained_count` in the confirmation
makes joining an empty key a definite zero rather than silence. Negotiated as
`FEATURE_CACHE_WATCH_RETAINED`, a bit of its own so an older watch-capable
broker is never asked for state it would silently not deliver. Survives
failover: a promoted replica serves the retained value from its rebuilt index,
and the watch is live on it.

> `services/felix-broker-service/tests/cache_watch.rs`, including
> `a_retained_watch_delivers_current_state_then_live_under_concurrent_writes`,
> `a_retained_watch_on_an_empty_key_reports_no_value` and
> `a_retained_value_survives_a_restart`;
> `crates/testing/felix-cluster/tests/caches/cache_failover.rs::a_retained_watch_survives_the_loss_of_the_owner`.

**A counter folds deltas into a durable sum** (#350). `counter_add` /
`counter_get`, negotiated as `FEATURE_COUNTERS` and offered only with durable
storage. Scoped and routed exactly as a cache key — same scope, same shard,
same owner, same forwarding — but stored beside the cache, so a counter and a
cache value sharing a key are unrelated. Each add answers with the sum
including itself; never-written is distinct from zero. The sum survives
restart, compaction (checkpoints, offsets never renumbered), and leader
failover, where the promoted replica folds the true sum from its shipped log
and keeps counting. **At least once**: a retried add after a lost
acknowledgement double-counts — deltas carry no dedupe identity, and
`docs/projections.md` records the decision.

> `services/felix-broker-service/tests/counters.rs`, including
> `an_add_answers_with_the_sum_including_it` and
> `a_counter_survives_a_restart`;
> `crates/server/felix-storage/src/counter_log/tests.rs::compaction_moves_neither_the_sum_nor_the_offsets`;
> `crates/testing/felix-cluster/tests/caches/cache_failover.rs::a_counter_survives_the_loss_of_its_owner`.

A cache declares a consistency level, as a stream does, and it defaults to
`Leader`: a write is acknowledged once durable on the leader and replication
follows, so losing the leader between the acknowledgement and the ship loses
the write. Under `Quorum` a put or delete is acknowledged only once a majority
of the shard's replicas hold it, and survives the loss of its leader.

> `crates/testing/felix-cluster/tests/caches/cache_failover.rs::a_quorum_acknowledged_cache_write_survives_its_leader`
> and `a_quorum_cache_write_without_a_majority_is_refused`.

Counter updates are the exception: the counter log feeds no quorum mark, so a
counter update on a `Quorum` cache is still acknowledged by the leader.

## Authorization

Enforced, contrary to what this document said for a long time. Tenant-scoped
tokens are verified at the broker, and publish, subscribe and cache operations
each check a permission. A forwarded publish is authorized at both the ingress
broker and the owner — routing does not launder a credential.

> `crates/testing/felix-cluster/src/scenarios.rs`, including
> `unauthorized_publish_is_refused` across both ingress paths.

Per-tenant quotas are **not** enforced.

## Where the guarantees stop

Stated because a guarantee without its failure model is a slogan.

- **The failure model is process loss and partition.** Kill, graceful stop,
  freeze, and severing a broker from its peers while it keeps running are all
  injectable and tested. A partitioned leader keeps heartbeating, so the control
  plane goes on believing it is healthy while it can reach nobody — and it
  cannot acknowledge a `Quorum` publish, because it is not a majority on its own.

  > `a_partitioned_leader_cannot_reach_a_quorum`,
  > `a_healed_partition_restores_the_quorum`,
  > `a_partitioned_leader_still_serves_a_leader_stream`.

- **Clock skew between brokers cannot affect lease safety**, because no lease
  reads a wall clock. Each broker measures its own elapsed time on a monotonic
  clock and gives up a quarter of the lease as margin, so two brokers'
  disagreement about what time it is has nothing to act on. The assumption that
  *does* matter is bounded **process suspension**, and that is injectable — a
  broker frozen past its lease and resumed is the test above.

  A broker suspended *between* the commit-time lease check and its write
  reaching disk is a residual window bounded by the margin. The margin is a
  choice rather than a proof, and it is the one clock-shaped assumption left.
- **Idempotent producers, not exactly-once delivery.** A producer that takes an
  id from the broker and numbers its batches can re-send a batch whose
  acknowledgement never arrived and have it land once: the shard's leader
  answers a sequence it already holds rather than appending it. On a durable
  stream the sequences are in the log — each record carries its producer and
  sequence, and is replicated with them — so this holds across a failover, a
  planned move and a restart: whichever broker leads next answers from the
  records it holds. A producer whose batches retention has removed entirely is
  forgotten and told so (`unknown_producer`), which the client reports rather
  than guess. On an in-memory stream the sequences live in the leader's memory
  and last as long as it does. The consumer side is unchanged: a
  subscriber can still see a record twice on redelivery, and there are no
  transactions. See [`docs/protocol.md`](protocol.md), "Idempotent producers".

  > `a_re_sent_batch_lands_once`, `a_non_leader_names_the_leader`, and
  > `a_gap_is_refused_with_the_expected_sequence` in
  > `crates/testing/felix-cluster/tests/clients/idempotent.rs`, on a `Quorum` stream replicated
  > three ways; `racing_re_sends_append_once` in `crates/server/felix-broker` for the
  > two re-sends that arrive at once.
- **No cross-region ordering or routing guarantees.**
- **Cluster metadata is as durable as its store.** Over Postgres it is what
  the database keeps. Over the embedded Raft group, a member that comes back
  with a wiped volume withholds its vote until it has caught up, so it cannot
  help elect a leader that lacks an acknowledged write
  (`docs/metadata-raft-design.md`, "Rejoining after a lost volume").

  > `the_group_survives_restart_kill_freeze_and_wipe_without_losing_a_write`.
- **A group is bound to the shard the caller names.** Consuming a whole
  multi-shard stream through a group means polling each shard's group
  separately, for the same reason a subscription reads one shard.

  Delivery is by poll rather than push: a consumer takes work when it has
  capacity, and the broker cannot know when that is. A poll can ask the broker
  to wait for work, so an idle consumer costs one open request rather than a
  round trip per attempt. Only the broker leading a shard serves its groups, and
  ownership is re-checked while a poll waits — a shard that moves mid-wait ends
  the wait rather than being served by its former owner.

  The rules are fixed. A group's position on a shard is durable, monotonic, and
  survives a restart. Above that position the broker tracks what has been handed
  out: a record claimed by one consumer is not handed to another while the claim
  stands, a claim that lapses makes the record owed again, and the cursor moves
  only over a contiguous run of acknowledgements — never past a gap, which would
  mark a record finished that nobody finished. Two groups over one shard are
  independent; each sees every record.

  **Retention outranks a group.** A record removed by retention before a group
  reached it is skipped, and the group moves past it — leaving it owed would
  stall the group for ever on a record that exists nowhere. That is the one case
  where a queue drops work, it is counted rather than silent, and it means a
  retention window shorter than a group is allowed to fall behind loses work.

  The in-flight set is deliberately **not** durable. A leader that dies loses it
  and the group resumes from its cursor, so those records are delivered a second
  time. That is at-least-once, which is what a queue offers regardless;
  persisting it would narrow the redelivery window at the cost of a write per
  delivery and still would not make delivery exactly-once.

  **Group state survives failover whole** (#314, and now the dead letters too).
  Both of a shard's group logs — the cursors and the offsets its groups gave up
  on — replicate beside the shard's records, on the same replica set at the
  same generation. A promoted replica resumes each group where it had reached,
  lists what it had abandoned, and serves an operator's redrive. A redrive is
  itself recorded in the dead-letter log before it is acknowledged, so a record
  redriven before a failover is still owed after it, until a consumer finishes
  it.

  > `a_redriven_record_is_still_owed_after_a_restart` and
  > `a_finished_redrive_is_not_redelivered_after_a_restart` in
  > `crates/server/felix-broker`.

  > `crates/testing/felix-cluster/tests/queues/consumer_groups.rs`, including
  > `a_group_position_survives_a_leader_failover` and
  > `a_dead_letter_survives_a_leader_failover`.
- **Retention is per stream and unbounded by default.** A stream with no
  retention policy grows until the disk does not.
