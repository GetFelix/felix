---
title: "Choosing a Configuration"
description: "A few recommended sets of Felix settings, what each one is good for, and what you give up with it."
---

Felix has many settings, and most of them can stay at their defaults. The
choices that matter are few: whether a stream is durable, how many brokers
must hold a record before the publisher is told it is safe, how many copies
each shard keeps, and when bytes are flushed to disk. This page gives a small
number of tested combinations of those choices and says when to use each.

Start from the set that matches your workload. Change a setting only when you
know which guarantee you are trading for it. Every other setting is in the
[configuration reference](/reference/configuration/) and the
[environment reference](/reference/environment-variables/).

| You are building | Use |
| --- | --- |
| Anything where an acknowledged record must not be lost: orders, payments, events other systems depend on | [Production default](#production-default) |
| A job or task queue | [Work queue](#work-queue) |
| Shared state, sessions, feature flags, counters | [Replicated cache](#replicated-cache) |
| Metrics, logs, telemetry, where a small loss on a failover is acceptable | [High-throughput telemetry](#high-throughput-telemetry) |
| Live updates nobody replays: presence, live dashboards, game ticks | [Live fan-out](#live-fan-out) |
| Trying Felix, writing code against it, CI | [Development](#development) |

A few terms used below. A stream is split into **shards**. Each shard has a
**leader**, the broker that takes its writes, and **followers** that hold
copies. The **replication factor** (RF) is the number of copies, leader
included. **Consistency** decides when a publish is acknowledged: `Leader`
answers once the leader has the record, `Quorum` once a majority of the
copies do.

## Production default

**Good for** any stream whose records other systems rely on.

**Why.** A `Quorum` acknowledgement means a majority of the shard's copies hold
the record, so it survives losing the leader, its machine or its disk. With
three copies in three zones, one zone can go down and the stream keeps
taking writes. `on_commit` makes every acknowledged record reach the device
before the answer, so a power loss loses nothing that was acknowledged.

`Leader` cannot promise that. A `Leader` acknowledgement means one broker has
the record. When that leader dies, a follower is promoted only if the
leader's last report showed it caught up, and anything the leader
acknowledged after that report is lost. If no follower was reported caught
up, the shard waits for the old leader to come back with its disk. The size
of that window is `felix_broker_replication_lag_records`.

`Quorum` also keeps working through a control-plane outage. A `Quorum` stream
goes on acknowledging writes its followers hold, while a `Leader` stream
stops once its lease runs out, 5 to 11 seconds into the outage with the
defaults. And a `Quorum` stream is failed over on its followers' word, about
5 seconds after its leader stops answering, instead of waiting for the leader
to be marked down.

**Settings.** On every broker, in addition to the cluster settings from
[Kubernetes](/deployment/kubernetes/) or [Docker Compose](/deployment/docker-compose/):

```bash
FELIX_DURABLE_STORAGE_DIR=/var/lib/felix
FELIX_DURABLE_FSYNC_MODE=on_commit
FELIX_NODE_ZONE=zone-a                    # this broker's zone
FELIX_DURABLE_RETENTION_BYTES=10737418240 # per shard; unset keeps everything
```

Run at least three brokers in at least three zones. Leave
`FELIX_INTERNAL_FENCE` and `FELIX_QUORUM_READS` unset, and leave
`FELIX_ACK_ON_COMMIT` unset so a publish is answered after it is written.

```bash
felixctl stream create orders --shards 4 --replication 3 --consistency quorum
```

**Fleet features.** A new cluster finalizes the ones this set relies on by
itself at its first broker registration: `generation_start`, `majority_ack`,
`lease_free_reads` and `fenced_caches`, along with the rest except
`publisher_principal`. Check with `felixctl features ls`. On a cluster
upgraded from an older release, finalize those four in that order once every
broker and control-plane member runs the new release, as in the
[upgrade runbook](/deployment/upgrades/#runbook-rolling-out-a-fleet-feature).
Finalizing is one-way. Leave `joint_fence` and `replica_election` alone
unless you want [elections](#riding-out-a-control-plane-outage).

**Trade-off.** Each publish waits for a second broker and for a device flush.
Concurrent publishes to a shard share one flush (group commit), so the cost
falls as concurrency rises; `felix_storage_sync_batch_appends` shows how many
appends each flush served. A shard that cannot reach a majority refuses
writes after `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` (5 s) with `quorum_timeout`,
which means the outcome is unknown, not that the write failed. Use an
idempotent producer so a retry cannot land twice. While a follower is down,
retention keeps the records it has not acknowledged, so plan disk for that.

`FELIX_DURABLE_FSYNC_MODE=periodic` (the default, a flush every 250 ms) is a
cheaper middle ground. A majority must then lose power at once to lose an
acknowledged record, which separate zones make unlikely. The cost shows after
a machine crash: the broker comes back under a new storage id and its shards
are copied to it again, and if a leader and a follower of the same shard lose
power together, that shard stays closed until an operator steps in.

Depth: [Choosing durability](/deployment/production/#choosing-durability),
[Consistency](/architecture/semantics/#consistency-how-many-brokers-must-hold-it),
[Durability policies](/architecture/durable-storage/#durability-policies).

## Work queue

**Good for** jobs where each record is handled by one worker, retried on
failure, and dead-lettered after too many attempts.

**Why.** A queue in Felix is a consumer group on a stream, so it inherits the
stream's durability. Use the production default stream, and workers share
its records through the group.

**Settings.** The production default, plus the group limits if the defaults
do not fit your jobs:

```bash
FELIX_GROUP_VISIBILITY_TIMEOUT_MS=30000   # how long a worker holds a record
FELIX_GROUP_MAX_ATTEMPTS=5                # then the record is dead-lettered
```

```bash
felixctl stream create jobs --shards 8 --replication 3 --consistency quorum
```

Shards set how many workers can make progress in parallel. A worker can
extend its hold on a record that takes longer than the visibility timeout.

**Fleet features.** As for the production default. `group_cursor_dependencies`
is the one that matters most here; a new cluster finalizes it by itself.

**Trade-off.** Delivery is at least once. A record is redelivered when its
worker does not acknowledge it in time, and an acknowledgement can be lost on
a failover, so handlers must be safe to run twice. Consumer groups do not
take part in elections, so a group waits for the control plane when its
shard's leader dies during an outage.

Depth: [Queues](/features/queues/).

## Replicated cache

**Good for** keyed state many services read: sessions, feature flags, rate
limit counters.

**Why.** A replicated `Quorum` cache acknowledges a put once a majority holds
it, and with `lease_free_reads` finalized a read confirms with a majority
that its broker still leads the shard before it answers. That rules out a
stale read from a replaced leader without relying on clocks.

**Settings.** The production default broker settings. A cache is backed by a
log only on brokers with `FELIX_DURABLE_STORAGE_DIR`; without it the cache is
in memory and lost on restart.

```bash
felixctl cache create sessions --shards 4 --replication 3 --consistency quorum
```

**Fleet features.** `fenced_caches` and `lease_free_reads`, after
`generation_start` and `majority_ack`. A new cluster finalizes all four by
itself.

**Trade-off.** Every read costs a round trip to the nearest majority, shared
by concurrent reads of the shard. `FELIX_QUORUM_READS=lease` makes reads
cheaper by trusting the lease and the clocks, but a broker started with it
does not report `lease_free_reads`, so the fleet cannot finalize it, and once
it is finalized such a broker is refused. A `Leader` cache is faster still
and can lose its newest writes on a failover.

Depth: [Cache](/features/cache/#6-consistency-levels),
[Cache semantics](/architecture/semantics/#cache-semantics).

## High-throughput telemetry

**Good for** metrics, logs and traces, where throughput and latency matter
more than every last record.

**Why.** `Leader` answers after one broker writes the record, with no
replication round trip on the publish path. The follower copy still lets a
failover keep almost everything.

**Settings.** The broker defaults for fsync (`periodic`, every 250 ms).

```bash
FELIX_DURABLE_STORAGE_DIR=/var/lib/felix
```

```bash
felixctl stream create telemetry --shards 16 --replication 2 \
  --retention-secs 86400
```

Publish in batches, with `publish_batch` in the Rust client or its equivalent
in the others. A batch is one acknowledgement and one entry in each
subscriber's queue, so it costs far less per record than single publishes.
More shards spread the load over more brokers and cores.

**Fleet features.** Nothing beyond what a new cluster finalizes by itself.

**Trade-off.** On a failover the stream loses what the leader acknowledged
after its last replica report, which `felix_broker_replication_lag_records`
measures. A power loss can lose the last 250 ms on the leader. During a
control-plane outage writes stop once the leader's lease runs out. If the
follower was not reported caught up when the leader died, the shard waits
for the old leader.

Depth: [Performance](/features/performance/),
[Benchmarks](/features/benchmarks/).

## Live fan-out

**Good for** messages that are only useful right now: presence, cursor
positions, live prices on a screen.

**Why.** An in-memory stream skips the disk and replication entirely. It has
the lowest latency and no storage to size.

**Settings.**

```bash
felixctl stream create presence --shards 4 --durable false
```

**Fleet features.** None. In-memory streams are not replicated.

**Trade-off.** Nothing survives a broker restart, and a subscriber cannot
replay. Records carry no offsets, so a subscriber that falls behind and
loses records cannot tell from the stream; only
`felix_sub_queue_dropped_total` on the broker shows it.

## Development

**Good for** trying Felix, local work and CI.

**Why.** One command starts a control plane and brokers on free ports.

**Settings.** `task cluster:up` (see [Local development](/deployment/local/)).
Its brokers have durable storage, so streams can stay at the
`felixctl stream create` defaults: one shard, one copy, `Leader`, durable. A
broker you run by hand without `FELIX_DURABLE_STORAGE_DIR` refuses durable
streams, so give it a data directory or create streams with
`--durable false`. Test against the consistency you will run in production, though, since
`Quorum` changes what errors a publish can return.

**Fleet features.** None to run by hand.

**Trade-off.** One copy. Losing the data directory loses the stream.

## Settings that apply to every set

**Subscriber queues.** Each subscriber has its own bounded queue, so a slow
subscriber never slows a publisher. When the queue is full, the default
`FELIX_SUB_QUEUE_POLICY=drop_new` drops new records for that subscriber. On a
durable stream the Rust client reports the drop with the offset to resume
from, and its cluster subscription resubscribes from there by itself. Keep the default. `block` makes every publisher of
the shard wait for the slowest subscriber. A consumer that must process every
record should read through a consumer group or resume from offsets.

**Zones.** Set `FELIX_NODE_ZONE` on every broker and run at least as many
zones as your largest replication factor. Placement puts a shard's copies in
different zones when it can. `felix_shards_zone_unspread` is non-zero while it
cannot.

**Moves and restores.** When a broker stays down for
`FELIX_SHARD_RESTORE_AFTER_MS` (5 minutes), its shards are copied to another
broker. Keep that above your longest planned restart so a rolling upgrade
does not start copies. A broker whose storage was replaced rejoins as a
follower and counts again once its leader shows it caught up.
See [Adding and removing brokers](/deployment/scaling/) and
[Sharding](/features/sharding/#where-shards-live).

## Riding out a control-plane outage

Run the control plane highly available, with three or five Raft members or
two or more instances over a highly available Postgres
([Control-plane HA](/deployment/control-plane-ha/)). That is the main
defence.

With the production default, brokers keep serving `Quorum` streams and caches
while the control plane is down. What they cannot do on 0.7.0 by default is
replace a leader that dies during the outage: that shard waits until the
control plane is back. Replica elections close that gap. They are off by
default in 0.7.0 and need `FELIX_INTERNAL_ELECTION=true` on every broker and
the `joint_fence` and `replica_election` fleet features finalized; see
[Failover without the control plane](/deployment/failover-without-control-plane/)
before turning them on.
