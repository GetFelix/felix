---
title: "Diagnosing a Cluster"
description: "What to look at, in order, when a shard will not serve, a follower falls behind, a subscriber misses records or lags, a client is refused, local disk fills while offload is on, or a broker will not start."
---

This page is organised by what you see. Each section says what to run, in
order, how to read the answer, what usually causes it and what to do. Errors
and log lines are quoted as Felix prints them. For problems getting a broker
or a client set up in the first place, see the
[troubleshooting guide](/reference/troubleshooting/).

## The tools

Three places answer most questions.

`felixctl inspect shard` asks the brokers that hold a shard for their own view
of it: whether the leader serves and why not, the fence a promoted leader waits
on, its lease, the tail and commit mark, and each follower's position.
`felixctl inspect subs` asks each broker for the subscriptions it serves: each
one's queue, the records it has dropped, and how far behind the tail it is.
Both go over the authenticated client listener and need a broker token allowed
`node.view` on `cluster:*`. See [felixctl](/getting-started/felixctl/#inspecting-a-cluster).

The broker's metrics listener (`FELIX_BROKER_METRICS_BIND`, port `8080` in the
chart) serves `/metrics`, `/replication/halted` and `/backup/offsets`. It has no
authentication, so it carries counts and listings and nothing that changes
anything. Keep it on an internal network.

The control plane says what placement intends: `felixctl shard ls` for the
stored assignments, and `felix-controlplane admin plan` for what the next
placement pass would do with each shard and why it is waiting.

More `felixctl inspect` commands are coming
([#1077](https://github.com/GetFelix/felix/issues/1077)): `inspect conns` for
connections, `inspect decisions` for the control plane's placement decisions, `inspect segments` and `inspect records`
for a data directory read offline, and `inspect record` for which replicas
hold one offset.

## A shard is not serving, or refuses writes

**What you see.** Publishes to one shard fail with `shard_unavailable`, while
other shards of the same stream work. The error's `detail.reason` is one of:

| `reason` | Message |
| --- | --- |
| `not_assigned` | `shard is not assigned to any broker` |
| `owner_unavailable` | `shard owner is unavailable: ...` |
| `not_ready` | `this broker is still opening the shard` |
| `moving` | `shard is moving to another broker` |
| `stale` | `routing view is behind: have generation N, caller has M` |
| `fenced` | `lease lapsed: this broker may no longer lead the shard, so it commits no writes until the lease is renewed` |

`shard_unavailable` is retryable: nothing was applied. A client that retries
rides out a move or a short failover. Look further when it does not clear.

**What to run.** Inspect the shard, naming the shard the errors are for:

```bash
felixctl inspect shard orders --shard 3
```

```
shard       acme/default/orders/3 (stream)
generation  42, move to broker-c staged
leader      broker-a  not serving: fencing, 0 of 2 replicas took the fence (4 attempts, next in 2s)
lease       held, 7.1s left
offsets     tail 1048576  committed 1048510

REPLICA   ROLE      NEXT OFFSET  LAG  FENCE  STATE
broker-a  leader    1048576      -    -      fencing
broker-b  follower  -            -    no     fencing
broker-c  learner   -            -    no     fencing
```

**How to read it.** The `leader` line is the leader's own answer. `serving`
means it takes writes; anything else gives the reason:

| Reason | What it means | What to do |
| --- | --- | --- |
| `opening` | The broker is recovering the shard's log before it serves. Clients get `not_ready`. | Wait. A large active segment takes a while to scan. If it does not finish, read the broker's log for the shard. |
| `fencing` | A promoted leader is waiting for a majority of its replicas to take its generation. Clients get `not_ready`. | See [Stuck fencing](#stuck-fencing) below. |
| `failed` | The log could not be opened. The detail is the error. The shard is not retried until the assignment changes. | See [A broker will not start](#a-broker-will-not-start-or-will-not-open-a-shard): the same storage errors apply. |
| `draining` | The leader stopped serving for a move. Writes are held for the cut-over and then forwarded. | Look at the move with `felix-controlplane admin plan`. A move that does not finish is waiting for its destination to catch up. |
| `lease_lapsed` | The broker could not renew its lease with the control plane, so it may no longer lead. Clients get `fenced`. | Check that the broker reaches the control plane (`FELIX_CONTROLPLANE_URL`) and that its node credential is accepted. Writes resume once the lease is renewed. |
| `behind_generation` | The broker still serves an older generation than the assignment it holds. | Usually passes within a pass of the assignment feed. If it persists, the broker is not applying assignments: read its log. |
| `not_assigned_here` | This broker does not lead the shard. | The detail names the leader. Inspect that broker. |

If the leader line reads `unreachable`, felixctl could not reach the leader at
the address the cluster advertises for it. Each replica's own phase and tail is
still shown. Clients see `owner_unavailable` until the control plane notices
the broker is gone and fails the shard over; see
[the next section](#a-shard-lost-its-leader-and-waits-for-a-replica).

### Stuck fencing

A broker promoted to lead a shard by a failover fences it before it serves: it
asks every replica to take the new generation and refuse the old leader, and
takes the log of any replica that is ahead. It serves once a majority, counting
itself, has answered. Once the fleet has finalized `majority_ack` it never
opens on the lease instead, because the old leader no longer stops writing when
its lease runs out. A replica that cannot be reached, or does not offer the
fence, counts as one that has not answered. Before `majority_ack`, a shard
whose replica does not offer the fence opens on the lease.

`0 of 2 replicas took the fence` means neither replica answered in the latest
attempt. The attempts back off from 200 ms, doubling to a 2 s cap. `FENCE`
shows which replica took it. A detail that goes on to say a replica
`has accepted a newer generation` means a replica already follows a later
leader: this promotion is over, and the assignment feed will move on.

A shard handed back to the broker that led it before a move at the generation
right after the draining one, and a move's destination taking over, do not
fence. A broker given a shard back at any later generation does, for example
after a move finished and its destination then failed.

**What to do.** Make a majority of the replicas reachable from the leader on
the peer listener. Check each replica's broker is running, that the peer port
(`5001` in the chart) is open between them, and that peer mTLS is accepted
(see [A peer is refused](/reference/troubleshooting/#a-peer-is-refused)). The shard opens on the next attempt after
a majority answers. `felix_broker_promotions_opened_total{path="fenced"}`
counts promotions that opened this way, and
`felix_broker_shard_phase{phase="fencing"}` how many shards wait now.

## A shard lost its leader and waits for a replica

**What you see.** Publishes fail with `owner_unavailable` or `not_assigned`
for minutes, and `felixctl shard ls` still names a leader whose broker is down.
The control plane's `felix_shards_unplaceable` gauge is above zero.

**What to run.**

```bash
felix-controlplane admin plan
felixctl inspect shard orders --shard 2
```

**How to read it.** `plan` lists the shard as `unplaceable` with one of:

- `the leader is gone and no replica holding this shard's log can take over`
- `the only copy of this shard's log is on broker-1, which is not serving; waiting for it to return`

The control plane promotes only a follower its leader last reported as holding
the whole log. Promoting one that was behind would lose acknowledged records,
so when no follower was reported caught up, the shard waits. A report older
than its freshness window counts as no report. `inspect shard` shows each
replica's own tail and accepted generation, which tells you how far each got.

**What to do.** Bring the old leader back if you can: the shard resolves on its
own when it returns, or when a replica's report says it caught up. If the
leader's data is gone for good,
[`felixctl placement abandon`](/deployment/moving-shards/#abandoning-a-shards-log)
gives up the shard's log and places it afresh:

```bash
felixctl placement abandon orders 2 --yes
```

The new leader starts from whatever it holds, usually nothing, at a new
generation. Records only the old leader held are lost, acknowledged ones
included. The control plane refuses with `409 not_stranded` while the leader
is serving or a replica can take over without loss.

## A follower is lagging or halted

**What you see.** `felix_broker_replication_lag_records` on a leader keeps
growing, or `felix_broker_replication_halted` is above zero. On the control
plane, `felix_shard_replicas_halted` counts the same halts.

**What to run.**

```bash
curl -s http://broker-a:8080/replication/halted | jq
felixctl inspect shard orders --shard 3
```

```
REPLICA   ROLE      NEXT OFFSET  LAG      FENCE  STATE
broker-a  leader    1048576      -        -      active
broker-b  follower  1048510      66       -      shipping
broker-c  learner   900          1047676  -      halted (diverged)
```

**How to read it.** `STATE` is the leader's view of each follower:

| State | Meaning |
| --- | --- |
| `shipping` | The follower answered the last batch and is moving. A small `LAG` is normal under load. |
| `stalled` | The last batch did not reach it or was refused. A new cursor starts stalled until the first answer. |
| `copying` | A move's destination still copying the log. It counts toward no quorum yet. |
| `rebuilding` | The follower discarded its copy at the leader's request and is copying it again. |
| `halted` | Shipping stopped. The reason is in brackets. |

`/replication/halted` lists the same halts with a `remedy`. `diverged` means
the follower holds different bytes at an offset the leader also holds.
`needs_bootstrap` means it needs records the leader's retention removed. Both
are rebuilt by the leader itself, as many at once as
`FELIX_REPLICATION_REBUILD_MAX_CONCURRENT` allows (default `1`), and the entry
clears when the rebuild is done. `fenced` means the follower knows a newer
leader: this broker is no longer the leader, and it clears when the assignment
feed catches up.

**What to do.** For a `stalled` follower, check that its broker is up and the
peer port is reachable. For a halt that stays, see
[Replication has stopped for a replica](/reference/troubleshooting/#replication-has-stopped-for-a-replica).
Placement replaces a copy that stays halted past `FELIX_SHARD_RESTORE_AFTER_MS`.

## Quorum writes time out

**What you see.** Publishes to a `Quorum` stream fail with `quorum_timeout`:

```
the batch is durable here but did not reach a majority within 5s
```

Felix does not refuse a `Quorum` publish up front when it cannot reach a
majority. The leader writes it, ships it, and waits up to
`FELIX_PUBLISH_QUORUM_TIMEOUT_MS` (default `5000`) for a majority to hold it.
The retry class is `outcome_unknown`: the record is on the leader and may yet
reach a majority, so a retry can write it twice unless the producer is
idempotent. `leadership_lost` is the same outcome when the leader changed
during the wait. `felix_broker_publish_quorum_failed_total{reason}` counts
both.

**What to run.**

```bash
felixctl inspect shard orders --shard 0
```

**How to read it.** With three replicas a majority is the leader and one
follower. If both followers are `stalled` or `halted`, or listed as
`unreachable`, no majority can form, and every `Quorum` write to the shard
times out. A `committed` mark well below `tail` that does not move is the same
thing seen from the mark.

**What to do.** Restore a majority: start the followers' brokers or fix the
peer network between them. Placement restores a follower whose broker has been
gone for `FELIX_SHARD_RESTORE_AFTER_MS` by copying the shard to another broker.

## A subscriber misses records

**What you see.** A subscriber on a durable stream sees offsets jump, or ends
with `subscription fell behind; records from offset N were dropped`. On an
in-memory stream there are no offsets, so records are simply missing.

**What to run.** Find the subscriptions that have dropped records:

```bash
felixctl inspect subs orders --dropping
```

```
NODE      STREAM/SHARD           SUB  CONNECTION          PRINCIPAL  POLICY    QUEUE    DROPPED  POSITION  TAIL     BEHIND
broker-a  acme/default/orders/0  4    17 10.0.4.12:50122  p:billing  drop_new  512/512  3812     1040000   1048576  8576
```

Then confirm the gap from the subscriber's side, reading the shard from just
before where it went missing:

```bash
felixctl sub orders --shard 0 --from 1039000 --format offsets
```

Each line is `shard`, `offset` and payload, tab-separated. A gap in the offsets
your application saw is records this subscriber did not get.

**How to read it.** Each subscriber has its own bounded queue on the broker
that serves its shard, and the stream's overflow policy decides what a full one
costs. `QUEUE` is the queue's depth and capacity, counted in batches, and
`DROPPED` is the records lost from it since the subscription started. With
`drop_new`, the default, a publish that finds the queue full drops that batch
for this subscriber only, so a slow subscriber never slows a publisher.
`drop_old` is accepted and behaves as `drop_new`. `CONNECTION` and `PRINCIPAL`
say which client it is: the broker's connection id, the client's address and
the token's `sub`.

`DROPPED` matches the gap: a subscriber that has dropped 3812 records is
missing exactly 3812 offsets. A client that offered
`FEATURE_SUBSCRIPTION_LAGGED` has its subscription ended at the first drop, with
`subscription_lagged` naming where to resume; any other client sees only the
jump. `felix_sub_queue_dropped_total` on the broker's `/metrics` counts the same
drops across every subscriber, without saying whose they were.

**Likely causes.**

- The application handles records more slowly than they arrive. The queue
  stays full (`512/512`) and `DROPPED` keeps climbing.
- A burst is larger than the queue. The queue is empty again now and `DROPPED`
  no longer moves.
- The client is on a slow or lossy network: QUIC flow control holds the writes
  back and the queue fills behind them.

**What to do.** Make the subscriber keep up, or give it a larger queue with
`queue_capacity` on `subscribe` (up to `FELIX_SUBSCRIBER_QUEUE_CAPACITY_MAX`),
then resubscribe from the offset the gap starts at. A subscriber that must not
miss anything should read through a consumer group or `stream_read` instead.
`FELIX_SUB_QUEUE_POLICY=block` makes the broker wait for room instead of
dropping, but then every publisher of the shard waits for the slowest
subscriber.

## A subscriber falls behind

**What you see.** Records arrive late, and the delay grows. Nothing is
reported as dropped yet.

**What to run.**

```bash
felixctl inspect subs orders --shard 0
```

```
NODE      STREAM/SHARD           SUB  CONNECTION          PRINCIPAL  POLICY    QUEUE    DROPPED  POSITION  TAIL     BEHIND
broker-a  acme/default/orders/0  4    17 10.0.4.12:50122  p:billing  drop_new  498/512  0        1040000   1048576  8576
broker-a  acme/default/orders/0  5    18 10.0.4.31:41870  p:audit    drop_new  0/512    0        1048576   1048576  0
```

Run it a few times and watch `QUEUE` and `BEHIND`.

**How to read it.** `POSITION` is one past the last offset the broker has taken
from the subscriber's queue to write to the client. `TAIL` is the shard's next
offset, and `BEHIND` the difference. A subscriber that keeps up has a queue
near `0` and `BEHIND` near `0`, like subscriber 5 above. Subscriber 4's queue is
almost full: unless it catches up, the next batches are dropped and it moves
to [the section above](#a-subscriber-misses-records).

`POSITION` is `-` on an in-memory stream, which has no offsets, and for a new
subscription until its first live batch. A subscription that started from an
offset is caught up from disk first, and that history does not count here.

With `--json` each broker prints one line with `node_id`, `subscriptions` and
`next_cursor`, and every subscription carries `behind` when it has a position:

```bash
felixctl inspect subs --json | jq -c '.subscriptions[] | select(.behind > 10000)'
```

**Likely causes.** The same as for drops, earlier: a slow consumer, a burst,
or a slow network. One subscriber behind while others on the same shard keep
up points at that client. Every subscriber of the shard behind together points
at the broker or its network.

**What to do.** Speed up or scale out the consumer, for instance by splitting
the work over a consumer group. If every subscriber of a broker falls behind
together, look at that broker's CPU and network before the clients.

## A client is refused with `forbidden`

**What you see.** An `error` with code `forbidden` and the message `forbidden`
or `tenant mismatch`. The retry class is `fatal`: the same token will be
refused again.

**How to read it.** `tenant mismatch` means the request names a tenant other
than the one the token was minted for. `forbidden` means the token lacks the
action on that object. What each request needs:

| Request | Grant |
| --- | --- |
| Publish | `stream.publish` on `stream:{tenant}/{ns}/{stream}` |
| Subscribe, read, `offset_for_time` | `stream.subscribe` on the stream |
| Cache get, watch | `cache.read` on the cache or the key |
| Cache put, delete, counters | `cache.write` on the cache or the key |
| Consumer group poll, ack, nack | `group.consume`, or `stream.subscribe` |
| Group seek, delete, redrive, discard | `group.manage`, or `stream.manage` |
| `shard_inspect`, `subscriptions_list` (`felixctl inspect`) | `node.view` on `cluster:*` |

`felixctl inspect` refused with `shard_inspect needs node.view:cluster:*` (or
`subscriptions_list needs ...`) needs
a token with exactly that grant. A wildcard such as `node.view:*` does not
count, and no tenant admin can grant it.

**What to do.** Mint a token with the grant, or fix the tenant the client
authenticates under. See [Security](/features/security/).

## Local disk is filling while offload is on

**What you see.** With `FELIX_DURABLE_OFFLOAD_DIR` set, a broker's data disk
keeps growing although the streams have a retention bound, and the broker logs:

```
offload pass failed; un-copied segments stay on local disk until it succeeds
retention is keeping segments past their bound until offload can copy them; local disk will keep growing
```

**What to run.** On the broker's `/metrics`:

```bash
curl -s http://broker-a:8080/metrics | grep felix_storage_offload_
```

**How to read it.** Retention deletes a segment only once its copy is verified
and recorded, and it keeps waiting however long that takes.
`felix_storage_offload_failing_logs` is the number of shard logs whose last
offload pass failed, `felix_storage_offload_held_bytes` the bytes retention
is keeping for want of a copy, and `felix_storage_offload_failures_total`
rises once per failed pass. The log lines name the shard, the directory and
the error. They repeat at most every 30 seconds at first, backing off to every
15 minutes. Publishes and reads are not affected until the disk is full.

**Likely causes.**

- The offload directory's mount is missing, so the directory cannot be created.
- The mount is read-only, or the broker's user cannot write to it.
- The offload volume is full.

**What to do.** Fix the offload directory. The next pass, one
`FELIX_DURABLE_RETENTION_INTERVAL_SECONDS` later, copies the backlog oldest
first, logs `offload is copying again`, and retention deletes what it was
holding; `felix_storage_offload_held_bytes` drops back to 0. If the data disk
will fill before the directory can be fixed, unset `FELIX_DURABLE_OFFLOAD_DIR`
and restart the broker: retention then deletes without waiting for a copy, and
the segments it deletes are not offloaded. Once the disk is full, writes to
it are refused (`felix_storage_full_total`). See
[Durable storage](/architecture/durable-storage/).

## A broker will not start, or will not open a shard

**What you see.** The broker exits at startup, or `inspect shard` says
`failed` with the open error, and `felix_broker_shard_open_failures_total`
rises.

**How to read it.** Startup recovery tells a torn tail from interior
corruption. A tail cut short by a crash is repaired, and the broker logs
`repaired a torn tail in the active segment` and starts.
`felix_storage_recovery_truncated_bytes` says how much it dropped. A record
that is damaged anywhere else is not repaired, because that would silently
drop records a client was told are stored. The broker refuses with:

```
corruption detected: record checksum mismatch (expected ..., found ...) (shard=t/ns/s/0, segment=4, position=0)
```

A checksum failure on the very last record counts as interior corruption
unless `FELIX_DURABLE_REPAIR_CHECKSUM_TAIL` is `true`. That setting is
defensible with `FsyncMode::None` and not with `OnCommit`.

**What to do.** Keep the damaged directory for diagnosis and restore the shard
from a replica or a backup point; see
[Backup and restore](/deployment/backup-and-restore/).
[Durable storage](/architecture/durable-storage/) has the recovery rules in
full. `inspect segments` will report the verdict startup would reach, without
starting the broker
([#1077](https://github.com/GetFelix/felix/issues/1077)).
