---
title: "Demo: Leader vs Quorum"
description: "The same fault put to both consistency levels on a real three-node cluster."
---

## What this shows

[`Quorum` and `Leader`](/architecture/semantics/#consistency-how-many-brokers-must-hold-it)
are one field on a stream. This demo puts the **same fault** to both and shows
what that field actually bought.

```bash
task cluster:consistency
# slower, to read as it runs: task cluster:consistency -- --pace 3
```

It runs the same script twice, once per level. Each run starts a real
three-node cluster with one stream, `orders`, replicated three ways. The
brokers run the Raft shard engine, so the stream's shard is one Raft group of
three members.

## The fault

A publisher sends a record to the shard's leader every 20 ms and stops at the
first publish that is not answered within a second. While it runs, the leader
is **cut off from its two followers**. It keeps running and clients can still
reach it, but no broker can. Then the leader is killed, the partition is
healed, and the two brokers that are left elect one of themselves.

The demo reads the whole stream back from the new leader and counts the
records the publisher was told were written that are no longer there.

## What happens

**Quorum stops answering.** A `Quorum` publish is acknowledged once a majority
of the group holds it. A leader with no followers has no majority, so the
first publish after the cut is never answered and the publisher stops. Every
record it was told about is on the new leader:

```
── Quorum: reading the stream back from <new leader>
  All <n> acknowledged records are here.
```

**Leader keeps answering, and those records are lost.** A `Leader` publish is
acknowledged from the leader's own log while its lease holds (1.5 seconds by
default, `FELIX_SHARD_RAFT_LEASE_MS`). For that long after the cut the leader
goes on answering, and nothing it answers reaches another broker. When it is
killed, those records go with it:

```
── Leader: reading the stream back from <new leader>
  <m> of <n> acknowledged records are gone: Leader-<first> to Leader-<last>.
  <old leader> answered them alone, and they were only ever on its disk.
```

The demo fills in the broker names and the counts, which differ from run to
run. What does not change is their shape: the
missing records are the last ones acknowledged, with no gap before them. Once
the lease runs out the leader answers a `Leader` publish as it would a
`Quorum` one, so the loss is bounded by the lease.

## The point

Both shards stay available. The two followers are a majority, so they elect a
leader whichever level the stream has.

| | Acknowledged when | A lost leader costs |
|---|---|---|
| **Quorum** | a majority holds the record | nothing that was acknowledged |
| **Leader** | the leader has written it, while its lease holds | what it acknowledged alone |

`Leader` is the default because one round trip beats two and most streams would
rather have the latency than the guarantee. Set `consistency: Quorum` on the
ones that must keep every acknowledged record across a failover.

## Its counterpart

[`task cluster:failover`](/architecture/testing/#the-cluster-harness) is the other half:
a quorum-acknowledged record **surviving** the loss of the broker that
acknowledged it, read back from the broker elected in its place, while a
client configured with one address keeps publishing.

## Honest limits

- Three brokers on one machine. The fault is one a single machine can produce.
- The demo exits non-zero if the `Quorum` stream is missing any acknowledged
  record. It does not fail when the `Leader` stream loses none: a cut that
  lands between two publishes can leave nothing answered alone, and the demo
  says so when that happens.
- It also exits non-zero when a publisher is acknowledged nothing before the
  cut, since such a run shows nothing.
- A replay on a broker that has just taken a shard over can stop below
  acknowledged records
  ([#3112](https://github.com/GetFelix/felix/issues/3112)). The demo reads
  until a replay reaches a record it published after the failover, and prints
  `replay stopped short (#3112)` each time one does not.
- The 0.7 releases replicate with an older engine, where the same fault makes
  a `Leader` shard unavailable instead: no follower is promoted until the old
  leader returns with its disk. This demo does not show that engine.

## See also

- [Semantics: consistency](/architecture/semantics/#consistency-how-many-brokers-must-hold-it)
- [Cross-broker Publishing](/demos/cross-broker-cluster/)
