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

It starts a real three-node cluster with two streams that are identical except
for `consistency`, both replicated three ways.

## The fault

A leader **cut off from its replicas**. The followers are frozen with `SIGSTOP`,
not killed, so the leader still believes they are there and keeps trying to
ship to them. It is healthy, and alone.

Each stream's own followers are frozen in turn, so the run does not depend on
the two streams happening to share a leader and cannot flake on where placement
put them.

## What happens

**Quorum refuses the write.** No majority is reachable, so the publish is not
acknowledged, and the broker says exactly what it does and does not know:

```
publish failed: the batch is durable here but did not reach a majority within 5s
```

The publisher is told while it still holds the record. The shard stays
available throughout.

Note the shape of that message. It is not "the write failed": the batch *is*
durable on the leader. The demo is careful about the same thing: the refused
record may still be **present** afterwards, because it landed on the leader's
log before the answer came back. That is not a bug. A refusal means *"this
cannot be vouched for"*, not *"this did not happen"*, and a publisher that
retries needs to expect a duplicate.

**Leader takes the write, and then the shard goes unavailable.** The leader
acknowledges from its own log alone. It is then killed while its replicas are
still frozen, so they never receive the record.

No replica is promoted. Both are alive and reachable, and the control plane
still refuses:

```
the leader is gone and no replica holding this shard's log can take over
```

Promoting either one would open the shard **without** a record that was
acknowledged, and a reader could not tell the difference between "never
published" and "lost". So the shard stays unavailable until the old leader
returns with its disk.

## The point

Neither outcome is data loss, and that is the part worth taking away.

`Leader` does not trade safety for latency. It trades **availability** for
latency, and it moves the moment you find out:

| | You learn about it | While you |
|---|---|---|
| **Quorum** | at publish time | still hold the record, and can retry |
| **Leader** | at failover time | wait for a dead broker's disk |

`Leader` is the default because one round trip beats two and most streams would
rather have the latency than the guarantee. Set `consistency: Quorum` on the
ones that would rather be refused.

## Its counterpart

[`task cluster:failover`](/architecture/testing/#the-cluster-harness) is the other half:
a quorum-acknowledged record **surviving** the loss of the broker that
acknowledged it, read back from the replica promoted in its place. That one
shows the guarantee working; this one shows what it costs and what happens
without it.

## Honest limits

- Three brokers on one machine. The fault is one a single machine can produce.
- The demo asserts both outcomes and exits non-zero if either changes, in
  particular if a shard is ever served **without** a record its leader
  acknowledged, which would be silent loss rather than the unavailability shown
  here.
- Recovery is described but not demonstrated. The harness can restart a
  killed broker over its data directory (`Cluster::restart_node`), but this demo
  does not, so the shard coming back with the old leader's disk is not
  something you watch happen here.

## See also

- [Semantics: consistency](/architecture/semantics/#consistency-how-many-brokers-must-hold-it)
- [Cross-broker Publishing](/demos/cross-broker-cluster/)
