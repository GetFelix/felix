---
title: "Moving shards by hand"
---

Placement moves shards on its own: off a draining broker, and from a broker
leading more than its share to one leading less (see
[Adding, draining and removing brokers](/felix/deployment/scaling/)). This
page is for when you want to steer that: see what is moving and what would
move next, move a shard yourself, cancel a move, stop placement starting any,
or give up a shard whose log is out of reach.

Every command below is `felix-controlplane admin`, a client of the control
plane's HTTP API, so the same things can be done with `curl` against the
endpoints in the [control-plane API](/felix/api/control-plane-api/#shard-moves-and-placement).

```bash
export FELIX_CONTROLPLANE_URL=http://felix-controlplane:8443
export FELIX_TOKEN=<a Felix token>
```

Looking needs `node.view:cluster:*`. Anything that changes a move needs
`node.manage:cluster:*`, the same permission that drains a broker. `--url`
and `--token` override the two variables, and `--json` prints the API's
response instead of a table.

## What is moving

```bash
felix-controlplane admin moves
```

```text
SHARD           STEP    REASON    LEADER    DESTINATION  LAG   STARTED_MS
t1/ns/orders/0  staged  operator  broker-1  broker-3     4210  1790000000000
t1/ns/orders/5  fenced  drain     broker-2  broker-1     0     1789999990000
```

| Column | Meaning |
| --- | --- |
| `STEP` | `staged`: the destination is copying and the leader still serves. `fenced`: the leader has stopped; the cut-over follows its drained report. `restoring`: a copy is being added to bring the shard back to its replication factor; see below. `replacing`: a follower on a draining broker is being replaced; leadership does not move. The old follower leaves once the new one is within the lag bound, and on a `Quorum` stream once it also holds what a majority of the replica set holds, so a record acknowledged before the swap is never left on too few copies. |
| `REASON` | `drain`, `balance`, `operator` (you asked), `replace`, or `restore` |
| `LAG` | records the destination is behind, from the leader's latest report; `-` without one |
| `STARTED_MS` | when the move started, on the control plane's clock |

If placement is paused, a line above the table says so.

## Restoring the replication factor

```bash
felix-controlplane admin replication
```

```text
SHARD           LEADER    COPIES  UNAVAILABLE  HALTED                      RESTORING
t1/ns/orders/0  broker-1  2/3     broker-2     -                           broker-4
t1/ns/orders/3  broker-3  2/3     -            broker-2 (needs_bootstrap)  -
```

The shards with fewer copies on serving brokers than their replication factor.
`COPIES` is current over desired. A copy being added does not count until it
is seated. `UNAVAILABLE` lists members whose broker is not serving. `HALTED`
lists members whose broker is serving but whose leader has stopped shipping to
them, with the reason: `diverged` (its bytes disagree with the leader's) or
`needs_bootstrap` (it needs records the leader's retention has removed). A
halted copy is in no quorum, so it does not count either. `RESTORING` is the
broker a copy is going to. `--json` prints the full listing
from `GET /v1/placement/replication`, every shard included.

Placement fills these in on its own. Once a follower's broker has been down or
gone for `FELIX_SHARD_RESTORE_AFTER_MS` (five minutes by default), the shard
is copied to a live broker outside its set. A set that a failover left short
because too few brokers were live is topped up as soon as one is free. The copy
is seated once it has caught up, and on a `Quorum` stream once it also holds
what a majority of the old set holds. The seat drops the lost follower in the
same write. If the broker being copied to fails, the copy is dropped and
another broker is picked. If the lost broker comes back first, its copy is
kept and the new one dropped. Restores take move slots after drains and before
rebalancing, and `pause` stops new ones.

A halted copy is replaced the same way once it has been halted for the restore
delay. The wait gives its leader time to rebuild it, which usually clears the
halt on its own; `GET /replication/halted` on the leader's metrics listener
says what the leader is doing about it. Placement never picks a broker whose
copy of the shard is halted as a destination, and a move whose destination
halts is given up and started again elsewhere: `plan` and the logs show the
step as `halted`. Moving a shard onto a halted copy by hand is refused with
`destination_halted`. If a drain has nowhere to go but a halted copy, `plan`
says `no live node can take this shard: the copy on broker-2 is halted
(diverged)`.

A shard that stays in this list past the delay has nowhere to go. Every live
broker outside its set may be at `max_shards` or in a region the stream may not
use. The restore may also be waiting for a move slot, or placement may be
paused. `plan` says which.

## What placement would do next

```bash
felix-controlplane admin plan
```

A dry run of the next placement pass: each shard it would place, move a step,
or leave waiting (and why), and nothing it would leave alone. Nothing is
written.

## Moving a shard

```bash
felix-controlplane admin move t1/ns/orders/0 broker-3
felix-controlplane admin move t1/ns/sessions/2 broker-3 --cache
```

A shard is named `<tenant>/<namespace>/<stream or cache>/<shard>`. The move
runs like one placement started: the destination copies the log, the leader
is fenced once it is close, and the destination takes over once the leader
has drained. Clients see what they see for any move: publishes are held for
the switch-over and forwarded, and subscriptions follow the shard.

It is refused, with the reason, where placement would not make it: the
destination is not live, already leads the shard or is full, the shard is
already moving, its leader is down, or the move limits
(`FELIX_SHARD_MOVES_MAX_CONCURRENT`, `FELIX_SHARD_MOVES_MAX_PER_NODE`) are
reached. It is not refused for a pause.

`--dry-run` shows what the move would do without starting it. When brokers
report zones, both say how many zones the shard's copies span now and will
span after the move:

```text
dry run, nothing written: stage: t1/ns/orders/0 leader broker-1 generation 12, moving to broker-3
zones: 2 -> 3
```

A move that would leave the shard in fewer zones is not refused; the line
says so, the control plane logs a warning, and `felix_shards_zone_unspread`
counts the shard until placement spreads it again.

While placement runs, it keeps leadership even, so it can move a shard you
placed on a broker that is now over its share. To keep a layout that is not
even, pause placement first.

## Cancelling a move

```bash
felix-controlplane admin cancel t1/ns/orders/0
```

Any move can be cancelled, whoever started it:

- **Staged.** The destination is dropped (unless the stream already kept a
  copy there) and the leader carries on as if nothing happened.
- **Fenced.** The leader that stopped serves again, at a new generation. It
  still has every write it accepted, because nobody else has led the shard
  since. Publishes held for the move go to it, and subscriptions that were
  told to follow the shard find it again and resume where they were, with no
  gap and no duplicate.
- **Cut over.** It is finished and cannot be cancelled. Move the shard back.

A cancelled move keeps its start time, which puts the shard behind others for
placement's next move. It does not stop placement choosing the same move
again, which for a draining broker it will: pause first.

## Pausing placement

```bash
felix-controlplane admin pause
felix-controlplane admin resume
```

Paused, placement starts no moves of its own, on any control-plane instance:
not a drain's, not a rebalance's, not a follower replacement. Moves already
under way finish, because a fenced leader has stopped serving and leaving it
there would keep its shard down; cancel one to stop it. New shards are still
placed, a broker that dies still has its shards failed over, and you can
still move shards by hand.

Pausing is how to hold the cluster still: during an incident, while moving a
few shards by hand, while taking a backup point
(`felix-controlplane admin backup-point`, see
[Backup and restore](/felix/deployment/backup-and-restore/)), or before
draining a broker you want to empty in a particular order. A drained broker keeps its shards while placement is
paused, so resume before relying on a drain.

## Abandoning a shard's log

:::danger[This loses data]
`abandon` discards a shard's log. Every record that only the old leader held
is gone, acknowledged ones included, and the old leader does not get them back
when it returns.
:::

```bash
felix-controlplane admin abandon t1/ns/orders/0
felix-controlplane admin abandon t1/ns/sessions/2 --cache
```

Placement holds a durable shard unplaced rather than lose records when its
leader is not serving and no replica holds everything the leader may have
acknowledged. That includes every durable shard with a replication factor of 1
whose broker is down. `admin plan` shows it waiting with one of these reasons:

```text
the only copy of this shard's log is on broker-2, which is not serving; waiting for it to return
the leader is gone and no replica holding this shard's log can take over
```

The shard comes back on its own when the old leader returns or a replica
catches up. Use `abandon` only when that will not happen, for example when the
broker's disk is lost and there is no backup to restore. The shard is then
placed as a new one: the new leader starts from whatever it holds, usually
nothing, at a new generation. The control plane logs a warning naming the
shard.

It is refused with `not_stranded` when the leader is serving or a replica can
take over without loss, and with `unplaceable` when no node can take the
shard. It needs `node.manage:cluster:*`.

## A worked example

Move one hot shard off `broker-1` without placement moving anything else
meanwhile:

```bash
felix-controlplane admin pause
felix-controlplane admin plan                       # nothing else is about to move
felix-controlplane admin move t1/ns/orders/0 broker-3
felix-controlplane admin moves                      # watch LAG fall, then the row go
```

Changed your mind while it was fenced?

```bash
felix-controlplane admin cancel t1/ns/orders/0      # broker-1 takes it back
```

Then `resume` when placement may even things out again.
