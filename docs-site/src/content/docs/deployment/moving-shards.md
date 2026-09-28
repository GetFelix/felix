---
title: "Moving shards by hand"
---

Placement moves shards on its own: off a draining broker, and from a broker
leading more than its share to one leading less (see
[Adding, draining and removing brokers](/felix/deployment/scaling/)). This
page is for when you want to steer that: see what is moving and what would
move next, move a shard yourself, cancel a move, or stop placement starting
any.

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
| `STEP` | `staged`: the destination is copying and the leader still serves. `fenced`: the leader has stopped; the cut-over follows its drained report. `replacing`: a follower on a draining broker is being replaced; leadership does not move. |
| `REASON` | `drain`, `balance`, `operator` (you asked), or `replace` |
| `LAG` | records the destination is behind, from the leader's latest report; `-` without one |
| `STARTED_MS` | when the move started, on the control plane's clock |

If placement is paused, a line above the table says so.

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
