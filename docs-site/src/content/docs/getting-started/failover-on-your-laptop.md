---
title: "See failover on your laptop"
description: "Start a three-broker Felix cluster with one command, kill the leader while writes are flowing, and check from outside that every acknowledged write is still there."
---

This page starts a three-broker cluster on your machine, writes to it
continuously, kills the broker that leads the stream, and then checks from a
separate process that nothing Felix acknowledged is missing. Everything runs
from the released container images, and the checker's source is in the
repository.

This is for development only. The credentials are fixed tokens, the
certificate is self-signed, and every host port is bound to 127.0.0.1. Do not
reuse the compose file anywhere that matters.

You need Docker, or Podman with the `docker-compose` provider (2.20 or
later). Nothing else is installed on your machine.

```bash
git clone https://github.com/GetFelix/felix
cd felix/demos/failover
./run.sh up
```

The brokers and control plane are the released images. The checker is built
from the repository's source on the first run (there is no released image of
it), which takes a few minutes. Then it
prints one line a second:

```text
[  18s] acknowledged   1800  in-flight  1  indeterminate  0  delivered   1800  gaps 0  duplicates 0  producers 4/4
```

## What is running

- Three brokers, one control plane, and a stream `default/orders` with one
  shard, **replication factor 3** and **`Quorum`** consistency. A publish is
  acknowledged once a majority of the three copies hold it.
- Brokers run with `FELIX_DURABLE_FSYNC_MODE=on_commit`, so "hold it" means
  flushed to disk on that broker. The default is `periodic`, which
  acknowledges before the flush; set the variable before `./run.sh up` to
  try it.
- A checker with four producers writing 25 records a second each, using the
  client's idempotent producer, and a reader on the same stream.

Two settings are demo tuning. The control plane declares a broker dead after
3 s instead of the default, so you do not wait about 20 s. And the brokers may
elect a leader among themselves, which is [off by default](/deployment/failover-without-control-plane/).

## Kill the leader

In a second terminal, from the same directory:

```bash
./run.sh kill-leader
```

`kill-leader` asks `felixctl topology` which broker leads the shard and sends
it `SIGKILL`. The checker's output, from one run of this scenario (the receipt below is from another, so the numbers differ slightly):

```text
[  20s] acknowledged   2000  in-flight  0  indeterminate  0  delivered   2000  gaps 0  duplicates 0
[  20s] writes waiting: oldest publish unanswered for 575 ms
[  21s] acknowledged   2014  in-flight  4  indeterminate  0  delivered   2014  gaps 0  duplicates 0
[  22s] acknowledged   2014  in-flight  4  indeterminate  0  delivered   2014  gaps 0  duplicates 0
...
[  26s] writes blocked 6015 ms (oldest waiting publish to first ack after)
[  27s] acknowledged   2099  in-flight  0  indeterminate  0  delivered   2099  gaps 0  duplicates 0  producers 4/4
```

Writes stopped for about 6 s, with the four writes in flight at the kill
unanswered, then resumed on the new leader. The control plane here declares a
broker dead after 3 s, so most of that is waiting to be sure. With its default
settings the window is longer.

Bring the broker back:

```bash
./run.sh restart broker-3     # the name kill-leader printed
```

## Kill the control plane, then the new leader

The control plane normally names the new leader. With election among replicas
enabled, the brokers can do it themselves:

```bash
./run.sh kill-controlplane
./run.sh kill-leader
./run.sh evidence
```

```text
== broker-1
... INFO felix_replication::election::candidate: standing for election: the leader is unreachable or stuck and a majority agreed stream=orders shard=0 generation=2 leader=broker-2 broker_suspected=true
... INFO felix_replication::driver: a majority took the fence; opening the shard for writes stream=orders shard=0 generation=2 caught_up_from=None
felix_broker_promotions_opened_total{path="fenced"} 1
```

Two of three brokers are alive, which is what an election needs, so a
second failure now would stop writes. Start the control plane and the killed
broker again before you try anything else:

```bash
./run.sh restart-controlplane
./run.sh restart broker-2
```

A broker restarted while the control plane is down waits for it before it
serves; see [Limits](/deployment/failover-without-control-plane/#limits).

## The receipt

```bash
./run.sh receipt        # while it runs
./run.sh down           # final receipt, then removes everything
```

This is the final receipt from a run with those three kills
(`./run.sh scenario` does all of them, hands off):

```text
==== failover receipt (final) ====
run                  6affb953, 196 s
producers ran        4 of 4, 0 stopped early
producers            4 idempotent producers x 25/s, 128-byte payloads
cluster              Quorum, replication factor 3, fsync on_commit, felix 0.7.2
sent                 16090
acknowledged         16090
in flight            0 (sent, no answer yet)
indeterminate        0 (errors that reached the checker; 0 found in the stream, 0 not. The SDK retries inside a publish and does not count that, so 0 here does not show the re-send path ran)
recovered by resend  0 (errored at least once, then acknowledged under the same sequence)
records read back    16090 (whole stream, from the oldest, after the snapshot)
GAPS                 0
DUPLICATES           0
reader resubscribes  0
write stall          at 23 s: writes blocked 6015 ms, every producer writing again after 6059 ms
write stall          at 85 s: writes blocked 18368 ms, every producer writing again after 36121 ms
verdict              every acknowledged write is in the stream exactly once; nothing extra
```

Zero acknowledged writes lost, zero duplicates, in this run. Read it as
exactly that: one run, on one laptop, with these three failures.

Two things in it are worth reading closely:

- **Indeterminate is 0.** The client retries a publish inside the call while it
  looks for the new leader, and does not report how often, so no publish
  returned an error to the checker. That is not evidence that the re-send path
  ran in this run; the checker's unit tests cover it. A publish that does
  return an error is counted as indeterminate while the checker re-sends it
  under the same sequence, and is never counted as lost.
- **The second stall is longer than the election.** The replicas elected a new
  leader about 7 s after the kill (the log lines above), but the first write
  was acknowledged 18 s after it, and the last of the four producers 36 s after.
  The clients took longer than the election to find and use the new leader. This
  run does not say why, and it is why the checker reports both numbers.

## What the checker checks

It is `demos/failover/src`, a separate program that uses the public client.

- Each producer numbers its writes `0, 1, 2, ...` and puts the number in the
  payload, so the check does not depend on the broker's own bookkeeping.
- A write is **acknowledged** when the broker answers `Ok` to its publish.
- A publish that errors has an unknown outcome. The producer sends the same
  bytes again under the same sequence until it is acknowledged, which the
  idempotent producer makes safe. Until then the write is **indeterminate**.
  One still unanswered when the checker stops is reported with whether it
  turned up in the stream, and either answer is correct.
- A run that acknowledged nothing, or in which a producer failed to start or
  stopped early, **fails** instead of passing with zero problems. `./run.sh
  receipt` and `./run.sh down` exit non-zero unless it passed.
- **The receipt does not trust the live reader.** It takes the set of writes
  acknowledged so far, reads the whole stream back from its oldest record, and
  checks that every one appears exactly once, that nothing appears twice, and
  that nothing this run never sent is there.

What it does not cover: a `SIGKILL` leaves the page cache intact, so it does
not test power loss; a killed container is not a partitioned or paused leader
(that is a different test); and it checks presence and multiplicity, not order.
Only one broker is down at a time among three. The
[README](https://github.com/GetFelix/felix/blob/main/demos/failover/README.md)
has the full list.

## Clean up

```bash
./run.sh down
```

This removes the `felix-failover` project's containers, network and volumes,
and nothing else.
