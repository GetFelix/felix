# Raft vs hybrid parity matrix

Raft becomes the default shard engine only if it is at least as fast as the hybrid on every
cell below, measured the same way on the same machines ([#2646](https://github.com/GetFelix/felix/issues/2646)).
This page covers how to run the matrix and how to read what it writes. The harness lives in
`scripts/perf/raft-parity/`.

## Run it

```bash
task perf:raft-parity                 # build, then the whole matrix, 3 rounds
task perf:raft-parity -- --smoke      # one round, two cheap cells, one box
ROUNDS=6 CELLS="rf3-oncommit-tput rf3-failover" task perf:raft-parity
```

It needs the three GCP build boxes (`felix-build-1..3`, running, same zone) and nothing else
on them. The run holds every build slot on each box so other lanes' jobs queue, and it refuses
to start until each box has a load under 1 and no TLC or build running. Run it from a quiet
laptop: nothing may compile while it measures.

What it does:

1. `build.sh` builds `felix-broker` (with the `raft` feature), `felix-controlplane` and
   `felix-loadgen` once, installs them on all three boxes, and checks the broker really contains
   the Raft engine. One binary serves both sides: `FELIX_SHARD_ENGINE` picks the engine.
2. `run.sh` runs every cell on both engines for `ROUNDS` rounds. Each cell on each side gets a
   fresh cluster (control plane in memory on box 1, a data directory per broker), one stream,
   one measurement, then everything is stopped and wiped. Odd rounds run hybrid first, even
   rounds Raft first. The generator rotates over the boxes by round, so use a multiple of 6: the side order and the generator box then balance.
3. Before measuring, it checks each broker's environment and, for Raft, the engine marker in
   every shard directory, so a run cannot silently compare an engine with itself.
4. `summarize.py` writes `results.md` and `results.json` under
   `/Volumes/External/worktrees/.scratch/raft-parity/runs/<run id>/` (override with `OUT`).
   `task perf:raft-parity` exits non-zero when the verdict is FAIL.

`run.env` in that directory records what the numbers came from: the git revision, the SHA256 of
all three binaries, each box's machine type, CPU, memory, kernel and disk, the load at start,
and every setting.

## The cells

| cell | what it measures |
|---|---|
| `{single,rf3}-{oncommit,quorum,leader}-lat` | Batch 1 ack latency (p50, p99, p999), one acked publish at a time per publisher. `oncommit` is Quorum with fsync on_commit, `quorum` is Quorum with periodic fsync, `leader` is Leader with periodic fsync. Single node is RF 1. |
| `{single,rf3}-{oncommit,quorum,leader}-tput` | Batched throughput (batch 16, 1 KiB, 6 shards) and the per batch ack p99. |
| `rf3-failover` | The shard's leader is killed with SIGKILL mid-run. The harness stamps the kill on the killed box's clock just before the signal and the generator measures from that instant (GCP's synced clocks make the skew negligible next to the recovery). First commit is the kill to the first acked publish after it. Caught up is the kill until a live-tail subscriber receives a record published after recovery. |
| `rf3-kafka` | Kafka's own `kafka-producer-perf-test`, `acks=all`, 64 KiB batches, same settings on both engines. |
| `rf3-mqtt` | MQTT 3.1.1 QoS 1 publishes, a window of 64 per connection, to a stream mounted at `bench/`. |
| `rf3-fanout` | One publisher, four subscribers. Gates delivered throughput and delivery p99. |
| `rf3-kafka-tail` | Kafka's `kafka-consumer-perf-test` tails the topic while `kafka-producer-perf-test` publishes at a fixed rate (`KAFKA_TAIL_RATE`, 20000 records/s). Gates the consumer's fetch rate. The tool reports no latency percentiles, so there are none. |
| `rf3-resume` | A resume loop on the shard's leader: each pass subscribes at a random offset below the tail, reads `RESUME_SPAN` records and drops the subscription, over `RESUME_WORKERS` loops. Gates replayed throughput and the p50 and p99 from the subscribe to the first record, which is where the leader's read check (a lease or a read index round on Raft) lands. A publisher runs beside it. |
| `rf3-follower-tail` | `TAIL_SUBS` plain tail subscribers connect to a node that does not lead the shard, with a publisher beside them. Raft serves them from the follower's own log after the read index check, so this measures that read. An engine that does not serve followers answers `NotLeader` and the subscriber moves to the leader, so the cell compares what a client attached to a follower gets on each engine. The loadgen JSON says which happened (`served_directly`, `redirected`). Gates delivered throughput, delivery p50 and p99 and offset gaps. |

The three read cells run on a single shard stream so an offset or a follower names one place.
Their publisher is `ingest` with `TAIL_PUBLISHERS` acked publishers, one batch in flight each. It is
closed loop and light on purpose, and its rate is recorded as information (`publish throughput`):
the delivered and replayed rates are bounded by it, and the latencies are what separate the
engines. `single-resume` runs the resume loop against one node, for checking the harness on one box.

Batched cells measure a throughput profile. Their ack latency is per batch and says nothing
about request latency; the `-lat` cells do. `throughput` counts what the publishers sent and
the brokers acknowledged. `delivered throughput` (fanout) counts subscriber deliveries and
differs from it by the fanout factor.

A cell that Raft refuses or cannot run (a Kafka produce or MQTT publish on a shard engine that
does not support it yet, say) is recorded with its failure status, has no data on the Raft side
and fails the gate. That is the intent: an unbuilt feature is a gap, not a pass. `rf3-kafka` and `rf3-mqtt` fail until #2633 lands.

## Read it

Each cell has a table of its metrics:

- **ratio** is Raft's standing. Above 1 means Raft is better, for latency as well as throughput
  (it is hybrid over Raft for a metric where lower is better).
- **tolerance** is 2 standard errors of the ratio, from the round to round spread of both sides,
  held between 3% and 10%. With few rounds the spread is itself uncertain, so the floor
  keeps three quiet rounds from turning a 1% dip into a failure and a lucky pair of rounds
  from passing a real one. The cap means a side that swings wildly cannot buy a pass. Environment variables:
  `NOISE_K`, `NOISE_FLOOR`, `NOISE_CAP`. A cell also fails if any scheduled round on either side
  did not measure.
- **verdict** is PASS when `ratio >= 1 - tolerance`. Metrics marked `info` are shown and do not
  gate. A cell passes when every gating metric passes. The matrix passes when every cell does.

The gate is that Raft is at least as fast within noise on every cell. A remaining gap ships
only if its root cause is measured and it is an inherent safety cost, documented with these
numbers. Run the matrix before the default switch and again on every release candidate.

The boxes are shared build machines, not release hardware, so read the ratios and not the
absolute numbers. To check the gate arithmetic itself, `task perf:raft-parity:test`.
