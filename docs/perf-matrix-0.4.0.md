# The 0.4.0 performance matrix

Three questions, four provisioned sessions, ~$16 of the $200 monthly credit.
Written before the run so the success criteria are not chosen after seeing the
numbers.

Storage runs on `Standard_L8as_v3` with local NVMe, and the ceiling question is
answered by running **both v0.3.1 and v0.4.0 on that same hardware** rather than
against the published D4ads number.

## The questions

**1. Did the commit path stop being the ceiling?**

> **Corrected 2026-09-20.** It was never the ceiling. `CommitSequencer` is
> keyed per (stream, shard), so twelve shards is twelve independent commit
> paths, and throughput does not move with shard count. The wall is one `quinn`
> endpoint driver at ~88% of one core (#557, and
> `docs/perf-investigation-sharding-ceiling.md`). The ~977 MB/s figure was also
> taken with a single generator that tops out near 1,050 MB/s, so it could not
> separate the broker's limit from the instrument's; with four generators the
> band is 842–926 MB/s. The question is left as it was asked. #511 was a real
> improvement and the reasoning still reads, but it was answering the wrong
> question.

The NVMe session recorded a hard wall: driven against a single broker, durable
`OnCommit` topped out at **~977 MB/s while the broker sat at ~48% CPU**. More
load backed up behind the commit sequencer as `publish queue full` rather than
consuming the idle cores, and the conclusion was that durable throughput scales
by adding brokers, not cores.

0.4.0 contains the fix. #511 replaced `Notify::notify_waiters()`, which woke
every parked publisher so that one could proceed, with a `oneshot` keyed by
the offset each waiter is waiting for. Its own description names the shape of
the old cost: *"a single commit costs N wake-ups when N publishers are in
flight, and the work per commit grows with load."* That is a ceiling that
arrives before the cores do, which is exactly what 977 MB/s at 48% CPU looks
like.

**2. What does the durability guarantee cost at RF=3?**

Every published figure is RF=1 `Leader`. #425 has asked for RF=3 `Quorum`
beside it since the external review, blocked on #411: replication as a single
2s timer sweep completing at the slowest follower, which would have dominated
any measurement. #457, #471, #478 and #494 removed that.

**3. What does durability cost on real premium storage?**

#375. The one cell nothing has measured: durable stream + `OnCommit` on Premium
SSD. The only durable `OnCommit` numbers in `sessions/` are local NVMe, a
different device class.

## What else 0.4.0 changed that these runs will see

The NVMe page named three honest limits. Two are addressed:

- **The commit-path ceiling.** #511, question 1 above.
- **Shard assignment did not balance.** 48/0 on two brokers, 11/5/8 on three,
  which the page calls *"the prerequisite for a balanced cluster-ceiling
  number, and the honest reason one isn't quoted here."* #388 replaced pure
  rendezvous with bounded-load rendezvous, citing the same 11/5/8. **A balanced
  multi-broker aggregate is quotable for the first time**; verify the split
  before trusting one.

The third is unchanged and should be re-measured as a baseline, not expected to
move: **cross-broker forwarding** decrypts, re-encrypts and decrypts again, so
a round-robin client spends roughly twice the CPU per byte of one connected to
the shard owner (~140 vs ~250 MB/s per vCPU).

## Session A: the ceiling, on L8as

The storage question moves to **`Standard_L8as_v3`**: the storage-optimized AMD
line, 8 vCPU with real dedicated NVMe rather than a D-series temp disk. No code
change is needed: `BROKER_VM_SIZE` and `USE_LOCAL_NVME` already reach the bicep
through `session.sh`, and `cloudinit/broker.yaml` already RAID0s whatever Azure
exposes under `/dev/disk/azure/local/by-index/*`, the L-series case its own
comment calls out. It is a session env, not a patch.

```
BROKER_VM_SIZE=Standard_L8as_v3 USE_LOCAL_NVME=true TIER=t1 ./session.sh
```

**The hardware change breaks the A/B, and it is handled by running both sides
on the new hardware.** The 977 MB/s @ 48% CPU figure was taken on
`Standard_D4ads_v5` (4 vCPU, RAID0 temp disk). L8as_v3 doubles the cores and
changes the disk class, so a bigger number there would say nothing about #511:
the CPU denominator alone moved. "We beat 977" is not a claim that data would
support.

So **two passes on identical L8as hardware, `v0.3.1` and `v0.4.0`**, and the
comparison is between those two rather than against the published 977. That
number stays what it is, a D4ads result, and the L8as pair becomes the
baseline going forward. Re-measuring the old release is the cost, and it is
small next to maintaining two hardware stories.

The published 977 figure is not superseded by this and should not be quoted as
if it were: it describes different hardware, and the perf docs need to say which
box each number came from.

Either way `run-nvme-ingest.sh` is the instrument: it already ramps
`c1, 3, 6, 12, 24` at 4 KiB for both in-memory and durable `OnCommit`. The old
cost was **O(N) wake-ups per commit with N publishers in flight**, so the
improvement must *widen with concurrency*. A single-concurrency comparison
proves nothing either way.

| | |
|---|---|
| Tier | `t1`, `USE_LOCAL_NVME=true` |
| Brokers | 3 × `Standard_L8as_v3` |
| Scripts | `run-nvme-ingest.sh`, then `run-nvme-multi.sh` |
| Runtime | ~1.5 h per release |
| Cost | ~$4 per release |

**What counts as an answer:**

- At `c12`/`c24`: higher MB/s at the same broker CPU, or the same MB/s at lower
  CPU. Either is the fix working; MB/s alone is not the metric.
- The gap between the two releases should **grow** from `c1` to `c24`. Flat
  across the ramp means something other than wake-all was the wall.
- `publish queue full` should appear later, or not at all.
- If the wall moved, find the new one. `run-nvme-multi.sh` samples every
  broker's CPU breakdown at peak; the prior run found iowait 0–1.7% with the
  cost in user + system + **softirq** (QUIC/UDP and AEAD). If softirq dominates
  at a higher ceiling, the wall moved from the commit path to crypto, a
  different and more interesting problem.
- On 8 vCPU, watch whether the plateau still arrives at ~48% CPU. If the old
  ceiling was the commit path, the CPU fraction at saturation should rise; if
  it pins near half the cores again on twice the cores, the wake-all fix did
  not reach the real constraint.

## Session B: the durability-cost matrix

| | |
|---|---|
| Tier | `t1`, Premium SSD (default) |
| Release | `v0.4.0` |
| Scripts | `run.sh`, then `run-durable-matrix.sh` |
| Baseline | `sessions/t1-a-results/` (v0.3.0) |
| Runtime | ~4 h + ~1 h |
| Cost | ~$5 |

`run.sh` reproduces the standard matrix against `t1-a`: the general 0.3.0 →
0.4.0 comparison, which no one has taken. `run-durable-matrix.sh` is new,
because `run.sh` publishes only to `perf` at the default fsync and has no
durable or fsync handling at all. It runs three rows on one topology:
in-memory, durable + `Periodic`, durable + `OnCommit`, and retakes the cache
write path at both fsync modes since the committed `cache-oncommit` rows
predate #390.

**Expect `OnCommit` here to be disk-bound at ~170 MB/s**, the managed-disk
throughput cap, which is why the NVMe session existed. That is the answer to
#375, not a regression, and the three rows side by side are the deliverable:
the cost of each durability level, stated rather than implied.

## Session C: RF=3 Quorum

| | |
|---|---|
| Tier | `t2`, brokers in zones 1/2/3 |
| Seed | `REPLICATION_FACTOR=3` before `session.sh` |
| Release | `v0.4.0` |
| Script | `run-quorum-matrix.sh` |
| Runtime | ~2 h |
| Cost | ~$3 |

Must be `t2`. On `t1` every broker sits in one proximity placement group, so
the inter-zone RTT the guarantee actually buys is absent and a Quorum/Leader
delta would be measuring the placement group. The script refuses to run on `t1`
unless told to on purpose.

Three passes of the identical sweep, so the only variable is what the broker
waits for: RF=3 `Leader` (`perf`), RF=3 `Quorum` (`perf-quorum`), and RF=3
`Quorum` + durable (`perf-durable-quorum`), the last being the configuration
the docs recommend and the one nothing has ever measured. #526 seeds all four
streams.

Each pass includes an `ingest --concurrency 12` point: `Quorum` holds a publish
until a majority confirms, so its cost is a queueing cost that a single
in-flight publisher cannot show.

## Sequence and budget

1. **A** first. It is the fastest, and its result shapes how the rest is
   reported, so a harness problem surfaces before the expensive runs. Two
   passes on one provisioned shape.
2. **B**, the long one.
3. **C**, which needs a different tier and a re-seed.

~$16 total against $200/month. Every session is one resource group,
`teardown.sh` is the last line of each run, and `session.sh` stamps an
`autoTeardownAfter` tag at +8 h as the backstop.

## Discipline

- **Clean machines.** Nothing else runs on the VMs for the duration, and no
  build or edit happens during a run.
- **Clear artifacts before restarting** a session, so a partial `results.jsonl`
  cannot be read as a complete one.
- **Every row records its session.** `session.json` carries tier, SKUs, release
  and RTT baseline; a number without it is not publishable.
- **A failed case is a data point.** Both new scripts continue past one and
  report what produced no result at the end, because losing four hours of paid
  cluster to a lone transient is worse than a gap in the table.

## What this deliberately does not do

- **No Kafka/Redpanda comparison.** That is #376 and needs OMB on matched
  hardware with matched durability; folding a half-fair comparison into this
  run would contaminate three good answers with one bad one.
- **No failover or chaos timing.** #136's cluster-scale latency budget is its
  own session, and the promotion rule is about to change (#527).
- **No cross-region.** T3 adds egress billing for a question none of the three
  above ask.
