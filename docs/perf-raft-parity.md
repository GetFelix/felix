# Raft vs hybrid parity matrix

> The hybrid is retired: a current broker refuses `FELIX_SHARD_ENGINE=hybrid`, so the
> hybrid side of this matrix cannot run on a current build and `perf-raft-rf3.yml` is gone.
> The page records how the parity gate was run. Compare with the v0.7.2 baseline side to
> measure against the hybrid now.

Raft becomes the default shard engine only if it is at least as fast as the hybrid on every
cell below, measured the same way on the same machines ([#2646](https://github.com/GetFelix/felix/issues/2646)).
This page covers how to run the matrix and how to read what it writes. The harness lives in
`scripts/perf/raft-parity/`. It runs on three Azure VMs (next section) or, as before, on the
three GCP build boxes (the section after).

## Run it on Azure

`scripts/perf/raft-parity/azure.sh` is the one entry point. Every step is one command, logs to
`.scratch/raft-parity-azure/<NAME>/logs/`, and is safe to run again. It needs the `az` CLI
logged in (`AZ_SUBSCRIPTION` to pick a subscription), `jq`, and an ssh key
(`~/.ssh/id_ed25519.pub`, or `SSH_KEY_FILE`). Nothing in it uses `az vm run-command`.

```bash
scripts/perf/raft-parity/azure.sh provision        # 3 VMs, ~10 min
scripts/perf/raft-parity/azure.sh build            # once per revision, 20-40 min the first time
scripts/perf/raft-parity/azure.sh smoke            # ~15 min; read it before the full run
ROUNDS=2 scripts/perf/raft-parity/azure.sh run     # the matrix; ROUNDS defaults to 2
scripts/perf/raft-parity/azure.sh fetch-results    # results.md, results.json and box logs in one archive
scripts/perf/raft-parity/azure.sh teardown         # deletes the resource group; always do this last
```

`azure.sh all` runs the six in order and tears the group down even when a step fails
(`KEEP=1` leaves it up). `azure.sh status` lists what exists and `azure.sh ssh 2` opens a shell.
Add the release as a third side with `BASELINE_REF=v0.7.2 azure.sh build` after the normal
build, then `BASELINE_REF=v0.7.2 azure.sh run` (see "Against the last release").

### When you cannot ssh to the VMs

Some networks block outbound ssh on every port. `scripts/perf/raft-parity/azure-inside.sh`
then runs the same steps from inside Azure: VM 1 gets its own key, reaches all three over
their private addresses, and runs `azure.sh` from a clone. The laptop only talks to Azure
through `az vm run-command`, which takes 30-90 s a call and returns about 4 KB of output.

```bash
scripts/perf/raft-parity/azure-inside.sh start           # after provision, or from deallocated
scripts/perf/raft-parity/azure-inside.sh keys            # VM 1's key on all three, hosts.env
scripts/perf/raft-parity/azure-inside.sh checkout <sha>  # clone on VM 1 at that commit
scripts/perf/raft-parity/azure-inside.sh launch smoke build smoke
scripts/perf/raft-parity/azure-inside.sh state smoke     # poll; the log ends in PARITY_DONE
scripts/perf/raft-parity/azure-inside.sh fetch smoke .scratch/parity
ROUNDS=2 scripts/perf/raft-parity/azure-inside.sh launch run run
scripts/perf/raft-parity/azure-inside.sh fetch run .scratch/parity
scripts/perf/raft-parity/azure-inside.sh teardown
```

`launch` starts a systemd unit on VM 1, so the job outlives the call that started it. `fetch`
brings back `results.md`, `results.json` and `cells.tsv` in base64 chunks and checks the
archive's hash. VM 1 runs the operator side next to its own broker and generator, which is
a few ssh sessions and no measurable load.

What the steps do:

- `provision` creates the resource group `felix-perf-parity-<NAME>` (the same naming as
  `scripts/perf/cloud/`, so its `teardown.sh` also works with `SESSION=parity-<NAME>`),
  a network that allows ssh only from your address, and three VMs in a proximity placement
  group. Cloud-init installs the build tools, Java for the Kafka cells and rustup, and mounts
  one ext4 volume (`noatime`) at `/mnt/build`, which is where every path in the harness
  already points and where both engines keep their data. The volume is the VM's local NVMe on
  L-series sizes and otherwise a Premium SSD v2 data disk made by `provision`. It records an
  `fsync` baseline (4 KiB write plus fsync) per box. If your network resets outbound port 22, set `SSH_PORT=443`
  and the VMs listen there too.
- `build` copies the committed tree to VM 3, builds there and installs the same binaries on all
  three, refusing a tree with uncommitted changes. `smoke`, `run` and `fetch-results` check that
  the installed build is the revision in your tree.
- `smoke` is one round of `single-oncommit-lat`, `rf3-oncommit-lat` and `rf3-oncommit-tput`
  on both engines, 15 s each. It exercises the whole path (cluster start, engine marker check,
  loadgen placement, CPU reads, summary) for a few dollars.
- `run` first stops anything an earlier run left under `/mnt/build/raft-parity/run`, deletes
  those directories and refuses to start if a broker, control plane, loadgen or Java process
  is still alive. Then it holds the load check the matrix always had: every box idle (load
  under 1, nothing building) before the first cell and between cells. The orchestrator runs on
  your machine over ssh, so keep it awake (`caffeinate` is used when present) and on the
  network for the whole run. Ctrl-C stops the cell, wipes it and exits.
- `ROUNDS` defaults to 2 for this campaign. The generator box rotates over the three VMs and
  the side order alternates by round, so a multiple of 6 balances both; with other values
  `results.md` carries "generator placement not balanced" and the ratios carry that bias.

Settings (environment): `NAME` (default `parity`), `LOCATION` (`centralus`), `VM_SIZE`
(`Standard_D16ls_v6`), `ZONE` (`1`, all three VMs and disks), `DATA_DISK` (`auto`, or `local` or
`premiumv2`), `DISK_GIB`, `DISK_IOPS` and `DISK_MBPS` (256, 12000, 500), `MAX_HOURS` (backstop,
below), `PLACEMENT=ppg` to add a proximity group (off by default; one zone is already close), `CELLS`, `SIDES`, and every other setting `run.sh` reads.

### Which VM

`Standard_D16ls_v6` in `centralus`: 16 vCPU, 32 GiB, three of them filling a 48 vCPU
`standardDlsv6Family` quota (the region's total cores must also have 48 free). The size has no
local disk, so each VM gets one Premium SSD v2 data disk, 256 GiB, 12000 IOPS and 500 MB/s,
identical on all three, mounted at `/mnt/build`. Premium SSD v2 is zonal, so the VMs and disks
all sit in `ZONE` (1). A box runs a broker and often the load generator too, which sixteen
vCPUs cover with room to spare.

The cells measure `fsync`, and here it goes to a managed disk over the network, a
millisecond-class write instead of a local NVMe's tens of microseconds. Absolute numbers in the
fsync-bound cells therefore differ from local NVMe runs (earlier Azure L-series runs and the GCP
boxes). The comparison the gate reads, Raft against hybrid, still runs both engines on the
same hardware, and the per-box `fio-fsync.json` records what an fsync cost. Sizes with local
NVMe still work: `VM_SIZE=Standard_L8as_v4 LOCATION=eastus2 ZONE= azure.sh provision` uses
the local volume (`DATA_DISK=auto` picks it for `Standard_L*`).

Quota: `provision` reads the size's family (`StandardDlsv6Family` here) and the region's
total cores, and stops with the shortfall if either has fewer than 3 x vCPUs free. Raise the
quota (Portal, Quotas, Compute) or use `VM_SIZE=Standard_D8ls_v6` (24 vCPUs).

### Cost

Estimates at approximate pay-as-you-go list prices in `centralus` (D16ls_v6 about $0.81/h
each, $2.42/h for the three; each Premium SSD v2 disk at 256 GiB, 12000 IOPS and 500 MB/s
about $0.11/h, $0.33/h for the three; about $0.3/h for OS disks and addresses; roughly
$3.1/h in all). They assume one
revision. Time estimates for the matrix come from the cell list (23 cells, two sides, a fresh
cluster per cell and side) and not from a measured Azure run, so treat them as the planning
figure and check the first one.

| phase | time | cost |
|---|---|---|
| provision and cloud-init | 10-15 min | under $1 |
| build, first time (later revisions reuse the target dir) | 20-40 min | $1-3 |
| smoke | 15 min | under $1 |
| full matrix, `ROUNDS=2`, hybrid and raft | about 4-5 h | $12-16 |
| full matrix, `ROUNDS=2`, plus the v0.7.2 baseline | about 6-7 h | $19-22 |
| full matrix, `ROUNDS=6` (balanced), hybrid and raft | about 12-15 h | $37-47 |

A whole campaign (provision, build, smoke, one full run, teardown) is about $16-21, or $23-28
with the baseline. Time is the cost driver, so run `teardown` the moment `fetch-results` has
written the archive. Two backstops cover a forgotten group: each VM is set to deallocate
`MAX_HOURS` (12) after provisioning, which stops compute billing but leaves the disks, and
the group carries an `autoTeardownAfter` tag for finding it. `azure.sh status` shows what is
left; `teardown` deletes the whole group, data disks included, and only a group tagged `felix-perf=parity-<NAME>`.

## Run it on GCP

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
   the Raft engine. The hybrid side needed `FELIX_SHARD_ENGINE=hybrid`, which a current broker refuses.
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
| `rf3-mqtt-lat` | One authenticated MQTT connection on the shard leader, one QoS 1 publish in flight, 1 KiB, one shard. Reports publish to PUBACK p50, p99 and p999; p50 and p99 gate, p999 is shown. The connection is on the leader so the number is the engine's and not the forwarding hop, which `rf3-mqtt` covers. |
| `rf3-fanout` | One publisher, four subscribers. Gates delivered throughput and delivery p99. |
| `rf3-kafka-tail` | One shard. `kafka-console-consumer` (partition 0, no consumer group, because Felix's Kafka layer refuses groups) tails the topic while `kafka-producer-perf-test` publishes at a fixed rate (`KAFKA_TAIL_RATE`, 15000 records/s; Raft's single-shard Kafka produce holds about 17000, [#2704](https://github.com/GetFelix/felix/issues/2704)). Gates the consumed rate over the producer's run time, and that the consumer got every record produced. The fetch rate over the consumer's own run includes the seconds before the producer starts, so it is recorded but not gated. There are no latency percentiles. |
| `rf3-mqtt-tail` | One shard. `TAIL_SUBS` MQTT subscribers (QoS 1, filter `bench/#`) sit on the shard's leader, because a broker serves a SUBSCRIBE only for a shard it leads, while `TAIL_PUBLISHERS` QoS 1 connections publish at a fixed rate (`MQTT_TAIL_RATE`, 15000 messages/s) for exactly the subscribers' window. Gates delivered throughput and the publisher's rate against 95% of the target. |
| `rf3-resume` | A resume loop on the shard's leader: each pass subscribes at a seed-derived offset in the first `RESUME_HISTORY` records (20000, written before the window opens, the same on both engines), reads `RESUME_SPAN` records and drops the subscription, over `RESUME_WORKERS` loops. Gates replayed throughput and the p50 and p99 from the subscribe to the first record, which is where the leader's read check (a lease or a read index round on Raft) lands. A publisher runs beside it. |
| `rf3-follower-tail` | `TAIL_SUBS` plain tail subscribers connect to a node that does not lead the shard, with a publisher beside them. Raft serves them from the follower's own log after the read index check, so this measures that read. An engine that does not serve followers answers `NotLeader` and the subscriber moves to the leader (one hop, host names resolved), so the cell compares what a client attached to a follower gets on each engine. The loadgen JSON says which happened (`served_directly`, `redirected`). Gates delivered throughput, delivery p50 and p99 and offset gaps. |
| `rf3-idle-tput` | `rf3-oncommit-tput` (batch 16, 1 KiB, 6 shards, Quorum on_commit) with `IDLE_GROUPS` (2000) idle groups on the same three brokers: extra RF 3 streams that no publisher touches, so only their heartbeats, timers and tasks compete with the busy shards. Gated like the other throughput cells (throughput, ack p99, failed sends, broker CPU per message), so the busy shards must still match hybrid with the idle groups present. Both engines create the idle streams through the same control plane calls, `IDLE_BATCH` (200) groups at a time with `IDLE_SHARDS` (10) shards per stream, and each batch must have leaders (on Raft also an open member on every broker) within `IDLE_BATCH_WAIT_SECS` (180) before the next starts; forming 2000 groups at once never got leaders on a small box ([#2317](https://github.com/GetFelix/felix/issues/2317)). The cell fails with `idle-not-formed` otherwise. After the last batch `IDLE_SETTLE_SECS` (30) pass, then the window opens; the CPU reads bracket the window only, so formation CPU is not counted. The time from the first batch to the last leader is recorded in `idle.txt` and the results (`idle group formation`, reported, not gated). `IDLE_GROUPS=0` makes it plain `rf3-oncommit-tput`. |
| `single-resume` | The resume loop against one node, to check the harness on one box. Not in the default list. |

The read cells run on a single shard stream so an offset or a follower names one place.
The Felix cells share one background publisher shape: `TAIL_PUBLISHERS` acked publishers held to
`KAFKA_TAIL_RATE` records a second in total (`ingest --rate`), the same rate the Kafka producer
uses (`rf3-mqtt-tail`'s MQTT publishers use `mqtt-publish --rate` at `MQTT_TAIL_RATE`), for exactly the reader's window. A fixed rate keeps the load identical on both engines, so
the delivered and replayed rates are bounded by it and the latencies are what separate them. The
harness waits for the publisher to finish before it reads its result, and fails the cell if the
publisher's JSON is missing, if it failed a send, or if it published under 95% of the target.
The achieved rate is recorded (`publish throughput`) and `summarize.py` gates the same 95% on both
engines: a cell where either side could not hold the rate is invalid, not a pass.

The loadgen runs on the box that is neither the shard's leader nor the target follower, for every
round and both engines, and the JSON records the placement. Resume and tail counters that must be
zero are gated absolutely, not as a ratio: failed resumes, short reads (which record no latency),
first records below the resume offset, and offset gaps. A non-zero count on either engine fails
the cell and is reported as a gap.

Batched cells measure a throughput profile. Their ack latency is per batch and says nothing
about request latency; the `-lat` cells do. `throughput` counts what the publishers sent and
the brokers acknowledged. `delivered throughput` (fanout) counts subscriber deliveries and
differs from it by the fanout factor.

A cell that Raft refuses or cannot run (a Kafka produce or MQTT publish on a shard engine that
does not support it yet, say) is recorded with its failure status, has no data on the Raft side
and fails the gate. That is the intent: an unbuilt feature is a gap, not a pass. `rf3-kafka` and `rf3-mqtt` fail until #2633 lands.

## Broker CPU

Every cell except `rf3-failover` also records how much CPU the brokers spent. `node.sh cpu` reads
each broker process's `utime` and `stime` from `/proc/<pid>/stat` (clock ticks) just before the
measuring command and just after it, on every broker box. For the cells with a `--start-at`
(`rf3-mqtt-tail`, `rf3-resume`, `rf3-follower-tail`) the first read waits for the start time, so
the idle wait before the window is not counted; the others open at the loadgen's start, which
includes its connection setup. `summarize.py` sums the difference over the brokers and divides by
the messages the generator counted in the window: acknowledged publishes (ingest, MQTT, Kafka),
deliveries (fanout, MQTT tail, follower tail), replayed records (resume) or consumed records
(Kafka tail). The result is broker CPU milliseconds per 1000 messages, the same quantity as the
us/msg of an earlier profile. The tail cells' publisher is held to the same rate on both engines, so
its CPU is a fixed part of both sides.

The gate is not noise adjusted: Raft's mean must be at most 110% of hybrid's (`CPU_TOLERANCE`),
and a missing or backwards sample (a restarted broker) fails the cell. The report lists the
CPU per cell and the ratio in its own table. The per category CPU profiles that explain a
failing ratio are captured separately and are not part of this harness.

## Against a branch, without Azure

`perf-raft-pr.yml` (Actions, dispatched by hand) compares a branch with main on the Raft engine,
three brokers on one runner, in one job: `rf3-oncommit-1k` (batch 64) and `rf3-oncommit-1k-b1`
(batch 1), six rounds of 15 s by default, about 15 minutes after the build. It is a report and
not a gate; a shared runner is too noisy for that. Use it to see whether a Raft-side change like
[#2532](https://github.com/GetFelix/felix/pull/2532) moved the numbers before spending Azure
time.

## Against the last release

The gate also asks that no cell regresses against the last release (v0.7.2). Add the release
as a third side:

```bash
scripts/perf/raft-parity/build.sh                        # the current binaries first
BASELINE_REF=v0.7.2 scripts/perf/raft-parity/build.sh    # then the release's
SIDES="baseline hybrid raft" ROUNDS=6 task perf:raft-parity
```

- The baseline build checks out the tag in its own worktree (`parity-baseline-v0.7.2` next to
  this one) and builds `felix-broker` and `felix-controlplane` from it on box 3, without the
  `raft` feature. It installs them in `/mnt/build/raft-parity/bin-baseline`, so the current
  `bin` stays as it was. The build refuses a baseline broker that contains the Raft engine.
- The baseline side starts the release's own control plane and broker, with the same settings
  as the other sides except the engine variable, which v0.7.2 does not read (it has the hybrid
  engine only). The harness checks that the broker has no engine variable and no Raft marker.
- Every side uses the current `felix-loadgen`. The v0.7.2 loadgen has no `mqtt-publish`,
  `mqtt-subscribe`, `resume` or failover kill-file scenario, and one client for all sides is the
  fairer comparison. `build.sh` copies it into `bin-baseline`, and `run.sh` refuses to start
  if the two copies differ.
- The sides rotate their starting order by round; use `ROUNDS` as a multiple of 6.
- A cell the release cannot run goes in `BASELINE_NA="cell ..."`. It is recorded as `na`,
  leaves no cluster behind, and is left out of the baseline gate. Nothing is listed by default:
  `rf3-follower-tail` still runs on v0.7.2 because a follower that answers `NotLeader` makes
  the subscriber move to the leader. A baseline cell that fails to run for any other reason is
  a failure, not n/a.

`results.md` gains a "Versus v0.7.2" section. For each cell, the current hybrid and the current
Raft are each compared with the baseline by the rule used between Raft and hybrid (same noise
band, same 10% CPU tolerance, ratios above 1 are better), and the cell fails if either is
worse. The section names which side and metric failed. The absolute rules (zero gaps, 95%
publisher rate) apply to the current sides only. `--strict` and `task perf:raft-parity` exit
non-zero if either gate fails.

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

On GCP the boxes are shared build machines, not release hardware, so read the ratios and not the
absolute numbers. To check the gate arithmetic itself, `task perf:raft-parity:test`.
