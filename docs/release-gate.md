# The release gate for one engine

This page says what must be proven before the per-shard Raft engine replaces
the hybrid, and before the bridge build is released. It is a checklist with
sizes and pass conditions. Nothing here is a new design.

The gate itself is defined in [`replication-engine.md`](replication-engine.md)
("The no-regression gate", criteria 1 to 7). The engine is in
[`per-shard-raft.md`](per-shard-raft.md), the control plane's store in
[`metadata-raft-design.md`](metadata-raft-design.md) and the upgrade path in
[`hybrid-retirement.md`](hybrid-retirement.md). The workflows that run the
gate are `gate.yml` and its four parts (pull request #2247).

## What the gate decides

The gate is passed twice, on two builds.

| Decision | Build | Compared with |
| --- | --- | --- |
| Raft becomes the default and the bridge build is released | Both engines in the tree | The hybrid on the same commit, and the latest v0.7 release |
| 0.8.0 is released | Raft only | The bridge build, and the latest v0.7 release |

A row of this page applies to both unless it says otherwise. A failure is
fixed in the engine. It is not written up as a limit. A run that was red and
passes on a rerun counts as red until the first failure is explained.

## Rules for every run

- Runs are in GitHub Actions. Nothing here runs on a laptop.
- A run names its commit, engine, seeds and runner in its report, so it can be
  repeated.
- Simulator seeds are 1 to 2000 in every mode, the same on both engines.
- "History clean" means the history checker reports no violation and no
  undecided cache key, and every final read is whole
  ([`history-checker.md`](history-checker.md), rules 1 to 21).
- A stall counts against Raft only if the hybrid does not stall on the same
  seed and mode (criterion 1). A stall that the hybrid shares needs an open
  issue.
- Sleep is on in every Raft run.

## 1. The fault matrix

"Sim" is the deterministic simulator (`crates/testing/felix-sim`). "History"
is the campaign on real processes
(`crates/testing/felix-cluster/src/history`). A gap number points at
"Gaps" below.

| # | Fault | Harness | Size | Passes when |
| --- | --- | --- | --- | --- |
| 1 | Process, link, clock and disk faults | Sim modes `off` and `clocks`. History nemeses `all` and `adversarial` | 2000 seeds per mode. 20 minutes per nemesis, three seeds | History clean. Every shard has one leader 60 s after the heal |
| 2 | Power loss of one broker | Sim mode `power-loss`. Storage power-loss suite | 2000 seeds. 110 storage seeds | History clean. Every torn tail is repaired. No vote is lower after the restart than before it |
| 3 | Power loss of a majority | Sim mode `power-loss` with two of three brokers cut together | Part of the 2000 seeds, at least 200 with a majority cut | With fsync on commit nothing acknowledged is lost. Every shard is served again |
| 4 | A wiped copy: one shard directory, and a whole data root | Sim and history wipe fault (gap 1). Cluster test ported from `failures/wiped.rs` | 2000 seeds in `placement`. One 20 minute history run | The copy comes back with a new copy id as a learner. It never votes under its old id. History clean |
| 5 | Wiped copies, a majority of one group | Sim wipe fault (gap 1) | At least 200 of the 2000 seeds | The group re-forms. Only records that no disk holds are lost, and the checker is told which. Offsets are never reused |
| 6 | One shard directory restored to an older copy | Sim restore fault (gap 2), aimed inside one mark interval of an acknowledged write | 2000 seeds, and one pinned seed for the example in "What this does not catch" | With the exact mark: the copy rotates its id and no acknowledged record is lost. With the trailing floor the pinned seed is an expected violation and single-directory restore is documented as unsupported |
| 7 | A whole data root restored to an older copy | Sim restore fault (gap 2). Cluster test | 2000 seeds | The broker starts no member until the restore check has run. Every copy id in the root rotates |
| 8 | Re-form after rotated voters, RF 3 and RF 1 | Sim kills under relaxed fsync, and the wipe fault (gap 1). Cluster test for the operator command | 2000 seeds | Every shard is served. The base is the highest reported log id. A retired member never votes or acknowledges in the old epoch |
| 9 | A partition during a re-form | Sim faults aimed at a re-form in progress (gap 3) | 2000 seeds. Pinned seeds for each step | No commit in the old epoch after any voter retired. With an unreachable voter the control plane waits. No two bases for one epoch |
| 10 | Control plane down, then a leader crash | History nemesis `election`. Sim control-plane pause for a whole run. Failover test | 20 minutes, three seeds. 2000 seeds. Three rounds | Failover completes. `Quorum` acknowledgements and consumer groups continue. The acknowledgement path makes no control-plane request |
| 11 | Faults on the control plane's own group | History mode with three control-plane members (#2226). Sim mode with the metadata group (#2232) | 20 minutes, three seeds. 2000 seeds | History clean. A member's vote never goes down. No acknowledged metadata write is lost |
| 12 | Membership change under load | Sim mode `placement`. History kinds `MoveShard`, `Drain` and `InterruptedMove` | 2000 seeds. Part of `all` and `adversarial` | History clean. No group stops on an index collision. A move ends or is undone. The write pause is within the cell in section 3 |
| 13 | Sleep and wake | Sim mode `sleep` (#2239). History run with an idle phase (gap 4). The scale run that kills a broker leading sleeping groups | 2000 seeds. 20 minutes. Nightly at 2,000 members | `SleepersWake` and `AsleepIsLevel` hold. History clean. No election storm stalls the run |
| 14 | The switch, with faults | Sim switch under every mode (#2242). History switch campaign (gap 5) | 2000 seeds per mode. 20 minutes, three seeds | History clean across the switch. A generation 0 record is never read as a Raft entry. Only copies that acknowledged the base vote |
| 15 | A crash in the middle of the switch | Sim crash at each step of the switch (#2242). The upgrade test (#2257) | Every step, on leader, follower and control plane | The shard is a closed hybrid shard or a Raft shard with one base, never both. A voter that crashed before its member start comes up from the recorded base |
| 16 | The bridge upgrade | Upgrade test (#2257): the latest v0.7 release with data, to the bridge build, switch under faults, latch, to the candidate | One run per release candidate, with clients writing the whole time | History clean over the whole path (gap 6). Every acknowledged record is readable at its offset on the candidate. 0.8.0 refuses an unlatched cluster and writes nothing |
| 17 | Rollback | Upgrade test, backwards (gap 6): candidate to bridge build, and bridge build to v0.7 before anything is finalized | One run per release candidate | The older build starts on the newer build's directories and serves. Each rollback the table in `hybrid-retirement.md` marks "No" is refused at start and changes nothing |

Rows 1, 2, 10 and 12 run today on the hybrid. The others wait for the code
or the harness they name.

## 2. History checker assertions

The history checker judges what clients saw. Those rules do not depend on
the engine and carry over unchanged.

| Rule | Under Raft |
| --- | --- |
| 1 to 6: lost write, duplicate, prefix, real-time order, phantom, failed write visible | Unchanged |
| 7 and 20: cache reads | Unchanged |
| 8: atomic commit | Unchanged |
| 9 to 11: delivery order, lost delivery, missing delivery | Unchanged |
| 12 to 19: dependency cycles | Unchanged |
| 21: one leader per generation | Becomes one leader per term (#2258) |
| Modes `lease` and `lease-free` | Removed. There is one mode |

New for Raft:

| Check | Fails when | Reads |
| --- | --- | --- |
| No two leaders per term | Two brokers open one shard at one term, or a broker opens at a term whose blank entry it never committed | Broker logs, collected in every run and not only in `election` (#2258) |
| `AckedSurvive` | After the final heal, a record acknowledged at `Quorum` is missing, at its offset, from any copy that is a voter, or from a majority of the placed set. Or two copies hold different terms for one entry above the base | A dump of every copy's log at the end of the run (gap 7) |
| Stream reads | A `Quorum` read of a stream misses a record whose append was acknowledged before the read began. Rule 7 says this for cache keys only | The history (gap 7) |
| Two records at one offset | A QUIC subscriber, a Kafka consumer or a cache watcher holds two records at one offset on a `Leader` stream whose leader was partitioned | The duplicate-offset nemesis of the engine page |
| Lost records are named | A run that re-forms a group or cuts a majority's power loses a record that some surviving disk still holds | The same dump. The checker gets the list of records no disk holds and excuses only those (gap 7) |

Rule 1 already says an acknowledged write is in the final read. `AckedSurvive`
is stronger: it looks at the disks, so it catches a record that one more
failover would lose although the final read still shows it.

The simulator's cluster checkers move as the hand-off table in
[`hybrid-retirement.md`](hybrid-retirement.md) says, and `SleepersWake`,
`AsleepIsLevel` and `IdleGroupsSleep` are added with sleep.

## 3. Perf cells

There are two comparisons, with different rules.

| Comparison | Sides | Rule |
| --- | --- | --- |
| Engine A/B | Raft and the hybrid, same commit | Throughput at least 0.90 and p99 at most 1.20 of the hybrid, in every cell (criteria 2 and 3) |
| Release A/B | The candidate and the latest v0.7 release | No regression: the candidate is no worse than the release by more than the cell's noise band |

**Noise.** Every perf job runs three sides on one runner: the baseline, the
candidate, and the baseline a second time. The sides alternate trial by
trial and the first side rotates each round. The two baseline sides are an
A/A pair. A cell's noise band is the difference between their medians, and
never less than 3%. A cell whose band is over 10% for throughput or 20% for
p99 measured nothing and is rerun, on a dedicated runner if it happens
twice. A release cell fails if the candidate's median is worse than the
baseline's by more than the band. Six rounds of 30 seconds is the minimum.

**What a cell may claim.** Latency comes only from cells with batch 1.
Batched cells count for throughput only. Throughput is what publishers sent
and delivered throughput is what subscribers got, reported apart. Both sides
use the same harness and load generator, built from the candidate. Nothing
is compiled on the runner while a trial runs, and each trial starts from an
empty data directory.

| # | Cell | Sizes | Release A/B rule | Engine A/B rule |
| --- | --- | --- | --- | --- |
| 1 | Single node, RF 1 | 1 KiB and 64 KiB, batch 1 and batched | Within the band | 0.90 and 1.20 |
| 2 | RF 3 steady, `Quorum` | 1 KiB and 64 KiB, batch 1 and batched | Within the band | 0.90 and 1.20 |
| 3 | RF 3 steady, `Leader`, delivered latency | 1 KiB, batch 1 | Within the band | At most one replication round trip over the hybrid |
| 4 | Fanout 10, batch 64 | 16 KiB and 64 KiB | Within the band | 0.90 and 1.20 |
| 5 | Cache, mixed puts and counter adds | 1 KiB | Within the band. Fsyncs per write no higher | 0.90 and 1.20 |
| 6 | `Quorum` read p99 | 1 KiB | Within the band | 1.20 |
| 7 | Failover time, control plane up | 300 RF 3 streams led by the killed broker | No longer than the release plus 5% | No longer than the hybrid plus 5% |
| 8 | Failover time, control plane down | The same | Reported. v0.7 does not fail over this way by default | No longer than the hybrid with its election on, plus 5% |
| 9 | RF 3 promotion burst | 1 KiB and 64 KiB | Reported and explained | Reported and explained |
| 10 | Groups per broker, memory | 5,000 members per broker, three brokers, RF 3 | Total resident memory reported against the release on the same shards | Raft adds under 1 GB with one broker killed under load. Idle CPU under 5% of one core. Every group asleep, no ticks to sleeping groups |
| 11 | The exact log mark | Cells 1 and 2 at batch 1, mark exact against the trailing floor, same commit | Not a release cell | Within the band in every cell, and no more synced writes per group commit, counted. If so the exact mark ships. If not, the trailing floor ships with the named residual |
| 12 | Move under load, write pause | One RF 3 stream moved 20 times while publishers write at a fixed rate | Longest gap between acknowledgements no longer than the release plus 5%. No write fails for good | The same, against the hybrid |
| 13 | First write after sleep | p50 and p99 | Not a release cell | At most one round trip over a write to an awake group |
| 14 | The control plane's group | The cells of `metadata-raft-design.md`, "Perf gate" (#2224) | Within the band, against redb on the same openraft version | Does not apply |

Cells 7 to 9 follow the failover rule. A failover cost blocks the release
when its cause can be isolated and fixed. A cost that is the price of safety
ships, explained and documented. The 5% covers the failure-detection timer,
which quantizes the outage.

Cells 1, 2 and 4 on real networks (cloud VMs, a disk per broker) are run
once per release for the write-up. They do not gate.

## 4. What runs where

| Check | Pull request CI | Nightly | Release gate |
| --- | --- | --- | --- |
| Simulator | Pinned seeds, on both engines while both exist | 2000 seeds in `off`. 250 seeds in each other mode, a window that moves each night. `sleep` joins the window | 2000 seeds in every mode, on the release commit |
| History campaign | Process faults, short | `all` and `adversarial`, 20 minutes | `all`, `adversarial` and `election`, 20 minutes, three seeds each. The metadata group mode. The switch campaign |
| Raft history rules | Unit tests of each rule, with a history that must fail it | In every campaign | In every campaign |
| Crash points of the Raft log and the metadata store | Yes | Fuzz | Must be green on the release commit |
| Storage power-loss suite | Eight seeds per scenario | 110 seeds | Must be green |
| TLA+ | The registered configs | Walks | Every `FelixRaft` config and twin that the pull requests name, including #2244, #2266 and #2267 |
| Perf | The fast subset, advisory. Cells 1 and 2 as an engine A/B on pull requests that touch replication or storage | Nothing | Every cell of section 3, both comparisons |
| Failover | Cluster tests | Scale run with sleeping leaders | Cells 7 and 8 |
| Groups per broker | No | 2,000 members | 5,000 members. 10,000 reported |
| Upgrade and rollback | No | Weekly, on the bridge branch | Rows 16 and 17 on each release candidate |

The release gate also needs two things that are not workflows:

- The project's own long-running clusters have switched and latched on the
  bridge build before the hybrid is compiled out of main.
- The write-up in `docs/` with the numbers, the conditions of every cell and
  the size report of criterion 7.

## 5. Gaps

No harness exists for these. Each has an issue and blocks the rows that name
it.

| # | Gap | Issue | Blocks |
| --- | --- | --- | --- |
| 1 | A wipe fault in the simulator and the history campaign, for one shard directory and for a data root | #2273 | Matrix 4, 5, 8 |
| 2 | A restore fault in the simulator: a shard directory or a data root put back to an older copy | #2274 | Matrix 6, 7 |
| 3 | Simulator faults aimed at a re-form in progress | #2275 | Matrix 9 |
| 4 | An idle phase in the history campaign, so groups sleep before the faults | #2276 | Matrix 13 |
| 5 | A history campaign that switches shards from the hybrid to Raft under faults | #2277 | Matrix 14 |
| 6 | The upgrade test does not go backwards, and does not check a client history across the path | #2278 | Matrix 16, 17 |
| 7 | History rules that read the disks: `AckedSurvive`, term agreement, named losses. And a stale-read rule for streams | #2279 | Section 2 |
| 8 | A perf cell for the write pause of a move under load | #2280 | Cell 12 |
| 9 | A perf cell that prices the exact log mark, with a count of synced writes per group commit | #2281 | Cell 11 |

Already filed: the metadata group in the history campaign (#2226) and in the
simulator (#2232), the metadata benchmark (#2224), the sleep mode (#2239),
the switch in the simulator (#2242), the upgrade test (#2257) and the
groups-per-broker run (#2243).

## How this maps to the gate workflows

| Workflow | Covers | Does not cover yet |
| --- | --- | --- |
| `gate-history.yml` | Matrix 1, 2, 10, 12. Section 2 rules 1 to 21 | Matrix 3 to 9, 11, 13 to 15. The Raft rules |
| `gate-perf.yml` | Cells 1 and 2, batched only | Batch 1 cells. Cells 3 to 6 and 11 to 14. The A/A side. The engine A/B |
| `gate-failover.yml` | Cell 7. Control plane down as pass or fail | Cell 8 as a time. Cell 9 |
| `gate-groups.yml` | A stub | Cell 10 |
| `gate.yml` | The report | Matrix 16 and 17. The stall comparison between engines |

Changes the workflows need, in the order the gate needs them:

1. **An A/A side in `gate-perf.yml` and `gate-failover.yml`.** Run the
   baseline twice, and have `gate_perf.py` compute the band and apply the
   release rule. Keep 0.90 and 1.20 as the engine rule.
2. **A second comparison.** An input that picks what the baseline is: a
   release tag, or the other engine on the same commit. The engine A/B
   builds once and sets the engine at start.
3. **Batch 1 cells.** Today every cell is batched, so its p99 is not a
   request latency. Add batch 1 for cells 1 and 2 and judge p99 only there.
4. **A true single node.** The single-node cell starts three brokers with
   RF 1. Start one.
5. **Cells 3 to 6**: `Leader` delivered latency, fanout 10 at 16 and 64
   KiB, the cache mixed cell with fsyncs per write, and `Quorum` read p99.
6. **Time the failover with the control plane down** on both sides, in
   place of pass or fail. Under Raft the `replica_election::` tests are
   retired, so take the test by a name that both engines have.
7. **Simulator modes.** Add `sleep`, the metadata group mode and the switch
   to `modes`. Let a mode be a list of faults, so wipe, restore and re-form
   faults can be turned on without a new workflow.
8. **History campaigns.** Three seeds per nemesis. Add the metadata group
   mode, the switch campaign, the idle phase and the duplicate-offset
   nemesis. Drop the `lease` and `lease-free` choice when it goes.
9. **The stall comparison.** `gate.yml` takes the run id of the other
   engine's sweep, and the report lists the seeds that stall on Raft only.
10. **`gate-groups.yml`**: three brokers, 2,000, 5,000 and 10,000 members,
    idle and with one broker killed under load, against the hybrid on the
    same shards.
11. **A `gate-upgrade.yml` part** for matrix 16 and 17. It runs on the
    bridge branch too.
12. **A dedicated runner for perf.** Hosted runners are noisy. The report
    refuses a perf verdict from a cell whose band is over the cap.

## Questions

Each has a recommendation.

1. **Is the release A/B rule "within the noise band", or the 0.90 and 1.20
   of the engine gate?** Recommended: the band. The engine gate's 10% was
   written for a new engine against the old one. Against a release it would
   let 9% go every release.
2. **How many history seeds per nemesis at the release gate?** Recommended:
   three seeds of 20 minutes. One seed is one schedule. More than three
   costs runner hours that the 2000 simulator seeds spend better.
3. **Does every simulator mode need 2000 seeds on both engines?**
   Recommended: yes for the first pass, because criterion 1 compares stalls
   seed by seed. For 0.8.0, Raft only.
4. **Does total broker memory at 5,000 members gate against v0.7?**
   Recommended: no. Gate on the 1 GB that Raft adds, as designed, and
   report the total. If the total is more than 1.5 times the release, the
   write-up says why.
5. **Is control-plane-down failover time a release cell?** Recommended:
   reported against the release, gated against the hybrid with its election
   on. v0.7 ships with the election off, so the release has no number to
   match.
6. **What does "no measurable cost" mean for the exact mark?** Recommended:
   within the band in cells 1 and 2 at batch 1, and the counted synced
   writes per group commit do not rise. The count is exact, so it decides
   when the latency is inside the noise.
7. **Does the trailing-floor fallback block the release if the exact mark
   fails its cell?** Recommended: no. Ship the floor with the one second
   interval, the pinned expected violation and the operations note, as the
   design says.
8. **Must the upgrade test run from every v0.7 patch release?**
   Recommended: from the latest and from v0.7.0. The storage and metadata
   formats do not change inside the line.
9. **How long must the project's own clusters run on the bridge build
   before the hybrid is compiled out?** Recommended: seven days after the
   latch with no history or liveness finding. Shorter does not cover a
   weekly job. Longer holds the deletion for no new information.
10. **Do the real-network cells gate?** Recommended: no. They cost cloud
    credit and vary by region. They are run once for the write-up, and a
    surprise there opens an issue.
