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
- Sleep is deferred (#2167). Raft runs tick due groups and send idle
  heartbeats; no group is put to sleep.

## 1. The fault matrix

"Sim" is the deterministic simulator (`crates/testing/felix-sim`). "History"
is the campaign on real processes
(`crates/testing/felix-cluster/src/history`). A gap number points at
"Gaps" below. The tier of each row is in section 4.

Three terms have one meaning in every row:

- **Served**: 60 s after the heal the shard has exactly one leader, and a
  `Quorum` write to it is acknowledged. A group that must re-form gets 90 s,
  counted from when the last of its voters' brokers is reachable again: the
  re-form time, then the same 60 s. The design gives the re-form time no
  value yet (#2296). The gate assumes 30 s.
- **Fails over**: a `Quorum` write to the shard is acknowledged by a new
  leader within 20 s of the kill. This applies only while a majority of the
  shard's voters stays reachable. Without one the shard cannot elect, and
  the row's "served" decides after the heal.
- **Settles**: the group has as many voters as its factor, no learner and
  no joint membership, 120 s after the heal plus 30 s for each other move
  in flight at the heal. This assumes the control plane's default pacing:
  one move at a time (`FELIX_SHARD_MOVES_MAX_CONCURRENT=1`) and a reconcile
  every 5 s. A run that changes either says so and scales the bound.

Every time above is multiplied by `FELIX_TEST_TIMEOUT_SCALE`, as the
liveness check of the history campaign is today
([`history-checker.md`](history-checker.md), "Liveness").

| # | Fault | Harness | Size | Passes when |
| --- | --- | --- | --- | --- |
| 1 | Process, link, clock and disk faults | Sim modes `off` and `clocks`. History nemeses `all` and `adversarial` | 2000 seeds per mode. 20 minutes per nemesis, three seeds | History clean. Every shard is served |
| 2 | Power loss of one broker, fsync on commit | Sim mode `power-loss`. Storage power-loss suite | 2000 seeds. 110 storage seeds | History clean. Every torn tail is repaired. No vote is lower after the restart than before it |
| 3 | Power loss of a majority at once, fsync on commit | Sim mode `majority-cut` (gap 10) | 2000 seeds, at least 200 with two of three cut together | Nothing acknowledged is lost. No copy id rotates. Every shard is served, with no re-form |
| 4 | A wiped copy: one shard directory, and a whole data root | Sim mode `wipe` and history wipe fault (gap 1). Cluster test ported from `failures/wiped.rs` | 2000 seeds. One 20 minute history run | The copy comes back with a new copy id as a learner. It never votes under its old id. History clean. The group settles |
| 5 | Wiped copies, a majority of one group | Sim mode `wipe` (gap 1), then the operator re-form command | 2000 seeds, at least 200 with a majority wiped | After the operator runs the re-form command, the group is served. Only records that no disk holds are lost, and the checker is told which. Offsets are never reused |
| 6 | One shard directory restored to an older copy | Sim restore fault (gap 2), with a nemesis aimed inside one mark interval of an acknowledged write, and one pinned seed for the example in "What this does not catch" | 2000 seeds | See "Row 6" below |
| 7 | A whole data root restored to an older copy | Sim restore fault (gap 2). Cluster test | 2000 seeds | The broker starts no member until the restore check has run. Every copy id in the root rotates. History clean |
| 8 | Operator re-form after rotated voters, RF 3 and RF 1 | Sim wipe fault (gap 1) and relaxed-fsync power loss (gap 10), each followed by the operator re-form command. Cluster test for the command | 2000 seeds | Every shard is served. The base is the highest reported log id. A retired member never votes or acknowledges in the old epoch |
| 9 | A partition during the operator re-form command | Sim faults aimed at a re-form command in progress (gap 3) | 2000 seeds. A pinned seed for each step | No commit in the old epoch after any voter retired. While a voter is unreachable the control plane writes no new epoch, for a partition of at least 120 s. No epoch has two bases. Served after the heal |
| 10 | Control plane down, then a leader crash | History nemesis `election`. Sim control-plane pause for a whole run. Failover test | 20 minutes, three seeds. 2000 seeds. Six rounds | Every shard the killed broker led fails over. `Quorum` acknowledgements and consumer groups continue. The acknowledgement path makes no control-plane request, counted |
| 11 | Faults on the control plane's own group | History mode with three control-plane members (#2226). Sim mode with the metadata group (#2232) | 20 minutes, three seeds. 2000 seeds | History clean. A member's vote never goes down. No acknowledged metadata write is lost. Every member's state is equal at the same applied index |
| 12 | Membership change under load | Sim mode `placement`. History kinds `MoveShard`, `Drain` and `InterruptedMove` | 2000 seeds. Part of `all` and `adversarial` | History clean. No group stops on an index collision. Every group settles. The write pause is within cell 12 |
| 13 | Sleep and wake. Deferred (#2167): not run in 0.8 | Sim mode `sleep` (#2239). History run with an idle phase (gap 4). The scale run that kills a broker leading sleeping groups | Not run | Not run |
| 14 | The switch, with faults, stream and cache shards | Sim switch under every mode (#2242). History switch campaign (gap 5). Cache shard schedules (gap 12) | 2000 seeds per mode. 20 minutes, three seeds | History clean across the switch. A generation 0 record is never read as a Raft entry. Only copies that acknowledged the base vote. For a cache shard: two copies with skewed clocks compute the same digest, and the counter sums after the switch equal the checkpoint |
| 15 | A crash in the middle of the switch, stream and cache shards | Sim crash at each step of the switch (#2242), including the counter checkpoint and the state digest (gap 12). The upgrade test (#2257) | Every step, on leader, follower and control plane | The shard is a closed hybrid shard or a Raft shard with one base, never both. A copy that failed its digest is a learner |
| 16 | The bridge upgrade | Upgrade test (#2257, gaps 6 and 11): the latest v0.7 release with data, to the bridge build, switch under faults, latch, to the candidate | One run per night and per release candidate, with clients writing the whole time | History clean over the whole path. Every acknowledged record is readable at its offset on the candidate. 0.8.0 refuses an unlatched cluster and writes nothing. After the latch: a voter that crashed between the compare-and-swap and its member start comes up from the recorded base; a learner left from a failed first seal comes up as a learner; a directory with no member mark after a re-form starts from the current epoch, base and voters |
| 17 | Rollback | Upgrade test, backwards (gap 6): candidate to bridge build, and bridge build to v0.7 before anything is finalized | The same runs as row 16 | The older build starts on the newer build's directories and is served. Each rollback the table in `hybrid-retirement.md` marks "No" is refused at start and changes nothing |
| 18 | The control plane's log across the upgrade | Upgrade test (gap 11) | The same runs as row 16 | A hybrid command committed after the latch applies as a no-op on both builds. A candidate member that replaces one before every member has purged stops, writes nothing and starts again on the bridge build. A snapshot from before the latch is refused. Every member's state is equal at the same applied index, across mixed builds |
| 19 | Races with the switch and the latch | Sim aimed schedules (gap 12) | A pinned seed each, and 2000 seeds with the schedules on | `placement abandon` before the compare-and-swap makes the switch start over; after it, abandon is refused. A shard created between the operator's check and the latch entry leaves the latch unset and is named, or is a Raft shard |
| 20 | Two records at one offset on a `Leader` stream | History duplicate-offset nemesis (gap 13) | 20 minutes, three seeds | No QUIC subscriber, Kafka consumer or cache watcher holds two records at one offset |
| 21 | The redb conversion | The tool's tests (#2234), and inside the upgrade test (gap 11) | A crash at every step of `convert` and of `restore`. One member at a time under traffic | Every acknowledged metadata write is on every member afterwards. A second `convert` after a crashed `restore` refuses. An older binary on a converted directory refuses and changes nothing |
| 22 | Re-form races | Sim aimed schedules (gap 3) | A pinned seed each, and 2000 seeds with the schedules on | A voter of a newer membership that the control plane does not know is waited for. A voter missing from a forced re-form that comes back under its old id stops, and nothing it sends is accepted. A retired copy whose group elected anyway rotates its copy id and rejoins as a learner |
| 23 | Power loss under relaxed fsync, one broker and a majority | Sim modes `relaxed-power-loss` and `majority-cut-relaxed` (gap 10) | 2000 seeds, at least 200 with a majority cut | Every copy that lost unsynced bytes rotates its id. A cut majority re-forms and is served. Only records no surviving disk holds are lost |

Rows 1, 2, 10 and 12 run today on the hybrid. The sim modes of rows 3, 4, 5
and 23 run too, but they check only part of each row. The modes have no
operator re-form step. A shard left unserved is a tracked stall in the sweep,
which fails nothing (#2336). Nothing counts the seeds that cut or wiped a
majority, so the "at least 200" quotas are not checked. The history wipe fault
is not built yet. The others wait for the code or the harness they name. Row
23 applies only to logs that accept a relaxed fsync mode (#2248).

**Row 13, term limits.** Deferred with sleep (#2167). A count of terms per fault says little: one real
failure may cost several terms and a harmless event must cost none. The
limits are per kind of fault, from "When a broker dies with sleeping
leaders" in [`per-shard-raft.md`](per-shard-raft.md). Each limit holds
when it is the only fault on that group. The simulator's seeds overlap
faults, and a group hit by two at once is judged by "served" alone.

| Fault | Limit |
| --- | --- |
| A false liveness alarm about a healthy leader's broker | No new term in any group |
| A follower's broker restarts | No new term in any group it follows |
| A message of the sleep handshake is dropped | No new term. A dropped wake message from a follower, or its answer, is different: it starts an ordinary election by design |
| A broker that leads sleeping groups dies | On each surviving broker, the elections started in one tick interval are at or under the ticker's cap. The rest queue |

A leader transfer that the control plane asks for and a move are not
faults here. The terms they add are not counted.

**Row 6.** The decision on the per-shard log mark (#2245) is: build the
exact mark, and ship it if cell 11 shows it is free. Row 6 is written for
both outcomes.

- The exact mark ships: all 2000 seeds pass with the aimed nemesis on. The
  copy rotates its id and no acknowledged record is lost.
- The trailing floor ships: the aimed nemesis is off in the sweep, because
  any seed can hit the window. The 2000 seeds run with restores older than
  one mark interval and must pass. The window is pinned by the model twin
  and the one pinned seed, both recorded as expected violations, and
  restoring a single shard directory is documented as unsupported. This is
  the one decided exception to "not written up as a limit".

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
| `AckedSurvive` | See below | A dump of every copy's log and commit index at the end of the run (gap 7) |
| Stream reads | A `Quorum` read of a stream misses a record whose append was acknowledged before the read began. Rule 7 says this for cache keys only | The history (gap 7) |
| Two records at one offset | A QUIC subscriber, a Kafka consumer or a cache watcher holds two records at one offset on a `Leader` stream whose leader was partitioned | The duplicate-offset nemesis (gap 13) |
| Lost records are named | A run that re-forms a group or cuts a majority's power loses a record that some surviving disk still holds | The same dump. The checker gets the list of records no disk holds and excuses only those (gap 7) |

**`AckedSurvive`.** After the final heal the checker waits, for at most
60 s, until every live voter of a group reports the leader's commit index.
A group that does not get there fails on that alone. Then it fails when:

- A record acknowledged at `Quorum` is above the commit index.
- Such a record is missing, at its offset, from any voter.
- Two voters hold different terms for one entry at or below the commit
  index and above the base. Entries above the commit index are not
  compared: an uncommitted tail may differ.
- The voters that hold the record are fewer than a majority of the placed
  set, unless the control plane shows the shard as under-replicated. After
  a re-form a group may have one voter. Then the record must be on every
  voter there is, and the run reports the shard.

Rule 1 already says an acknowledged write is in the final read. `AckedSurvive`
is stronger: it looks at the disks, so it catches a record that one more
failover would lose although the final read still shows it.

The simulator's cluster checkers move as the hand-off table in
[`hybrid-retirement.md`](hybrid-retirement.md) says, and sleep checkers
(`SleepersWake`, `AsleepIsLevel`, `IdleGroupsSleep`) wait for sleep (#2167).

## 3. Perf cells

There are two comparisons, with different rules.

| Comparison | Sides | Rule |
| --- | --- | --- |
| Engine A/B | Raft and the hybrid, same commit | Throughput at least 0.90 and p99 at most 1.20 of the hybrid, in every cell (criteria 2 and 3) |
| Release A/B | The candidate and the latest v0.7 release | No regression: the paired rule below. Throughput is at least 0.97 of the release. p99 is at most 1.10 of it |

The bridge candidate runs the release A/B twice: on Raft, which a new
cluster gets, and on the hybrid, which an upgraded cluster keeps until its
operator switches. Both must pass. The 0.8.0 candidate has one engine.

**The paired rule.** The sides alternate trial by trial on one runner and
the side that goes first flips each round, so each round is a pair. Each
round gives one ratio, candidate over baseline. The figure for a cell is the
median of those ratios, with an exact interval for a median: the widest
interval six rounds allow (96.9% two-sided), and 99% from 12 rounds. That
interval is read off the sorted ratios and assumes nothing about their
shape:

| Rounds | Lower end | Upper end |
| --- | --- | --- |
| 6 | The smallest ratio | The largest |
| 12 | The 2nd smallest | The 2nd largest |
| 24 | The 6th smallest | The 6th largest |

Throughput, where higher is better, has a bound of 0.97:

- Pass: the interval's lower end is above 0.97.
- Fail: its upper end is below 0.97.

p99, where lower is better, has its own bound of 1.10. Decided: a tail is
noisier than a median, so it is held to "no worse than 10%":

- Pass: the interval's upper end is under 1.10.
- Fail: its lower end is above 1.10.

Any other figure where lower is better (p50, an outage, a pause) is turned
over, baseline over candidate, and judged like throughput against the
bound its cell names.

Otherwise the cell is undecided and rounds are added: 6, then 12, then 24.
A cell still undecided at 24 rounds fails as not measured, and is rerun on
a dedicated runner. So every cell is decided within 24 rounds.

**What the rule promises.** The interval is this wide, and not 95%, because it is
looked at three times and six rounds are few. A candidate whose true
median is exactly at the bound passes with a chance of 1 in 64 at 6 rounds
and under 1 in 300 at each later look, 2.0% over the three looks. A worse
candidate passes less often. So:

- A true 3% throughput regression is caught at least 98 times in 100. It
  ends as a fail or as not measured, never as a pass. The same holds for a
  true 10% p99 regression.
- How soon a regression is called a fail depends on the noise, taken here
  as the standard deviation of the round ratios. At 2%, a 5% regression
  fails outright 88 times in 100 and a 6% one nearly always.
- A candidate equal to the release passes 99.7 times in 100 when that
  noise is 2%, and 86 in 100 when it is 3%. For p99 the same figures hold
  at a noise of 6% and of 9.5%. A noisier cell needs the dedicated runner.

Each side's own median is printed for reading and decides nothing.

**A/A is a validity check, not the band.** Each job also pairs the baseline
with itself. If the half width of that interval at the final round count is
wider than the cell's margin (3% for a bound of 0.97, 5% for 0.95, 10% for
p99), the runner was not steady and the cell's verdict is void. Whether
the interval contains 1.0 is not tested: across thirty cells a steady
runner would miss it somewhere in most runs.

**What a cell may claim.** Request latency comes only from cells with batch
1. A batched cell is judged on throughput and on its batch latency, which
compares the two sides and says nothing about one request. Throughput is
what publishers sent and delivered throughput is what subscribers got,
reported apart. Both sides use the same harness and load generator, built
from the candidate. Nothing is compiled on the runner while a trial runs,
and each trial starts from an empty data directory. A trial is 30 seconds.

| # | Cell | Sizes | Release A/B | Engine A/B |
| --- | --- | --- | --- | --- |
| 1 | Single node, RF 1 | 1 KiB and 64 KiB. Batch 1 for p50 and p99, batch 64 for throughput | 0.97. p99 1.10 | 0.90 and 1.20 |
| 2 | RF 3 steady, `Quorum` | The same | 0.97. p99 1.10 | 0.90 and 1.20 |
| 3 | RF 3 steady, `Leader`, delivered latency | 1 KiB, batch 1 | 0.97 for p50. p99 1.10 | At most one replication round trip over the hybrid |
| 4 | Fanout 10, batch 64 | 16 KiB and 64 KiB | 0.97 on delivered throughput. On publish-to-delivery batch latency, 0.90 for p50 and 1.10 for p99 | 0.90 on delivered throughput, 1.20 on the batch latency |
| 5 | Cache, mixed puts and counter adds | 1 KiB. Batch 1 for p99, batch 64 for throughput | 0.97. p99 1.10. Fsyncs per write no higher, counted | 0.90 and 1.20 |
| 6 | `Quorum` read p99 | 1 KiB, batch 1 | 1.10 | 1.20 |
| 7 | Failover time, control plane up | 300 RF 3 streams led by the killed broker. Six rounds at least | 0.95 | 0.95, against the hybrid |
| 8 | Failover time, control plane down | The same | Reported. v0.7 does not fail over this way by default | 0.95, against the hybrid with its election on |
| 9 | RF 3 promotion burst | 1 KiB and 64 KiB | Reported and explained | Reported and explained |
| 10 | Groups per broker | Three brokers, RF 3. 1,500 shards, the size v0.7 is tested at, and 5,000 | At 1,500, per broker, idle: resident memory at most 250 MB over the v0.7 release on the same shards, and idle CPU under 2% of one core. Total memory reported at 5,000 | At 5,000: Raft adds under 1 GB with one broker killed under load, idle CPU under 5% of one core, with due groups ticked and idle heartbeats sent |
| 11 | The exact log mark | Cells 1 and 2 at batch 1, mark exact against the trailing floor, same commit | Not a release cell | 0.97, p99 1.10, and no more synced writes per group commit, counted. If so the exact mark ships. If not, the trailing floor ships (row 6) |
| 12 | Move under load, write pause | One RF 3 stream moved 20 times per round while publishers write at a fixed rate. Six rounds at least | 0.95 on the longest gap between acknowledgements. No write fails for good | The same, against the hybrid |
| 13 | First write after sleep. Deferred (#2167) | Not run | Not run | Not run |
| 15 | `Quorum` on a `Periodic` stream, RF 3 | 1 KiB, batch 1 for p50 and p99, batch 64 for throughput. Default fsync policy | 0.97 against the v0.7 release. p99 1.10 | 0.90 and 1.20 |
| 14 | The control plane's group | The cells of `metadata-raft-design.md`, "Perf gate" (#2224) | 0.97 against the v0.7 release, and against redb on the same openraft version | Does not apply |

Cell 4 exists because of a regression the fast subset missed: p50 and p99
rose four to seven times at 64 KiB, batch 64, fanout 10, with throughput
unchanged. Throughput alone would pass it again.

**Cell 10's budget.** Decided: memory and idle CPU are absolute limits, not
ratios. With three brokers at RF 3 each broker holds 1,500 members, about
500 of them leaders. The design measured 180 KB per leader member and
145 KB per follower, so Raft's state is about 235 MB. The budget is 250 MB
per broker on top of what the v0.7 release uses on the same 1,500 shards,
measured in the same job, 60 s after settling. Idle CPU is the design's
limit for 2,000 members, under 2% of one core. A ratio would not work for
either: v0.7's idle CPU is close to zero, and a small base makes a ratio
of memory swing.

The 0.95 of cells 7, 8 and 12 covers the failure-detection timer, which
quantizes an outage. Cells 7 to 9 also follow the failover rule: a cost
blocks the release when its cause can be isolated and fixed, and a cost
that is the price of safety ships, explained and documented.

Cells 1, 2 and 4 on real networks (cloud VMs, a disk per broker) are run
once per release for the write-up. They do not gate.

## 4. What runs where

| Check | Pull request CI | Nightly | Release gate |
| --- | --- | --- | --- |
| Simulator modes `off`, `clocks`, `placement`, `power-loss` | Pinned seeds, on both engines while both exist | 2000 seeds in `off`. 250 seeds in each other mode, a window that moves each night | 2000 seeds in every mode, on the release commit |
| Simulator faults: wipe, restore, re-form, majority cut, relaxed fsync, the switch and latch races (rows 3 to 9, 19, 22, 23) | The pinned seed of each | Each joins the moving window as its own mode. `wipe`, `majority-cut`, `majority-cut-relaxed` and `relaxed-power-loss` are in `dst-nightly.yml` | 2000 seeds each. Dispatch `all`, then `disks` |
| Simulator with the metadata group, and the switch (rows 11, 14, 15) | Pinned seeds | In the moving window | 2000 seeds each |
| History campaign | Process faults, short | `all`, `adversarial` and `election`, 20 minutes, one seed. The metadata group mode. The switch campaign, while both engines are on main and on the bridge branch after that. The duplicate-offset nemesis | The same set, three seeds each |
| Raft history rules | Unit tests of each rule, with a history that must fail it | In every campaign | In every campaign |
| Crash points of the Raft log, the metadata store and the conversion tool | Yes | Fuzz | Green on the release commit |
| Storage power-loss suite | Eight seeds per scenario | 110 seeds | Green |
| TLA+ | The registered configs | Walks | Every `FelixRaft` config and twin that the pull requests name, including #2244, #2266 and #2267 |
| Perf | The fast subset, advisory. Cells 1 and 2 as an engine A/B on pull requests that touch replication or storage | Cells 1, 2 and 4 as a release A/B, six rounds. A fail opens an issue | Every cell of section 3, both comparisons |
| Failover | Cluster tests | Scale run with a killed leader | Cells 7 and 8 |
| Groups per broker | No | 2,000 members | 1,500 and 5,000 members. 10,000 reported |
| Upgrade and rollback (rows 16 to 18, 21) | No | On main, with main as the candidate, every night. On the bridge branch, weekly | On each release candidate |

The nightly upgrade run on main is what keeps the member start from a
recorded base alive. 0.8.0 needs it to finish a latched cluster, and the
pull requests that delete the hybrid could remove it with every other test
still green.

The release gate also needs two things that are not workflows:

- The project's own long-running clusters have switched and latched on the
  bridge build before the hybrid is compiled out of main (Questions, 9).
- The write-up in `docs/` with the numbers, the conditions of every cell and
  the size report of criterion 7.

## 5. Gaps

No harness exists for these. Each has an issue and blocks the rows that name
it.

| # | Gap | Issue | Blocks |
| --- | --- | --- | --- |
| 1 | A wipe fault in the simulator and the history campaign, for one shard directory and for a data root | #2273 | Matrix 4, 5, 8 |
| 2 | A restore fault in the simulator: a shard directory or a data root put back to an older copy | #2274 | Matrix 6, 7 |
| 3 | Simulator faults aimed at a re-form in progress, and the re-form races | #2275 | Matrix 9, 22 |
| 4 | An idle phase in the history campaign, so groups sleep before the faults. Deferred with sleep (#2167) | #2276 | Matrix 13 |
| 5 | A history campaign that switches shards from the hybrid to Raft under faults | #2277 | Matrix 14 |
| 6 | The upgrade test does not go backwards, and does not check a client history across the path | #2278 | Matrix 16, 17 |
| 7 | History rules that read the disks: `AckedSurvive`, term agreement, named losses. And a stale-read rule for streams | #2279 | Section 2 |
| 8 | A perf cell for the write pause of a move under load | #2280 | Cell 12 |
| 9 | A perf cell that prices the exact log mark, with a count of synced writes per group commit | #2281 | Cell 11, matrix 6 |
| 10 | The simulator cannot lose unsynced bytes under relaxed fsync, and cuts the power of one broker only | #2292 | Matrix 3, 8, 23 |
| 11 | The upgrade test does not cover the control plane's log, the post-latch starts or the conversion, and does not run on main | #2293 | Matrix 16, 18, 21 |
| 12 | Aimed schedules for abandon against the switch, a shard create against the latch, and cache shards in the switch | #2294 | Matrix 14, 15, 19 |
| 13 | The duplicate-offset nemesis | #2295 | Matrix 20 |

Already filed: the metadata group in the history campaign (#2226) and in the
simulator (#2232), the metadata benchmark (#2224), the conversion tool
(#2234), the sleep mode (#2239, deferred to v0.9.0), the switch in the simulator (#2242), the
upgrade test (#2257) and the groups-per-broker run (#2243).

## How this maps to the gate workflows

| Workflow | Covers | Does not cover yet |
| --- | --- | --- |
| `gate-history.yml` | Matrix 1, 2, 10, 12. Section 2 rules 1 to 21 | Matrix 3 to 9, 11, 13 to 15, 19, 20, 22, 23. The Raft rules |
| `gate-perf.yml` | Cells 1 and 2, batched only, judged on medians | The paired rule. Batch 1. Cells 3 to 6 and 11 to 14. The engine A/B |
| `gate-failover.yml` | Cell 7. Control plane down as pass or fail | Cell 8 as a time. Cell 9. The paired rule |
| `gate-groups.yml` | A stub | Cell 10 |
| `gate.yml` | The report | Matrix 16 to 18 and 21. The stall comparison between engines |

Changes the workflows need, in the order the gate needs them:

1. **The paired rule in `gate_perf.py`**, for perf, failover and the move
   pause: per-round ratios, the exact interval (the widest six rounds allow,
   96.9% two-sided, and 99% from 12 rounds), the three verdicts, the
   separate p99 bound, and more rounds for an undecided cell. Add the
   baseline-against-itself pair as the validity check, on half width only. Keep 0.90 and 1.20 on medians as the engine rule.
   Drop the `failover_slack` input: the bound is 0.95.
2. **A second comparison.** An input that picks what the baseline is: a
   release tag, or the other engine on the same commit. The engine A/B
   builds once and sets the engine at start. For the bridge candidate the
   release A/B runs once per engine.
3. **Batch 1 cells.** Today every cell is batched, so its p99 is not a
   request latency. Add batch 1 for cells 1 and 2 and take p50 and p99
   only there.
4. **A true single node.** The single-node cell starts three brokers with
   RF 1. Start one.
5. **Cells 3 to 6**: `Leader` delivered latency, fanout 10 at 16 and 64
   KiB with delivered throughput and batch latency, the cache mixed cell
   with fsyncs per write, and `Quorum` read p99.
6. **Time the failover with the control plane down** on both sides, in
   place of pass or fail. Under Raft the `replica_election::` tests are
   retired, so take the test by a name that both engines have.
7. **Simulator modes.** Add the metadata group, the switch (and `sleep` once it returns), and
   one mode per fault of rows 3 to 9, 19, 22 and 23, so each can be swept
   and can join the nightly window.
8. **History campaigns.** Three seeds per nemesis. Add the metadata group
   mode, the switch campaign, the idle phase and the duplicate-offset
   nemesis. Drop the `lease` and `lease-free` choice when it goes.
9. **The stall comparison.** `gate.yml` takes the run id of the other
   engine's sweep, and the report lists the seeds that stall on Raft only.
10. **`gate-groups.yml`**: three brokers at 1,500, 2,000, 5,000 and 10,000
    members, idle and with one broker killed under load, against the hybrid,
    and at 1,500 against the release with the absolute budget of cell 10.
11. **A `gate-upgrade.yml` part** for matrix 16 to 18 and 21, callable from
    a nightly schedule on main and a weekly one on the bridge branch.
12. **Nightly callers** for the `election` nemesis, the metadata group
    mode, the switch campaign and the release A/B of cells 1, 2 and 4.
13. **A dedicated runner for perf.** Hosted runners are noisy. The report
    prints no verdict for a cell whose validity check failed.

## Questions

Each has a recommendation.

1. **Is the release A/B judged by the paired rule, or by the 0.90 and 1.20
   of the engine gate?** Decided: the paired rule, with two bounds.
   Throughput must be at least 0.97 of the release. p99 has its own bound:
   the upper end of its interval must be under 1.10. Both are decided
   within 24 rounds. The engine gate's 10% on throughput was written for a
   new engine against the old one. Against a release it would let 9% go
   every release.
2. **How many history seeds per nemesis at the release gate?** Recommended:
   three seeds of 20 minutes, and only because `election`, the metadata
   group mode and the switch campaign also run every night. Without the
   nightly runs three seeds are too few and the gate needs ten.
3. **Does every simulator mode need 2000 seeds on both engines?**
   Recommended: yes for the first pass, because criterion 1 compares stalls
   seed by seed. For 0.8.0, Raft only.
4. **Does total broker memory gate against v0.7?** Decided: yes, as an
   absolute budget per broker at 1,500 shards, the size v0.7 is tested at:
   at most 250 MB over the release, and idle CPU under 2% of one core. At
   5,000 the total is reported, and the gate there is the 1 GB that Raft
   adds with one broker killed.
5. **Is control-plane-down failover time a release cell?** Recommended:
   reported against the release, gated against the hybrid with its election
   on. v0.7 ships with the election off, so the release has no number to
   match.
6. **What does "free" mean for the exact mark?** Recommended: cell 11
   passes the paired rule (0.97, and 1.10 for p99), and the counted synced writes per group
   commit do not rise. The count is exact, so it decides when the latency
   is undecided.
7. **Is the bound for failover and the move pause 0.95?** Recommended: yes.
   The failure-detection timer quantizes an outage, so 0.97 would fail on
   one tick. Six rounds at least, where the workflow runs three today.
8. **Must the upgrade test run from every v0.7 patch release?**
   Recommended: from the latest and from v0.7.0. The storage and metadata
   formats do not change inside the line.
9. **How long must the project's own clusters run on the bridge build
   before the hybrid is compiled out?** Recommended: seven days after the
   latch with no history or liveness finding, and only if in that time the
   clusters went through at least one failover and one re-form. If they did
   not, cause both on purpose. Days without either prove nothing about the
   paths the hybrid's removal leaves alone to do the work.
10. **Do the real-network cells gate?** Recommended: no. They cost cloud
    credit and vary by region. They are run once for the write-up, and a
    surprise there opens an issue.
