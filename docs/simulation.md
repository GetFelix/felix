# Deterministic simulation

The cluster harness ([cluster-harness.md](cluster-harness.md)) runs real broker
processes over real QUIC. That finds bugs, but slowly: a failover takes
seconds of wall time, and a fault that needs a partition to land between two
particular messages may take hundreds of runs to hit. The simulator runs
several brokers in one test process instead, on tokio's virtual time, with
the network and the clocks under the test's control. Everything a run does,
from the faults to the clients' operations, is drawn from one number, the
seed, so a failing seed can be run again.

It lives in `crates/testing/felix-sim`. A run starts a small cluster (three
to five brokers and a control plane), drives clients against it, injects faults
for most of two virtual minutes, heals everything, and then checks what the
clients saw and what the brokers ended up holding. Two virtual minutes take a
few seconds of real time in a release build (see [Running it](#running-it)).

## Running it

Run a batch of seeds:

```bash
cargo test -p felix-sim --test seeds -- --ignored --exact seeds
```

`FELIX_SIM_SEEDS` sets how many seeds (default 4) and `FELIX_SIM_SEED_START`
the first one. Each seed is 120 seconds of virtual time and takes one to ten
seconds of real time in a release build (`--release`), and longer in a debug
one. The clock is paused, so waiting costs
nothing, but real work does not: starting a cluster opens a durable log for
each of 50 to 200 shards on every broker, and every crash restart opens them
again. The data directories are in memory, so that is CPU work and no disk
work, but the broker tasks that run between timers still take real time that
no virtual clock skips. Run seeds one at a time on a busy machine.

A run that makes no progress ends instead of hanging. A thread of its own
watches the virtual clock, and if it has not moved for four minutes of real
time, or the whole run has used `real_time_limit` (eight minutes), the run
stops and reports a `stalled` finding. That means some broker task never
waits on a timer, which holds the paused clock still; the finding says so and
prints no census, since the busy task would hold that up too.

`FELIX_SIM_POWER_LOSS=1` turns on power cuts ([Power loss](#power-loss)) the
same way.

`FELIX_SIM_PLACEMENT=1` turns on placement faults (below) for `seeds`,
`replay` and `each_seed_runs_the_same_twice`:

```bash
FELIX_SIM_PLACEMENT=1 FELIX_SIM_SEEDS=8 \
  cargo test -p felix-sim --release --test seeds -- --ignored --exact seeds
```

When a seed fails, the test prints the command that replays it:

```bash
FELIX_SIM_SEED=<n> task sim:replay
```

A replay runs the same cluster, the same faults at the same virtual times and
the same client operations, and it is the same run, event for event.
`task sim:replay` builds with `RUSTFLAGS="--cfg tokio_unstable"`, the only way
tokio lets a runtime seed `select!`, in a target directory of its own, since
the flag changes every crate's build. To diff two runs:

```bash
FELIX_SIM_SEED=<n> FELIX_SIM_HISTORY=run1.txt task sim:replay
FELIX_SIM_SEED=<n> FELIX_SIM_HISTORY=run2.txt task sim:replay
diff run1.txt run2.txt
```

`FELIX_SIM_HISTORY` writes the run's trace (every fault, operation, final
read, opened generation and finding, one per line) to a file. A plain
`cargo test -p felix-sim --test seeds replay` also works, but without the flag
tokio picks among ready `select!` branches at random: the faults and
operations still repeat, the interleaving under them does not, and a
timing-sensitive failure may need a few replays to show again. Such a build
refuses `FELIX_SIM_HISTORY`, since two of its histories differ wherever two
branches raced.

To skew each node's clocks as well, set `FELIX_SIM_CLOCKS` to `bounded` or
`unbounded` ([Clock skew](#clock-skew) says what each does). It needs the
`tokio_unstable` build, so run it through `task sim:replay`:

```bash
FELIX_SIM_SEED=<n> FELIX_SIM_CLOCKS=bounded task sim:replay
```

A seed that fails with skew prints a replay command that includes it.

`task sim:determinism` checks that replay is exact. It runs each seed twice in
one process and fails on the first event where the two traces differ, then
replays seed 1 (`FELIX_SIM_REPLAY_SEED`) in two separate processes through
`task sim:replay` and compares the two histories byte for byte. The second
check runs the command a failing seed prints, and it catches anything that
differs from process to process but not between two runs in one process. CI
runs it on seeds 1 to 20 in the `Simulator determinism` job. A plain
`cargo test` skips the in-process test, because it cannot pass without the
flag.

Several things the brokers do would otherwise make two runs differ, and under
the simulator each is held to the run's one thread and its seed. Only some of
this is gated. The seeded randomness (`felix_common::random`), the retry
jitter, the blocking-work runner and the clock hook exist only in debug builds
or under the `felix-common/sim` feature; a plain release build does not have
them. Some changes run in every build: passes over routing tables and
assignment sets walk in key order, the election walk starts at an offset drawn
from the seeded source each round (the OS's randomness in a plain broker) so
followers do not all stand on the same shards first, the deadlines and
backoffs read `tokio::time::Instant` instead of `std::time::Instant`, and the
maps hash with `ahash` instead of std's SipHash (see Map order below).

- **Blocking work.** Shard opens, reads, rollovers, the log's append and
  flush threads, and the membership task's checks of the data directory run
  on real threads in a broker, and finish in real time.
  `felix_common::blocking` runs that work as a task on the simulated runtime
  instead, once the simulator has installed its clock. Flushes run too, against
  the in-memory filesystem (see [The filesystem](#the-filesystem)),
  where they cost no real time.
- **Retry jitter.** Election backoff, the peer redial backoff and the
  membership report delay draw from `felix_common::jitter`, which the
  simulator seeds.
- **Hash keys and ids.** The broker's maps, its producer ids and the
  replication confirmed-read slots use `felix_common::random::hash_state`,
  and storage ids, node ids and lease ids use `felix_common::random::uuid_v4`.
  Both draw from the seeded generator, so map layouts and ids repeat.
- **Map order.** A std `HashMap` takes its hash keys from the OS, so the
  order it is walked in, and the order it drops its entries in, differs from
  run to run. Both decide what runs next: a `retain` that drops a quorum mark
  wakes its waiters, a crash drops a broker's maps whole, and a pass over a map
  starts work entry by entry. So storage, the broker, the broker service,
  replication and the control plane take `HashMap` and `HashSet` from
  `felix_common::random`. Those are std's maps with an `ahash` hasher whose
  keys come from the seeded generator, even when the map is built by `collect`
  or `Default`; outside a simulator each map gets random keys, as std's do.
  The harness's own maps are `BTreeMap`s. A cache shard's compaction copies
  live entries forward in log order rather than in its index's order.
- **Wall time.** Expiry stamps and deadlines read `felix_common::clock::system_time`,
  which is virtual time counted from the fixed simulated epoch. That includes
  the control plane's stream and cache incarnations, the cache's expiry
  stamps and versions, retention's clock, counter timestamps and the names
  of retired shard directories.
- **Real-time deadlines.** The peer pool's backoff, the forward budget, the
  routing hold, the shard lifecycle's fence timers and the log's "persist the
  commit offset at most this often" clock read tokio's clock, so they follow
  the paused clock instead of the host's.
- **Walk order.** A pass over a routing table, an assignment set or the shards
  a broker stands for visits them in key order, because hash order changes
  from process to process and these passes start work shard by shard.
- **Data directories.** Each broker's directory is named after the broker
  (`felix-sim/b1`), not after the process, so paths hash and sort the same
  way every run. A second simulation running at the same time in the process
  gets a suffixed name.
- **`select!`.** Tokio picks among ready branches at random unless the runtime
  is given a seed, which tokio allows only under `--cfg tokio_unstable`.

Seeds known to fail are kept as ignored tests named after the issue that
tracks them.

The unit tests of the network, the control plane and the checkers run with the
rest of the crate:

```bash
cargo test -p felix-sim
cargo test -p felix-cluster --lib history::
```

## What a run does

1. **Start.** The control plane gets a tenant, a durable stream with 50 to 200
   shards and a cache, all replicated three ways with quorum acknowledgement.
   Each broker opens a real durable log in its own in-memory data directory, waits
   for the stream catalog (the `seeded` gate a real broker has), registers, and
   starts its shard tasks. Once every broker has registered, the run finalizes
   replica election, so brokers elect a new leader themselves when one is lost.
2. **Faults.** For the first 90 seconds a schedule drawn from the seed crashes
   and kills brokers or cuts their power and restarts them, partitions them,
   freezes them, drops one direction of a link, and makes the control plane
   slow, paused or down. Faults may
   overlap. Eight steps in a hundred pause the control plane and, a second or
   two later, crash the broker that leads the most shards, which is the case
   election has to get right when nobody can say who leads. The step is drawn
   like any other, so about two seeds in three have it and the rest never do.
   The crash is skipped when half the brokers or more are already down, so
   some runs keep a quorum. With clock skew on, a second schedule steps and
   re-skews each node's clocks over the same 90 seconds
   ([Clock skew](#clock-skew)). With placement faults on, the nemesis also acts
   as an operator; see [Placement faults](#placement-faults). While the faults
   run, the checkers marked "every second" below sample the cluster.
3. **Heal and settle.** Every link is healed, the control plane answers again,
   drained brokers are put back to `live` and crashed brokers restart. Skewed
   clocks stay skewed. The cluster then has 60 virtual seconds to get every
   shard served again. The clients keep running until second 120.
4. **Check.** Clients stop, work in flight drains for 20 seconds, a last read
   takes each shard and cache key, and the checkers below run on the clients'
   history and the brokers' state.

A crash drops a broker's process state and reopens its data directory, so it
also tests recovery. Most crashes are process crashes: the kernel keeps what
the broker wrote, so nothing unsynced is lost. A power cut, in the runs that
have one, is the exception (see [Power loss](#power-loss)).

The seed draws each crash as one of two kinds, half and half. A clean crash
records the stop as clean in the broker's run state, so the restart keeps its
storage id. A kill leaves the run state as a kill after a failed fsync does
(`run_state::forget_boot`): the restart cannot trust what it acknowledged, takes
a new storage id and starts with `UncleanStart`, so it leads no shard from a
copy it led before until a leader has shipped it. The choice comes from its own
random stream, so a seed keeps the same faults at the same times; only what a
crash leaves behind changed, which changes the trace after the first kill.

### Power loss

With power loss on (`SimConfig::with_power_loss`, or `FELIX_SIM_POWER_LOSS=1`
for the seed tests), the brokers run with `FsyncMode::OnCommit`, so a record
is synced before it is acknowledged, and each crash that would be a kill cuts
the power instead. It is off by default, and turning it on keeps every fault
of a seed at the same time. A kill there would mean nothing: the run state of
an `OnCommit` broker never says it acknowledged unsynced records.

A cut at a random instant would almost never find an append in flight, because
a simulated sync takes no time. So the cut is armed and lands inside the next
append to one of the broker's log segments, or after two virtual seconds with
no append in flight. Earlier batches in that segment may still be unsynced, as
under group commit, and the segment may be new, with its header unsynced. In
the same instant:

1. The device keeps what a power loss would leave
   ([The filesystem](#the-filesystem)), and the append in flight never comes
   back whole. Its size reached the disk in full or only in part, but at
   least one byte past where it began, and each 512 B sector of its data
   arrived or did not, at least one not when the size is whole. A missing
   sector can sit before one that arrived, which is where recovery has to
   tell a torn tail from corruption inside the log.
2. The broker stops. Clients waiting on it get no answer, and peers lose it.
   Anything it writes after the cut is lost.
3. The data directory becomes what the device held, and the broker restarts
   on it later, as after any crash.

The run state still says the broker was running, so the restart reads the
stop as a crash. Under `OnCommit` it still keeps its storage id and does not
run `UncleanStart`, because nothing it acknowledged was unsynced. Its
copy stays trusted, so the power loss does not count towards the shards
`ElectionSettles` exempts. If a restart after a power loss did run
`UncleanStart`, the copy would attest untrusted like a killed one.

Recovery must then repair the torn append. The storage records each segment
whose torn tail it repairs, or that it removes as unfinished, under a
simulated directory. The `TornTailRepaired` checker fails the seed when the
segment a cut tore is opened again but not among them on that broker before
its next power loss or the end of the run. Recovery runs when a log is opened,
so a segment never opened again (the broker stayed down, or no longer hosts
that shard) is not held to a repair. A segment that came back without its name, or
with nothing past where the append began, has nothing to repair and is not
held to it.

Clients in these seeds pad each record to between 1 and 4 KiB, so an append
spans several sectors. A read checks the padding, so a record with damaged
bytes shows up in the history as one nobody wrote.
Under a simulator every sync runs on the run's own runtime, so what the
device holds at the cut does not depend on thread timing and a seed replays
exactly. The faults list says, for each cut, whether it landed in an
append.

### Placement faults

Most liveness and safety bugs in replication have needed a shard's set or
leader to change while something else went wrong: a seat replaced while the
leader died, or a broker named leader that never opened. The faults above
change sets only as a side effect of failover. With placement faults on
(`SimConfig::with_placement_faults`, or `FELIX_SIM_PLACEMENT=1` for the seed
tests), the nemesis also acts as an operator. It calls the control plane's
operator API, the one `felixctl` uses, and never writes an assignment or a
node record itself, so placement runs its real move, drain, replacement and
abandon code:

| Fault | What the operator does |
| --- | --- |
| `Move` | `POST /v1/shard-moves`: moves a shard's leadership to another broker. Three in ten are cancelled (`DELETE /v1/shard-moves/...`) a few seconds later. |
| `Drain` | `POST /v1/nodes/{id}/drain`, then `PATCH /v1/nodes/{id}` back to `live`. Leaderships move away. With four or more brokers the follower seats the broker holds are replaced: a newcomer copies in and a pending set swaps it for the drained broker. |
| `Remove` | Drains a broker, stops it, deregisters it and retries `DELETE /v1/nodes/{id}` each second. The delete is refused while any shard still names the broker, which with a hundred shards to reseat is usually the whole window. The broker comes back later and registers again. |
| `Abandon` | The procedure in `docs/replication-design.md`: stops the broker assigned the stream shards the broker holds after unclean kills, sends `POST /v1/placement/abandon/...` for each (retried each second for up to 30 s while the control plane answers `not_stranded`), then starts the broker again. While that broker is up, `not_stranded` is the expected answer. A cache shard is left alone. |
| `KillBeforeOpen` | Watches for the next new leader of any shard and stops it before it opens the shard. A new leader is one placement names, seen in the store's change feed, or one that won an election and routes the shard to itself above every generation the control plane wrote. |
| `SeatRace` | Drains a broker, waits for a shard whose seat on it is being replaced, stops that shard's leader, then stops the next leader named or elected for it before it opens. That is seat replacement plus two promotions, the shape behind #1922 and #1929. Drawn only with four or more brokers. |

Placement faults have their own random stream, so turning them on adds steps to
a seed's schedule and keeps every step it had. They are off by default, so the
seeds pinned to an issue keep the run they pin. With them on, placement may
copy four shards at once instead of one, so a drain moves enough shards during
the window for two promotions to land on a shard whose seat is being replaced.
The stops follow the crash rule: never more than a minority down. Each fault
logs every request and its answer, refusals included, in the fault timeline.

The shards `ElectionSettles` exempts are the ones the broker holds on
purpose. A promoted leader needs its trusted copies to meet every majority of
the set: a copy holds everything its seat acknowledged only if its broker
started clean, or a leader has shipped it level since an unclean restart. A
learner waiting for readmission is never trusted. So the check reads, for each
shard nobody serves, what every broker attests of its copy, as it would answer
a fence. The shard is exempt when the untrusted copies are a majority of the
set, the leader and a joining follower included.

The attestation alone could hide a bug: a broker that never vouches a copy it
could have vouched would make its own exemption. So an untrusted copy counts
only when no leader had the chance to vouch it:

- its broker is down after an unclean kill, so its next start distrusts the
  copy whatever came before, or
- its broker is up, and no broker served the shard while it was up since its
  last unclean start.

The sim notes every second, and before each fault, which brokers are up while
some broker serves each shard. A copy still untrusted after a broker served
beside it is vouchable. Its shard is not exempt and fails the check, marked
`vouchable` in the stuck list, and the run prints those shards on their own
line. A broker down after a clean stop, or one with no copy, counts as
trusted, so the rule errs towards exempting less. Reading the attestations
rather than kill timing covers every order: a follower killed after the leader
in the same term, or one restarted and still a learner when the leader died.

A shard with a majority of trusted copies and no leader is not exempt. An
elected leader that names a follower with an empty storage id, or a joint
fence that cannot reach the old set's majority, still fails the check. An
abandoned log ends the hold until a member is stopped uncleanly again.

An abandoned shard loses what it acknowledged by design. Appends
acknowledged before the abandon count as unknown (they may or may not be on
the new log), and reads started before it are dropped. An append still being
retried at the abandon can only be acknowledged by the new leader, since the
old one is stopped first, so it is held to the new log. Everything started after the abandon
is checked as usual, so the new log losing an acknowledged write still fails
the run. The run names abandoned shards in a warning.

`tests/seeds.rs` pins one seed per fault (`seed_*_replays_the_same_twice`).
Each test checks that its seed reaches the fault and runs the same twice; it
does not judge what the checkers found.

A pause is approximate. The simulator cannot stop one broker's tasks on a
runtime they share, so a frozen broker is cut off from every peer and from the
control plane, as a partition from everyone, but keeps running. Its peers see
what a paused process looks like. What the simulator does not reproduce is the
paused broker waking up with stale timers and acting on a lease that ran out
while it slept.

Clients publish through the same function the broker's publish handler uses
(`serving::direct_publish`): resolve the shard, enter the shard's fence, append,
and wait for a quorum. They skip only the QUIC connection. Cache puts and gets
go through the broker's cache path in the same way. Every operation is recorded
with its start time, its end time and its result: acknowledged, refused (it
was certainly not written) or unknown (it may or may not have been written,
for example when the quorum wait timed out).

## Checkers

A seed fails when any checker reports a problem, except a warning. History
checkers look only at what clients saw; cluster checkers look at the brokers.
Each cluster checker is named after the invariant in the formal model
(`docs/formal/FelixShard.tla`) that it tests on the real code.

A cluster checker runs at up to three kinds of point:

- **Quiet**: about once a virtual second from the start of the run. A broker
  that is still starting has no view of its shards yet and is left out of the
  sample; the others are read, so the control plane pausing while a broker
  restarts is still seen. This is the only way to see a state that lasts a few
  seconds, such as two leaders, so it samples during the faults. It sees the
  history recorded so far.
- **Healed**: right after the nemesis heals everything. A checker may wait for
  the cluster, as `ElectionSettles` does. It sees the history recorded so far.
- **End**: after the final reads, with the complete history.

| Checker | Runs | Fails when |
| --- | --- | --- |
| Streams (history rules 1-19) | End | A read is not a prefix of one order of the stream, an acknowledged record is lost, a record shows up twice or was never written, or reads and writes form a cycle (G0, G1c, G-single, G2). See [history-checker.md](history-checker.md). |
| Cache (history rule 20) | End | A cache key's puts and gets cannot be put in one order that respects real time, as one variable would. Unknown puts may or may not have happened. A key too tangled to decide within the search budget is reported as undecided and fails the seed, with the key and the number of steps tried. |
| `ElectionSettles` | Healed | Some shard is not served by exactly one broker 60 virtual seconds after the faults heal. A shard the broker holds on purpose is left out: nobody serves it and a majority of the set are copies attested untrusted that no leader could have vouched (see Placement faults above). A shard with a copy still untrusted after a broker served beside it is reported and marked `vouchable`. The broker waits for `felixctl placement abandon`. The run prints how many shards that is. The report prints a census for each stuck shard: what every broker and the control plane think of it. |
| `OneLeaderPerGeneration` | every second, Healed, End | Two brokers had the same shard open for writes in the same generation, at any time in the run. A broker that restarts and opens a generation again is one leader. |
| `AtMostOneServing` | every second, Healed, End | Two brokers would take a write for one shard at the same instant. The brokers are read in one step, so none moves between two reads. It covers shards protected by the lease only. The model asserts it for those, and for a shard acknowledged by a majority of followers it asserts `AckedHeldByLeader` instead: a deposed leader may keep serving there until it learns of the newer generation, because it cannot collect a majority once its successor has fenced one. Each broker says which kind a shard is from its own write gate, so a shard opened before election was finalized stays checked. Once election is finalized that is every stream shard. A run does not finalize `fenced_caches`, so its cache shards stay on the lease and are the ones checked. |
| `AckedHeldByLeader` | every second, Healed, End | A broker serving a shard does not hold, at the offset the client was told, an append that was acknowledged before the sample. A broker at the newest generation any broker knows must hold all of them. A deposed one still serving is held only to the offsets below its quorum mark at its own generation. Past that mark its log may hold records no majority took, at offsets where its successor has since acknowledged others. Below it, a hole or a different record is a lost acknowledgement. After the heal, a shard served by nobody at the newest generation is looked at again until the settle deadline, and at the end its server is held to every acknowledged append. The record is read from the broker's log whether or not a quorum has committed it. A shard with no server is left to `ElectionSettles`. |
| `AdoptedMatchesOpened` | every second, Healed, End | A broker that serves a shard at generation `g` does not route the shard to itself at `g`, or its lifecycle did not open `g`. The model's version of this also compares the replica set with the control plane's; the simulator does not read the set. |
| `TornTailRepaired` | End | A power cut landed inside an append, the broker opened that segment again (recovery runs on open), and recovery did not repair its torn tail before the broker's next power loss or the end of the run. A segment never opened again, because the broker stayed down or no longer hosts that shard, is not held to a repair. |
| `GenerationBurn` | End | A warning, never a failure. A shard went through more generations than `2 * faults + 2`, counting the faults in the plan that could have cost its leader: a crash, freeze or isolation of a broker that has led it, a split with a former leader in the minority, a dropped link to one, a paused or downed control plane, or the pause-then-crash fault. Many elections for few faults often means candidates deposing each other. |

A violation that lasts is reported once, with a census of the shard. After three
of one kind the rest are only counted.

### Writing a checker

A checker implements the `Checker` trait in `crates/testing/felix-sim/src/check.rs`:
a `name` and an async `check`, which receives a `CheckContext` with the
cluster, the `Point` it is called at, when the faults healed, the history and
the plan of faults. It returns findings. Return at once for points it has
nothing to say at.

```rust
struct NoTwoLeaders;

#[async_trait::async_trait]
impl Checker for NoTwoLeaders {
    fn name(&self) -> &'static str {
        "no-two-leaders"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        let mut findings = Vec::new();
        for key in ctx.cluster.shard_keys() {
            if ctx.cluster.servers_of(&key).await.len() > 1 {
                findings.push(Finding::new(self.name(), "two leaders").with(ctx.cluster.census(&key).await));
            }
        }
        findings
    }
}
```

The cluster exposes what each broker believes through `SimCluster::stands`
(one instant, nothing awaited) and `snapshot` (also the log tail), and the
`census` prints it for a failure. To run the checker, add it to the list that
`run_seed_with` takes, or to `default_checkers`. A checker keeps its own state
between calls, which is how `OneLeaderPerGeneration` remembers a leader that
crashed. Put the decision in a function of plain values and test it with a
hand-built observation that breaks the rule and one that keeps it, as the
tests under `check/` do; running a cluster in a unit test takes too long.

Do not weaken a checker to make a seed pass. File the seed as an issue and pin
it with `#[ignore = "#<issue>"]` in `tests/seeds.rs`.

### Known failing seeds

The bugs the earlier seeds found (#1941, #1943, #1948, #1949, #1955) are
fixed, and the tests that pin them in `tests/seeds.rs` pass. Those pins check
the one run their seed gives, so they run only with `--cfg tokio_unstable`, in
`task sim:determinism`. The default batch of `seeds` (seeds 1 to 4) still hits
liveness stalls (#2024, #2032, #2033), so it is ignored in the workspace tests
and runs with `cargo test -p felix-sim --test seeds -- --ignored --exact seeds`.
It runs in its own CI job on every pull request, which does not block merging
until those are fixed. A seed whose bug is still open is pinned with
`#[ignore = "#<issue>"]`; none is pinned today.

Seeds 1 to 20, built with the flag, as of the change that made replay exact:

| Seeds | What fails | Issue |
| --- | --- | --- |
| 1, 3, 4, 5, 6, 13, 14, 19 | Nothing: they pass. | |
| 7, 8, 10, 12, 15, 16, 17, 18, 20 | After the heal some shards are served by nobody (`ElectionSettles`), so they cannot be read (`final-reads`). The census shows the control plane still naming the old leader while the brokers route to an elected one. | not filed yet |
| 2, 11 | A shard cannot be read to its tail after the heal (`final-reads`). | not filed yet |
| 9 | The history checker finds a read that misses an append acknowledged before it began (rule 16, G-single-realtime). | not filed yet |

With power loss on, seed 3 cuts the power during an append, and
`seed_3_power_cuts_run_the_same_twice_and_are_repaired` checks that it replays
the same and that recovery repairs the torn tail.

Seeds 1 to 8 with placement faults on, built with the flag, as of the change
that added them. None breaks a history rule or a cluster check other than
`ElectionSettles`:

| Seeds | What fails | Issue |
| --- | --- | --- |
| 8 | Nothing: it passes. | |
| 1, 2, 5, 6, 7 | A few shards (3 to 7) are served by nobody after the heal (`ElectionSettles`, `final-reads`). Seed 6 passes without placement faults. | not filed yet |
| 3, 4 | Most shards are served by nobody after the heal: all 178 on seed 3, where the control plane keeps a failed leader with a move to `b2` that never ends, and 103 on seed 4. | not filed yet |

Seed 33, which `seed_33_abandoning_held_shards_replays_the_same_twice` pins,
stops `b3` at 58 s, the broker the control plane has promoted for the two
held shards `stream/12` and `stream/35`. The control plane refuses the abandon
(`not_stranded`) at first and accepts it once `b3` has been down long
enough; both are abandoned at 78 s. The seed still fails `ElectionSettles`:
about 48 of 156 shards are served by nobody after the heal, with the control
plane naming a leader in `Assigning` that never opens.

Any change to the code a run executes can move a seed from one row to
another, because the run takes a different path from the first difference
on. Rerun the seeds rather than trusting this table.

## The nightly sweep

`.github/workflows/dst-nightly.yml` runs 2000 seeds every night at 01:13
Pacific, the same seeds each night, so a red night replays as it is. It builds
the seeds test once, in release with `--cfg tokio_unstable` as `task sim:replay`
does, and hands it to shards that each run a range of seeds two at a time
through `scripts/sim-sweep.sh`.

The opt-in nemeses run too. Each night also runs 250 seeds with each of clock
skew (`bounded`), placement faults and power loss, a window that moves 250
seeds a night, so eight nights cover seeds 1 to 2000 in every mode.

A failing seed is sorted by its log:

| Kind | What it is | Effect |
| --- | --- | --- |
| Real bug | A history violation, an undecided cache key, a safety finding (`at-most-one-serving`, `one-leader-per-generation`, `acked-held-by-leader`, ...) or a crash. | Fails the shard. A scheduled run opens an issue labelled `dst-nightly`, which a green night closes. |
| Tracked stall | Only the liveness findings `stalled`, `election-settles` and `final-reads`, listed above under known failing seeds. | Listed apart in the summary. Fails nothing. |

Each line in the run summary gives the seed, its categories and the exact
command that replays it, for example
`FELIX_SIM_PLACEMENT=1 FELIX_SIM_SEED=412 task sim:replay`. A real bug runs
once more to say whether it repeats; a stall does not, because the run is
exact and a stall takes minutes. The logs of failing seeds are in the
`sim-sweep-<mode>-<first seed>` artifacts.

For the release gate, start the workflow by hand (Actions, "Simulation sweep
(nightly)", Run workflow). The inputs are:

| Input | Meaning | Default |
| --- | --- | --- |
| `first` | The first seed. | 1 |
| `seeds` | How many seeds per mode. | 2000 |
| `shards` | Jobs per mode, lowered to `seeds` when larger. | 50 |
| `modes` | `off`, `clocks`, `placement`, `power-loss` (comma separated), or `all`. | `off` |
| `parallel` | Jobs running at once, 1 to 20. | 4 |

`modes` `all` with 2000 seeds is 200 jobs (the limit is 256), and a job takes at most 50 seeds. Repeated modes count once. Dispatched runs queue behind each other. The gate is no
real bugs in any mode; tracked stalls are expected until their issues are fixed.

Timing: a seed that passes takes a few seconds to a minute in a release build,
and one that stalls waits out the watchdog, up to eight minutes. A shard of
40 seeds (a nightly default-mode shard) or 50 (an opt-in one) therefore takes
minutes when the seeds pass, and at worst 160 or 200 minutes if every seed
stalls. Each job has a 240 minute limit, under the runners' 6 hours. These
figures come from the numbers above, not from a measured run; the first
dispatched run replaces them.

## What runs for real and what is simulated

The code under test is the real replication, election, promotion and routing
code, and the control plane's real API and placement code. Only the edges are
swapped out, each at a seam that already existed or that costs a release build
nothing:

| What | Real broker | Under simulation |
| --- | --- | --- |
| Peer network | `PeerPool`, QUIC with mTLS | `SimNet`, in memory |
| Timers and deadlines | tokio time | tokio time, paused and advanced by the test |
| Lease and wall clocks | `felix_common::clock` | the same functions, reading each node's own clock over virtual time |
| Data directory | files on disk | `felix_storage::sim_fs`, in memory |
| Control plane | its own process, over HTTP | its real router, called in-process through `ControlPlaneLink`, with faults |
| Clients | QUIC connections | direct calls into the same publish and cache functions the handlers use |

### The filesystem

Each simulated broker's data directory lives in memory. Storage reaches the
disk through one module (`crates/server/felix-storage/src/fs.rs`), and with
felix-storage's `sim` feature a path under a directory the simulator
registered goes to an in-memory tree instead of the OS. Every other path stays
on disk, so tests that share a build with the simulator still use real files.
A release broker is built without the feature and calls `std::fs` directly.

Nothing in memory costs real time or holds a file descriptor, so starting 200
shards and restarting them after a crash no longer waits on the disk.

The tree keeps two copies of everything, because that is what a power loss
needs:

- **Files** hold their current bytes (the page cache) and their durable bytes,
  as of the last `sync_data` or `sync_all` of that file. Preallocation
  reserves space and leaves the length alone, as `fallocate` does.
- **Directories** hold their current entries and their durable entries, as of
  the last flush of that directory. A create, rename or unlink is only
  durable once its parent is synced, so the missing `sync_dir` after a rename
  is something the model can catch.

`felix_storage::sim_fs::crash(root, seed)` replaces the tree with what a power
loss at that moment would leave. It follows the rules of the Linux power-loss
observer used by the storage tests (`io/power_loss.rs`):

- Each file keeps its durable bytes. Each unsynced 4 KiB page then arrives
  whole, arrives torn on a 512 B sector, or does not arrive.
- A length change can reach the disk without the data it covers, which then
  reads back as zeros.
- A directory change that was not synced may or may not stick, per name.
- A rename is one change: its two names, even in different directories, come
  back together, either as before the rename or as after it, and unsynced
  renames are kept oldest first. Syncing either directory commits it, along
  with the directories it moved into, as a filesystem journal does. A file
  replaced by a rename comes back with the bytes of its last flush. A create
  made before a rename that is lost can still stay (a temporary file next to
  the old file), and a create or unlink of a name after a kept rename gets its
  own choice. A directory removed or replaced after a rename is not brought
  back when the rename commits. A file synced through a handle after its
  unlink commits the unlink, so the name does not come back. One whose unlink
  is lost comes back with the bytes of its last flush, without any writeback
  that happened before the unlink. The Linux observer pairs renames within one
  directory only.
- Renames are kept in order, but creates and unlinks of different names are
  not ordered against each other. POSIX allows that. ext4 and xfs journal all
  metadata in order, so the model can leave states those filesystems would not.
- A tree where everything was synced comes back unchanged.

`sim_fs::arm_power_cut(root, seed, on_cut)` is the version the nemesis uses.
It waits for the next append to a log segment, tears that append as described
under [Power loss](#power-loss), and fixes what the device holds at that
moment. `on_cut` runs inside the write, to stop the broker, and is told which
segment it tore. `sim_fs::reboot(root)` then puts the device's contents in
place. `sim_fs::cut_power` cuts at once instead, and
`sim_fs::torn_tails_repaired` lists the segments recovery repaired.

The same seed gives the same result: listings are in name order and the model
uses no clock and no OS randomness. Handles opened before a crash are cut off
from the new tree, as they would be from a rebooted disk.

What it leaves out:

- Only files, directories, and the flushes above are modelled. There are no
  permissions, timestamps, links, symlinks or file locks.
- A file on disk and a file in memory cannot be renamed into each other.
- Flushes are exact. Real storage can also complete a write that was in
  flight when the flush started; the observer's "adversarial capture" has no
  counterpart here because a simulated flush is instant.
- The machine does not reboot. The run state records the boot a run started
  in, and a restart after a real power loss sees a new one. The simulator
  cannot fake that, so it cuts the power only under `OnCommit`, where the boot
  does not matter.

### The peer network

Replication, suspicion and election already reach peers only through the
`PeerRequester` trait, and answer them through `PeerRequestHandler`.
`spawn_shard_tasks` in `services/felix-broker-service/src/node/cluster.rs` is
generic over the requester, so the simulator starts the same shard tasks a
broker does. A broker passes its `PeerPool` and the compiler builds the same
code it always did: nothing on the publish or peer path goes through a trait
object.

`SimNet` gives each running node a `SimPeer`, its `PeerRequester`. A request
runs on the target's handler in a task of its own, so it finishes even if the
caller gives up, which is how a real broker behaves. It keeps these parts of
the pool and the listener:

- Each message goes through the peer codec in both directions.
- The caller waits as long as the pool would (`peer::pool::request_timeout`).
- A request the target never offered the capability for fails with
  `Unsupported` and is not sent (`peer::pool::required_capability`).
- The handler is told who asked only when the target keeps ballots.
- A ping is answered by the network, not the handler, as the listener does.
- A learner's ack reads as a plain ack to a caller that did not offer
  `LEARNER_ACKS` (`peer::server::for_caller`).

It does not model the listener's limits. A real listener refuses with
`Overload` past its per-stream permits, and answers an older peer's requests
on one stream one at a time. Under simulation every request runs at once.

Faults are set per directed link:

| Fault | What the caller sees |
| --- | --- |
| `partitioned` | `Unavailable` at once, like the pool's partition file. A request or answer crossing when it lands is lost, and the caller times out. |
| `drop` (a probability) | A lost request or a lost answer. Either way the caller times out; with a lost answer the handler has already run. |
| `delay` | Each message takes that long to cross. Past the timeout the caller gives up, and the request still lands. |

Drops come from the seed passed to `SimNet::new`, so a seed replays the same
losses.

Each `SimNet::start` is a new incarnation of a node. A crashed node is
unreachable, and its old `SimPeer` is refused with `ShuttingDown` even after the
node restarts. A crash leaves some short tasks running (a fence in flight, a
confirmed read); this is what keeps them from reaching the live cluster. A
request to a node that crashes before answering fails with `Disconnected` at
the moment of the crash, and the handler stops. A request sent to the old
incarnation never reaches the new one, as a connection does not outlive its
process.

### The control plane

Brokers reach the control plane only through the `ControlPlaneLink` trait,
confirmed assignment reads included, so a learner is backed in the simulator
as in a broker.
The simulator's link builds the same request a broker would send over HTTP and
hands it to the control plane's real router in the same process, with an
in-memory store. The control plane's background loops (placement and the sweep
that marks silent brokers down) are not spawned; the simulator runs one round
of each on their real cadence. Placement keeps what it has seen between
rounds (when a fence started, how long a suspicion or a pending replica set
has stood, whether a restore moves), as the real loop does, so timeouts that
depend on it fire. A control plane that goes down forgets it, as a restarted
process does; a pause keeps it. A fault applies to every call:

| Fault | What a broker sees |
| --- | --- |
| slow | Every answer arrives late by a set time. |
| paused | No answer, until the pause ends; then the waiting calls are answered late. Placement and the sweep do not run. |
| down | Every call fails at once. Placement and the sweep do not run. |

### Time

The deadlines, backoffs and stickiness windows the simulator runs read
`tokio::time::Instant`, so pausing tokio's clock pauses them. A few readings
in `felix-replication` still use the host clock, all on paths that decide
nothing, such as the shipping log's rate limiter. With a
paused runtime, tokio jumps the clock forward whenever every task is waiting,
so a test that waits out minutes of timeouts takes far less real time.

Leases are measured on `felix_common::clock::boottime`, and the control plane
stamps heartbeats with `felix_common::clock::wall_millis`. Neither is tokio's
clock, so the simulator installs its own with `felix_common::clock::sim::install`.
From then on both read the simulation's elapsed time, with wall time counted
from a fixed date so every run sees the same timestamps. The hook exists in
debug builds and under the `felix-common/sim` feature. A plain release build
does not have it.

### Clock skew

Each broker and the control plane has its own clocks over the shared virtual
time (`crates/testing/felix-sim/src/clock.rs`):

- a **lease clock** (what `boottime` reads), which runs at its own rate. It is
  monotonic, as `CLOCK_BOOTTIME` is: a rate change keeps the drift already
  built up, and it never steps.
- a **wall clock** (what `wall_millis` and `system_time` read), which is the
  shared time plus an offset. A step moves the offset either way, as an NTP
  correction does. It does not drift with the lease clock.

So every read through `felix_common::clock` is a per-node read: the lease and
its fence checks, the control plane's heartbeat stamps and expiry, record and
retention times, cache TTLs, lease-claim times and the broker's token-expiry
checks. In the simulator the control plane's silence watch (the second check
before a broker is marked down) also reads the control plane's lease clock.
A real control plane times it on tokio's clock, because the cluster harness's
clock faults step the lease clock and the watch is what keeps a stepped clock
from expiring a live broker. So under skew a slow control plane waits longer
to mark a silent broker down, and a fast one is still held back by the
heartbeat stamps, which are on its wall clock.

Timers and deadlines on tokio's clock are not skewed, as in the cluster
harness. So the simulator cannot catch code that waits out a lease with a
tokio sleep and acts without checking the lease clock afterwards: that sleep
lasts the same everywhere. A validity decision has to read
`felix_common::clock` to be tested here.

`SimConfig::clock_plan` pins a skew instead of drawing one.
`seed_8_slow_brokers_and_a_fast_control_plane_stay_safe` in `tests/seeds.rs`
runs every broker's lease clock at 0.8x and the control plane's at 1.2x, the
worst pair the bounded mode allows, and checks only the safety findings.

`SimConfig::clocks` (or `FELIX_SIM_CLOCKS`) picks how far:

| Mode | Lease clock rate | Wall clock offset | Why that bound |
| --- | --- | --- | --- |
| `off` (default) | 1x | 0 | |
| `bounded` | 0.8x to 1.2x | within 1 s | Lease safety assumes a bound on drift rate, and its margins are checked against clocks that drift by a quarter ([replication-design.md](replication-design.md#the-clock-assumption-stated-precisely)). Lease-claim tokens allow 2 s of skew between two nodes ([auth.md](auth.md)). |
| `unbounded` | 0.25x to 0.7x, or 1.3x to 4x | up to 30 s | Past the rate bound, with wall clocks drawn from a much wider range, to show what breaks there. |

A run draws each node's starting rate and offset from the seed, then from 2 to
10 seconds in, and every 3 to 12 seconds after that until the fault window
ends, picks a node and either steps its wall clock
to a new offset or changes its lease clock's rate, within the same bounds. Every
change is in the trace (`clock: b2 wall clock steps to -420 ms`). The
schedule has its own stream of the seed, so turning skew on moves no other
fault. A `bounded` seed that breaks the history checker or a safety check
(`AtMostOneServing`, `AckedHeldByLeader`) is a bug in Felix; an `unbounded`
one is expected to. Liveness checks such as `ElectionSettles` also fail on
some seeds with skew off, so compare against an `off` run of the same seed
before blaming the clocks.

The wall-clock bound matters only where two nodes compare wall times. The
sim's clients claim no leases and set no cache TTLs yet, so today wall skew
reaches the control plane's heartbeat stamps (stamped and judged on its own
clock), record and retention times, and the broker's token-expiry check. No
cross-node wall-time comparison is exercised yet.

All nodes run in one process, so a clock read has to know whose code is
asking. The harness marks the code it runs for a node: a broker's startup, a
client's call into a broker, a peer message handled by its target, and every
control plane request. Under `--cfg tokio_unstable` the runtime's task hooks
carry that mark to each task the code spawns, and to the tasks those spawn.
That is why skew needs the `tokio_unstable` build, and a run with skew in a
plain build panics with the command to use instead. CI's non-blocking `sim-seeds`
job builds that way and runs half its seeds with `bounded` skew. Clients and checkers
belong to no node and read the shared time. The control plane mints and
checks tokens on the host clock (`jsonwebtoken` validates expiry against it),
so token expiry on the control plane is not skewed; the sim's tokens last ten
years.

