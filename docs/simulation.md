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
clients saw and what the brokers ended up holding. Two virtual minutes take
minutes of real time, not seconds (see [Running it](#running-it)).

## Running it

Run a batch of seeds:

```bash
cargo test -p felix-sim --test seeds seeds
```

`FELIX_SIM_SEEDS` sets how many seeds (default 4) and `FELIX_SIM_SEED_START`
the first one. Each seed is 120 seconds of virtual time and takes three to five
minutes of real time with faults. The clock is paused, so waiting costs
nothing, but real work does not: starting a cluster opens a durable log for
each of 50 to 200 shards on every broker, and every crash restart opens them
again. That disk work, and the broker tasks that run between timers, take real
time that no virtual clock skips. Run seeds one at a time on a busy machine.

A run that makes no progress ends instead of hanging. A thread of its own
watches the virtual clock, and if it has not moved for four minutes of real
time, or the whole run has used `real_time_limit` (eight minutes), the run
stops and reports a `stalled` finding. That means some broker task never
waits on a timer, which holds the paused clock still; the finding says so and
prints no census, since the busy task would hold that up too.

When a seed fails, the test prints the command that replays it:

```bash
FELIX_SIM_SEED=<n> cargo test -p felix-sim --test seeds replay
```

A replay runs the same cluster, the same faults at the same virtual times and
the same client operations. For the interleaving beneath them to repeat too,
build with tokio's seeded `select!`:

```bash
RUSTFLAGS="--cfg tokio_unstable" FELIX_SIM_SEED=<n> FELIX_SIM_HISTORY=run1.txt \
  cargo test -p felix-sim --release --test seeds replay
```

`FELIX_SIM_HISTORY` writes the run's history (every operation, fault and final
read) to a file, so two runs can be diffed.

Several things the brokers do would otherwise make two runs differ, and under
the simulator each is held to the run's one thread and its seed. Only some of
this is gated. The seeded randomness (`felix_common::random`), the retry
jitter, the blocking-work runner and the clock hook exist only in debug builds
or under the `felix-common/sim` feature; a plain release build does not have
them. Three changes run in every build: passes over routing tables and
assignment sets walk in key order, the election walk starts at a per-broker
offset (a hash of the node id) so followers do not all stand on the same
shards first, and the deadlines and backoffs read `tokio::time::Instant`
instead of `std::time::Instant`.

- **Blocking work.** Shard opens, reads, rollovers and the log's append and
  flush threads run on real threads in a broker, and finish in real time.
  `felix_common::blocking` runs that work as a task on the simulated runtime
  instead, once the simulator has installed its clock. Real flushes are
  skipped too: the simulator never cuts the power, so they would only cost
  real time.
- **Retry jitter.** Election backoff, the peer redial backoff and the
  membership report delay draw from `felix_common::jitter`, which the
  simulator seeds.
- **Hash keys and ids.** The broker's maps, its producer ids and the
  replication confirmed-read slots use `felix_common::random::hash_state`,
  and storage ids, node ids and lease ids use `felix_common::random::uuid_v4`.
  Both draw from the seeded generator, so map layouts and ids repeat.
- **Wall time.** Expiry stamps and deadlines read `felix_common::clock::system_time`,
  which is virtual time counted from the fixed simulated epoch.
- **Real-time deadlines.** The peer pool's backoff, the forward budget, the
  routing hold and the shard lifecycle's fence timers read tokio's clock, so
  they follow the paused clock instead of the host's.
- **Walk order.** A pass over a routing table, an assignment set or the shards
  a broker stands for visits them in key order, because hash order changes
  from process to process and these passes start work shard by shard.
- **`select!`.** Tokio picks among ready branches at random unless the runtime
  is given a seed, which tokio allows only under `--cfg tokio_unstable`. Without
  that flag a replay still has the same faults and operations, but a
  timing-sensitive failure may need a few replays to show again.

Replay is not exact yet. Known gaps: storage reads the host clock in a few
places and does real file I/O, and the control plane draws a few ids from the
OS. There is at least one more gap nobody has found: seed 59 still diverges by
about 2 ms around 10.5 s of virtual time, and the cause is unknown. A replay
can differ from the original run, so a failure may need a few replays to show
again.

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
   Each broker opens a real durable log in its own temporary directory, waits
   for the stream catalog (the `seeded` gate a real broker has), registers, and
   starts its shard tasks. Once every broker has registered, the run finalizes
   replica election, so brokers elect a new leader themselves when one is lost.
2. **Faults.** For the first 90 seconds a schedule drawn from the seed crashes
   and restarts brokers, partitions them, freezes them, drops one direction of
   a link, and makes the control plane slow, paused or down. Faults may
   overlap. Eight steps in a hundred pause the control plane and, a second or
   two later, crash the broker that leads the most shards, which is the case
   election has to get right when nobody can say who leads. The step is drawn
   like any other, so about two seeds in three have it and the rest never do.
   The crash is skipped when half the brokers or more are already down, so
   some runs keep a quorum. While the faults run, the checkers marked "every
   second" below sample the cluster.
3. **Heal and settle.** Every link is healed, the control plane answers again
   and crashed brokers restart. The cluster then has 60 virtual seconds to get
   every shard served again. The clients keep running until second 120.
4. **Check.** Clients stop, work in flight drains for 20 seconds, a last read
   takes each shard and cache key, and the checkers below run on the clients'
   history and the brokers' state.

A crash drops a broker's process state and reopens its data directory, so it
also tests recovery. It is a process crash, not a power cut: the kernel keeps
what the broker wrote, so nothing unsynced is lost. The harness records the
stop as clean in the broker's run state for that reason. Without that, a
restart on a platform that cannot sync the filesystem after a crash (anything
but Linux) would give the broker a new storage id, and it would refuse to lead
shards it led before.

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
| `ElectionSettles` | Healed | Some shard is not served by exactly one broker 60 virtual seconds after the faults heal. The report prints a census for each stuck shard: what every broker and the control plane think of it. |
| `OneLeaderPerGeneration` | every second, Healed, End | Two brokers had the same shard open for writes in the same generation, at any time in the run. A broker that restarts and opens a generation again is one leader. |
| `AtMostOneServing` | every second, Healed, End | Two brokers would take a write for one shard at the same instant. The brokers are read in one step, so none moves between two reads. It covers shards protected by the lease only. The model asserts it for those, and for a shard acknowledged by a majority of followers it asserts `AckedHeldByLeader` instead: a deposed leader may keep serving there until it learns of the newer generation, because it cannot collect a majority once its successor has fenced one. Each broker says which kind a shard is from its own write gate, so a shard opened before election was finalized stays checked. Once election is finalized that is every stream shard. A run does not finalize `fenced_caches`, so its cache shards stay on the lease and are the ones checked. |
| `AckedHeldByLeader` | every second, Healed, End | A broker serving a shard does not hold, at the offset the client was told, an append that was acknowledged before the sample. A broker at the newest generation any broker knows must hold all of them. A deposed one still serving is held only to the offsets below its quorum mark at its own generation. Past that mark its log may hold records no majority took, at offsets where its successor has since acknowledged others. Below it, a hole or a different record is a lost acknowledgement. After the heal, a shard served by nobody at the newest generation is looked at again until the settle deadline, and at the end its server is held to every acknowledged append. The record is read from the broker's log whether or not a quorum has committed it. A shard with no server is left to `ElectionSettles`. |
| `AdoptedMatchesOpened` | every second, Healed, End | A broker that serves a shard at generation `g` does not route the shard to itself at `g`, or its lifecycle did not open `g`. The model's version of this also compares the replica set with the control plane's; the simulator does not read the set. |
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

These seeds fail today. Each is pinned in `tests/seeds.rs` as an ignored test
named after its issue, which passes when the bug is fixed; run them with
`cargo test -p felix-sim --test seeds -- --ignored`. The default batch
is seeds 1 to 4, so it fails until these are fixed.

| Seed | What it finds | Issue |
| --- | --- | --- |
| 1 | The replication driver spins on a retry with no delay and the virtual clock stops (a `stalled` finding). | [#1941](https://github.com/GetFelix/felix/issues/1941) |
| 1, 5 | A cache get or delete after a failover or restart misses a put that a quorum acknowledged. | [#1943](https://github.com/GetFelix/felix/issues/1943) |
| 2, 3, 4 | The same stall as seed 1, within the first minute of faults. Seeds that stall give the cluster checkers little to look at. | [#1941](https://github.com/GetFelix/felix/issues/1941) |
| 6 | The run can stall about 55 virtual seconds in, after the control plane pause and crash: the same driver spin as seed 1. | [#1941](https://github.com/GetFelix/felix/issues/1941) |
| 2, 6 | With the driver spin worked around: the control plane pauses, a shard leader crashes, and after the heal the restarted leader stays in `fencing`, so `ElectionSettles` reports the shard unserved. | [#1948](https://github.com/GetFelix/felix/issues/1948) |
| 2, 6 | With the driver spin worked around: after the heal a leader's committed mark stays below its log tail, so the shard cannot be read (`final-reads`). | [#1949](https://github.com/GetFelix/felix/issues/1949) |
| 4 | With the driver spin worked around: a broker frozen while its cache shards moved serves them again at the old generation next to the new leader (`AtMostOneServing`). | [#1955](https://github.com/GetFelix/felix/issues/1955) |

A stalled run may not stall on another replay: the brokers' interleaving
differs, and seed 2 ran to the end the first time and stalled the second.

## What runs for real and what is simulated

The code under test is the real replication, election, promotion and routing
code, and the control plane's real API and placement code. Only the edges are
swapped out, each at a seam that already existed or that costs a release build
nothing:

| What | Real broker | Under simulation |
| --- | --- | --- |
| Peer network | `PeerPool`, QUIC with mTLS | `SimNet`, in memory |
| Timers and deadlines | tokio time | tokio time, paused and advanced by the test |
| Lease and wall clocks | `felix_common::clock` | the same functions, reading virtual time |
| Control plane | its own process, over HTTP | its real router, called in-process through `ControlPlaneLink`, with faults |
| Clients | QUIC connections | direct calls into the same publish and cache functions the handlers use |

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

Brokers reach the control plane only through the `ControlPlaneLink` trait.
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
