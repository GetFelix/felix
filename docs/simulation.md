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
clients saw and what the brokers ended up holding.

## Running it

Run a batch of seeds:

```bash
cargo test -p felix-sim --test seeds seeds
```

`FELIX_SIM_SEEDS` sets how many seeds (default 4) and `FELIX_SIM_SEED_START`
the first one. Each seed is 120 seconds of virtual time, which takes a few
seconds of real time.

When a seed fails, the test prints the command that replays it:

```bash
FELIX_SIM_SEED=<n> cargo test -p felix-sim --test seeds replay
```

A replay runs the same cluster, the same faults at the same virtual times and
the same client operations. It is close to exact but not guaranteed: some of
the code under test still draws its own randomness (election backoff jitter,
hash map order) from the process, so a timing-sensitive failure may need a few
replays to show again. Seeds known to fail are kept as ignored tests named
after the issue that tracks them.

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
2. **Faults.** For about the first 90 seconds a schedule drawn from the seed
   crashes and restarts brokers, partitions them, pauses them, and makes the
   control plane slow, paused or down. One fault the schedule always tries is a
   paused control plane while a shard leader crashes.
3. **Heal and settle.** Every link is healed, the control plane answers again
   and crashed brokers restart. The cluster then has 60 virtual seconds to get
   every shard served again.
4. **Check.** The checkers below run on the clients' history and the brokers'
   state.

A crash drops a broker's process state and reopens its data directory, so it
also tests recovery. A pause is approximate: the broker keeps running but is
cut off from everyone, the control plane included, which looks the same to its
peers but lets its own timers keep firing.

Clients publish through the same function the broker's publish handler uses
(`serving::direct_publish`): resolve the shard, enter the shard's fence, append,
and wait for a quorum. They skip only the QUIC connection. Cache puts and gets
go through the broker's cache path in the same way. Every operation is recorded
with its start time, its end time and its result: acknowledged, refused (it
was certainly not written) or unknown (it may or may not have been written,
for example when the quorum wait timed out).

## Checkers

A seed fails when any checker reports a problem. History checkers look only at
what clients saw; cluster checkers look at the brokers. Each cluster checker is
named after the invariant in the formal model (`docs/formal/FelixShard.tla`)
that it tests on the real code.

| Checker | Fails when |
| --- | --- |
| Streams (history rules 1-19) | A read is not a prefix of one order of the stream, an acknowledged record is lost, a record shows up twice or was never written, or reads and writes form a cycle (G0, G1c, G-single, G2). See [history-checker.md](history-checker.md). |
| Cache (history rule 20) | A cache key's puts and gets cannot be put in one order that respects real time, as one variable would. Unknown puts may or may not have happened. A key too tangled to decide within the search budget is reported as undecided, not as a pass. |
| `ElectionSettles` | Some shard is not served by exactly one broker 60 virtual seconds after the faults heal. The report prints a census for each stuck shard: what every broker and the control plane think of it. |
| `OneLeaderPerGeneration` | Two brokers led the same shard in the same generation. |
| `AtMostOneServing` | Two brokers serve writes for one shard at the same moment, checked at quiet points between faults. |
| `AckedHeldByLeader` | The current leader's log lacks a record a client was told is on a quorum. |
| `AdoptedMatchesOpened` | A broker serves a generation of a shard that differs from the one it opened. |
| Generation burn | A shard went through more generations than the faults explain. A warning, not a failure. |

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
of each on their real cadence. A fault applies to every call:

| Fault | What a broker sees |
| --- | --- |
| slow | Every answer arrives late by a set time. |
| paused | No answer, until the pause ends; then the waiting calls are answered late. Placement and the sweep do not run. |
| down | Every call fails at once. Placement and the sweep do not run. |

### Time

The deadlines, backoffs and stickiness windows the simulator runs read
`tokio::time::Instant`, so pausing tokio's clock pauses them. A few readings
in `felix-replication` still use the host clock, all on paths `SimNet` replaces
or that decide nothing: the pool's dial backoff, the shipping log's
rate limiter, and the reconnect jitter in `peer/config.rs`. With a
paused runtime, tokio jumps the clock forward whenever every task is waiting,
so a test that waits out minutes of timeouts takes far less real time.

Leases are measured on `felix_common::clock::boottime`, and the control plane
stamps heartbeats with `felix_common::clock::wall_millis`. Neither is tokio's
clock, so the simulator installs its own with `felix_common::clock::sim::install`.
From then on both read the simulation's elapsed time, with wall time counted
from a fixed date so every run sees the same timestamps. The hook exists in
debug builds and under the `felix-common/sim` feature. A plain release build
does not have it.
