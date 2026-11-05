# Deterministic simulation

The cluster harness ([cluster-harness.md](cluster-harness.md)) runs real broker
processes over real QUIC. That finds bugs, but slowly: a failover takes
seconds of wall time, and a fault that needs a partition to land between two
particular messages may take hundreds of runs to hit. The simulator runs
several brokers in one test process instead, on tokio's virtual time, with
the network and the clocks under the test's control. A run is driven by a seed,
and a failing seed can be replayed.

It lives in `crates/testing/felix-sim`. So far that crate holds the simulated
network; the harness that starts brokers and a control plane on it, the fault
schedule, and the checkers come next.

## What runs for real and what is simulated

The code under test is the real replication, election, promotion and routing
code. Only three things are swapped out, each at a seam that already existed or
that costs a release build nothing:

| What | Real broker | Under simulation |
| --- | --- | --- |
| Peer network | `PeerPool`, QUIC with mTLS | `SimNet`, in memory |
| Timers and deadlines | tokio time | tokio time, paused and advanced by the test |
| Lease and wall clocks | `felix_common::clock` | the same functions, reading virtual time |

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
caller gives up, which is how a real broker behaves. It keeps the parts of the
pool that matter to correctness:

- Each message goes through the peer codec in both directions.
- The caller waits as long as the pool would (`peer::pool::request_timeout`).
- A request the target never offered the capability for fails with
  `Unsupported` and is not sent (`peer::pool::required_capability`).
- The handler is told who asked only when the target keeps ballots.

Faults are set per directed link:

| Fault | What the caller sees |
| --- | --- |
| `partitioned` | `Unavailable` at once, like the pool's partition file. An answer already on its way back is lost. |
| `drop` (a probability) | A lost request or a lost answer. Either way the caller times out; with a lost answer the handler has already run. |
| `delay` | Each message takes that long to cross. Past the timeout the caller gives up, and the request still lands. |

Drops come from the seed passed to `SimNet::new`, so a seed replays the same
losses.

Each `SimNet::start` is a new incarnation of a node. A crashed node is
unreachable, and its old `SimPeer` is refused with `ShuttingDown` even after the
node restarts. A crash leaves some short tasks running (a fence in flight, a
confirmed read); this is what keeps them from reaching the live cluster. A
handler that was answering when its node died loses its answer, and the caller
sees `Disconnected`.

### Time

Every deadline, backoff and stickiness window in `felix-replication` reads
`tokio::time::Instant`, so pausing tokio's clock pauses them all. With a
paused runtime, tokio jumps the clock forward whenever every task is waiting,
so a test that waits out minutes of timeouts takes far less real time.

Leases are measured on `felix_common::clock::boottime`, and the control plane
stamps heartbeats with `felix_common::clock::wall_millis`. Neither is tokio's
clock, so the simulator installs its own with `felix_common::clock::sim::install`.
From then on both read the simulation's elapsed time, with wall time counted
from a fixed date so every run sees the same timestamps. The hook exists in
debug builds and under the `felix-common/sim` feature. A plain release build
does not have it.

## Running it

```bash
cargo test -p felix-sim
```
