---
title: "Testing Distributed Behaviour"
description: "The cluster harness and its fault injection, the history checker, and the TLA+ models behind Felix's replication claims."
---

Unit tests cannot show that a replicated log survives a killed leader or a
skewed clock. Felix backs those claims with three tools: a cluster harness that
runs real brokers and injects faults, a history checker that records what
clients saw under those faults, and TLA+ models of the replication protocol.

## The cluster harness

`crates/testing/felix-cluster` starts a cluster on one machine. Brokers are
real `felix-broker` processes, each with its own ports, identity, credential
and data directory. The control plane runs inside the harness process so the
harness can mint node and client tokens.
[docs/cluster-harness.md](https://github.com/gabloe/felix/blob/main/docs/cluster-harness.md)
is the full reference.

```bash
task cluster:up        # start three nodes and hold until Ctrl-C
task cluster:status    # start, print membership and shard ownership, tear down
task cluster:failover  # kill the leader and keep publishing
task cluster:test      # cargo test -p felix-cluster
```

The harness runs the prebuilt `target/<profile>/felix-broker` and does not
rebuild it. After a broker change, run
`cargo build -p felix-broker-service --bin felix-broker` before
`cargo test -p felix-cluster`, or the tests exercise the old binary. Most
`task cluster:*` shortcuts build it first. `cluster:failover` and
`cluster:test` do not.

Cluster tests are `#[serial]`, since each starts several broker processes.
Start-up returns only once every shard has a leader, a publish has succeeded
and every leader has reported a caught-up replica, so a test can fail over
straight away. `FELIX_TEST_TIMEOUT_SCALE` multiplies every harness deadline for
slow machines. CI sets it to 3.

### Faults

`Cluster::inject` applies a fault and returns once it is in effect.
`Cluster::heal` undoes one, and `Cluster::heal_all` undoes everything still
injected. Faults from different families compose.

| Fault | Effect | Mechanism |
| --- | --- | --- |
| `Drop`, `Delay` | Traffic on one link, one direction, lost or late | Harness-owned proxies (`ClusterConfig::proxy_links`) |
| `Refuse` | A broker's requests to chosen peers fail at once | `FELIX_PEER_PARTITION_FILE` |
| `Suspend` | `SIGSTOP`: the broker stays alive, holds its lease and answers nothing | Signal (Unix only) |
| `Clock` | Time stepped or running at a different rate | `FELIX_CLOCK_FAULT_FILE` |
| `Fsync` | Flushes delayed, failing with `EIO`, or failing once | `FELIX_STORAGE_FAULT_FILE` |

Process-level faults are methods too: `stop_node`, `kill_node`,
`pause_node`, `partition_node`, `restart_control_plane`, `drain_node` and
`add_node`.

The clock and storage seams are compiled only into debug builds and builds with
the `fault-injection` feature. A release broker ignores
`FELIX_CLOCK_FAULT_FILE` and `FELIX_STORAGE_FAULT_FILE`. A broker's lease clock
never goes backwards, so the harness refuses a backward step on a broker.

`crates/testing/felix-cluster/tests/failures/` holds the fault tests, one
module per family plus failover, fencing, promotion and quorum scenarios. Each
fault test first checks the fault actually took effect.

## The history checker

A Jepsen-style check of what clients observed. Concurrent clients append to and
read `Quorum` streams on a three-broker cluster while a nemesis kills, pauses
and partitions brokers. In long runs it also cuts links, skews clocks and fails
fsyncs. Once the faults are healed, the checker compares the recorded history
with what a replicated append-only log promises: no lost acknowledged writes,
no duplicates, reads that are prefixes of the final log, real-time order, no
phantom values, and failed writes that stay absent.

The code is in `crates/testing/felix-cluster/src/history/`, and
[docs/history-checker.md](https://github.com/gabloe/felix/blob/main/docs/history-checker.md)
explains the model and how to read a violation.

```bash
cargo build -p felix-broker-service --bin felix-broker
cargo test -p felix-cluster --test history -- --nocapture
```

It runs in every `task test` with a fixed seed, for about a minute per test.
`FELIX_HISTORY_SEED` sets the seed (a number, or `random`).
`FELIX_HISTORY_DURATION_SECS` sets how long the nemesis runs, and setting it
also switches the main campaign to every fault family.

`.github/workflows/history.yml` runs the campaign nightly for 20 minutes with a
random seed. The seed is printed at the start and in the job summary on
failure. Replay a red night with:

```bash
FELIX_HISTORY_SEED=<seed> FELIX_HISTORY_DURATION_SECS=1200 \
    cargo test -p felix-cluster --test history -- --nocapture
```

The seed fixes the fault schedule and the clients' choices, not the thread
interleaving, so a failing seed makes a failure likely to recur rather than
certain. Run it a few times.

## The TLA+ models

`docs/formal/` models one shard's protocol: the lease, replication to a
majority, promotion, handoff and reads. `FelixShard.tla` is the main model.
`FelixShardFigure8.tla`, `FelixShardReads.tla` and `FelixPlacementPacing.tla`
cover a history two leaderships in, linearizable reads, and placement pacing
across shards.
[docs/formal/README.md](https://github.com/gabloe/felix/blob/main/docs/formal/README.md)
lists every invariant and configuration.

```bash
task tla:check      # model-check every configuration with TLC (Java or Docker)
task tla:pairing    # check that a change to modelled code also touches the spec
```

Each `.cfg` declares its outcome in `scripts/check_tla.sh`: pass, or violate a
named invariant. The configurations that must fail show that a check is
load-bearing, so a violation that quietly became a pass fails the run. The full
suite takes about half an hour on a CI runner.

`scripts/check_spec_pairing.py` fails a pull request that touches the modelled
code (lease, membership, replication, serving, shard lifecycle, and the control
plane's reports and placement) without touching `docs/formal/`. When the change
does not affect the protocol, say so with a line in a commit message or the PR
description:

```text
Spec-Unaffected: log message wording only
```

`scripts/check_spec_evidence.py`, part of `task docs:evidence`, checks that
every test the spec cites still exists. The CI `formal` job runs the pairing
check and then `scripts/check_tla.sh`.
