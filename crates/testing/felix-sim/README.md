# felix-sim

A deterministic simulation of a Felix cluster. A run starts three to five real
brokers and the real control plane in one process, on tokio's paused clock,
with an in-memory network between the brokers in place of QUIC. It drives
logical clients against the cluster while a nemesis injects faults, then checks
what happened. Everything comes from a seed, so a failure can be run again. The
crate is internal to the Felix repository and is not published to crates.io.
`docs/simulation.md` has the full design.

## Faults

The nemesis (`src/nemesis.rs`, `Fault`) draws these from the seed:

- `Crash`: a broker stops, then restarts.
- `Freeze`: a broker stops running for a while, then continues.
- `Isolate`: a broker loses its network.
- `Split`: a minority of brokers is cut off, with or without the control plane.
- `OneWay`: traffic from one broker to another is dropped.
- `ControlPlane`: the control plane is slow, paused or down.
- `PausedThenCrash`: the control plane pauses, then a broker that leads shards
  crashes.

## Checks

A seed fails when any check reports a problem, except a warning.

- The history checker in `felix-cluster` reads what clients saw. It checks that
  reads are a prefix of one order of the stream, that acknowledged records are
  not lost or duplicated, that reads and writes do not form a cycle, and that
  each cache key is linearizable.
- The cluster checkers in `src/check/` read the brokers and the control plane
  while the run goes: `ElectionSettles`, `OneLeaderPerGeneration`,
  `AtMostOneServing`, `AckedHeldByLeader`, `AdoptedMatchesOpened` and
  `GenerationBurn`.
- `ElectionSettles` skips a shard the broker holds on purpose: its leader was
  killed uncleanly, and killed members that were already down then make up a
  majority of the set with it. The broker waits for `felixctl placement
  abandon`. Every other check still covers the shard. If its final read cannot
  be taken, only the lost-write rule is skipped and a warning names it. The run
  prints how many shards were exempt.
- A run that stops making progress ends with a `stalled` finding instead of
  hanging.

## Running it

Run one seed, with the broker log:

```bash
FELIX_SIM_SEED=<n> RUST_LOG=info cargo test -p felix-sim --test seeds replay -- --nocapture
```

Run a batch of seeds. `FELIX_SIM_SEEDS` is how many (default 4) and
`FELIX_SIM_SEED_START` is the first (default 1):

```bash
FELIX_SIM_SEED_START=100 FELIX_SIM_SEEDS=10 \
  cargo test -p felix-sim --test seeds -- --ignored --exact seeds --nocapture
```

A failing seed prints the command that replays it. `FELIX_SIM_HISTORY=<file>`
writes the run's history to a file so two runs can be diffed.

`seeds` is `#[ignore]`, so `cargo test --workspace` skips it. It runs in the
non-blocking `sim-seeds` job in CI, which does not gate merges while its known
liveness stalls are open. One seed takes a few seconds in a debug build and the
default batch of four takes a few minutes, so run batches one at a time on a
busy machine.

The same seed injects the same faults at the same virtual times. The brokers'
own interleaving can still differ, so a failure can take more than one replay
to show again. `docs/simulation.md` covers making the interleaving repeat too.
