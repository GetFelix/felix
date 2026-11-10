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

- `Crash`: a broker stops, then restarts. Half the crashes record a clean
  stop. The other half are kills that leave the run state unclean, or, with
  power loss on (`FELIX_SIM_POWER_LOSS=1`), power cuts on brokers that fsync
  on commit.
- `Freeze`: a broker stops running for a while, then continues.
- `Isolate`: a broker loses its network.
- `Split`: a minority of brokers is cut off, with or without the control plane.
- `OneWay`: traffic from one broker to another is dropped.
- `ControlPlane`: the control plane is slow, paused or down.
- `PausedThenCrash`: the control plane pauses, then a broker that leads shards
  crashes.

Clock skew is a separate schedule, off by default. With
`FELIX_SIM_CLOCKS=bounded`, each broker and the control plane gets its own
lease clock (0.8x to 1.2x true speed) and wall clock (within 1 second), and the
nemesis steps or re-skews one every few seconds. `unbounded` goes past those
bounds (0.25x to 4x, 30 seconds) to show what breaks there. Skew needs the
`tokio_unstable` build, so use it through `task sim:replay`. See "Clock skew"
in `docs/simulation.md`.

With placement faults on (`FELIX_SIM_PLACEMENT=1`, or
`SimConfig::with_placement_faults`), the nemesis also acts as an operator. It
calls the control plane's operator API, the one `felixctl` uses, so placement
runs its real move, drain, replacement and abandon paths:

- `Move`: a shard's leadership moves to another broker, and the move is
  sometimes cancelled.
- `Drain`: a broker is drained, then put back. Its leaderships move away and,
  with four or more brokers, its follower seats are replaced.
- `Remove`: a broker is drained, stopped and deregistered, and its record is
  deleted once no shard names it. It comes back later as a new registration.
- `Abandon`: for the stream shards the broker holds after unclean kills, the
  broker they are assigned to is stopped, their logs are abandoned (as
  `felixctl placement abandon` does) and the broker is started again, the
  procedure `docs/replication-design.md` gives.
- `KillBeforeOpen`: the next broker named or elected leader of a shard is
  stopped before it opens the shard.
- `SeatRace`: a broker is drained so a follower seat is being replaced, the
  shard's leader is stopped, then the leader named next is stopped before it
  opens. This is the seat replacement plus two promotions behind #1922 and
  #1929.

Placement faults draw from a stream of their own, so turning them on adds steps
to a seed's schedule and keeps the ones it had. They are off by default for
now, which keeps the seeds pinned in `tests/seeds.rs` on the run they pin.

## Checks

A seed fails when any check reports a problem, except a warning.

- The history checker in `felix-cluster` reads what clients saw. It checks that
  reads are a prefix of one order of the stream, that acknowledged records are
  not lost or duplicated, that reads and writes do not form a cycle, and that
  each cache key is linearizable.
- The cluster checkers in `src/check/` read the brokers and the control plane
  while the run goes: `ElectionSettles`, `OneLeaderPerGeneration`,
  `AtMostOneServing`, `AckedHeldByLeader`, `AdoptedMatchesOpened`,
  `TornTailRepaired` and `GenerationBurn`.
- `ElectionSettles` skips a shard the broker holds on purpose: nobody serves
  it and a majority of its copies attest untrusted, as each broker would
  answer a fence, where no leader could have vouched them. A copy counts if
  its broker is down after an unclean kill, or is up and no broker served the
  shard while it was up since its last unclean start. A copy still untrusted
  after a broker could vouch it (a link up, a healthy control plane, five
  seconds) is vouchable: its shard fails, marked `vouchable`. A copy with no
  injected unclean stop behind it is not counted. The run prints each held
  shard with the faults that explain it. The broker waits for
  `felixctl placement abandon`. A shard with a trusted
  majority and no leader still fails. Every other check still covers an
  exempt shard. If its final read cannot be taken, only the lost-write rule
  is skipped and a warning names it. The run prints how many shards were
  exempt. An abandoned log ends the hold until a member is stopped uncleanly
  again.
- A shard whose log an operator abandoned loses what it acknowledged by
  design. Appends acknowledged before the abandon count as unknown and reads
  started before it are dropped; everything after it is checked as usual. A
  warning names the shard.
- `TornTailRepaired` fails a seed when a power cut landed inside an append and
  the broker's recovery did not repair the torn tail it left.
- A run that stops making progress ends with a `stalled` finding instead of
  hanging.

## Power loss

A power cut lands inside an append to one of the broker's logs. That append
never comes back whole: its size and its sectors reach the disk only in part.
Every other byte that was not synced may be lost or torn, and the broker stops
in the same instant. It restarts on what the device held.
Brokers in these seeds sync every record before acknowledging it, so the
acknowledged-record and history checks must still hold. `docs/simulation.md`
has the details.

## Running it

Run one seed, with the broker log:

```bash
FELIX_SIM_SEED=<n> RUST_LOG=info task sim:replay
```

Run a batch of seeds. `FELIX_SIM_SEEDS` is how many (default 4) and
`FELIX_SIM_SEED_START` is the first (default 1):

```bash
FELIX_SIM_SEED_START=100 FELIX_SIM_SEEDS=10 \
  cargo test -p felix-sim --test seeds -- --ignored --exact seeds --nocapture
```

Run one seed with skewed clocks:

```bash
FELIX_SIM_SEED=<n> FELIX_SIM_CLOCKS=bounded task sim:replay
```

Add `FELIX_SIM_PLACEMENT=1` to any of these commands to turn placement faults on,
and `FELIX_SIM_POWER_LOSS=1` to cut the power in place of kills.
A failing seed prints the command that replays it. `FELIX_SIM_HISTORY=<file>`
writes the run's history to a file so two runs can be diffed.

`seeds` is `#[ignore]`, so `cargo test --workspace` skips it. It runs in the
non-blocking `sim-seeds` job in CI, which does not gate merges while its known
liveness stalls are open. That job builds with `--cfg tokio_unstable` and runs
seeds 1 and 2 without clock skew and seeds 3 and 4 with `bounded` skew. One seed takes a few seconds in a debug build and the
default batch of four takes a few minutes, so run batches one at a time on a
busy machine.

A replay repeats the same faults at the same virtual times and the same client
operations. Run through `task sim:replay`, which builds with
`RUSTFLAGS="--cfg tokio_unstable"` so tokio seeds `select!`, it is the same
run, event for event:

```bash
FELIX_SIM_SEED=<n> FELIX_SIM_HISTORY=run1.txt task sim:replay
```

A plain `cargo test` replay leaves `select!` unseeded, so the interleaving
under the faults can differ and a timing-sensitive failure may take a few
replays to show again. It refuses `FELIX_SIM_HISTORY` for that reason.
`task sim:determinism` checks that replay is exact, in one process and across
two, and CI runs it on every pull request. `docs/simulation.md` has the details.
