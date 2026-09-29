---
title: "How Felix Is Tested"
description: "The history checker, fault injection on a real cluster, and the TLA+ models behind the replication claims."
---

The replication rows on [What Felix Is For](/felix/getting-started/what-felix-is-for/)
rest on three kinds of evidence. A history checker runs clients against a real
three-broker cluster while it injects faults, then checks what the clients saw.
The cluster harness injects those faults, and individual tests use it to pin
one scenario at a time. TLA+ models cover the interleavings no injected fault
reliably reaches. This page says what each one does and where the detail lives.

## The history checker

The checker is a Jepsen-style test in `crates/testing/felix-cluster/src/history/`.
Six clients append unique values to three single-shard `Quorum` streams and
read them back, and put and get three keys of a `Quorum` cache, while a
nemesis injects faults. Every operation is recorded
with its start time, its end time and its result. Once the faults are healed,
the checker takes a final read of each stream from its leader and compares the
history with what a replicated append-only log promises.

The model is Elle's list-append. Felix hands a reader the offset of every
record, so the checker reads positions directly and does not have to infer an
order. An append ends as acknowledged, as a definite failure (only a typed
refusal that says the broker applied nothing), or as unknown (a timeout, a lost
connection, `outcome_unknown`). Half the clients are idempotent producers that
re-send under the same sequence, so the campaign also exercises deduplication
across leader changes.

It checks seven rules:

1. No acknowledged append is missing from the final read.
2. No value sits at two offsets.
3. Every read agrees with the final log at the offsets it holds.
4. An append acknowledged before another was sent sits at an earlier offset.
5. A read never sees a value nobody appended, or appended to another stream.
6. A value whose append definitely failed never appears.
7. A cache get never returns a value that a later put had already replaced
   before the get began, whether that put was acknowledged or an earlier get
   saw it.

A read that is missing records is allowed, because a subscriber may drop under
`DropNew` and the offsets show the gap. What a read does hold has to match.

The nemesis waits 1-4 s, injects one fault for 2-6 s, heals it, and repeats.
Faults never overlap, so a majority is always one fault from whole. The per-PR
run lasts 45 seconds with a fixed seed and uses process faults only: kill,
pause (`SIGSTOP`) and partition. The nightly workflow
(`.github/workflows/history.yml`) runs for 20 minutes with a random seed and
adds link faults (dropped or delayed peer traffic, lost control-plane replies),
clock faults (a broker's lease clock at 0.5x or 20x, the control plane's wall
clock stepped 15 s forward) and disk faults (slow fsyncs, one failed fsync).
It prints the seed first, so a failing night can be replayed with
`FELIX_HISTORY_SEED`.

`FELIX_HISTORY_MODE` picks the replication path. In `lease` mode the campaign
tests the report and lease path every stream uses by default. In `lease-free`
mode it finalizes `generation_start`, `majority_ack` and `lease_free_reads`
after start-up and fails at once if any broker does not turn them on. The
nightly run and the per-PR main campaign use `lease-free`; the per-PR
every-family campaign uses `lease`, so each pull request covers both.

Detail, including how to read a violation and how to add a fault:
[`docs/history-checker.md`](https://github.com/gabloe/felix/blob/main/docs/history-checker.md).

## Fault injection

`crates/testing/felix-cluster` starts a control plane and several real broker
processes on one machine. Tests drive them through the client API and inject
faults as values: `Cluster::inject` applies one and returns once it is in
effect, and `Cluster::heal` undoes it. Faults from different families compose,
so one scenario can cut a leader's links and speed up its clock at once.

| Family | What the broker sees |
| --- | --- |
| Process | Killed, stopped gracefully, suspended with `SIGSTOP`, or partitioned from its peers while it keeps heartbeating |
| Link | Traffic in one direction dropped or delayed, through proxies the harness owns |
| Clock | A clock stepped or running fast or slow, through `FELIX_CLOCK_FAULT_FILE` |
| Disk | Flushes slowed, failing with `EIO`, or failing once |

Clock and disk faults read files that only debug and fault-injection builds
honour. A release build ignores them and reads the real clocks and disk.
`crates/testing/felix-cluster/tests/failures/` has one module per family, and
each test first checks that its fault took effect, so a fault that silently did
nothing fails the test instead of passing it.

The harness runs a prebuilt `target/<profile>/felix-broker`, so build it
(`cargo build -p felix-broker-service --bin felix-broker`) before running the
cluster tests on their own. `task test` builds it for you.

The same crate holds a conformance suite that runs one set of assertions
against a single broker and a three-node cluster: a client must not be able to
tell how many brokers there are or which one it reached.

Detail: [`docs/cluster-harness.md`](https://github.com/gabloe/felix/blob/main/docs/cluster-harness.md).

## TLA+ models

The models in `docs/formal/` cover one shard: three brokers, one control plane
and discrete time, with clocks that may drift. TLC explores every interleaving
within the configured bounds.

- `FelixShard.tla` models the lease, replication to a majority, promotion, the
  promotion fence, planned moves and cancelled moves. Its invariants include
  that no two brokers serve the shard at once, that whoever serves holds every
  acknowledged record, that two brokers never disagree on an acknowledged
  record, and that no log holds a re-sent write twice.
- `FelixShardFigure8.tla` starts the same model from a history two
  leaderships in, to check the generation-start record against the loss Raft's
  paper shows in its Figure 8.
- `FelixShardReads.tla` adds reads, and checks that a `Quorum` read confirmed by
  a majority round never returns a stale value.
- `FelixPlacementPacing.tla` checks that concurrent moves across shards stay
  within the copy limits, including with two control-plane instances.

Each configuration declares whether it must pass or which invariant it must
break. The breaking ones remove one piece of the design, such as the fence, the
start record or the read round, and exist to show that piece is load-bearing.
If one of them stopped finding its violation, the check would fail.

CI runs TLC over every configuration on each code change (`task tla:check`,
about half an hour on a four-core runner). Model checking shows the spec is
consistent, not that it still describes the code, so CI also runs
`scripts/check_spec_pairing.py`: a change to code the model covers must change
the spec too, or carry a `Spec-Unaffected:` trailer saying why not.

Detail, with every configuration and its state count:
[`docs/formal/README.md`](https://github.com/gabloe/felix/blob/main/docs/formal/README.md).

## Elsewhere

The decoders that parse input from outside the process are fuzzed; see
[Fuzzing](/felix/development/fuzzing/). The wire-protocol conformance runner
(`task conformance`) holds a catalogue of required scenarios, and CI checks the
Python and TypeScript clients' results against it.
