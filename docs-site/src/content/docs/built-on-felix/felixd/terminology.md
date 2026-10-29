---
title: "felixd terminology"
description: "What the words used in the felixd docs mean: workload, fuel, epoch, fence, snapshot, fold, replay, needs-operator and the rest, each with a link to where it is used."
tableOfContents:
  maxHeadingLevel: 2
---

The felixd pages use these words without stopping to define them. Each entry says
what the term means in felixd and links to where it is used. Terms are grouped by
area. Words that Felix itself defines, such as stream, shard, offset and cache, are
in the [Felix terminology](/reference/terminology/).

## Programs and the contract

### Workload

A program felixd runs, plus the spec that says what it reads and under which
limits. Its only inputs and outputs are Felix streams. It is identified by an id of
1 to 64 characters of `a-z`, `0-9` and `-`, which names its streams. See
[How it works](/built-on-felix/felixd/how-it-works/).

### Spec

The JSON value in the `workloads` cache that says what should run: the module's
hash, the input stream, the output stream, the snapshot cadence, the limits, the
state and, if the workload failed, why. The limits are in the spec so every node
runs the workload under the same ones. See [The spec](/built-on-felix/felixd/how-it-works/#the-spec).

### Component

The WebAssembly artifact a workload is built as. A component carries typed
interfaces, and felixd accepts one that implements the `workload` world and imports
nothing. See [The workload contract](/built-on-felix/felixd/contract/).

### Module

A component as stored. Its sha256 in hex is its identity, and it is kept in the
`modules` cache under that hash. A spec names its module by hash.

### WIT

The WebAssembly Interface Type language that describes a component's imports and
exports. The workload contract is written in WIT, in
[`wit/workload.wit`](https://github.com/GetFelix/felixd/blob/main/wit/workload.wit).

### Contract

The interface between felixd and a workload and what each side promises. The
current version is 0.2. A component built against 0.1 is refused. See
[The workload contract](/built-on-felix/felixd/contract/).

### Step

The workload function felixd calls once per input record, in input order. It
returns outputs, or a rejection, or poisons the workload. See
[What `step` returns](/built-on-felix/felixd/contract/#what-step-returns).

### Inspect

The workload function that returns its state as JSON. `felixd replay` prints it.

### Input

One record on the workload's input stream, with metadata: the stream, the incarnation,
the offset, the broker's append time, and the key and payload. When the input is
another workload's output stream, only the outputs addressed to `out` are inputs.

### Input offset

The offset of a record on the input stream. An output records the input offset of
the step that produced it, a snapshot records the last input offset its state
includes, and a replay's target can be an input offset. The state as of an input
offset includes that input.

### Output

A record a step produces, written to the workload's output stream. It has a
[name](#output-name), an optional [key](#output-key), a payload and a header of
input offset, index and epoch. Outputs are written exactly once. See
[One step](/built-on-felix/felixd/how-it-works/#one-step-and-exactly-once-output).

### Output name

The label on an output. 1 to 64 bytes of lowercase letters, digits and `-`. The
default is `out`, and a downstream workload reads the outputs named `out`.

### Output key

An optional byte string on an output. A downstream workload receives it as the
input's key.

### Index

An output's position among the outputs of one step, from 0, whatever their names.
Together with the input offset it names an output uniquely.

### Rejection

The result of a step that returns `reject`: the input is bad and the workload
carries on. It is recorded on the output stream as that input's only output, with
an error code and detail. See
[What `step` returns](/built-on-felix/felixd/contract/#what-step-returns).

### Poison

The result of a step that returns `poison`, which says the workload cannot go on.
The word also names the class of failure that comes from an input the workload
deterministically fails on. It fails the workload and is not retried. A recorded
way past a poison input is not built ([#98](https://github.com/GetFelix/felixd/issues/98)).

### Contract failure

A result that breaks the contract: an invalid output name, an effect request, a
chunk over 4 MiB, more than 65,536 chunks, or more than 8 MiB of output from one
step. It would break the same way on any node, so it fails the workload. See
[What counts as a failure](/built-on-felix/felixd/contract/#what-counts-as-a-failure).

### Trap

A WebAssembly runtime error in the workload, such as an unreachable instruction or an
out of bounds access. A trap fails the workload. Running out of fuel is a trap.

### Needs-operator

A workload that no node can run through no fault of its own and that a person has to
fix: a module built against contract 0.1, or a restore that ran out of
`snapshot_fuel`. It is not marked failed. Today it is a logged error on the agent,
and the agent leaves the workload alone until its spec changes. It is not yet a
recorded state ([#112](https://github.com/GetFelix/felixd/issues/112)).

### Failed and degraded

`failed` is a value of a spec's `state`. A holder writes it after the failure is
recorded on the output stream, and it stays until `felixd run` writes a new spec.
`degraded` is a design idea for a workload that keeps failing on every node, to be
recorded after a bounded number of takeovers. It does not exist
([#99](https://github.com/GetFelix/felixd/issues/99)). See
[When a workload fails](/built-on-felix/felixd/how-it-works/#when-a-workload-fails).

## Fuel and limits

### Fuel

wasmtime's measure of work: the runtime deducts from a counter as WebAssembly
instructions execute, roughly one unit per instruction, and traps when it reaches
zero. It counts instructions and not time, so the same code on the same input burns
the same fuel on any machine. felixd uses it to fail a runaway step at the same
point on every node and to bound a call that would otherwise run for ever. Fuel is
set again before each step; a snapshot or a restore gets one budget for all of
its calls. Running out in a `step` fails the workload, in a snapshot
skips the snapshot, and in a restore leaves the workload to an operator. See
[What fuel is](/built-on-felix/felixd/contract/#what-fuel-is).

### fuel_per_call

The fuel given to each `step` and each `inspect`. The default is 50,000,000, set
with `felixd run --fuel`.

### snapshot_fuel

The fuel given to a whole snapshot or a whole restore, every chunk call together.
The default is 500,000,000, ten times the step default, set with
`felixd run --snapshot-fuel`.

### max_memory_bytes

The most linear memory the workload may grow to. The default is 64 MiB.

### snapshot_every

A snapshot is written after this many inputs. The default is 1000.

### snapshot_every_ms

Also a snapshot at the first input of each interval of this many milliseconds, by
the inputs' broker append times. Absent unless set.

## Ownership and failover

### Node

A machine running a felixd agent. A node is named by its node id, which defaults to
the host name and process id.

### Agent

The `felixd agent` process on a node. It watches the `workloads` cache, claims and
runs workloads, renews their leases and announces the node in the `nodes` cache.

### Lease

The right to run a workload: an entry in the `leases` cache with a TTL, holding the
owner's node id and epoch. A lease decides who should run the workload. It is not
what keeps the output correct, because it depends on clocks. See
[Claiming a workload](/built-on-felix/felixd/how-it-works/#claiming-a-workload).

### TTL

The time a lease lives without renewal. The default is 10 seconds and the minimum
is 2 seconds, set with `felixd agent --lease-ttl-ms`. The holder renews every third
of the TTL and stops by its own clock two thirds of the TTL after its last
successful renewal.

### Claim

Two things, one on each side of the lease. A claim of a lease is a `cache_put_if` on
"absent" that wins when the broker counts the previous lease as gone. A **claim
record** is the first record a new owner appends to the output stream, carrying its
epoch and node id, which fences the stream.

### cache_put_if

A Felix [conditional cache write](/features/cache/#9-conditional-writes) that applies
only if the key is absent or at an expected version. felixd uses it to claim and
renew leases, to take epochs and to write `failed` into a spec.

### Epoch

A number that grows with each claim of a workload. A claim takes an epoch one above
every earlier claim's and every epoch on the workload's streams, and the owner
stamps it on every record it writes. A record from a later epoch is how a stale
owner learns it has been replaced. See
[Claiming a workload](/built-on-felix/felixd/how-it-works/#claiming-a-workload).

### Fence

What stops a stale owner from writing. A new owner appends a claim record to the
output stream, so a stale owner's `publish_if` at its old tail is refused, and every
snapshot and failure record is written the same way. See
[Epochs and the fence](/built-on-felix/felixd/how-it-works/#epochs-and-the-fence).

### Stale owner

A node that no longer holds a workload but does not know it yet, for example one that
was frozen past its lease. The fence refuses its writes.

### publish_if

A Felix [conditional publish](/features/atomic-commits/#a-commit-at-an-expected-offset)
that applies only if the stream's tail is at the offset the writer expects. Every
write felixd makes to a workload's streams is one. It is a compare-and-set on the
whole shard, which is why a workload's streams have one shard.

### Tail

A stream's next offset. A writer expects the tail it last saw, and a `publish_if` at
a different tail is refused with the real one. The tail can be past the last output
because claims and the generation-start records Felix appends take offsets that
hold no output.

### Incarnation

A stream deleted and created again under the same name is a different incarnation of
it. The contract carries an incarnation in each input's source. Specs do not record
one yet, so it is always 0 ([#72](https://github.com/GetFelix/felixd/issues/72)).

### Failover

Another node taking over a workload after its holder stopped renewing. It is the same
operation as resume. See [How it works](/built-on-felix/felixd/how-it-works/#epochs-and-the-fence).

### Placement

Which node claims which workload. Today it is first claim wins: after a random delay
a node with room claims any free workload. See
[Claiming a workload](/built-on-felix/felixd/how-it-works/#claiming-a-workload).

## State and resume

### Snapshot

A workload's state saved on `wl.<id>.snap` as chunks followed by a manifest. It lets
a new owner start from a recent point. Only a manifest makes a snapshot. See
[Snapshots](/built-on-felix/felixd/how-it-works/#snapshots).

### Chunk

A piece of a snapshot, at most 4 MiB, with its sha256. A workload returns its state
as a list of chunks. A chunk that did not change since the last snapshot is not
written again.

### Manifest

The record that makes a snapshot. It holds the input offset the state includes, the
output stream's next offset at that moment, and the sha256 and the stream offset of
each chunk.

### Resume

What a new owner does to pick a workload up: claim, read the latest snapshot
manifest and the last output, compute the fold, restore the snapshot if there is
one, and subscribe to the input. See
[Resume](/built-on-felix/felixd/how-it-works/#resume).

### Fold

The computation that decides how to resume. From the latest snapshot's header, the
last output record and the output stream's tail it returns whether to restore, which
input offset to subscribe from, and which outputs the stream already holds. Replay
uses the same fold.

### Restore

Loading a snapshot's chunks into a fresh instance with `restore` and `restore-chunk`,
under `snapshot_fuel`.

### Reconcile

Deciding what to do when a write is refused or its answer is lost: compare what is
on the stream at the expected tail with the batch, then skip what landed, stop if a
later epoch wrote, or send again.

### Replaying (during resume)

The state of an owner that is running inputs whose outputs are already on the stream.
It takes no snapshot until it is past them.

## Replay and checking

### Replay

Rebuilding a workload's state at an input offset or time by restoring the nearest
earlier snapshot and running the inputs after it. `felixd replay` does it, takes no
lease and writes nothing. See [Replay](/built-on-felix/felixd/how-it-works/#replay).

### Divergence

A replayed output that differs from the recorded one. During a resume it fails the
workload. `felixd check` reports it as a record that differs, and
`felixd replay --diff` reports the first one with the input offset, the epoch, the
node and both digests.

### Check

`felixd check`: replay each workload over its whole input and compare the result with
its output stream. See [Checking](/built-on-felix/felixd/how-it-works/#checking).

### Determinism

The property that a workload given the same inputs produces the same outputs and the
same state on any node. Failover, exactly-once output and replay all rest on it.
felixd enforces it by letting a workload import nothing, canonicalizing NaN bit
patterns and applying fixed limits. What breaks it: a different felixd build, a
changed module or input under a workload that has output, and possibly a different
machine architecture. See
[Why the guarantees hold](/built-on-felix/felixd/how-it-works/#why-the-guarantees-hold).

### Exactly-once output

Each input's outputs appear once on the output stream, in order, across takeovers,
lost answers and partial batches.

## Later

### Effect

A request from a workload to the outside world, written as an output and answered by
a recorded input. The contract reserves the shape. Nothing performs an effect, and a
workload that returns a request fails
([#71](https://github.com/GetFelix/felixd/issues/71)).

### Executor

The component that would perform an effect and answer it. It would use the request's
id, which is the output header, as an idempotency key, and refuse a request whose
epoch is below the highest it has seen. Not built.

### NineLives

The proposed name for the distributed operating system felixd is a first step toward.
It is not decided, and felixd would stay the name of the agent on each node. See
the [discussion](https://github.com/GetFelix/felixd/discussions/42).
