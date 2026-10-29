---
title: "The felixd workload contract"
description: "Workload contract 0.2: the WIT interfaces a felixd workload exports, what a step receives and may return, named and keyed outputs, rejection, poison and contract failures, fuel and the other limits."
---

The contract is the interface between felixd and a workload, and what each side
promises. This page describes version 0.2, which is the only version felixd
runs. The WIT is in
[`wit/workload.wit`](https://github.com/GetFelix/felixd/blob/main/wit/workload.wit)
and the full note, with the decisions behind it, is
[docs/contract.md](https://github.com/GetFelix/felixd/blob/main/docs/contract.md).
For how the contract is used, see [How it works](/built-on-felix/felixd/how-it-works/).

## Interfaces

The WIT package is `felixd:workload@0.2.0`. A workload exports two interfaces and
imports nothing.

| Export | Holds |
|---|---|
| `felixd:workload/step@0.2.0` | the input and output types and the function `step` |
| `felixd:workload/state@0.2.0` | `snapshot`, `snapshot-chunk`, `restore`, `restore-chunk` and `inspect` |

The host identifies the contract by the exported interface names, which carry the
version. A minor version may only add what an older guest never sees. A change to
a type a guest already uses is a new version.

The runtime checks a component before it runs it. A component that imports
anything is refused, because a workload gets nothing from the host, not even a
clock or randomness. A component that does not export both interfaces is refused
and the error lists what it does export.

## What `step` receives

The host calls `step` once per input record, in input order. Every field of the
`input` is a function of the input stream and the spec alone, so a live step, a
resume after takeover, `felixd check` and `felixd replay` pass identical values.
The host never fills in a field from the node it runs on.

| Field | Meaning |
|---|---|
| `source.stream` | the input stream's name, as the spec records it |
| `source.incarnation` | which Felix incarnation the name resolved to. Always 0 today, until specs record incarnations ([#72](https://github.com/GetFelix/felixd/issues/72)) |
| `offset` | the record's offset in the input stream |
| `timestamp-micros` | the broker's append time for the record, in microseconds since the Unix epoch. 0 if the broker keeps no time |
| `body` | `record` with an optional `key` and a `payload`, or `response`, the answer to an effect request. No version of felixd delivers a response yet |

The time is the broker's, so a replay sees the same time the live run did. A
workload that needs "now" uses this field, or a clock record written into its
input.

When the input is another workload's output stream, `step` gets the output's key
and payload, not the record around it. It receives only outputs addressed to the
name `out`. Claims, failures, rejections and outputs under other names are passed
over. Reading several output names at once waits for the determinism work in
[#62](https://github.com/GetFelix/felixd/issues/62).

## What `step` returns

`step` returns either a list of outputs or an error.

**Outputs.** Each output is a `record` with a **name**, an optional **key** and a
**payload**. A name is 1 to 64 bytes of lowercase ASCII letters, digits and `-`.
All outputs land on the workload's single `wl.<id>.out` stream, with the name and
key in the record header, and the **index** counts every output of the step
whatever its name. Outputs are written exactly once and in order, across
takeovers. The convention for a workload with one kind of output is the name
`out`, and it is the name a downstream workload reads.

**Rejection.** The error `reject` carries an error code and detail. The host
writes one rejection record at that input's offset and index 0, fenced and
reconciled like an output, and steps the next input. The workload's state stays
as the step left it, because replay reproduces it either way. The code is a number
a program compares, the detail is text for people and is never compared. A
rejection is not a failure: the workload keeps its lease and stays `running`. Use
it for a bad input the workload can go on without. The example `window`
workload rejects a record that is not 16 bytes with code 1.

**Poison.** The error `poison` carries a reason and says the workload cannot go
on. It fails the workload.

**Effects.** An output can instead be a `request` for an effect, and an input can
be a `response`. These cases exist in the types so that effects can be added
without another breaking change. Nothing performs an effect today
([#71](https://github.com/GetFelix/felixd/issues/71)). A step that returns a
request breaks the contract and fails the workload with the reason that this
build has no effect executors. The planned design is that a request is an output
written exactly once, its id is the output header (input offset, index and epoch)
so an executor can use it as an idempotency key, and the response comes back as a
recorded input that replay reads instead of performing the effect again.

## Snapshots and restores

The `state` interface saves and loads the workload.

| Function | Does |
|---|---|
| `snapshot` | starts a snapshot and returns how many chunks it has |
| `snapshot-chunk(index)` | returns chunk `index` of that snapshot |
| `restore(chunks)` | starts a restore of that many chunks on a fresh instance |
| `restore-chunk(index, chunk)` | gives the host's chunk `index` to the workload |
| `inspect` | returns the state as JSON, for replay and inspection |

Between `snapshot` and its last `snapshot-chunk` the host calls nothing else.
`snapshot` must not change the state, and restoring a snapshot's chunks on a
fresh instance must give a workload that behaves exactly like the one
snapshotted. A chunk may be empty. A chunk over 4 MiB, or more than 65,536 chunks,
fails the workload.

A workload that keeps stable chunk boundaries, by key range for example, lets the
host write only the chunks that changed: it hashes each chunk and does not write
one whose sha256 the previous snapshot already holds.

## What counts as a failure

| What happened | Result |
|---|---|
| `step` returns `reject` | A rejection record. The workload carries on |
| `step` returns `poison` | The workload fails, with the reason |
| `step`, `snapshot-*`, `restore-*` or `inspect` traps | The workload fails |
| `step` runs out of fuel | The workload fails |
| A snapshot runs out of snapshot fuel | The snapshot is skipped and the workload carries on |
| A restore runs out of snapshot fuel | [Needs operator](/built-on-felix/felixd/terminology/#needs-operator), not failed |
| The result breaks the contract: an invalid output name, an effect request, a chunk over 4 MiB, more than 65,536 chunks, or more than 8 MiB of output from one step | The workload fails. It would break the same way on any node |
| The module is built against contract 0.1 | [Needs operator](/built-on-felix/felixd/terminology/#needs-operator). No node claims it and it is not marked failed |

A failed workload is recorded as described in
[When a workload fails](/built-on-felix/felixd/how-it-works/#when-a-workload-fails).
Failures are not retried, because a deterministic failure would fail again on any
node. Retrying a trap that depends on the host on another node is not built yet
([#98](https://github.com/GetFelix/felixd/issues/98)), nor is a recorded way to
get past a poison input. Today the way out is a new spec: fix the module under a
new id.

A `degraded` state, for a workload that fails everywhere, is only in the design
([#99](https://github.com/GetFelix/felixd/issues/99)). There is `failed` and
nothing between it and `running`.

## Limits and fuel

The limits are in the workload's spec, so every node applies the same ones. Set
them with `felixd run --fuel`, `--max-memory` and `--snapshot-fuel`.

| Limit | Default | What it bounds |
|---|---|---|
| `fuel_per_call` | 50,000,000 | each `step`, and each `inspect` |
| `snapshot_fuel` | 500,000,000, ten times the step default | a whole snapshot, or a whole restore, every chunk call together |
| `max_memory_bytes` | 64 MiB | the workload's linear memory |

The Wasm stack size is 512 KiB, and the number of table elements (100,000),
instances (16), tables (16) and memories (4) are bounded too. These are fixed in
felixd and not in the spec, because where a deep recursion overflows decides
which step traps.

### What fuel is

Fuel is wasmtime's measure of work done. With metering on, the runtime takes fuel
from the instance as it executes WebAssembly instructions, roughly one unit per
instruction, and traps when the fuel reaches zero. It counts instructions, not
seconds, so the same code on the same input uses the same fuel on any machine.

felixd meters fuel for two reasons.

- **Determinism.** A wall-clock timeout would fail a step on a slow node and let
  it pass on a fast one, and a replay would disagree with the live run. Fuel runs
  out at the same instruction everywhere, so every node fails the workload at the
  same step.
- **Bounding a step.** A call runs on a blocking thread and can run for as long as
  its fuel allows. A loop that never ends is stopped when the fuel is gone instead
  of holding the thread forever.

Fuel is set again before every call, so it never carries over from one call to the
next. A step gets `fuel_per_call`. A snapshot gets one `snapshot_fuel` for the
`snapshot` call and every `snapshot-chunk` call together, and a restore gets one
for `restore` and every `restore-chunk`. State is allowed to be large, so a
snapshot has a much bigger allowance than a step.

What happens on running out depends on the call:

| Where | What happens |
|---|---|
| In a `step` | The workload fails. The same input would run out on any node |
| In a snapshot | The snapshot is abandoned. Nothing is written, the previous snapshot stays current, and the workload keeps running and replays from the older snapshot if it restarts. The agent drops the instance, because the trap leaves it unusable, and does not try another snapshot until `snapshot_every` more inputs have passed. Every node skips the same snapshots |
| In a restore | The workload goes to needs-operator. The snapshot is fine and the limit is too low. An operator raises `snapshot_fuel` by running `felixd run` again for the same id |

Where fuel runs out depends on the code wasmtime generates, so it can move between
wasmtime versions. Every node of a cluster has to run the same felixd build.

## 0.1 modules

The host refuses a component that does not export the 0.2 interfaces. A component
with the old top-level `step`, `snapshot`, `restore` and `inspect` functions is
reported as built against `felixd:workload@0.1.0` and told to rebuild against 0.2.
`felixd run` refuses it before storing anything. An agent that finds one in a spec
does not claim the workload and does not mark it failed. It logs that an operator
has to act. The fix is a new module under a new id.

There is no shim. A 0.1 workload's outputs would need a name, its snapshots a
chunk layout and its errors a class it never stated, and a shim that chose them
would be a second contract to keep deterministic.

## Not in this version

- Module upgrades at a recorded input offset ([#64](https://github.com/GetFelix/felixd/issues/64)),
  which need spec history as records ([#72](https://github.com/GetFelix/felixd/issues/72)).
- Several inputs ([#58](https://github.com/GetFelix/felixd/issues/58)). `source`
  already names which input a record came from.
- Typed spec and failure records ([#73](https://github.com/GetFelix/felixd/issues/73)).
- Effects ([#71](https://github.com/GetFelix/felixd/issues/71)).

## An example

This is the `counter` workload from the repository. It counts its input records
and outputs the new total as eight big-endian bytes under the name `out`. Its
state is one chunk.

```rust
wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use std::cell::Cell;

use exports::felixd::workload::state;
use exports::felixd::workload::step::{self, Addressed, Input, Output, StepError};

thread_local! {
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

struct Counter;

impl step::Guest for Counter {
    fn step(_input: Input) -> Result<Vec<Output>, StepError> {
        let total = COUNT.get().wrapping_add(1);
        COUNT.set(total);
        Ok(vec![Output::Record(Addressed {
            name: "out".to_string(),
            key: None,
            payload: total.to_be_bytes().to_vec(),
        })])
    }
}

impl state::Guest for Counter {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        COUNT.get().to_be_bytes().to_vec()
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "counter state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let bytes: [u8; 8] = chunk.try_into().expect("counter state is eight bytes");
        COUNT.set(u64::from_be_bytes(bytes));
    }

    fn inspect() -> String {
        format!("{{\"count\":{}}}", COUNT.get())
    }
}

export!(Counter);
```

The other example, `window`, groups `(timestamp_ms, value)` records into ten
second windows and outputs a window's count, sum, min and max as JSON when the
next window opens. See the
[workloads](https://github.com/GetFelix/felixd/tree/main/workloads) directory.
