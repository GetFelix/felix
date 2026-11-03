---
title: "felixd record formats"
description: "The records felixd writes: output stream version 4 with outputs, claims, failures, rejections and notes, snapshot stream version 3 with chunks and a manifest, the merge log, spec records, admission requests and decisions, the cache values, and which older versions are still read."
---

felixd writes these kinds of data to Felix: records on a workload's output stream,
its snapshot stream and, with several inputs, its merge log; records on the system
streams `sys.specs`, `sys.intake` and `sys.facts`; and JSON values in caches. The byte layouts are in
[docs/records.md](https://github.com/GetFelix/felixd/blob/main/docs/records.md).
This page says what each record is for and which versions a build reads.

The formats are defined in `felixd-core`, which does no I/O. Binary records start
with a version byte and integers are big endian. A reader refuses a version it
does not know, so an older felixd fails a workload whose stream holds a newer
record instead of misreading it.

## Output stream, version 4

`wl.<id>.out` holds records of ten kinds. Each begins with the version (4), a kind byte
and the writer's [epoch](/built-on-felix/felixd/terminology/#epoch).

| Kind | What it is | What follows the epoch |
|---|---|---|
| 0, output | One output of one step | the input offset that produced it, its index among that step's outputs, the output's name, an optional key, and the payload |
| 1, claim | A new owner taking the stream | the new owner's node id, in UTF-8, to the end of the record |
| 2, failure | The workload failed, from an older build | the version of the spec it failed under, and the reason. It has no error class and reads as poison |
| 3, rejection | A step rejected an input and the workload carried on | the input offset, index (always 0), the error code and the detail |
| 4, claim under a spec | A new owner taking the stream, naming the spec it runs | the `sys.specs` offset of the spec record, the highest spec record claimed so far, then the node id |
| 5, failure | The workload failed | the version of the spec it failed under, an error class (transient, needs operator or poison) and the reason |
| 6, needs operator | No node can run a spec record until an operator acts | the spec record's offset, a reason code and detail for people |
| 7, trim | The floors below which a resume reads nothing, so older history may be deleted | the oldest kept snapshot, the floors of the snapshot stream, the merge log and each input |
| 8, degraded | The holder gave up on a workload stuck on an error and let go of its lease | the spec record, an error class, the node and the error |
| 9, divergence | A holder replayed inputs and got an output that differs from the stream | the spec record, where it differs, the digests of both sides, the node and the versions of the contract, wasmtime and felixd |

An output's name is 1 to 64 bytes of `a-z`, `0-9` and `-`. Its key is optional, so
a key flag says whether a key length and a key follow.

The pieces fit together like this.

- **The claim** is what fences the stream. A new owner appends it before it resumes,
  so a stale owner's next `publish_if` is refused. Claims written before the node
  id was added end after the epoch, and a reader takes their node as unknown.
- **The failure record** is the last record of a run. An owner of the same spec
  version that finds it writes `failed` into the spec and stops. A claim under a spec
  also lets `felixd check` and `felixd replay --diff` judge each input under the limits
  of the spec that applied.
- **Needs operator, trim, degraded and divergence** are notes, not outputs. No step
  produced them, so the resume fold, `felixd check` and downstream workloads pass over
  them as they pass over a claim. A needs-operator note is written once per workload,
  spec record and reason, and a new spec record clears it. A degraded note is an
  observation only, and `felixd ps` shows `degraded` until the next claim. A divergence
  note names the output that differs, and is the record to read when a workload fails
  with `replayed output differs`.
- **A rejection** is the only output its input has. The resume fold, the checks
  that skip already written outputs, reconcile and `felixd check` match it by
  `(input offset, index)` and compare it exactly like an output, so a stream with a
  rejection where a replay produces an output, or the other way round, diverges.
- When a stream is another workload's input, readers pass over claims, failures,
  notes, rejections and outputs under other names, and read the outputs addressed to
  `out`. A record there that does not decode fails the reader, since it cannot tell
  what the record holds.

## Snapshot stream, version 3

A snapshot is chunk records followed by one manifest record, on `wl.<id>.snap`.
Each record is `version | kind | epoch | body`.

| Kind | What it is | Body |
|---|---|---|
| 0, chunk | A piece of the workload's state | the sha256 of the bytes, then the bytes |
| 1, manifest | The snapshot itself | the input offset the state includes, the output stream's next offset at that moment, the chunk count, then for each chunk its sha256 and the offset on `.snap` of a chunk record holding it |

Only a manifest is a snapshot. Chunks without one are ignored, and `felixd check`
checks only manifests. A manifest may name chunk records written for an earlier
snapshot, which is how a chunk that did not change is not written again. Reading a
chunk back checks its bytes against the hash the manifest names, and a mismatch
fails the workload. A snapshot has at most 65,536 chunks of at most 4 MiB each.

The **output tail** in a manifest is what lets a resume know where the stream
stood. Every output of inputs up to the snapshot's input offset lies below it, and
`felixd check` verifies that.

A chunk is held by its offset on `.snap` for now. The manifest already records each
chunk's sha256, so a move to a content-addressed chunk store
([#77](https://github.com/GetFelix/felixd/issues/77)) is a storage swap and not a
format change. Retention ([#63](https://github.com/GetFelix/felixd/issues/63)) must
keep every chunk the live manifest references; nothing is trimmed unless the fleet has finalized `head_trim`.

## Merge log, version 1

A workload with several inputs also has `wl.<id>.merge`, which records the order its
lease holder chose to step the inputs' records in. Each record is
`version | kind | epoch | body`.

| Kind | What it is | Body |
|---|---|---|
| 0, entry | One merged record | the input's position in the spec's `inputs` (2), the record's offset on that input (8), its append time in microseconds (8), then `next` |
| 1, claim | The holder taking the log | `next`, then the holder's node id |

`next` is a count (2) followed by that many offsets (8), one per input: the offset after
the last record of that input merged so far. The entry's offset on the merge log is the
step's offset, so outputs, rejections and snapshots name it where a one-input
workload's name the input offset. The log is fenced like the output stream, and
`felixd check` reports a record merged twice, out of order or missing.

## Spec records, requests and decisions

Every spec felixd writes is also a record on `sys.specs`, in the system namespace
`<namespace>.sys`. Each is JSON with the workload `id` and its `spec`. Its offset is the
spec's version, so a later record for an id supersedes an earlier one and the stream is
the spec history. Records are never rewritten, which is why `sys.specs` must never be
trimmed. A claim names the spec record it runs.

Admission uses two more streams. A request is a record on `sys.intake`, as JSON with the
workload id, the spec and the module's length. It does not say who asked, because a
publisher could write anything there. Admission reads the requester from the record's
[publisher](/built-on-felix/felixd/contract/#the-publisher). Admission's decisions are
ordinary outputs on `wl.admission.out`, keyed by the workload id, each either `accepted`
or `rejected` with a reason such as `no_requester` or `not_permitted`. Once
admission is set up, a node runs a workload's spec record only if it names a decision that
accepted exactly that spec. When a scheduler runs, nodes also append facts about
themselves to `sys.facts`, and the scheduler's assignments are outputs on
`wl.scheduler.out`.

## Versions still read

This build writes output version 4 and snapshot version 3. It reads every older
version.

| Stream | Version | Layout | Read as |
|---|---|---|---|
| output | 4 | with a name and a key; claim, failure and rejection kinds | as written |
| output | 3 | the same without a name or key in an output, and no rejections | name `out`, no key |
| output | 2 | an output with no kind byte: version, input offset, index, epoch, payload | name `out`, no key |
| snapshot | 3 | chunk records and a manifest | as written |
| snapshot | 2 | one record: version, input offset, output tail, epoch, state | one chunk |
| snapshot | 1 | the same without an epoch | one chunk, epoch 0 |

A build from before a record kind or version existed can fail the workload that
carries it. Builds after 0.1.0-preview.1 instead leave a workload alone when they meet a record
from a newer build, so upgrade every node before you rely on a feature of the new
build. Making new formats wait until every node advertises that it reads them is
not built ([#65](https://github.com/GetFelix/felixd/issues/65)).

## Cache values

These are JSON. Each value of `workloads`, `leases`, `epochs`, `nodes` and
`drains` is wrapped as `{"format": 1, "value": ...}`, and a reader refuses a
format it does not know.

| Cache | Key | Value |
|---|---|---|
| `workloads` | workload id | the spec: `module`, `input` (or `inputs`, a list of 2 to 64 names, for several), `output`, `snapshot_every` (default 1000), `snapshot_every_ms` and `keep_snapshots` (absent unless set), `state`, `limits`, `record` (the `sys.specs` offset it comes from) and, when failed, `failure`. A spec without `limits` gets the defaults |
| `leases` | workload id | `owner` and `epoch`. The epoch is 0 until the claim has taken one |
| `epochs` | workload id | the last epoch handed out, as a decimal number. It has no TTL, so an epoch outlives the lease that used it |
| `nodes` | node id | the felixd `version`, `max_workloads`, how many workloads the node `held`, and `draining` once it has read a drain mark |
| `drains` | node id | a drain mark: a nonzero `nonce` that `felixd drain` picks anew each time. `felixd undrain` deletes it |
| `modules` | sha256 of the module in hex | the module's bytes. A module over 8 MiB is stored in 4 MiB chunks under `<sha256>/<n>`, with a manifest under the hash |

The epoch of a new claim is one more than the larger of the number in `epochs` and
the epochs on the last records of the workload's streams. The assumption is that
`epochs` is not recreated while a holder that has not yet written anything may
still be running: its epoch would then be on no record, so the next claim could be
given the same one.

## Stream and cache names

| Name | Built from |
|---|---|
| `wl.<id>.out` | the workload id: 1 to 64 characters of `a-z`, `0-9` and `-` |
| `wl.<id>.snap` | the same id |
| `workloads`, `leases`, `epochs`, `nodes`, `drains`, `modules` | fixed |
| `wl.<id>.merge` | the same id, for a workload with several inputs |
| `sys.specs`, `sys.intake`, `sys.facts` | fixed, in `<namespace>.sys` |
