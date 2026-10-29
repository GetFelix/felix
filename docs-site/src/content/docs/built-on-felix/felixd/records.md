---
title: "felixd record formats"
description: "The records felixd writes: output stream version 4 with outputs, claims, failures and rejections, snapshot stream version 3 with chunks and a manifest, the cache values, and which older versions are still read."
---

felixd writes four kinds of data to Felix: records on a workload's output stream,
records on its snapshot stream, and JSON values in caches. The byte layouts are in
[docs/records.md](https://github.com/GetFelix/felixd/blob/main/docs/records.md).
This page says what each record is for and which versions a build reads.

The formats are defined in `felixd-core`, which does no I/O. Binary records start
with a version byte and integers are big endian. A reader refuses a version it
does not know, so an older felixd fails a workload whose stream holds a newer
record instead of misreading it.

## Output stream, version 4

`wl.<id>.out` holds four kinds of record. Each begins with the version (4), a
kind byte and the writer's [epoch](/built-on-felix/felixd/terminology/#epoch).

| Kind | What it is | What follows the epoch |
|---|---|---|
| 0, output | One output of one step | the input offset that produced it, its index among that step's outputs, the output's name, an optional key, and the payload |
| 1, claim | A new owner taking the stream | the new owner's node id, in UTF-8, to the end of the record |
| 2, failure | The workload failed | the version of the spec it failed under, and the reason |
| 3, rejection | A step rejected an input and the workload carried on | the input offset, index (always 0), the error code and the detail |

An output's name is 1 to 64 bytes of `a-z`, `0-9` and `-`. Its key is optional, so
a key flag says whether a key length and a key follow.

The pieces fit together like this.

- **The claim** is what fences the stream. A new owner appends it before it resumes,
  so a stale owner's next `publish_if` is refused. Claims written before the node
  id was added end after the epoch, and a reader takes their node as unknown.
- **The failure record** is the last record of a run. An owner of the same spec
  version that finds it writes `failed` into the spec and stops.
- **A rejection** is the only output its input has. The resume fold, the checks
  that skip already written outputs, reconcile and `felixd check` match it by
  `(input offset, index)` and compare it exactly like an output, so a stream with a
  rejection where a replay produces an output, or the other way round, diverges.
- When a stream is another workload's input, readers pass over claims, failures,
  rejections and outputs under other names, and read the outputs addressed to
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
keep every chunk the live manifest references; nothing is trimmed today.

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

A build that meets a version it does not know fails the workload. The design for
rolling upgrades is that such a node stops claiming the workload without marking it
failed, and that new formats are written only after every node advertises that it
reads them ([#65](https://github.com/GetFelix/felixd/issues/65)). That is not built.

## Cache values

These are JSON today. They are to become versioned typed records
([#73](https://github.com/GetFelix/felixd/issues/73)).

| Cache | Key | Value |
|---|---|---|
| `workloads` | workload id | the spec: `module`, `input`, `output`, `snapshot_every` (default 1000), `snapshot_every_ms` (absent unless set), `state`, `limits` and, when failed, `failure`. A spec without `limits` gets the defaults |
| `leases` | workload id | `owner` and `epoch`. The epoch is 0 until the claim has taken one |
| `epochs` | workload id | the last epoch handed out, as a decimal number. It has no TTL, so an epoch outlives the lease that used it |
| `nodes` | node id | the felixd `version`, `max_workloads` and how many workloads the node `held` |
| `modules` | sha256 of the module in hex | the module's bytes |

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
| `workloads`, `leases`, `epochs`, `nodes`, `modules` | fixed |
