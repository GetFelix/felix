# Felix Durable Segment Format (v9)

This document defines the on-disk representation of a durable Felix stream. It is
the source of truth for anyone reading, writing, repairing, or replicating
segment files, and it is intentionally independent of `felix-wire`: network
framing is allowed to change shape for latency reasons, while bytes already on
disk must stay readable by every later build.

Implementation: [`crates/server/felix-storage/src/segment/format.rs`](../crates/server/felix-storage/src/segment/format.rs).
The two must move together: a change to one without the other is a bug.

## Goals

- **Versioned.** A reader that does not understand a file says so instead of
  guessing.
- **Self-validating.** Every record carries a checksum over its own header and
  payload, so bit rot and torn writes are detected on read, not inferred.
- **Skippable.** The next record's position is derivable without decoding the
  current one's payload, which is what makes index rebuilds and recovery scans
  cheap.
- **Bounded.** No length read from disk is used to size an allocation before it
  has been range-checked.

## Conventions

- All integers are **big-endian**, matching `felix-wire`.
- Offsets are **logical**: a `u64` that is stable for the life of a record and
  independent of where it lands in a file.
- A *segment* is one file of records. A *shard* is a directory of segments plus
  their indexes.

### Magic numbers

Both file kinds start with a four-byte ASCII magic sharing the prefix `FLS`
(**F**e**L**ix **S**egment), with the last byte naming the kind:

| Magic | ASCII | Expands to | File |
| --- | --- | --- | --- |
| `0x464C5347` | `FLSG` | Felix Se**g**ment | segment data (`*.log`) |
| `0x464C5349` | `FLSI` | Felix Segment **I**ndex | sparse offset index (`*.index`) |

The small state files have magics of their own, listed with each file below.
The storage id's is `0x464C4944` (`FLID`), chosen apart from `FLSI`, and the
run state's `0x464C524E` (`FLRN`), apart from the replica state's `FLRS`.
The control-plane view's is `0x464C4356` (`FLCV`).

They are deliberately distinct from `felix-wire`'s frame magic `0x464C5831`
(`FLX1`): storage bytes and network bytes are separate formats with separate
versioning, and a file that turned up on a socket, or a frame that turned up in
a segment, should be rejected on its first four bytes rather than misparsed.

## Directory layout

```text
<root>/
  storage-id                                 ← this data directory's identity
  run-state                                  ← whether the last run stopped cleanly
  registration                               ← the node incarnation of the last registration
  controlplane.view                          ← what the control plane last said about this node's shards
  record-generations                         ← a log here may hold a v9 segment
  replaced-storage                           ← the node was once registered under another storage id
  lost-copy                                  ← whether this directory ever replaced a copy the node lost
  rotated-ids                                ← ids a crash rotated away while every ballot was kept
  copy-trust                                 ← which copies a leader vouched for since the id last rotated
  copy-trust-kept                            ← a release that keeps copy-trust ran here
  run-token                                  ← this run's token, which the run witness outside the directory names
  node-id                                    ← the id offloaded keys start with, when offload is on without FELIX_NODE_ID
  acme_default_orders_0-0d3aed4b998d2798/     ← one directory per stream shard
    00000000000000000000.log                 ← segment data
    00000000000000000000.index               ← sparse offset index
    00000000000000000001.log
    00000000000000000001.index
    epochs                                   ← where each generation began
    replica                                  ← accepted generation and commit offset, once replicated
    ballot                                   ← the leader that generation was accepted from
    replica-set                              ← the replicas this one counts, and the set it is leaving
    durable.mark                             ← how far the active segment was synced
    producers                                ← producer snapshot, when any producer wrote here
    head                                     ← trim records' head entries, once the log holds a v7 segment
    offload.manifest                         ← segments copied to the object store, once offload has run
    keys.idx                                 ← a cache shard's key index, once compaction has run
  acme_default_orders_0-0d3aed4b998d2798-0006411a0ccf2e40/
                                             ← the same shard of a stream with incarnation 0x6411a0ccf2e40
  .deleted/                                  ← directories of deleted streams, waiting to be removed
  .streams/
    acme_default_orders-5c1b0e2fd3a4b7c8      ← which incarnation of a stream this broker holds
    .floors                                   ← deleted floors from catalog tombstones
```

The directory name is a readable rendering of the `ShardKey` plus an FNV-1a hash
of the exact key. The readable part is lossy (anything outside `[A-Za-z0-9-]`
becomes `_`, and components are truncated), so the hash is what guarantees
uniqueness. Dots are excluded deliberately: with no dots in the name, a
component like `..` is not merely escaped but unrepresentable.

A stream created once the fleet finalized `stream_incarnations` has an
incarnation, a number the control plane gives each creation of a name and that
only increases for the name. Its shard directories end in `-` and the
incarnation as 16 hex digits, so a stream deleted and created again under the
same name never opens the old one's directories. A stream from before the
finalize is incarnation 0 and keeps the name above. The same naming applies
under `groups/` and `dead-letters/`, which belong to a stream shard. A cache
created once the fleet finalized `cache_incarnations` has one too, and its
shard directories under `caches/` and `counters/` are named the same way; a
cache from before keeps incarnation 0 and its directory names. The cache
store's root has its own `.streams/` (markers, `.floors`, `.incarnations`, in
the formats below, one marker per cache) and `.deleted/`, and so does
`counters/` for `.deleted/`.

`.streams/` holds one marker per durable stream, and per ephemeral stream once
it has an incarnation, named like a shard directory without the shard. It is
text, replaced whole by writing a sibling, flushing it, renaming it over the
old one and flushing the directory, so a crash leaves the old marker or the
new one:

```text
felix-stream-marker 1
incarnation <decimal>
retiring <0|1>
tenant <hex of the UTF-8 bytes>
namespace <hex>
stream <hex>
shards <decimal>       (optional)
deleted <decimal>      (optional)
```

`incarnation` is the one last registered here. `retiring 1` is a delete that
started and has not finished; startup finishes it before anything registers.
`shards` is the most shards the stream had here, which a delete of
incarnation 0 reads to find the dead letters the layout before 0.6 kept per
group. `deleted` records that the control plane said every incarnation of the
name up to and including this one is deleted: their data goes, and none of
them is registered again. The optional lines appear at most once each, in this
order. A marker that does not parse (damage) is logged and read as unknown:
nothing is deleted on its account, and a record without an incarnation is
refused while an incarnated stream of the name has directories here. It never
stops a start. See `docs/durable-storage.md`, "Deleting a stream".

`.streams/.incarnations` records, written the same way, that this broker has
seen a stream with an incarnation; from then on, and from the start after a
restart, it opens a stream's logs only once the catalog has registered it.

`.streams/.floors` holds the deleted floors of catalog tombstones that had
nothing to delete here: a `felix-stream-floors 1` header, then one line per
name, `<deleted> <tenant> <namespace> <stream>` with the names in hex. It is
replaced whole and flushed like a marker, so a snapshot's tombstones cost one
flush however many names they cover. A registration at or below a name's
floor is refused, as for the `deleted` line of its marker.

`.deleted/` holds directories taken out of service when a stream is deleted.
Moving a directory there is a rename, so a crash leaves it either where it was
or where nothing opens it; the broker deletes what is there in the background
and again at startup. Being dotted, it is never read as a shard. See
`docs/durable-storage.md` ("Deleting a stream").

File names are zero-padded to 20 digits so lexicographic and numeric order agree.
Recovery still parses the number and sorts on it rather than trusting directory
iteration order, which is filesystem-defined.

A file name is a **segment id**, not an offset. The two coincide for the common
log (segment 0 begins at offset 0), but they are independent, and a shard's
first segment may begin anywhere. That is what lets a replica be given a shard
whose early history is already gone everywhere: its log *begins* at the oldest
surviving offset, and the offset is read back from the segment's own header
rather than inferred from its name. A read below that offset is `Trimmed`,
exactly as it is on a leader whose retention removed the same records.

## Segment file

A 32-byte header, then records back to back, with a sparse index in a companion
file:

<p align="center">
  <img src="assets/storage/segment-file.svg" alt="A segment file: a 32-byte header followed by variable-length records, with a sparse index file whose entries point at record boundaries" width="900">
</p>

### Segment header (32 bytes)

<p align="center">
  <img src="assets/storage/segment-header.svg" alt="Segment header byte layout: magic, version, flags, base_offset, created_at_micros, header_crc, reserved" width="882">
</p>

| Offset | Size | Field | Value |
| --- | --- | --- | --- |
| 0 | 4 | `magic` | `0x464C5347` (`"FLSG"`) |
| 4 | 2 | `version` | `3` when written, `4` for a segment that holds a generation-start record, `5` for one that holds a commit record, `6` for one that holds a record with its publisher, `7` for one that holds a trim record, `8` for one that holds a record with a key and headers, or `9` for one whose records carry a generation (see [Generations](#generations)); `2` is still read |
| 6 | 2 | `flags` | `0`; any other value is rejected |
| 8 | 8 | `base_offset` | logical offset of this segment's first record |
| 16 | 8 | `created_at_micros` | wall clock at creation, informational |
| 24 | 4 | `header_crc` | CRC-32 (IEEE) over bytes `0..24` |
| 28 | 4 | `reserved` | `0` |

The header is written once and fsynced before any record claims to live in the
segment, so damage here is never a torn write. It is always an error.

### Record

<p align="center">
  <img src="assets/storage/record.svg" alt="Record byte layout: payload_len, offset, timestamp_micros, header_crc over bytes 0 to 20, checksum over bytes 0 to 24 and the payload, then the payload" width="882">
</p>

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 4 | `payload_len` | the body's length, ≤ `MAX_PAYLOAD_BYTES` (64 MiB); bits 28 to 31: the record's kind, bit 27: the body ends with a publisher, bit 26: the body starts with a key and headers. All below |
| 4 | 8 | `offset` | logical offset; ascends by exactly 1 within a segment |
| 12 | 8 | `timestamp_micros` | publish time |
| 20 | 4 | `header_crc` | CRC-32 over bytes `0..20` |
| 24 | 4 | `checksum` | CRC-32 over bytes `0..24`, **followed by** the tag and the body |
| 28 | 20 or 8 | `tag` | 20 bytes when bit 31 is set: `producer_id u64`, `sequence u64`, `len u32`. 8 bytes on a trim record: `floor u64` |
| 28, 36 or 48 | n | body | the payload, opaque bytes; with bit 26, preceded by the key and headers and their `u32` length; with bit 27, followed by the publisher and its `u8` length |

The diagram shows a record without a tag, which is every record not written by
an idempotent producer.

`header_crc` is what makes recovery decidable. It is verified *before* any other
field is used, so `payload_len` is only ever acted on once it is known to be
intact. Without it, a bit flip in the length field produced exactly the same
symptom as an unfinished write (a record claiming more bytes than the file
holds), and recovery had to guess. Guessing wrong meant silently truncating a
record that had been fsynced and acknowledged.

`payload_len` comes first and is covered by both checksums, so a reader can step
to the next record with `position + 28 + tag + payload_len` without touching payload
bytes. That property is what makes the index rebuild and the recovery scan cost
proportional to record *count* rather than to bytes decoded.

The checksum covers the header prefix as well as the payload, so a corrupted
offset or timestamp is caught by the same check as a corrupted payload.

### Generations

From v9 every record header carries the generation of the leader that wrote
the record: its Raft term. The 8-byte field sits between the timestamp and
`header_crc`, so the header grows from 28 to 36 bytes and every later field
moves up 8:

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 20 | `payload_len`, `offset`, `timestamp_micros` | as above |
| 20 | 8 | `generation` | `u64` |
| 28 | 4 | `header_crc` | CRC-32 over bytes `0..28` |
| 32 | 4 | `checksum` | CRC-32 over bytes `0..32`, the tag and the body |
| 36 | | tag and body | as above |

The generation is inside `header_crc`, so it is checked before any field is
used and it is cut off with its record on a torn tail. A segment's version
says which layout its records have; a v8 record and a v9 record never decode
as each other, because the header checksum sits at a different place.

Generations never fall along a log. The startup scan checks this inside each
segment, and the open checks it across segments: a segment's first known
generation must not be below the last known one before it. A record whose
generation is below the one before it is `GenerationFell` corruption: every
checksum held, so it was written that way.

**When a log moves onto v9.** A broker in a cluster writes v9 only once its
fleet has finalized `record_generations`, which an operator does; a new
cluster does not. Every broker that reports the feature offers
`WRITER_GENERATIONS` to its peers, and it needs `generation_start`. Once it
is finalized, each stream log starts its next segment at v9 and stays there; the segment that was active is never
rewritten. A lone broker has no fleet and stays on the older formats. Cache
and counter logs stay on them too.

**What goes in the field.** Every record in a v9 segment carries one: events,
generation-start, commit and trim records alike. It is the generation of the
leader that wrote the record, the same on every replica:

- A leader stamps its own records with the generation it leads at, which the
  broker notes when it opens the shard to lead it. That must also be the
  generation the log accepted. A newer accepted generation means a fence from
  another leader landed, so the append is refused (`WriterUnknown`), never
  stamped with the newer generation. That holds even after the log's history
  takes the new leader's labels, since following a leader labels the history
  too. The generation must also be the newest in the log's history, starting
  at or before the append; an append that lands before the broker recorded
  where its leadership starts is refused the same way. The check runs when
  the append is staged, and the history's start for a new generation is the
  tail at the time, which an append still in flight has not moved yet. So a
  record stamped with the previous generation can sit at or past where the
  history says the next one starts. The stamp is still the true writer.
- A record the sender holds in an older segment has no stamp to send. It
  goes as generation 0, "writer unknown", and is stored that way, never with
  the generation the sender's history gives it: histories label copied
  records with the generation they were copied under, so two replicas'
  histories can disagree about one record. Generation 0 is also a shard's
  first generation, so a generation-0 leader's records read as unknown.
  That loses nothing for the floor, since nothing comes before them, and the
  switch to Raft puts every unknown record below the snapshot base (see
  "Unknown writers and the switch to Raft" in `docs/replication-engine.md`).
- A follower stamps a replicated record with the generation the leader sent
  for it (`writer_generations` on the peer wire, see
  `docs/internal-protocol.md`), never its own. The same holds for a promoted
  leader taking a replica's tail, and for a follower being rebuilt or caught
  up, since all of them copy through the same batches.
- A follower on v9 refuses a batch that does not name its writers
  (`WriterUnknown`, answered as `UnsupportedKind`) and stores nothing.

Membership and counter records are still not written.

**The floor.** Known generations along a log never fall; generation 0 is
skipped by the floor and by the startup scan's check. The writer refuses a
record below the newest known generation (`GenerationBelowFloor`) instead
of raising it, because a raised stamp would name a generation that did not
write the record. On a follower that refusal means its record before the
batch disagrees with the leader's, so it answers `LogConflict` and the
leader stops shipping to it, as for any divergence; the rebuild policy may
then rebuild it. It is not truncated automatically, since the stamps do not
say where the disagreement starts. The log keeps the floor across a roll, a
reopen and a truncation. A reopen takes it from the active segment's full
scan, or from the sealed segments before it when the active one holds no
known generation. A sealed segment is not walked for it: the open reads its
first record and its resume scan, and if neither shows a known generation
the segment keeps the floor before it. Walking it would read the whole of a
log copied from pre-v9 segments on every start. A cut takes the floor from
the header of the record just below the cut.

**Rollback.** A build that reads only up to v8 refuses a v9 segment with
`SegmentVersion`, so a broker cannot roll back past the release that wrote
one. Before the first v9 segment, a broker writes a `record-generations`
marker under the durable root and syncs it. A broker holding the marker
refuses to register with a fleet that has not finalized `record_generations`,
as the `record-headers` marker does for `record_headers`.

### Producer marks

An idempotent producer's batch is stored with its producer and sequence, so
every copy of the log says whose it is:

- **Bit 31** of `payload_len`: the record opens a batch. The 20-byte tag names
  the producer, the batch's sequence, and how many records the batch has.
- **Bit 30**: the record continues the batch the record before it belongs to.
- Both set is `RecordFlags`, and neither is an ordinary record.

Marking every record rather than only the first is what lets a log tell a
batch it holds all of from one whose leader stopped partway: a batch is held
once its last record is, and one whose next record does not continue it was
abandoned. The step to the next record is `28 + (20 if bit 31) + payload_len`,
still from the header alone.

Only a v3 or later segment may hold marks. A v2 build reading a mark's bits would see a
length past the limit, and recovery could take that for a torn tail and cut it
off; a v3 header makes the v2 build refuse the segment instead. So a v3 build
that reopens a v2 active segment rolls it before writing the first marked
record, and leaves unmarked records in it as before.

### Generation-start records

**Bit 29** of `payload_len` marks a leader's generation-start record: the first
record a leader writes at a new generation, before it serves, and again when
it opens a new stint at a generation it already started. Its payload
is the generation, a big-endian `u64`, and it has no tag. It is the replication
protocol's, not a client's: it lets the leader's quorum mark cover the records
it inherited (`docs/replication-design.md`, "The generation-start record").

It takes an offset like any record, and every reader but replication skips it:
subscriptions and replay, Kafka fetch, consumer groups, and the cache and
counter projections. Replication ships and compares it exactly as stored.

At most one of bits 29, 30 and 31 is set; any two is `RecordFlags`. (Bits 29
and 28 together are the v7 trim record.) Only a v4
segment may hold the record, for the reason only a v3 one may hold marks: a v3
build reading bit 29 would see a length past the limit and could cut the record
off as a torn tail, where a v4 header makes it refuse the segment. A v4 build
that reopens an older active segment rolls it before writing the record.

A segment is written at v4 only to hold that record, and the record is written
only once the fleet has finalized `generation_start` (see the upgrades page in
the docs site). Until then this build creates v3 segments and a v3 build can
still open everything it wrote; after it, the first record rolls the log onto a
v4 segment, and later segments stay at v4. Indexes stay at v3.

### Commit records

**Bit 28** of `payload_len` marks an atomic commit: an event and the state
updates committed with it, in one record so that replication, truncation and
the committed mark take all of it or none
([`atomic-commit.md`](atomic-commit.md)). It has no tag. The payload is the
broker's (`felix_broker::CommitRecord`), big-endian:

```text
u8   version (1)
u32  event length, then the event
u32  operation count, then per operation:
       u8   0 put, 1 delete
       u32  key length, then the key (UTF-8)
       u32  value length, then the value (put only)
```

Readers outside replication see the record as its event: subscriptions and
replay, Kafka fetch and consumer groups get the event bytes at the record's
offset. The stream shard's state view is projected from the operations.
Replication ships and compares the record exactly as stored.

Payloads are capped below 2^28, so the length never reaches bit 28. At most
one of bits 28 to 31 is set, apart from the trim record's pair. Only a v5 segment may hold the record, for the
same reason only a v4 one may hold a generation-start record, and a segment
is written at v5 only to hold one. The record is written only once the fleet
has finalized `atomic_commit`, so until then no log leaves v4.

### Publishers

**Bit 27** of `payload_len` says the record carries the principal that
published it. It is not a kind: it combines with any of bits 28 to 31. The
publisher sits at the end of the body, after the payload, followed by one byte
giving its length:

```text
u8[n]  payload
u8[m]  publisher        # m <= 255
u8     m
```

`payload_len` counts all of it, so the step to the next record is still read
from the header alone, and the checksum covers the publisher with the payload.
A length byte that claims more than the body holds is `RecordPublisher`
corruption: the checksum held, so the record was written that way. The payload
digest a producer batch is checked against covers the payload only.

Bodies are capped at 2^26 bytes, so the length never reaches bit 27. Only a
v6 segment may hold such a record, for the reason only a v5 one may hold a
commit: a v5 build reading bit 27 would see a length past the limit. A segment
is written at v6 only to hold one, and the broker writes one only once it is
enabled (the `publisher_principal` fleet feature in a cluster,
`FELIX_RECORD_PUBLISHERS` on a single broker). Until then no log leaves v5.

### Trim records

Bits 29 and 28 set together mark a trim record: a request that reads below
an offset, its floor, be refused. It is the storage half of the conditional
head trim in issue 1662. The 8-byte tag holds the floor, a big-endian `u64`.
The body is the trimming caller's headers, in the broker's layout. The step to
the next record is `28 + 8 + payload_len`, still from the header alone.

The four kind bits were all taken, so this kind is the one code that uses two
of them. Before v7 that pair was `RecordFlags` in every segment, so a segment
older than v7 that holds it is still `RecordFlags`: recovery's scan refuses
it at the same point an older build did, and the reader refuses it in a sealed
segment the scan did not visit. Before v9, any other pair or triple of kind
bits is `RecordFlags`; v9 gives three of them meaning (see "Membership and
counter records" below).

Recovery treats a trim like any other record. One cut short is a torn tail
and is repaired. One whose floor or headers fail the checksum is fatal unless
it is the last record and `repair_checksum_tail` is set.

A normal read (`read_range`) leaves trim records out, so their offsets are
gaps in what it returns, and a read that starts at one gets the next record
after it. `read_range_with_trims` returns them, for replication and for
finding the trim that set a log's floor. Once the record is durable, and on
a log held at its commit offset committed, its floor applies: a normal read
below it is `Trimmed`, and the sealed segments below it are deleted (see
`docs/durable-storage.md`, "Conditional head trim"). The `head` file below
spares an open from reading the sealed segments for them.

Only a v7 segment may hold the record, and a segment is written at v7 only
to hold one. Nothing writes one unless a caller appends a record marked as a
trim, so until then no log leaves v6, and a log that never trims is written
byte for byte as before. As with commit and generation-start records, the
roll happens before the record is written, so a crash or a failed append in
between leaves an empty v7 active segment. An older build then refuses the
log even though it holds no trim.

### Membership and counter records

v9 adds three kinds, each a code of two or three kind bits:

| Bits | Kind | Body |
| --- | --- | --- |
| 30 and 29 | membership | a change to the shard's replica set, in the replication engine's layout |
| 30 and 28 | counter delta | a signed add to one counter, in the counter store's layout |
| 30, 29 and 28 | counter checkpoint | every counter's running sum, written when deltas are compacted |

None of them has a tag, so the step to the next record is still
`36 + payload_len`. Bit 30 alone means "continues a producer batch", so every
build before v9 refuses these codes as `RecordFlags`, and so does a v9 reader
in a segment older than v9. The pairs that include bit 31 (bits 31 and 30,
31 and 29, 31 and 28) and the triples that include it stay `RecordFlags` in
every version.

Only a v9 segment holds them, and nothing writes them yet. An append that
holds one is refused (`Unsupported`) before it can roll the log onto v9. They
exist so the replication engine and the cache shard's single log (see
`docs/replication-engine.md`, decisions 1 and 4) have kinds to write once the
fleet reads v9. Recovery treats them like any other record: one cut short is a
torn tail, and a damaged one in the middle of a log stops the open.

Replication does not carry them yet either. A leader holds a follower before
the first such record (`kind_unsupported` in
`felix_broker_replication_shipped_total`) rather than send it under another
mark.

### Record kinds

Every kind takes an offset, and offsets stay contiguous whatever the kind.
Only **data** records reach a client: plain records, producer batch records
and commit records (returned as their event). Generation-start, trim,
membership and counter records are skipped by every client read:
subscriptions and replay, `StreamRead`, Kafka fetch and offset lookups,
consumer groups, cache watches, and the cache and counter projections. One
predicate, `RecordMark::is_data`, makes that call at each of those sites, so
a new kind must be decided once and cannot be missed at one site.

A skipped record still holds its offset, so a reader sees a gap there. A
subscriber that negotiated event offsets and skip counts (`docs/protocol.md`,
"Event batch offsets") is told how many offsets before a batch held no event,
so a jump it covers is not a drop and any other jump is. A consumer group's
record reports the same count. A Kafka consumer is moved past a trailing run
by an empty batch. Replication reads every kind (`read_replicated_from`),
but cannot yet ship membership and counter records (see above).

| Kind | Bits | From | Data |
| --- | --- | --- | --- |
| plain | none | v2 | yes |
| opens a producer batch | 31 | v3 | yes |
| continues a producer batch | 30 | v3 | yes |
| generation start | 29 | v4 | no |
| commit | 28 | v5 | yes |
| trim | 29 and 28 | v7 | no |
| membership | 30 and 29 | v9 | no |
| counter delta | 30 and 28 | v9 | no |
| counter checkpoint | 30, 29 and 28 | v9 | no |

### Keys and headers

**Bit 26** of `payload_len` says the record carries a key and headers, the
optional parts a Kafka record has and an MQTT message needs. Like the
publisher bit, it is not a kind and combines with any of them. The key and
headers sit at the front of the body as one block, after its length:

```text
u32    k
u8[k]  key and headers
u8[n]  payload
       publisher and its u8 length, with bit 27
```

Storage keeps the block as it is given and does not look inside it. Its
layout is the wire's record headers block, byte for byte, so the broker
stores what a publisher sent and replays it without re-encoding. See
[The record headers block](protocol.md#the-record-headers-block).

A record stored without the block has no bit 26, so it is byte for byte what
v7 wrote. A block length that claims more than the body
holds is `RecordHeaders` corruption: the checksum held, so the record was
written that way. The payload digest a producer batch is checked against
covers the payload only.

Bit 26 is also the top bit of the largest body, 2^26 bytes, which every
older build could write. So bit 26 over a zero length is that body, in any
version, and never a record with headers. A body with headers stops one
byte short of the limit, so the two never meet. Before v8, bit 26 over a
non-zero length is `RecordTooLarge`, exactly as an older build judged it.

Only a v8 segment may hold such a record, for the reason only a v6 one may
hold a publisher. A segment is written at v8 only to hold one, so a log that
never stores a key or headers stays on v7 or older, and an older build reads
it. A record refused for its size is refused before the roll. As with the
other kinds, the roll happens before the record is written, so a crash, or a
conditional append whose condition fails, in between leaves an empty v8
active segment, which an older build refuses.

Replication carries a record's key and headers block with the record, as the
leader stored it, so a follower's log is the leader's byte for byte in this
respect and a promoted follower delivers the same headers. A cluster member
accepts headers once its fleet has finalized `record_headers`, since an older
build refuses to open a v8 segment.

A data directory that stored such a record must not join a fleet without the
feature. So before it stores the first one, whether from a client, a forward
or a leader, a broker writes a `record-headers` marker file under the durable
root and syncs it. A broker holding the marker refuses to register with a
fleet that has not finalized `record_headers`, as the `head-trimmed` marker
does for `head_trim`.

A follower from an older build cannot read the block, so the leader holds
shipping at the first record that has one rather than send it bare, logs the
stall, and counts it as `headers_unsupported` in
`felix_broker_replication_shipped_total`. That follower catches up once it is
upgraded.

## Index file

Index files accelerate reads and are **never trusted**. Every entry is used only
as a starting position for a scan that re-validates real records, and any index
that fails to load (missing, short, wrong generation, garbage) is rebuilt from
its segment. Consequently they carry no checksums.

### Index header (24 bytes)

<p align="center">
  <img src="assets/storage/index-header.svg" alt="Index header byte layout: magic, version, flags, base_offset, reserved" width="672">
</p>

| Offset | Size | Field | Value |
| --- | --- | --- | --- |
| 0 | 4 | `magic` | `0x464C5349` (`"FLSI"`) |
| 4 | 2 | `version` | `3` when written; `2` and `4` are still read, the layout is the same |
| 6 | 2 | `flags` | `0` |
| 8 | 8 | `base_offset` | must equal the segment's `base_offset` |
| 16 | 8 | `reserved` | `0` |

### Index entry (16 bytes)

<p align="center">
  <img src="assets/storage/index-entry.svg" alt="Index entry byte layout: an eight-byte logical offset and an eight-byte file position" width="672">
</p>

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | `offset` |
| 8 | 8 | `position`: byte position of that record in the segment |

Entries are emitted for the segment's first record and thereafter every
`index_spacing_bytes` of segment data. They are strictly ascending by offset,
which is what `seek_position`'s binary search relies on.

Entries reach the file 256 at a time, and the rest when the segment is
sealed, so after a crash the active segment's index can be short. Recovery
scans the active segment in full and rewrites its index, so that costs nothing.
A torn final entry, the signature of a crash mid-append, is tolerated on load:
the file is read up to the last whole entry.

## Compatibility and corruption behaviour

| Condition | Behaviour |
| --- | --- |
| Unknown `magic` | Reject: `CorruptionKind::SegmentMagic` / `IndexMagic` |
| Unknown `version` | Reject: `SegmentVersion` / `IndexVersion`. Never "best effort" |
| Non-zero `flags` | Reject: `SegmentFlags`. Unknown bits may change the layout behind them, so they are not masked off |
| Bad header CRC | Reject: `SegmentHeaderChecksum` |
| Short read | `Truncated { needed, available }`: the one shape recovery may repair |
| Bad record header CRC | `RecordHeaderChecksum`: the length cannot be trusted |
| Bad record CRC | `RecordChecksum` |
| More than one kind bit set, other than bits 29 and 28 together from v7, or the membership and counter codes from v9 | `RecordFlags` |
| `payload_len` over the limit | `RecordTooLarge`, raised *before* any allocation. In a v8 segment bit 26 over a non-zero length is a flag, so every length fits |
| A publisher or a key-and-headers block that does not fit its body | `RecordPublisher`, `RecordHeaders` |
| Offset gap within a segment | `OffsetOutOfOrder` |
| A v9 record whose generation is below the record before it, in its segment or the one before | `GenerationFell` |

Every error carries a `CorruptionSite` naming the shard, segment id and byte
position, because "corruption detected" is not enough to act on at 3am.

### What recovery may repair

Recovery truncates **only** damage confined to the end of the newest segment,
because only there can a record have been mid-write when the process died:

- A `Truncated` failure is always repairable. The header verified, so
  `payload_len` is the length the writer intended, and a file ending short of it
  is *provably* an unfinished write, because nothing can have acknowledged a record that
  was never finished. This is the ordinary crash case and needs no operator
  involvement.
- A `RecordHeaderChecksum` failure is **not** repairable by default. The header
  cannot be trusted, so this may equally be an unfinished write or a complete,
  acknowledged record whose header rotted. `repair_checksum_tail` opts in.
- A `RecordChecksum` or `OffsetOutOfOrder` failure is likewise opt-in: the header
  verified, so the record is complete on disk and the damage is rot rather than a
  torn write.
- A `GenerationFell` failure is never repaired, except past the durable mark
  (below). Both checksums held, so no crash produced it.
- In a v9 segment the header is 36 bytes, generation included. A file that
  ends inside it is `Truncated`, and the zero-fill rule below measures the
  damaged header at 36 bytes too.
- A `RecordTooLarge` failure is opt-in for the same reason: the writer rejects
  oversized records, so a verified header carrying an impossible length is damage
  the checksums did not catch.
- Segment-header damage is never repairable, with one exception below: a
  blank first segment.
- **A zero-filled tail is always repairable**, whatever the failure kind and
  without `repair_checksum_tail`. After a power loss (not a process crash) the
  file size can reach disk while the data blocks it covers do not, and those
  read back as zeros inside `i_size`. The rule: every byte from the damaged
  record to end of file is zero, or the zeros start at a 512-byte boundary
  inside the damaged record and run to end of file. Nothing past those zeros
  can have been acknowledged, because an fsync that covered a later record
  covered the zeroed bytes too. Zeros followed by any non-zero byte are not
  this case and stay fatal, unless they lie past the durable mark (below).
- **Anything past the durable mark is repairable**, whatever it looks like.
  The `durable.mark` file records how far the active segment had been synced;
  bytes past it were never reported durable, so stale (non-zero) blocks or any
  other damage there is an unfinished write, not rot. Damage before the mark
  keeps the strict rules. A shard without a mark (written by an older build)
  keeps the strict rules everywhere.
- **An unfinished background roll.** The background roll installs the new
  segment before it flushes the retired one, and writes the new header without
  a flush of its own. Every flush syncs the retired segment before the active
  one, so a power loss in that window can leave only unflushed bytes damaged.
  A newest segment whose header is all zeros is discarded as an uninstalled
  roll. A torn or zero-filled tail (the two cases above) on the segment just
  before the newest is cut back. So is that segment coming back intact but
  ending before the newest one begins: its size and its pages reach the
  device separately, so the loss can land on a record boundary. The newest
  segment is kept if it starts
  exactly at the cut and discarded otherwise, since records past a gap cannot
  be kept in order. Any other damage there is still fatal, and so is a tear in
  bytes the durable mark says were synced: the mark only reaches the newest
  segment once the one before it was synced whole, so this repair never cuts
  below the mark or deletes a segment the mark vouches for. For the same
  reason, anything that seals the newest segment while that seal is still
  running (an inline roll past the overshoot ceiling, or `seal`) syncs the
  older segment first; a gap in front of a sealed segment is not repairable.
- **An empty segment inside the chain.** A background roll that loses the race
  to an inline one deletes the blank segment it built. The blank's directory
  entry was synced at creation, and the unlink is synced too, but a power loss
  before that sync brings it back as a file shorter than a header between two
  installed segments. Such a file cannot hold a record, so recovery deletes
  it, but only when the segments either side of it still meet exactly. A
  segment that held records and lost its bytes leaves an offset gap, which
  stays fatal.
- **A blank first segment.** When segment 0 is the log's only segment, it is
  no longer than a header, its header is missing or all zeros, and the durable
  mark vouches for nothing past the header, its creation failed (on a full
  disk, say) before it could hold a record. Recovery deletes it and the log
  starts again empty, or at its placed base. Creation syncs the header before
  anything can append, and preallocation does not change the file size, so a
  first segment longer than a header had a durable header. A zeroed header
  there is damage to acknowledged records and stays fatal, even when every
  byte after it is zero too.

The dividing line is whether the length is trustworthy. When it is, recovery can
prove the write was unfinished; when it is not, recovery refuses to choose
between "unfinished" and "rotted" and fails loudly instead.

The full rules live in `is_repairable_tail` and `is_zero_filled_tail` in
[`segment/scan.rs`](../crates/server/felix-storage/src/segment/scan.rs).

## Golden vectors

`format.rs` pins exact bytes for a segment header and a record. Changing either
is a format change: bump `FORMAT_VERSION`, update this document, and state the
migration path. It also pins the header CRC of every version from 2 to 9 and
every older record kind, and `writer.rs` pins the checksums of a segment and
its index at each of v3 to v6 holding every kind that version allows, so a
new kind cannot change what an older one writes.

```text
SegmentHeader::new(base_offset = 1, created_at_micros = 2):
  46 4C 53 47  00 03  00 00
  00 00 00 00 00 00 00 01
  00 00 00 00 00 00 00 02
  AD EB 65 E9
  00 00 00 00

SegmentHeader::at_version(1, 2, version = 4), for a segment holding a
generation-start record, differs only in the version and the CRC:
  version 00 04, header crc E7 D5 EE A2

encode_record(offset = 7, timestamp = 9, payload = "hi"):
  00 00 00 02
  00 00 00 00 00 00 00 07
  00 00 00 00 00 00 00 09
  C6 54 DE 27
  24 02 15 2C
  68 69

encode_record(offset = 7, timestamp = 9, payload = "hi", publisher = "al"):
  08 00 00 05
  00 00 00 00 00 00 00 07
  00 00 00 00 00 00 00 09
  7B 1D 74 DE
  51 5E 81 63
  68 69  61 6C  02

encode_record(offset = 7, timestamp = 9, payload = "hi", key and headers = "k"):
  04 00 00 07
  00 00 00 00 00 00 00 07
  00 00 00 00 00 00 00 09
  7C 17 D9 F2
  7B A6 A5 A2
  00 00 00 01  6B  68 69

encode_record_at(generation = 5, offset = 7, timestamp = 9, payload = "hi"),
in a v9 segment (header crc D0 FE 7E 9D):
  00 00 00 02
  00 00 00 00 00 00 00 07
  00 00 00 00 00 00 00 09
  00 00 00 00 00 00 00 05
  EC 4D 6F 2B
  01 BF 2C E8
  68 69

encode_record(offset = 7, timestamp = 9, trim payload = "hi", trim floor = 5):
  30 00 00 02
  00 00 00 00 00 00 00 07
  00 00 00 00 00 00 00 09
  3C 6E 25 09
  23 C7 73 0E
  00 00 00 00 00 00 00 05
  68 69
```

## Version history

| Version | Change |
| --- | --- |
| 1 | Initial format. Unreleased. |
| 2 | Added `header_crc` to the record header (24 → 28 bytes), making a corrupted length field detectable without reading the payload. |
| 3 | Producer marks: two flag bits in `payload_len` and an optional 20-byte tag. A v2 segment is read unchanged; an unmarked record is byte for byte a v2 record. |
| 4 | Generation-start records: a third flag bit in `payload_len`. Written only to hold that record, once `generation_start` is finalized; v2 and v3 segments are read unchanged. |
| 5 | Commit records: a fourth flag bit. Written only to hold one, once `atomic_commit` is finalized. |
| 6 | Publishers: bit 27 and a trailer on the body. Written only to hold a record with one, once publishers are enabled; every older segment is read unchanged, and a record without a publisher is byte for byte a v5 record. |
| 7 | Trim records: bits 29 and 28 together, and an 8-byte floor tag. Written only to hold one; every older segment is read unchanged, and every other record is byte for byte what v6 wrote. |
| 8 | Keys and headers: bit 26 and a length-prefixed block at the front of the body. Written only to hold a record with one; every older segment is read unchanged, and a record without a key or headers is byte for byte what v7 wrote. |
| 9 | Generations: an 8-byte generation in every record header (28 → 36 bytes). Membership, counter delta and counter checkpoint records. Written by stream logs once the fleet finalizes `record_generations`; membership and counter records are not written yet. Every older segment is read unchanged. |

A v1 segment is rejected on open with `CorruptionKind::SegmentVersion`, naming
the version found. v1 was only ever written by unreleased builds, so the
migration path is to discard the data directory rather than carry a second
decoder, and rejecting is the safe failure, because a v1 record read as v2
would misparse every field after the length.

## Versioning policy

`FORMAT_VERSION` is a single number covering both the segment and index layouts.

- **Additive changes** that keep existing readers correct (new `flags` bits with
  strictly appended data) still require a version bump, because current readers
  reject unknown flags rather than skipping them. That is deliberate: silently
  ignoring a bit that changes the meaning of following bytes is how formats
  become unreadable.
- **Any change to a field's position, width, or meaning** requires a bump and an
  explicit migration path. Segments already on disk are not rewritable in place.

## Limits

| Limit | Value | Why |
| --- | --- | --- |
| `MAX_PAYLOAD_BYTES` | 64 MiB | Bounds the allocation a corrupt length field can request |
| `MAX_PUBLISHER_BYTES` | 255 | The publisher's length is one byte; it is a principal id, never a token |
| Body of a record with a key and headers | `MAX_PAYLOAD_BYTES` − 1 | Bit 26 over a zero length is the legacy 2^26-byte body, so a body with headers never reaches it |
| Max records per segment | `u64` offsets, so effectively unbounded | Rollover is driven by size, not count |
| Oversized records | A record larger than `segment_size_bytes` is written to an otherwise-empty segment of its own | Splitting a record across segments would break the "offsets are contiguous within a segment" invariant that recovery depends on |

## `epochs`: the generation history

A shard directory may hold an `epochs` file beside its segments. It records
where each leadership generation began, as `(generation, start offset)` pairs,
one entry per leadership change, not per record. An entry names the generation
that wrote the records from its offset on, on a follower too: a follower copies
the entries from the leader's batches rather than labelling what it is sent
with the generation of the leader that sent it.

```text
 0   4  magic        u32  "FLEP"
 4   2  version      u16
 6   2  count        u16  entries following
 8   4  body_crc     u32  crc32 over the entries
12   …  entries           count × { generation u64, start_offset u64 }
```

Written through a temporary and a rename, so a crash leaves either the old file
or the new one. A half-written history is worse than none, because it would be
read back as a confident answer about where a generation began, and that
answer becomes a truncation point.

**Unlike the segments, this file is not authoritative and its loss is not
fatal.** Absent, short, or failing its checksum, it reads as empty: every shard
written before the file existed has none, and refusing to open those would
trade an outage for a convenience. What is lost is the ability to repair a
divergence automatically, never a record.

The entries are bounded, because every open reads the whole file and a
generation older than the oldest retained record cannot be one two brokers
diverge within.

See `docs/replication-design.md`, "Divergence and truncation", for what it is
for.

## `replica`: the accepted generation and the commit offset

A shard directory that has been replicated to, or led, holds a `replica` file:
the highest leadership generation this broker accepted a leader of the shard
at, the commit offset it knows, one past the last record a majority
acknowledged, and whether retention is held at that offset.

```text
 0   4  magic        u32  "FLRS"
 4   2  version      u16
 6   2  flags        u16  bit 0: retention held at the commit offset
 8   8  generation   u64  highest generation accepted
16   8  commit       u64  one past the last record known committed
24   4  crc          u32  crc32 over bytes 0..24
```

Written through a temporary, an fsync, a rename and a directory sync. A raised
generation is on disk before the batch that raised it is stored or
acknowledged, so a leader this broker has seen replaced is refused after a
restart too, whatever the routing view says by then.

The commit offset is written behind the advance that raised it, so no batch or
replication pass waits on this file. Under `FsyncMode::OnCommit` the write
starts at once, and one write in flight carries every advance made while it
runs, so a busy shard writes it far less often than once per batch. Under
`Periodic` and `None` it is written at most once a second, as its records are
written behind too. In every mode it is written with a raised generation and at
shutdown, and a write that fails is retried once a second until it lands,
without waiting for another batch: an idle shard sends none.

Read back behind after a crash, it permits *more* truncation than it should:
records committed since are unguarded until the leader's next batch carries the
offset again. It is never read back ahead. Why that is acceptable is in
`docs/replication-design.md`, "Divergence and truncation".

The flags were a reserved zero before the hold was recorded, and no build
checked them, so a file without the flag reads as not held and an older build
reads a file with it unchanged. The hold is written when replication turns it
on or off (`DiskLog::hold_retention_at_commit`), or when the log opens under a
`LogConfig::retention_hold` that differs from it, and restored at open, before
retention starts; see `docs/durable-storage.md`, "Retention".

**Unlike `epochs`, this file is authoritative.** Nothing in the log can rebuild
it, and reading a damaged one as zero would accept any leader, so a file that
is present and does not decode fails the open. Absent reads as zero: nothing
accepted, nothing known committed. Cache and counter compaction leave it
alone: they trim segments in place and never replace the directory.

A truncation or rebuild that would discard a record held below the commit
offset is refused with `StorageError::BelowCommit`; see
`docs/replication-design.md`, "Divergence and truncation".

The one thing that lowers the commit offset is a restore to a backup point
(`DiskLog::restore_to`), run offline against a copy: it writes the lowered
offset here first, then cuts the log to it. See `docs/durable-storage.md`,
"Restoring to a backup point".

## `ballot`: whom the generation was accepted from

Beside `replica`, once a broker that keeps ballots has accepted a generation
from a named leader: that generation and the leader's node id.

```text
 0   4  magic        u32  "FLBL"
 4   2  version      u16  1 or 2
 6   2  leader_len   u16  at most 1024
 8   8  generation   u64  the generation the leader was accepted at
16   1  flags        u8   version 2 only; bit 0 = committed, other bits 0
16+f n  leader       utf-8 node id, leader_len bytes (f = 1 in version 2, else 0)
...  4  crc          u32  crc32 over every byte before it
```

Version 2 adds `flags`. Its one bit says the vote is committed: a majority
granted it, which is what openraft's `Vote::committed` records. A ballot
without that bit is written as version 1, so only a committed vote produces a
version 2 file. A committed vote can be saved only past the same fleet gate
as segment format v9 (see [Generations](#generations)), and nothing in a
broker commits a vote yet. A
build that predates version 2 reads only version 1, so it refuses a version 2
file at open (the file "does not decode") and never ignores or rewrites it.
The refusal comes after the open's first repairs (torn-tail recovery, the
durable mark, a retention-hold change to `replica`), which are in formats both
builds read, so the newer build still opens the shard afterwards.
A version newer than the build reads fails the open with a message naming a
newer build. The crc is checked first, so a damaged version field reads as
damage, not as a newer format. An unknown flag bit, or the committed bit with
an empty leader, does not decode.

The mark belongs to the vote in the same file, so it is on disk exactly when
that vote is. The open reads it only when it takes the ballot's leader: a
ballot behind `replica`, or behind a newer `replica-set`, belongs to an older
generation, and the shard opens with no leader and no mark.

Written the same way as `replica`, and before it when a generation is raised,
so the ballot is on disk before anything from that leader is stored or
acknowledged. At the accepted generation only that leader is answered; see
`docs/replication-design.md`, "Ballots". On open, a ballot at the generation
in `replica` names its leader, one ahead of it is a raise that crashed between
the two writes and its generation is taken, and one behind it, or none, names
no leader. A file that is present and does not decode fails the open, as
`replica` does. A separate file so that a build that predates ballots ignores
it and still opens the shard.

An empty leader (`leader_len` 0) is a generation given up: a broker back from
a crash that may have cost it records it acknowledged writes it over its own
ballot, so no leader is accepted at that generation, this broker included,
and only a newer one replaces it (`DiskLog::give_up_generation`; see
`docs/replication-design.md`, "A leader's own crash").

## `replica-set`: the replicas this one counts

Beside `ballot`, once a broker that keeps replica sets has recorded the set of
an assignment: the assignment's generation, its members, and while a change of
set is pending the set being left.

```text
 0   4  magic        u32  "FLRS"
 4   2  version      u16  1
 6   2  flags        u16  bit 0: a set being left follows;
                          bit 1: a wiped mark follows;
                          bit 2: earlier sets follow; other bits must be 0
 8   .  set          the current set
 .   .  leaving      the set being left, only when flags bit 0 is set
 .   .  wiped        this copy's wiped mark, only when flags bit 1 is set:
                       2  node_len   u16  at most 1024
                       n  node       utf-8 node id the copy answers as
                       1  learner    u8   1 while still a learner, else 0
 .   .  earlier      only when flags bit 2 is set: a count (u16, 1 to 16),
                     then that many sets, newest first
 .   4  crc          u32  crc32 over everything before it
```

Each set is:

```text
 0   8  generation   u64  the assignment's generation
 8   2  count        u16  at most 64
10   .  members      count entries, each:
                       2  node_len   u16  at most 1024
                       n  node       utf-8 node id
                       1  flags     u8   bit 0: a storage id follows;
                                     bit 1: learner; bit 2: wiped;
                                     other bits must be 0
                      16  storage    the storage id the fence counts the member
                                     under, only when flags bit 0 is set
```

A learner is listed but not counted: it is re-admitted to the set and not yet
caught up, so it does not vote and no majority includes it.

Only a copy sets the wiped bit, on its own entry, when it finds it lost what it
held. It tells a wiped learner from a staged one, and outlasts the learner bit
until the copy's group logs have caught up too. The file keeps the mark a
second time, in the `wiped` section, apart from the member lists. A newer set
can list the copy only as leaving, or not at all, and name it again later; the
section survives all of those, and each newer set that lists the copy gets the
mark copied onto its entry. Only re-admission ends it. A build that predates
the mark refuses the file, as it would any unknown flag; brokers set it only
once `joint_fence` is finalized and the build allows it.

The earlier sets are older sets a fence may still need a majority of: sets
of generations this log holds no record of, and the sets their fences were
leaving. Accepting a newer set carries the kept ones forward until the log
durably holds a record of the set being replaced, so they are dropped only on
evidence that a later generation opened. They are newest first, each is older
than the current set, and none names the same members as another kept set;
a file breaking any of that is refused. A raise that adds a set is refused
once the file would hold more than 17 distinct sets, counting the current set
and the set being left, so a raise naming only kept sets always fits. A file without them is byte for byte what an older
build writes; a build that predates them refuses a file that has them, which
fails the open. Brokers write them only once `joint_fence` is finalized, so
a fleet that can still roll back has none. The one exception is a replica
that restarted and has not yet learned whether it is: it keeps them, since
dropping sets a finalized fleet needs is unsafe, and drops them at its next
set once it learns the feature is off.

A set is refused, on write and on read, when it lists a node id twice or a
storage id twice, or when the set being left is not older than the current one.
Each write has a temporary of its own (`replica-set.tmp.<pid>.<n>`), so
concurrent writes never share a file; two to one shard leave whichever renames
last, and the caller orders them. The open removes leftover temporaries.
Writing is blocking and meant for `spawn_blocking`.

Written the same way as `ballot`: a temporary file next to it,
fsynced, renamed into place, then the directory fsynced. A crash leaves the old
set or the new one, and a leftover temporary is never read. The write returns only after all of that, so a replica that
answers at a set afterwards has made a promise that survives a restart. The
pending set is cleared by writing a set without it.

A missing file means no set was ever recorded: a shard from a build that
predates it, or a new one. A file that is present and does not decode, whether
by checksum, a bad flag or count, a duplicate member, a truncation, or bytes after the last
member, fails the open, as `ballot` does: it cannot be rebuilt from the log,
and reading it as no set would let the replica count the wrong majority. A
separate file so that a build that predates it ignores it and still opens the
shard.

Nothing reads or writes it yet. The replica's set checks will, as described in
`docs/replication-design.md`, "Electing without the control plane".

## `offload.manifest`: segments with a copy in the object store

A stream shard whose broker runs with offload on (see `docs/durable-storage.md`,
"Tiered storage: offload") holds an `offload.manifest`: one entry per sealed
segment with a verified copy in the object store, ordered by base offset.

```text
 0   4  magic        u32  "FLOF"
 4   2  version      u16
 6   2  reserved     u16
 8   4  count        u32  entries that follow
12      entries, each:
         0   8  segment id      u64
         8   8  base offset     u64
        16   8  last offset     u64  inclusive
        24   8  size            u64  bytes in the object, the segment's valid bytes
        32   8  oldest          u64  timestamp_micros of the first record
        40   8  newest          u64  timestamp_micros of the last record
        48   4  checksum        u32  crc32 over the object's bytes
        52   2  key length      u16
        54   n  key             UTF-8 object key
 end-4   4  crc          u32  crc32 over every byte before it
```

A key is `<node id>/<shard directory name>/<base offset>-<segment id>.segment`,
with both numbers zero-padded to 20 digits. The node id keeps replicas of one
shard apart when brokers share an offload directory. Entries written by
0.6.0-preview.3 have keys without it and are read as they are: an entry names
its object by the full key, never by recomputing it.

Entries never overlap. Gaps between them are allowed: a segment retention
deleted while offload was off was never copied. Written through a temporary,
an fsync, a rename and a directory sync, and always before the local segment it
records is unlinked.

**This file is authoritative.** Once a segment's local file is gone, the entry
is the only record that its records still exist, so a manifest that is present
and does not decode fails the open. Absent reads as empty. A truncation drops
the entries holding offsets at or after the cut, and a reset drops them all,
both before any segment is touched.

## `producers`: the producer snapshot

Each idempotent producer's place in the log is derived from the marks: for
every producer, the newest sequence held and where its recent batches landed,
plus a batch still waiting for records. Each remembered batch also carries a
digest of its payloads, which is how a leader tells a re-send from a different
batch under the same sequence (see `docs/protocol.md`, "Idempotent producers"). Replaying the whole log on every open
would make opening a large shard slow, so the state is saved at each rollover,
as of the new segment's base offset, once the retired segment is sealed.

```text
 0   4  magic        u32  "FLPS"
 4   2  version      u16  2 (1 is still read; see below)
 6   2  reserved     u16
 8   8  as_of        u64  the state covers every record below this offset
16   4  producers    u32
20   4  body_crc     u32  crc32 over what follows
24   1  open              1 when a batch is waiting for records, then:
                          producer_id u64, sequence u64, len u32, first u64, held u32,
                          digest
     …  producers × { id u64, last_sequence u64, batches u16,
                      batches × { first u64, len u32, digest } }

digest = present u8 (0 or 1), value u64 (0 when absent)
```

The digest is CRC-64/XZ, chained over the batch's records in order: starting
from 0, each record takes it to `crc64(digest u64 ‖ r u64)`, where
`r = crc64(payload_len u64 ‖ payload)`. For an open batch it covers the
`held` records so far. It is not stored in the records: every replica computes
it from the payloads it holds, on append and when replaying the log, so it is
the same everywhere a batch is. Changing the function is a snapshot format
change.

Version 1 has no digest fields. It is still read, and its batches have no
digest, which a leader treats as matching whatever is re-sent under them: the
answer a re-send got before digests were kept. They age out as the producers
write, and the next snapshot is written as version 2. Any other version is
ignored, like a damaged snapshot.

An open reads it and replays only the marks after `as_of`. Those are normally
all in the active segment, which recovery scans in full anyway, so a current
snapshot costs no read at all. A snapshot that is missing, damaged, below the
oldest retained record or past the tail is ignored and the state is rebuilt from
the oldest record. A truncation removes it, durably, before anything is written
past the cut, since it would describe records that were replaced.

It is derived, like the index, so it is written through a temporary and a
rename but not flushed: one lost to a crash costs a longer open.

What is remembered is a function of the log. A producer is known while one of
its batches is in the log; retention that removes the last of them forgets it,
on every replica alike. Each producer keeps its last 64 batches, and past 4096
producers the one whose newest batch is oldest is forgotten.


## `head`: the trim records' head entries

One entry per trim record the log holds, which is everything needed to derive
the head floor: the floor is the largest among the entries below the apply
bound. Written at each roll once the log holds a v7 segment, as of the new
segment's base offset, after the retired segment is sealed.

```text
 0   4  magic        u32  "FLHD"
 4   2  version      u16  1
 6   8  as_of        u64  the entries cover every trim record below this offset
14   4  entries      u32
18   …  entries × { offset u64, floor u64, headers_len u32, headers }
 …   4  crc          u32  crc32 over everything before it
```

Entries are in ascending offset order. `headers` is the trim record's
payload.

It is derived and never trusted. An open uses it only when the checksum
holds, `as_of` is at or below the tail, and every entry at or above the
oldest retained offset matches the trim record the log holds there; entries
below that offset were deleted with their segments and are dropped. The trim
records after `as_of` are read from the log, from v7 segments only and header
by header, and those in the active segment come from recovery's scan. A
missing, short, stale or damaged file is rebuilt by reading every v7 segment
that way, and the open saves a fresh one. A truncation or reset removes it,
durably, before anything is written past the cut.

Like the producer snapshot it is written through a temporary and a rename but
not flushed: one lost to a crash costs a longer open.

## `keys.idx`: a cache's key index

A cache shard maps each key to the record that currently defines it. That index
lives in memory and is otherwise rebuilt by replaying the whole log when the
shard opens. At the end of each compaction pass, after the trim, it is written
here, and an open loads it and replays only the records past it. Only cache
shards have one.

```text
 0   4  magic            u32  "FXIX"
 4   2  format           u16  1
 6   1  store            u8   0 for a cache (1 is reserved for collections)
 7   8  covered_through  u64  every record below this offset is reflected, none at or past it
15   8  entry_count      u64
23   8  log_bytes        u64  payload bytes of every record below covered_through, live or not
31   4  last_checksum    u32  checksum of the record at covered_through - 1; 0 when nothing is covered
35   …  entry_count × { key_len u32, key, offset u64, version u64,
                        expires_at_millis u64, bytes u32 }
     4  crc              u32  CRC-32C over everything before it
```

Each `key` is a composite key, and entries are sorted by it, compared as bytes:

```text
kind u8 | key_len u32 | key | slot u8 | field
```

Kind 0 is a cache value, which has no slot or field. Kinds 1, 2 and 3 are
reserved for hashes, sets and lists, where slot 0 is the key's metadata and
slot 1 a field. The 28 bytes after the key are the in-memory entry: where the
record is, the version a conditional write compares against, the absolute
expiry (0 for none), and the record's payload length. `log_bytes` carries
compaction's garbage ratio across the restart.

It is derived, so an open checks it against the log before using it, and a
snapshot that fails any check is logged and ignored, and the whole log is
replayed:

- the length and the checksum match, and every key is a cache key;
- `covered_through` lies between the log's oldest offset and its tail. Below
  the oldest offset, a later pass trimmed records the snapshot never saw; past
  the tail, records it did see are gone;
- every entry points at an offset from the log's oldest up to
  `covered_through`;
- the record at `covered_through - 1` has `last_checksum`. Offsets alone cannot
  tell this log from one replication cut back and refilled to the same length.

A truncation removes it, durably, with the producer snapshot, and so does
dropping the cache's index after replication cut its log. It is written to
`keys.idx.tmp`, flushed, renamed over `keys.idx`, and the directory flushed;
the log is flushed first, so the records it covers are on the device before it
is. A temporary left by a crash is never read and is overwritten by the next
pass. Unlike the producer snapshot it is flushed: losing it costs a replay of
the whole log, which is what it exists to avoid.

It shortens an open, not memory: the index is still held whole in memory.


## `durable.mark`: how far the log was synced

A 32-byte file, rewritten in place whenever a flush, roll or seal moves it forward:

```text
 0   4  magic         u32  "FLSM"
 4   2  version       u16
 6   2  reserved      u16
 8   8  segment       u64  segment id the mark refers to
16   8  synced_bytes  u64  bytes of that segment known to be on the device
24   4  crc           u32  crc32 over bytes 0..24
28   4  reserved      u32
```

Recovery reads it before repairing anything. For segment `id`: equal to the
mark's segment, damage at or past `synced_bytes` is a torn tail; newer than
it, anything after the header is; older, the strict rules apply (a sealed
segment was synced whole). The mark is written only after the sync it
describes returns, so it never runs ahead of the device. It only moves
forward: a write that would put it behind the last one, such as a background
roll's seal finishing after a flush that already covered the new segment, is
dropped. Otherwise an idle shard, which never flushes again, would keep a mark
pointing at the new segment's header. Only a truncation or reset moves it
back, and neither runs while a seal is in flight. It is not itself fsynced per flush (that would double the flush cost), so
after a power loss it can lag by the filesystem's writeback delay; it is
synced at open (with its directory entry, so a fresh shard's first segment has
a mark), at clean shutdown and close, and after a truncation or reset. A
missing or corrupt mark reads as absent.

## `storage-id`: the data directory's identity

One file at the durable root, beside the shard directories, holding 16 random
bytes the broker generates the first time it opens the directory:

```text
 0   4  magic    u32  "FLID"
 4   2  version  u16
 6  16  id       the storage id
22   4  crc      u32  crc32 over bytes 0..22
```

Written to `storage-id.tmp`, synced, renamed over `storage-id` and the
directory synced, so a crash leaves the old id or the new one. Shard discovery
lists only directories, so neither file is read as a shard. A missing file
gets a new id, logged at info. A file that does not decode (wrong length,
magic, version or CRC) is replaced with a new id and a warning, rather than
failing the start. That includes a file a newer version wrote, read after a
downgrade: a new id only makes peers treat the directory as one that
may have lost data, which is the safe reading of damage. See
`docs/durable-storage.md`, "Storage identity".

## `run-state`: whether the last run stopped cleanly

One file at the durable root, beside `storage-id`, in one of two layouts told
apart by length and version:

```text
Version 1, 28 bytes:
 0   4  magic    u32  "FLRN"
 4   2  version  u16  1
 6   1  state    u8   1 = running, 2 = stopped cleanly
 7   1  flags    u8   bit 0: the run acknowledges records before syncing them
                      (FsyncMode::Periodic or None); other bits must be zero
 8  16  id       the storage id the run started under
24   4  crc      u32  crc32 over bytes 0..24

Version 2, 44 bytes, only for a run under way with bit 0 set:
 0   4  magic    u32  "FLRN"
 4   2  version  u16  2
 6   1  state    u8   1 = running
 7   1  flags    u8   1
 8  16  id       the storage id the run started under
24  16  boot     the machine's boot id when the run started
40   4  crc      u32  crc32 over bytes 0..40
```

A broker writes `running` before any store opens and `stopped cleanly` as the
last step of a drain that flushed everything. Both go through `run-state.tmp`,
synced, renamed and the directory synced, like `storage-id`. A relaxed run
with a boot id to name is written at version 2; every other record, a clean
stop included, at version 1, so a release that reads only version 1 still
finds a clean stop. No file means no release that writes it has run here. A
file that does not decode, or names another id than `storage-id` holds, reads
as a crash under a relaxed mode in an unknown boot. A start that finds
`running` with bit 0 set writes a new `storage-id` first, unless the record
is version 2, its boot is the one running now, and, on Linux only, a
`syncfs` of every filesystem under the data directory succeeds; so does one that, with no
file and an id loaded, finds a shard startup would repair.

The first log poisoned in a process, or a stop whose flush failed, rewrites a
version 2 record as version 1 with the same id, through `run-state.tmp` but
not synced: only a process crash has to find it. See
`docs/durable-storage.md`, "Storage identity".

## `registration`: the node's last registration

One file at the durable root, beside `run-state`, holding the node
incarnation the control plane answered at the broker's last registration:

```text
 0   4  magic        u32  "FLRG"
 4   2  version      u16
 6   8  incarnation  u64
14   4  crc          u32  crc32 over bytes 0..14
```

Rewritten after every registration through `registration.tmp`, synced, renamed
and the directory synced, like `run-state`. A cluster member's start rotates
`storage-id` when the control plane's current incarnation for the node is
ahead of this one. No file means no evidence either way. A file that does not
decode reads as older than any registration. An older release neither reads
nor writes it, so a downgrade leaves it as it was. See
`docs/durable-storage.md`, "Storage identity".

## `controlplane.view`: what the control plane last said

One file at the durable root, beside `registration`. It holds the control
plane's answers about the shards this node is in, so a later start can run
from them while the control plane is down. The envelope:

```text
 0   4  magic    u32  "FLCV"
 4   2  version  u16  1
 6   4  len      u32  body length
10 len  body     JSON, below
 ..  4  crc      u32  crc32 over every byte before it
```

The body is a JSON object. A reader ignores a field it does not know, so a
later version can add one with a default, as on the wire.

| Field | Contents |
| --- | --- |
| `node_id` | The node this view belongs to |
| `storage_id` | The storage id, in hex, that the node registered with when the view was written |
| `incarnation` | The node incarnation that registration answered |
| `fleet` | The fleet features the control plane had finalized, by name |
| `routes` | Every shard assignment that names this node as leader, replica, member of a pending set or successor, in the shape the assignment feed sends |
| `nodes` | `node_id`, `advertise_addr` and `region` of each node those routes name, where the node catalog had an address |
| `catalog` | `streams` and `caches`: the catalog entries of the streams and caches those routes belong to, in the shape the catalog feed sends |

The broker writes the file only once it has registered in this process and
taken the registration's features, a routes snapshot and a catalog snapshot.
After that it rewrites the file when the view changes, at most once a second,
and once more at a clean stop. Each write goes to `controlplane.tmp`, which
is synced and renamed over `controlplane.view`, and then the directory is
synced, like `registration`.

The file reads as absent when it is missing, shorter or longer than its
length says, fails its CRC, has another magic or version, has a body that
does not parse, or names another node. A file that reads as absent is
ignored with a warning. It never stops a start. `controlplane.tmp` is never
read. An older release neither reads nor writes the file. See
`docs/durable-storage.md`, "The control-plane view".

## `replaced-storage`: the node was registered under another id

One text file at the durable root, written when the control plane last
registered the node under a storage id other than this directory's, and never
removed:

```text
replaced
storage_id=<hex>
after=<incarnation>
```

`storage_id` is the id the directory runs under from then on, and `after` the
node incarnation the control plane held when the broker found the
replacement. A later replacement rewrites both. Before writing it the broker
removes `controlplane.view` durably, so a view beside the marker was written
after it. The incarnation alone could not tell: the control plane counts a
deleted node's incarnations from 0 again, and a restored control plane
rewinds them. The file's presence is what the replica code reads. A start
without the control plane also checks the fields: it may use a
`controlplane.view` only if the view names that storage id and an
incarnation above `after`. A file holding only `replaced`, as older releases
write it, or one that does not parse, refuses every view.

## `lost-copy`: the directory replaced a copy the node lost

One text file at the durable root, written at a start before the node
registers, holding `kept` or `lost` and a newline. It says `lost` once the
directory was found to replace a copy the node lost: a directory the control
plane last registered under another id than the one it had before this start
rotated it (a wipe, or a copy from elsewhere), or one older than the node's
last registration (a restore). Unlike `replaced-storage` it is not written
when a crash rotated the directory's own id. It never goes back to `kept`
and is never removed. Anything other than `kept` reads as `lost`. A directory
without it that has `replaced-storage` counts as `lost`, since older releases
also wrote that marker after their own crashes.

After an unclean crash, a broker attests a kept ballot to a continuing
leader (`FenceKeptOk`, `docs/internal-protocol.md`) only when this file says
`kept` and the control plane's record names the id from before the crash.
An older release neither reads nor writes the file.

## `rotated-ids`: ids a crash rotated away on this directory

One text file at the durable root, written at a start before the node
registers, through `rotated-ids.tmp`, synced, renamed and the directory
synced.

```text
rotated_ids
<storage id, 32 hex digits>
```

The first line is fixed. Each later line names an id this directory had until
an unclean crash rotated it, at a start that found every ballot kept: the
lost-copy marker says `kept`, and the control plane's record names that id.
Newest first, at most 16. A restore or a replacement for a lost copy adds
nothing. A set that names this node under one of these ids names this
directory's own copy, so it is no evidence of a wipe, while the directory
still keeps every ballot (`docs/replication-design.md`, "One fence rule for
every change of leader"). A missing file, or one that does not decode, names
no id, so such a set reads as naming a lost copy, which only costs a
readmission. So does a crash after the id rotated but before the old id was
recorded. An older release neither reads nor writes the file.

## `copy-trust`: which copies hold what they acknowledged

One text file at the durable root, kept apart from every copy's ballot so
that taking a ballot never changes it. Every start writes it if it is
missing, before anything reads it. Every write goes through `copy-trust.tmp`,
synced, renamed and the directory synced.

```text
copy_trust
storage_id <the directory's storage id, 32 hex digits>
untrusted all
untrusted except <key>
doubted <key>
unkept <key>
unkept_since <key> <generation>
readmitted <key> <generation> <storage id, 32 hex digits>
```

The first line is fixed. `storage_id` names the id the file was written
under. A file that names another id than `storage-id` holds, or none, reads
as every mark on every copy: an older release may have rotated the id after
a crash without touching this file, so after a downgrade and an upgrade its
vouches say nothing. A rotation writes the file under the id it is about to
take, before it takes it. Each later line names one of three marks and either
`all`, or a key. After `<mark> all`, `<mark> except <key>` names a copy
without the mark. Without `all`, `<mark> <key>` names a copy with it. A key
is the shard's replication key (tenant, namespace, stream, shard and kind) as
JSON, in lowercase hex. Lines are sorted.

- **untrusted**: the copy may lack records its seat acknowledged. A start
  that rotates the storage id (`run-state`) writes `untrusted all` before it
  writes the new id, so a crash in between still distrusts every copy. A copy
  loses the mark when a leader ships it a batch that leaves it level through
  the leader's generation start and the batch's commit offset, above zero,
  or, on a shard that never held a record, when an empty batch from a leader
  whose generation starts at 0 finds the copy empty
  (`docs/replication-design.md`, "One fence rule for every change of
  leader"). The next rotation marks every copy again.
- **doubted**: a copy this broker led before such a crash. Written before the
  copy takes a newer leader's ballot, and a batch that cannot write it is
  refused. Cleared when a newer leader ships the copy level with its
  generation start.
- **unkept**: a copy that took a ballot while its promises counted for
  nothing, as a learner answering a unified fence does. Written before that
  ballot, with `unkept_since` naming the newest generation such a ballot was
  taken at (absent reads as 0). Cleared only by a readmission: the copy's log
  counts a set newer than that generation naming it, under its own storage
  id, as a voter, and holds that set's generation start, shipped by that
  set's leader or a later one. A batch alone does not clear it. Such a ballot is no
  kept promise, even after a crash that keeps it.

A `readmitted` line is for a directory that lost its ballots (a wipe, a
restore, or a replacement for a lost copy). Its lost-copy marker never
clears, so on its own no copy there would keep a promise again. The line
names a copy the same kind of readmission brought back, the generation of
the set that did, and the storage id the copy had then. From then on the
copy's promises count for that set and later ones. It counts only while the
directory still has that storage id, so any rotation since, a restore among
them, ends it. The first readmission under an id stands. Writing the line
also marks the copy untrusted, so only a vouch from after the readmission
trusts it. The line counts only while the broker knows the fleet has not
finalized `replica_election` (`docs/replication-design.md`, "Readmission and
elections").

A rotation keeps `doubted`, `unkept` and the `readmitted` lines, which then
no longer match the id. A file that does not decode reads as every mark on
every copy, and the next write keeps that, so each copy clears only by the
rules above. A directory that ran before without the file, such
as one an older release ran on, starts with `untrusted all` only, and only
when `copy-trust-kept` is absent too. It doubts
nothing, since nothing says which copy it led and doubting all of them would
refuse an older caller's fence on every idle shard, and it marks nothing
unkept, since an older release never gave a learner a ballot. When the file
does not say since when a copy is unkept, the broker writes its current
ballot's generation as `unkept_since` before the copy takes a newer ballot. An older release
neither reads nor writes the file. A release from before `readmitted` lines
reads a file holding one as one that does not decode.

## `copy-trust-kept`: a release that keeps `copy-trust` ran here

One text file at the durable root holding `copy_trust` and a newline,
written, synced, the first time a start writes `copy-trust`, before it does,
and never removed. It is what tells a directory an older release ran on
from one whose `copy-trust` was lost: with it, a missing `copy-trust` reads
as every mark on every copy. It is a file of its own because `run-state` and
`storage-id` decode strictly, and an older release would take a changed one
for a crash or a new directory. An older release neither reads nor writes it.

## `run-token`: the run the witness names

One text file at the durable root: 32 lowercase hex digits, a random token
each start writes, through `run-token.tmp`, synced, renamed and the directory
synced, before any store opens. No newline.

The token pairs the directory with the **run witness**, a file outside the
directory in `/dev/shm` named `felix-run-<effective uid>-<crc32 of the
directory's absolute path, 8 hex digits>`:

```text
felix-run
boot=<the boot id, 32 hex digits>
token=<the token>
root=<the directory's absolute path>
```

The start removes the old witness before it writes the token and the new
witness after, so a crash at any step leaves no witness or one naming the new
token. A start from the control-plane view uses the directory only when the
witness matches byte for byte. See `docs/durable-storage.md`, "The run
witness". An older release neither reads nor writes either file.

## Consumer-group cursor values

A consumer group's cursor is a cache-log entry on its own root: the key is
the group's name, the value one of two layouts, told apart by length. This
is the value inside a record, not the segment format, so `FORMAT_VERSION`
does not cover it; the first byte versions the newer layout.

```text
Legacy, 8 bytes:
 0   8  offset        u64 BE  where the group resumes

Dependent, 33 bytes:
 0   1  version       u8      2
 1   8  offset        u64 BE  where the group resumes
 9   8  dlq_end       u64 BE  the shard's dead-letter log end at the commit
17   8  dlq_gen       u64 BE  the generation that wrote the entry just below
                              dlq_end (from the dead-letter log's `epochs`),
                              0 when dlq_end is 0
25   8  origin        u64 BE  the last position the group was put at needing
                              no dead letter
```

A cursor stands while the shard's dead-letter log reaches `dlq_end` and the
generation at that end is still `dlq_gen`; `dlq_end` 0 needs nothing. A
legacy value carries no dependency and is read as needing nothing, with its
offset as its origin: it was written by a build that made no such promise,
so there is nothing to check it against. Any other length, or a 33-byte
value whose first byte is not 2, is refused rather than read as an offset.

A build before the dependent layout refuses a 33-byte value, so a cluster
writes it only once the fleet finalizes `group_cursor_dependencies`; a lone
broker writes it from the start. Until then cursors are legacy values, and
a crash that keeps a cursor's tail and loses the dead letter it passed a
record on is not caught. See `docs/replication-design.md` ("Group state at
promotion").
