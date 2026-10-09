# Felix Durable Segment Format (v6)

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

They are deliberately distinct from `felix-wire`'s frame magic `0x464C5831`
(`FLX1`): storage bytes and network bytes are separate formats with separate
versioning, and a file that turned up on a socket, or a frame that turned up in
a segment, should be rejected on its first four bytes rather than misparsed.

## Directory layout

```text
<root>/
  node-id                                    ← the id offloaded keys start with, when offload is on without FELIX_NODE_ID
  acme_default_orders_0-0d3aed4b998d2798/     ← one directory per stream shard
    00000000000000000000.log                 ← segment data
    00000000000000000000.index               ← sparse offset index
    00000000000000000001.log
    00000000000000000001.index
    epochs                                   ← where each generation began
    replica                                  ← accepted generation and commit offset, once replicated
    ballot                                   ← the leader that generation was accepted from
    durable.mark                             ← how far the active segment was synced
    producers                                ← producer snapshot, when any producer wrote here
    offload.manifest                         ← segments copied to the object store, once offload has run
    keys.idx                                 ← a cache shard's key index, once compaction has run
```

The directory name is a readable rendering of the `ShardKey` plus an FNV-1a hash
of the exact key. The readable part is lossy (anything outside `[A-Za-z0-9-]`
becomes `_`, and components are truncated), so the hash is what guarantees
uniqueness. Dots are excluded deliberately: with no dots in the name, a
component like `..` is not merely escaped but unrepresentable.

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
| 4 | 2 | `version` | `3` when written, `4` for a segment that holds a generation-start record, `5` for one that holds a commit record, or `6` for one that holds a record with its publisher; `2` is still read |
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
| 0 | 4 | `payload_len` | the body's length, ≤ `MAX_PAYLOAD_BYTES` (64 MiB); bits 28 to 31: the record's kind, bit 27: the body ends with a publisher. Both below |
| 4 | 8 | `offset` | logical offset; ascends by exactly 1 within a segment |
| 12 | 8 | `timestamp_micros` | publish time |
| 20 | 4 | `header_crc` | CRC-32 over bytes `0..20` |
| 24 | 4 | `checksum` | CRC-32 over bytes `0..24`, **followed by** the tag and the body |
| 28 | 20 | `tag` | only when bit 31 is set: `producer_id u64`, `sequence u64`, `len u32` |
| 28 or 48 | n | body | the payload, opaque bytes; with bit 27, followed by the publisher and its `u8` length |

The diagram shows a record without a tag, which is every record not written by
an idempotent producer.

`header_crc` is what makes recovery decidable. It is verified *before* any other
field is used, so `payload_len` is only ever acted on once it is known to be
intact. Without it, a bit flip in the length field produced exactly the same
symptom as an unfinished write (a record claiming more bytes than the file
holds), and recovery had to guess. Guessing wrong meant silently truncating a
record that had been fsynced and acknowledged.

`payload_len` comes first and is covered by both checksums, so a reader can step
to the next record with `position + 28 + payload_len` without touching payload
bytes. That property is what makes the index rebuild and the recovery scan cost
proportional to record *count* rather than to bytes decoded.

The checksum covers the header prefix as well as the payload, so a corrupted
offset or timestamp is caught by the same check as a corrupted payload.

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
record a leader writes at a new generation, before it serves. Its payload
is the generation, a big-endian `u64`, and it has no tag. It is the replication
protocol's, not a client's: it lets the leader's quorum mark cover the records
it inherited (`docs/replication-design.md`, "The generation-start record").

It takes an offset like any record, and every reader but replication skips it:
subscriptions and replay, Kafka fetch, consumer groups, and the cache and
counter projections. Replication ships and compares it exactly as stored.

At most one of bits 29, 30 and 31 is set; any two is `RecordFlags`. Only a v4
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
one of bits 28 to 31 is set. Only a v5 segment may hold the record, for the
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
| More than one of the three kind bits set | `RecordFlags` |
| `payload_len` over the limit | `RecordTooLarge`, raised *before* any allocation |
| Offset gap within a segment | `OffsetOutOfOrder` |

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
migration path.

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
restart too, whatever the routing view says by then. Under `FsyncMode::OnCommit`
the commit offset is written through on every advance, before the
batch that carried it is acknowledged. Under `Periodic` and `None` it is written
at most once a second and at shutdown, as its records are written behind too;
read back behind after a crash, it permits *more* truncation than it should:
records committed since are unguarded until the leader's next batch carries the
offset again.

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
 4   2  version      u16
 6   2  leader_len   u16  at most 1024
 8   8  generation   u64  the generation the leader was accepted at
16   n  leader       utf-8 node id, leader_len bytes
16+n 4  crc          u32  crc32 over bytes 0..16+n
```

Written the same way as `replica`, and before it when a generation is raised,
so the ballot is on disk before anything from that leader is stored or
acknowledged. At the accepted generation only that leader is answered; see
`docs/replication-design.md`, "Ballots". On open, a ballot at the generation
in `replica` names its leader, one ahead of it is a raise that crashed between
the two writes and its generation is taken, and one behind it, or none, names
no leader. A file that is present and does not decode fails the open, as
`replica` does. A separate file so that a build that predates ballots ignores
it and still opens the shard.

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

A 32-byte file, rewritten in place after every flush:

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
describes returns, so it never runs ahead of the device. It is not itself
fsynced per flush (that would double the flush cost), so after a power loss it
can lag by the filesystem's writeback delay; it is synced at open (with its
directory entry, so a fresh shard's first segment has a mark), at clean
shutdown and close, and after a truncation or reset. A missing or corrupt mark
reads as absent.
