# Felix Durable Segment Format (v3)

This document defines the on-disk representation of a durable Felix stream. It is
the source of truth for anyone reading, writing, repairing, or replicating
segment files, and it is intentionally independent of `felix-wire`: network
framing is allowed to change shape for latency reasons, while bytes already on
disk must stay readable by every later build.

Implementation: [`crates/server/felix-storage/src/segment/format.rs`](../crates/server/felix-storage/src/segment/format.rs).
The two must move together — a change to one without the other is a bug.

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

Both file kinds start with a four-byte ASCII magic sharing the prefix `FLS` —
**F**e**L**ix **S**egment — with the last byte naming the kind:

| Magic | ASCII | Expands to | File |
| --- | --- | --- | --- |
| `0x464C5347` | `FLSG` | Felix Se**g**ment | segment data (`*.log`) |
| `0x464C5349` | `FLSI` | Felix Segment **I**ndex | sparse offset index (`*.index`) |

They are deliberately distinct from `felix-wire`'s frame magic `0x464C5831`
(`FLX1`): storage bytes and network bytes are separate formats with separate
versioning, and a file that turned up on a socket — or a frame that turned up in
a segment — should be rejected on its first four bytes rather than misparsed.

## Directory layout

```text
<root>/
  acme_default_orders_0-0d3aed4b998d2798/     ← one directory per stream shard
    00000000000000000000.log                 ← segment data
    00000000000000000000.index               ← sparse offset index
    00000000000000000001.log
    00000000000000000001.index
    epochs                                   ← where each generation began
    replica                                  ← accepted generation and commit offset, once replicated
    durable.mark                             ← how far the active segment was synced
    producers                                ← producer snapshot, when any producer wrote here
```

The directory name is a readable rendering of the `ShardKey` plus an FNV-1a hash
of the exact key. The readable part is lossy — anything outside `[A-Za-z0-9-]`
becomes `_`, and components are truncated — so the hash is what guarantees
uniqueness. Dots are excluded deliberately: with no dots in the name, a
component like `..` is not merely escaped but unrepresentable.

File names are zero-padded to 20 digits so lexicographic and numeric order agree.
Recovery still parses the number and sorts on it rather than trusting directory
iteration order, which is filesystem-defined.

A file name is a **segment id**, not an offset. The two coincide for the common
log — segment 0 begins at offset 0 — but they are independent, and a shard's
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
| 4 | 2 | `version` | `4` when written; `2` and `3` are still read |
| 6 | 2 | `flags` | `0`; any other value is rejected |
| 8 | 8 | `base_offset` | logical offset of this segment's first record |
| 16 | 8 | `created_at_micros` | wall clock at creation, informational |
| 24 | 4 | `header_crc` | CRC-32 (IEEE) over bytes `0..24` |
| 28 | 4 | `reserved` | `0` |

The header is written once and fsynced before any record claims to live in the
segment, so damage here is never a torn write — it is always an error.

### Record

<p align="center">
  <img src="assets/storage/record.svg" alt="Record byte layout: payload_len, offset, timestamp_micros, checksum, then the payload. The checksum covers bytes 0 to 20 and the payload" width="882">
</p>

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 4 | `payload_len` | low 29 bits: ≤ `MAX_PAYLOAD_BYTES` (64 MiB); top three bits: the record's kind, below |
| 4 | 8 | `offset` | logical offset; ascends by exactly 1 within a segment |
| 12 | 8 | `timestamp_micros` | publish time |
| 20 | 4 | `header_crc` | CRC-32 over bytes `0..20` |
| 24 | 4 | `checksum` | CRC-32 over bytes `0..24`, **followed by** the tag and the payload |
| 28 | 20 | `tag` | only when bit 31 is set: `producer_id u64`, `sequence u64`, `len u32` |
| 28 or 48 | n | `payload` | opaque bytes |

The diagram shows a record without a tag, which is every record not written by
an idempotent producer.

`header_crc` is what makes recovery decidable. It is verified *before* any other
field is used, so `payload_len` is only ever acted on once it is known to be
intact. Without it, a bit flip in the length field produced exactly the same
symptom as an unfinished write — a record claiming more bytes than the file
holds — and recovery had to guess. Guessing wrong meant silently truncating a
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

Only a v3 segment may hold marks. A v2 build reading a mark's bits would see a
length past the limit, and recovery could take that for a torn tail and cut it
off; a v3 header makes the v2 build refuse the segment instead. So a v3 build
that reopens a v2 active segment rolls it before writing the first marked
record, and leaves unmarked records in it as before.

### Generation-start records

**Bit 29** of `payload_len` marks a leader's generation-start record: the first
record a promoted leader writes at its generation, before it serves. Its payload
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
that reopens an older active segment rolls it before writing the record. Every
segment this build creates is v4, so a v3 build cannot open a log this one has
rolled, whether or not it ever wrote the record.

## Index file

Index files accelerate reads and are **never trusted**. Every entry is used only
as a starting position for a scan that re-validates real records, and any index
that fails to load — missing, short, wrong generation, garbage — is rebuilt from
its segment. Consequently they carry no checksums.

### Index header (24 bytes)

<p align="center">
  <img src="assets/storage/index-header.svg" alt="Index header byte layout: magic, version, flags, base_offset, reserved" width="672">
</p>

| Offset | Size | Field | Value |
| --- | --- | --- | --- |
| 0 | 4 | `magic` | `0x464C5349` (`"FLSI"`) |
| 4 | 2 | `version` | `4` when written; `2` and `3` are still read, the layout is the same |
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
| 8 | 8 | `position` — byte position of that record in the segment |

Entries are emitted for the segment's first record and thereafter every
`index_spacing_bytes` of segment data. They are strictly ascending by offset,
which is what `seek_position`'s binary search relies on.

A torn final entry — the signature of a crash mid-append — is tolerated on load:
the file is read up to the last whole entry.

## Compatibility and corruption behaviour

| Condition | Behaviour |
| --- | --- |
| Unknown `magic` | Reject: `CorruptionKind::SegmentMagic` / `IndexMagic` |
| Unknown `version` | Reject: `SegmentVersion` / `IndexVersion`. Never "best effort" |
| Non-zero `flags` | Reject: `SegmentFlags`. Unknown bits may change the layout behind them, so they are not masked off |
| Bad header CRC | Reject: `SegmentHeaderChecksum` |
| Short read | `Truncated { needed, available }` — the one shape recovery may repair |
| Bad record header CRC | `RecordHeaderChecksum` — the length cannot be trusted |
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
  is *provably* an unfinished write — nothing can have acknowledged a record that
  was never finished. This is the ordinary crash case and needs no operator
  involvement.
- A `RecordHeaderChecksum` failure is **not** repairable by default. The header
  cannot be trusted, so this may equally be an unfinished write or a complete,
  acknowledged record whose header rotted. `repair_checksum_tail` opts in.
- A `RecordChecksum` or `OffsetOutOfOrder` failure is likewise opt-in: the header
  verified, so the record is complete on disk and the damage is rot rather than a
  torn write.
- A `RecordTooLarge` failure is opt-in for the same reason — the writer rejects
  oversized records, so a verified header carrying an impossible length is damage
  the checksums did not catch.
- Segment-header damage is never repairable.
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
  46 4C 53 47  00 04  00 00
  00 00 00 00 00 00 00 01
  00 00 00 00 00 00 00 02
  E7 D5 EE A2
  00 00 00 00

encode_record(offset = 7, timestamp = 9, payload = "hi"):
  00 00 00 02
  00 00 00 00 00 00 00 07
  00 00 00 00 00 00 00 09
  C6 54 DE 27
  24 02 15 2C
  68 69
```

## Version history

| Version | Change |
| --- | --- |
| 1 | Initial format. Unreleased. |
| 2 | Added `header_crc` to the record header (24 → 28 bytes), making a corrupted length field detectable without reading the payload. |
| 3 | Producer marks: two flag bits in `payload_len` and an optional 20-byte tag. A v2 segment is read unchanged; an unmarked record is byte for byte a v2 record. |
| 4 | Generation-start records: a third flag bit in `payload_len`. v2 and v3 segments are read unchanged. |

A v1 segment is rejected on open with `CorruptionKind::SegmentVersion`, naming
the version found. v1 was only ever written by unreleased builds, so the
migration path is to discard the data directory rather than carry a second
decoder — and rejecting is the safe failure, because a v1 record read as v2
would misparse every field after the length.

## Versioning policy

`FORMAT_VERSION` is a single number covering both the segment and index layouts.

- **Additive changes** that keep existing readers correct — new `flags` bits with
  strictly appended data — still require a version bump, because current readers
  reject unknown flags rather than skipping them. That is deliberate: silently
  ignoring a bit that changes the meaning of following bytes is how formats
  become unreadable.
- **Any change to a field's position, width, or meaning** requires a bump and an
  explicit migration path. Segments already on disk are not rewritable in place.

## Limits

| Limit | Value | Why |
| --- | --- | --- |
| `MAX_PAYLOAD_BYTES` | 64 MiB | Bounds the allocation a corrupt length field can request |
| Max records per segment | `u64` offsets, so effectively unbounded | Rollover is driven by size, not count |
| Oversized records | A record larger than `segment_size_bytes` is written to an otherwise-empty segment of its own | Splitting a record across segments would break the "offsets are contiguous within a segment" invariant that recovery depends on |

## `epochs` — the generation history

A shard directory may hold an `epochs` file beside its segments. It records
where each leadership generation began, as `(generation, start offset)` pairs —
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
read back as a confident answer about where a generation began — and that
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

## `replica` — the accepted generation and the commit offset

A shard directory that has been replicated to, or led, holds a `replica` file:
the highest leadership generation this broker accepted a leader of the shard
at, and the commit offset it knows, one past the last record a majority
acknowledged.

```text
 0   4  magic        u32  "FLRS"
 4   2  version      u16
 6   2  reserved     u16
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

## `producers` — the producer snapshot

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


## `durable.mark` — how far the log was synced

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
