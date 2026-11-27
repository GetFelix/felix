# Raft entries as record batches: design

Status: decided, not built. Tracks
[#3359](https://github.com/GetFelix/felix/issues/3359). Building starts once
#3351, #3353 and #3329 are on main, and everything that changes bytes on disk
lands before format v9 freezes (see "Format v9 freeze" below).

Decided: in 0.8 a Raft entry is a record batch, with the base offset and the
record count in the batch header. A record's offset and its entry's Raft index
become separate numbers. Entries that hold no record take no offset, and a
batch commits or is cut as a unit.

## What the code does today

Checked against `7c5f53d`.

`felix_storage::raft_log` stores one record per openraft entry, and the
record's offset is the entry's index (`raft_log.rs` module doc,
`codec::entry_from_record`). A publish of `n` records is `n` entries
(`felix-replication` `raft/propose.rs`, the `commands` built per record).
The issue's four costs hold, with these corrections and additions:

- **Not every batch is rebuilt after a cut.** `RecordMark::Piece` marks the
  records of an idempotent producer's batch and `CondPiece` those of a
  conditional batch. A plain batch, with no producer and no expected
  offset, carries neither. A leader change that commits a prefix of it
  leaves that prefix readable as data while the publisher gets an unknown
  outcome. The issue understates the first cost.
- **One more kind of entry takes an offset.** A conditional publish that
  lands at the wrong index is stored as a blank (`EntryCodec::blank_at`),
  and so is every piece of a cut conditional batch. Both are counted in
  `skipped_before` with blanks, membership, trims and group writes.
- **Cache shards share the problem.** A cache write's version is the index
  of its entry (`raft/cache/state.rs`), and a cache watch resumes from an
  index. Blanks and membership changes take versions too.
- **The offset floor is an index.** The control plane's `offset_floor` is
  the highest committed index a leader reported, and a re-form pads its base
  up to it with a gap segment (`FOLLOWS_GAP`, `docs/storage-format.md`, "Gap
  segments").
- **Sharing a group needs more than this.** Separating index and offset is
  needed for several streams to share a group (#3365), but not enough: the
  batch would also have to name its stream, and each stream keep its own
  next offset. This design reserves the stream id in the frame header and
  does not decide the rest of #3365.
- **Two docs are stale.** `docs/storage-format.md` says nothing writes a
  conditional piece and the client read paths do not resolve one, and
  `RecordMark::Membership` says nothing writes one. `propose.rs` writes
  conditional pieces for any conditional batch of more than one record,
  `StreamLog::read_log_from` (`felix-broker` `durable.rs`) resolves them, and the Raft codec writes
  membership records. All three go away with this change.

## The entry

An entry is one batch. Its header names what it is, the generation of its
leader (its term plus one, as now), and the offsets it holds:

| Kind | Count | Notes |
| --- | --- | --- |
| data | 1 or more | a publish; an idempotent one also carries producer id and first sequence |
| commit | 1 | an atomic commit record, as now |
| cache | 1 or more | a cache shard's writes; see "Cache shards" |
| blank | 0 | a new leader's no-op |
| membership | 0 | payload is the engine's |
| trim | 0 | carries the floor, an offset |
| group | 0 | a consumer group's write |
| refused | 0 | a conditional batch that did not land at its expected offset |

`base_offset` is the offset of the batch's first record. For an entry with
count 0 it is the next offset, so a reader can find it but it holds none.

**Every member derives the base offset.** Entry `i` starts where entry `i-1`
ends: `base(i) = base(i-1) + count(i-1)`. By log matching, two members that
agree on entry `i-1` agree on `base(i)`, so it needs no extra agreement. The
leader stamps it in its own append, as `blank_at` decides at the final index
today. A follower computes it and refuses an append whose stamped value
differs; that is a bug, never a guess. Two places set a start that is not
derived: a snapshot carries the next offset after its last entry, and a new
re-form epoch carries the next offset of its base (see "Re-form" below).

**Every member decides a conditional batch from its own derived base.**
The entry carries the batch and its `expected` offset. Each member, leader
and follower alike, compares `expected` with the base offset it derived for
the entry and stores either the batch or a refused entry. A follower does
not take the outcome from the leader's stamp; it checks the stamp against
its own decision, and a disagreement is an error. The decision depends only
on the entry and the log before it, so every member reaches the same one.
The whole batch lands or none of it does, so the publisher hears one answer.

**A batch is at most one append.** The cap is `MAX_APPEND_BYTES`
(32 MiB) less framing, as for one record now. A larger publish is refused
with `TooLarge` before it takes an index. Splitting it into several entries
would bring back the cuts this design removes. The cap is decided for 0.8;
seek cost inside a large frame and the replication delay a large entry puts
on small ones are measured later and may lower it.

## On disk

The segment's unit becomes the batch frame. It replaces the v9 record frame
on a Raft log; nothing else writes v9 once the hybrid is gone.

| Field | Size | Notes |
| --- | --- | --- |
| `frame_len` | 4 | bytes after this field; the kind in the top bits, as now |
| `index` | 8 | the Raft index |
| `generation` | 8 | term plus one |
| `base_offset` | 8 | |
| `count` | 4 | records in the frame |
| `stream` | 4 | the stream the offsets belong to; reserved, see below |
| `kind_tag` | 0 to 24 | producer id and first sequence, a trim floor, or a conditional's expected offset for the refused kind |
| `header_crc` | 4 | over the fields above |
| `checksum` | 4 | over the header and every record |
| records | n | `count` records, each a short header (length, timestamp, the publisher and key/headers bits) and its body |

**The stream id is reserved now** so a group shared by several streams
(#3365) needs no second format change. In a group of one stream, which is
every group in 0.8, it is always 0, and an entry with count 0 also carries
0. A reader refuses a frame with any other value until shared groups exist,
since it would otherwise read another stream's offsets as its own. The
field is covered by `header_crc`.

What this keeps and what it changes:

- **Recovery works in frames.** A torn tail is a frame cut short and is
  dropped whole, which is what Raft expects of an entry. Interior corruption
  stays fatal. Records are never rewritten.
- **The step to the next frame is from the header alone**, so the index
  rebuild still costs per frame, not per byte. Records inside a frame are
  stepped by their own lengths without decoding payloads.
- **The sparse index holds both numbers**: `(index, base_offset,
  position)`. Both rise along the file, so one index answers a Raft read by
  index and a client read by offset. It is still derived and rebuilt from
  the segment when missing or short.
- **The segment header** keeps `base_offset` and adds the first index. Gap
  segments go: a re-form's jump in offsets is the epoch's first entry
  starting past the previous end, which the frame already says.
- **Bits 31 and 30 and the conditional piece code** are no longer written.
  `RecordMark::Piece`, `CondPiece` and `Continues` leave the Raft path.

## Offsets and indexes

| Operation | Today | With batch entries |
| --- | --- | --- |
| Raft read, append, truncate, purge | by index | by index, unchanged |
| client read, subscribe, seek to `o` | by offset = index | sparse index to the frame holding `o`, then from `o` inside it |
| read bound | first offset of an undecided conditional batch, else one past the last applied index | one past the last offset of the last applied entry |
| publish answer | the batch's first index | the batch's base offset, from the leader's append |
| trim floor, retention | offset = index | an offset; a sealed segment is deleted once its last offset is below the floor |
| snapshot | last log id | last log id and next offset |
| `raft-marks`, vouch rule | last log id | unchanged: both are about log ids |
| control plane floor | `offset_floor` is an index | `offset_floor` is the highest committed next offset; the exact log id stays an index and its report adds the entry's next offset |
| `raft-reserved` | the highest index a copy may count as flushed | that index and the highest next offset, both synced before a count |
| re-form and restore start | one index from every bound below | a first index and a first offset, each from its own bounds |
| backup point | a committed offset | a committed offset, always at a frame end |

A subscriber that starts inside a batch gets the rest of it. Fanout keeps one
`DeliveryEnvelope` per committed batch; a late subscriber that starts inside
one gets a slice of it, encoded separately.

**Re-form and restore start in both numbers.** The base is still chosen by
log id, as `FelixRaftReform` checks. Today one first index is the maximum of
several bounds (`raft_copy/vouch.rs`, `first_index`), and while an offset is
an index each bound also stands in for an offset. Once an entry holds many
records that stops being true: a relaxed copy can count an entry of 1000
records before its fsync, a power cut takes it, and a bound kept in indexes
would pad by one where 1000 offsets were handed out. So every bound gets an
offset form, and the epoch starts at a first index and a first offset, each
the maximum of its own column:

| Bound | First index from | First offset from |
| --- | --- | --- |
| the base | the base's next index | the base's next offset |
| control plane floor | the reported log id's index plus one | `offset_floor`, and the reported entry's next offset |
| `raft-reserved` | reserved index plus one | reserved next offset |
| carried trim floor | the trim entry's index plus one | the trim's floor |
| any copy's end (`held`) | the copy's next index | the copy's next offset |
| earlier epochs | each base's first index | each base's first offset |
| restore time line | the index line | the offset line (below) |

`raft-reserved` keeps its job in offsets: a raise covers the entry that
needs it plus `STEP` offsets past its end, so a single large entry never
outruns its own reservation, and the file gains the offset (a new version).
The rotation carries both. The epoch record, the `raft-epoch` file and the
bases the control plane keeps gain the first offset and the end of the lost
range (see "What goes away").

Three places outside `vouch.rs` fold the two numbers into one today and must
split them: `offset_floor` and `request`'s planned floor in
`felix-broker-service` `shards/raft/alone_cluster.rs`, and the abandon bound
(`floor.index.max(floor.offset_floor)`) in `felix-controlplane-service`
`model/shard/raft_reform/abandon.rs`.

**The restore time line moves to offsets.** It rests on a shard giving out
at most 2^13 offsets per millisecond (`docs/per-shard-raft.md`, "Re-forming
a group", the restore time line). With one record per entry, the entry rate
bounds that. With batch entries it does not: one 32 MiB entry of 20-byte
records is about 1.6 million offsets, some 200 ms of the allowance at once.
The line and its gap checks (`restore_line_clears`, `LineBehind`) are
restated in offsets, and the rate is enforced instead of assumed:

- **A token bucket per shard, at propose.** It fills at 2^13 offsets per
  millisecond on a monotonic clock, up to a fixed capacity. A publish takes
  its record count. A publish may take the bucket into debt by up to one
  entry, so a batch that is large but within the cap is never refused for
  its size; every later publish waits until the bucket is positive again.
- **Only at propose, never in the log append.** Blanks, membership changes,
  trims, group writes and the commits that ride on heartbeats take no
  offsets and never wait on the bucket, so the cap cannot stall a leader's
  own entries or an election.
- **The line's gap covers the burst.** Over any span of `t` ms a shard gives
  out at most `2^13 * t` offsets plus the bucket's capacity plus one entry's
  worth (32 MiB over the smallest record frame). The line rises at twice the
  rate, so the restore gap is raised by that burst, in offsets.

The index line stays for indexes, whose rate the entry count still bounds.

## What goes away

On the Raft path: `Piece`, `CondPiece` and the `Continues` refusal;
`log::conditional` (`resolve`, `read_range`, `read_bound`); `OpenBatch`,
`Holds::Piece` and the quiet runs in `raft/commit.rs`; the `Skips` table and
`SkipClaim`; the "send the rest" path for an idempotent batch (`NOT_THE_REST`,
`position`); and gap segments. The Kafka empty batch that moves a consumer
past a trailing run of offsets with no record stays (`felix-kafka`
`records.rs`, used from `api/fetch.rs`): internal entries no longer cause
such a run, but a re-form jump or a trim at the end of a page still does.

**`skipped_before` stays, with a different job.** It no longer counts
entries, because no entry that holds no record takes an offset. It counts
offsets the broker settled without delivering, for one of three reasons:

- **removed**: retention or a trim deleted them before the reader got
  there, which is what a group record's count already covers.
- **lost in re-form**: offsets in `[base next offset, offset_floor)`. The
  control plane saw them committed, and the re-form's base does not hold
  them, so acknowledged records there were lost (a documented loss,
  `docs/per-shard-raft.md`, "Re-forming a group").
- **skipped by re-form**: the rest of the jump, from `offset_floor` (or the
  base's next offset, if higher) to the epoch's first offset. These are the
  padding bounds: reservations, a copy's end, earlier epochs, the time line.
  No one saw them committed. Some may have held a record a relaxed copy
  counted before a power cut took it, so a reader must not read them as
  empty either; the reason says only that the broker cannot tell.

A reader is never left to take a re-form jump for a drop it caused itself,
nor for a hole that was always empty. The broker knows the split from the
epoch's start: the first offset and the end of the lost range are stored
with it, and the first entry of the epoch reads them. Every other skipped
run is a removal. A batch whose skipped run holds more than one reason is
split, so each batch names one. How the reason reaches a client is under "Wire and client
impact".

## Batch atomicity under a leader change

A batch is one entry, so Raft commits it whole or truncates it whole. No
member's log, and no reader, ever holds part of one. The publisher of an
entry lost in a leader change still gets an unknown outcome, as now. An
idempotent re-send is checked against whole batches: the producer table
holds the batch or does not, so the re-send is refused or written once.
Kafka's per-record sequences map to a first sequence and a count.

## Consumer groups and cache shards

**Groups.** Group writes become count-0 entries, folded in index order as
now. Acknowledged offsets in them are stream offsets and do not change. The
group record's `skipped_before` keeps counting retention holes and stops
counting blanks.

**Cache shards.** A cache proposal becomes one entry with one record per
write, and a write's version is `base_offset + position`. Every write takes
its offset whether it applies or is refused, because the offset is set at
the append and apply decides later (decided). A watch already treats its offsets as
sparse (`docs/protocol.md`, cache watches), so a refused write looks like any
other change it does not match. Blanks and membership no longer take
versions. The first step keeps one write per entry, so the switch and the
batching land separately.

## The TLA models

Each code PR changes its models in the same PR.

| Model | Change |
| --- | --- |
| `FelixRaft` | Entries carry a count; a member's offsets are the union of `[base, base + count)` over its committed entries. `OffsetsDense` drops `skipped` |
| `FelixRaftBatch` | Most of it models pieces, cuts and finishing a cut batch. Shrinks to whole-batch dedupe, and its cut knobs go |
| `FelixRaftCond` | Retired. Its two checks become trivial; the refused entry is checked in `FelixRaft` |
| `FelixRaftIdempotent` | Dedupe per batch, not per piece |
| `FelixRaftReform` | The floor is an offset; the epoch starts at a first index and a first offset, each the maximum of its own bounds; entries hold several records; relaxed fsync, so a power cut takes an entry counted before its fsync; `raft-reserved` in offsets |
| `FelixRaftPlacement` | `OffsetNeverReused` restated in offsets |
| `FelixRaftCache` | Version is the write's offset; `FloorCountsBlank` goes |
| `FelixRaftGroups`, `FelixHeadTrim`, `FelixRaftStreamPurge` | Trim floor in offsets, purge in indexes, and a purge never removes a batch at or above the floor |
| `FelixRaftReads`, `FelixRaftWritten` | Read bound and written answer in offsets |
| `FelixRaftRestore` | A restore point is a frame end |
| `FelixRaftRestoreRepeated` | The time line and its gap checks in offsets, the leader's offset rate cap, relaxed-fsync loss and `raft-reserved` in offsets |

The invariants, restated:

- **OffsetNeverReused**: no offset is ever given out with two records, in
  one epoch or across epochs. An offset names entry `e` and position `p`
  with `base(e) <= o < base(e) + count(e)`; two members, or two epochs, that
  deliver `o` deliver the same record.
- **AckedSurvive**: every record of an acknowledged batch is on the disks of
  a quorum, and in the log of every later leader, unless it is a documented
  re-form loss. An acknowledged offset is below every later epoch's first
  offset.
- **BatchWhole** (new): no committed log and no read holds a proper prefix of
  a proposed batch. This replaces `WholeBatchesOnly` and `ReadsStable`.
- **OffsetsDense**: a reader's next offset is the last one plus one, plus
  `skipped_before`. Within an epoch the count is only ever a removal; at an
  epoch's first entry it may be a re-form loss, and the reason says which.
- **LossReported** (new, `FelixRaftReform`): every offset in
  `[base next offset, offset_floor)` is delivered to a reader as a
  lost-in-re-form count, every other offset the re-form skips as a
  skipped-by-re-form count, and none as nothing.
- **ReservedCovers** (new, `FelixRaftReform`, `FelixRaftRestoreRepeated`):
  every offset a copy counted toward a commit is below a synced reservation,
  so no re-form or restore gives it out again.

## Wire and client impact

Offsets are still `u64`. Within an epoch, consecutive records have
consecutive offsets across batches, and a consumer never sees a gap left by
an internal entry. A Kafka consumer sees dense offsets as Kafka itself gives.
Publish answers keep their fields. Cache versions get denser, and clients
already treat them as opaque.

The one wire addition is the skip reason. It is part of the 0.8 baseline
feature set (#3362), not a negotiated feature: 0.8 starts fresh with no 0.7
peer to stay compatible with, so every 0.8 client and broker speaks it and
there is no bit to offer and no path for a client without it.

- **Event batches.** The 0.8 layout of a batch with `skipped_before`
  (`0x0800`) has a `u8 skip_reason` after it: 0 removed, 1 lost in re-form,
  2 skipped by re-form.
  A count that would hold both kinds of run is split across two batches,
  so one batch carries one reason.
- **Group records.** A `skipped_reason` field (`"removed"`,
  `"lost_in_reform"` or `"skipped_by_reform"`) beside `skipped_before`, sent whenever the count is
  not 0.
- **Cache watches** get the same field on their change frames.
- **The clients** (Rust, and the others in `clients/`) surface the reason
  on the delivered batch and in their gap handling, so an application can
  tell "the broker lost these" from "retention removed these".

`docs/protocol.md` ("Event batch offsets", the group record, cache watches)
changes with the wire step. The reason is also logged and counted on the
broker when a re-form loss is first delivered.

The broker-to-broker messages in `felix-replication` (`raft/wire.rs`) change:
an append carries batches. 0.8 starts fresh and does not upgrade from 0.7, so
no mixed-version group exists to negotiate with.

## Pull request sequence

Two rules fix the order. First, every quantity a reader or publisher
compares with an offset (read bound, publish answer, a conditional's
`expected`, cache versions and watch offsets, trim and retention floors)
moves from index to offset in the same step that lets an offset differ from
its index. Before that step every entry takes at least one offset, so the
two stay equal. Second, the re-form and restore bounds move to offsets before
any entry holds more than one record. Until then an offset is never above
its index, so index bounds still cover offsets.

| After step | Offset of an entry | Read bound, publish answer, `expected`, cache versions, floors | Re-form and restore bounds | Records per entry |
| --- | --- | --- | --- | --- |
| 1, 2 | equals its index | index (= offset) | index | 1 |
| 3 | equals its index: a count-0 entry still advances the offset by one | index (= offset) | index | 1 |
| 4 | at or below its index: count-0 entries take none | offset | index, which still covers offsets | 1 |
| 5, 6 | at or below its index | offset | offset | 1 |
| 7 on | unrelated to its index | offset | offset | 1 or more |

1. **Models.** `FelixRaft`, `FelixRaftReform` and `FelixRaftRestoreRepeated`
   with batch entries, the offset map, relaxed-fsync loss, reservations in
   offsets, the propose-time rate cap, `BatchWhole`, `LossReported` and
   `ReservedCovers`, run in Actions.
2. **Storage frame.** The batch frame in the segment writer, scan,
   recovery, reader and sparse index, used only by Raft logs; unit tests for
   a torn frame and for reads that start inside one. `docs/storage-format.md`.
3. **Adapter.** `raft_log` stores an entry as a frame, derives and checks
   the base offset, puts the next offset in snapshots, and serves reads by
   offset. Every entry advances the offset by `max(count, 1)`, so offsets
   still equal indexes and behaviour does not change.
4. **Offsets leave indexes.** Count-0 entries stop taking offsets. In the
   same step the read bound, the publish answer, a conditional's comparison,
   cache versions and watch offsets, and trim and retention floors become
   offsets, with their models (`FelixRaftReads`, `FelixRaftWritten`,
   `FelixRaftCache`, `FelixRaftGroups`, `FelixHeadTrim`,
   `FelixRaftStreamPurge`). What readers see changes here: the
   `skipped_before` counts and their tests, the Kafka trailing-run test, and
   the `docs/protocol.md` and `docs/storage-format.md` text on skipped
   offsets.
5. **Re-form and restore in offsets.** Every bound in "Re-form and restore
   start in both numbers": `raft_copy/vouch.rs`, `raft-reserved` in
   offsets, the epoch's first offset and lost range, the restore time line
   in offsets, and the propose-time token bucket. The control plane floor:
   `model/shard/raft_floor.rs` (`offset_floor.max(report.index)`) keeps an
   offset from the report's next offset, and the broker's copy of it
   (`shards/watch.rs`, `RaftFloor`) gains the field. The folded bounds split
   in `shards/raft/alone_cluster.rs` (`offset_floor`, `request`) and
   `raft_reform/abandon.rs`. Gap segments removed. Needs a metadata level.
6. **Skip reasons on the wire.** `skip_reason` in the 0.8 event batch layout
   and `skipped_reason` on group records and cache watches, as baseline
   (#3362), the client changes and `docs/protocol.md`. Lands with step 5.
7. **Propose batches.** A publish is one entry; every member decides a
   conditional from its own base; idempotent dedupe per batch.
   `FelixRaftBatch`, `FelixRaftIdempotent`. Needs steps 5 and 6 on main.
8. **Delete the piece machinery** listed in "What goes away", and retire
   `FelixRaftCond`.
9. **Cache shards batch writes per entry.** Versions are already offsets
   from step 4.
10. **Docs and status**: `docs/durable-storage.md`, `docs/per-shard-raft.md`
   and the docs-site status tables.

Steps 2 to 8 are the large ones and each needs its own adversarial review.
Step 4 is the one where a missed index-to-offset conversion exposes
uncommitted offsets, so its review checks every comparison against a
committed position. The perf check (RF 3 cells, instructions per message)
runs on step 7, where the change in openraft entries per record shows.

## Format v9 freeze

Every step through 9 lands before v9 freezes. Steps 2, 3 and 5 change bytes
on disk: the frame (with the reserved stream id), the snapshot's next offset,
`raft-reserved`, the `raft-epoch` file and the control plane's floor and
bases. Step 8 removes the piece codes, so the frozen format carries no dead
codes. Steps 4, 7 and 9 add no new fields but change what stored values mean:
whether a count-0 entry takes an offset, how many records a frame holds, and
cache versions in snapshots. A log written before them would read
differently after, which a frozen format cannot allow.

## Risks

- **This touches append, recovery, reads and replication at once**, late in
  0.8. The model goes first, and each step is reviewed on its own.
- **A base offset that two members derive differently reuses offsets.** The
  follower's check catches a disagreement at append. The two places that set
  a start, snapshot install and epoch start, are where a bug would hide, and
  both are modelled.
- **A read that seeks inside a large batch reads the whole frame** to verify
  its checksum, up to 32 MiB. If that shows in seek latency, a lower batch
  cap or per-record checksums inside the frame are the fixes.
- **One checksum covers a batch.** A bad byte in one record fails the whole
  frame; interior corruption is fatal either way.
- **Large entries can delay small ones** behind them in replication. Pacing
  is by bytes already; check it still bounds the delay.
- **Throughput and IPM move.** Fewer openraft entries should help;
  `scripts/perf/ipm-baseline.toml` will need a new baseline, justified by
  the RF 3 runs.

## Decisions

Decided on #3359:

1. A cache write refused at apply keeps its offset ("Cache shards").
2. An offset jump from a re-form loss is reported through `skipped_before`
   with a lost-in-re-form reason, never left for a reader to take as a
   silent drop ("What goes away", "Wire and client impact"). The reason is
   0.8 baseline wire, not negotiated (#3362).
3. The batch cap stays at 32 MiB, the append limit. Seek cost and
   replication delay are measured later.
4. The frame reserves a stream id before the v9 freeze, 0 in a group of
   one stream ("On disk").
