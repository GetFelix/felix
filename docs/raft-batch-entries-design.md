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
  next offset. This design leaves room in the header and does not decide
  #3365.
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

**A conditional batch is decided once, at the leader's append.** Its
`expected` offset is compared with the base offset it would take. If they
differ, the leader stores a refused entry, which followers copy. The whole
batch lands or none of it does, so the publisher hears one answer.

**A batch is at most one append.** The cap is `MAX_APPEND_BYTES`
(32 MiB) less framing, as for one record now. A larger publish is refused
with `TooLarge` before it takes an index. Splitting it into several entries
would bring back the cuts this design removes.

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
| `kind_tag` | 0 to 24 | producer id and first sequence, a trim floor, or a conditional's expected offset for the refused kind |
| `header_crc` | 4 | over the fields above |
| `checksum` | 4 | over the header and every record |
| records | n | `count` records, each a short header (length, timestamp, the publisher and key/headers bits) and its body |

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
| control plane floor | `offset_floor` is an index | `offset_floor` is the highest committed next offset; the exact log id stays an index |
| backup point | a committed offset | a committed offset, always at a frame end |

A subscriber that starts inside a batch gets the rest of it. Fanout keeps one
`DeliveryEnvelope` per committed batch; a late subscriber that starts inside
one gets a slice of it, encoded separately.

**Re-form.** The base is still chosen by log id, as `FelixRaftReform` checks.
The new epoch's next offset is the larger of the base's next offset and the
`offset_floor`, and it goes in the epoch record and the `raft-epoch` file
with the first index. The index needs no padding: a new epoch can start at
the base's next index, because no client sees it.

## What goes away

On the Raft path: `Piece`, `CondPiece` and the `Continues` refusal;
`log::conditional` (`resolve`, `read_range`, `read_bound`); `OpenBatch`,
`Holds::Piece` and the quiet runs in `raft/commit.rs`; the `Skips` table and
`SkipClaim`; the "send the rest" path for an idempotent batch (`NOT_THE_REST`,
`position`); gap segments; and the Kafka empty batch that moves a consumer
past a trailing run of skipped offsets.

**`skipped_before` stays, with a smaller job.** It no longer counts entries,
because no entry that holds no record takes an offset. It still counts
offsets the broker settled without delivering because retention or a trim
removed them, which is what a group record's count already covers. On a Raft
stream, a jump that `skipped_before` does not explain is a drop or a
re-form loss, never an internal entry.

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
the append and apply decides later. A watch already treats its offsets as
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
| `FelixRaftReform` | The floor is an offset; the base carries a next offset; no index padding |
| `FelixRaftPlacement` | `OffsetNeverReused` restated in offsets |
| `FelixRaftCache` | Version is the write's offset; `FloorCountsBlank` goes |
| `FelixRaftGroups`, `FelixHeadTrim`, `FelixRaftStreamPurge` | Trim floor in offsets, purge in indexes, and a purge never removes a batch at or above the floor |
| `FelixRaftReads`, `FelixRaftWritten` | Read bound and written answer in offsets |
| `FelixRaftRestore`, `FelixRaftRestoreRepeated` | A restore point is a frame end |

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
- **OffsetsDense**: within an epoch, a reader's next offset is the last one
  plus one, plus what `skipped_before` says retention removed.

## Wire and client impact

No client frame changes and no new flag. Offsets are still `u64`. Within an
epoch, consecutive records have consecutive offsets across batches, and a
consumer never sees a gap left by an internal entry. A Kafka consumer sees
dense offsets as Kafka itself gives. Publish answers keep their fields. Cache
versions get denser, and clients already treat them as opaque.
`docs/protocol.md` ("Event batch offsets") and the group record's
`skipped_before` text change to the smaller job above.

The broker-to-broker messages in `felix-replication` (`raft/wire.rs`) change:
an append carries batches. 0.8 starts fresh and does not upgrade from 0.7, so
no mixed-version group exists to negotiate with.

## Pull request sequence

1. **Models.** `FelixRaft` and `FelixRaftReform` with batch entries, the
   offset map and `BatchWhole`, run in Actions.
2. **Storage frame.** The batch frame in the segment writer, scan,
   recovery, reader and sparse index, used only by Raft logs; unit tests for
   a torn frame and for reads that start inside one. `docs/storage-format.md`.
3. **Adapter.** `raft_log` stores an entry as a frame, derives and checks
   the base offset, puts the next offset in snapshots, and serves reads by
   offset. One record per entry still, so behaviour does not change.
4. **Propose batches.** A publish is one entry; the conditional decision
   moves to the base offset; idempotent dedupe per batch; the publish answer
   and read bound in offsets. `FelixRaftBatch`, `FelixRaftIdempotent`,
   `FelixRaftReads`, `FelixRaftWritten`.
5. **Delete the piece machinery** listed in "What goes away", and retire
   `FelixRaftCond`.
6. **Re-form in offsets.** The control plane floor as an offset, the epoch's
   next offset, gap segments removed. Needs a metadata level.
7. **Groups, trims and retention** in offsets, with their models.
8. **Cache shards**: switch to offsets, then batch writes per entry.
9. **Docs and status**: `docs/protocol.md`, `docs/durable-storage.md`,
   `docs/per-shard-raft.md` and the docs-site status tables.

Steps 2 to 6 are the large ones and each needs its own adversarial review.
The perf check (RF 3 cells, instructions per message) runs on step 4, where
the change in openraft entries per record shows.

## Format v9 freeze

Steps 2, 3 and 6 change bytes on disk: the frame, the snapshot's next
offset, the `raft-epoch` file and the meaning of the control plane floor.
They must land before v9 freezes. Step 5 removes the piece codes, which
should also land first so the frozen format does not carry dead codes. Steps
4, 7 and 8 change behaviour inside a format that already allows them and
could follow the freeze, though landing them first is simpler.

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

## Open questions

1. **Cache writes refused at apply.** Should they keep their offsets (this
   design) or should offsets be given only to applied writes, which would
   move offset assignment from append to apply?
2. **`skipped_before` on a re-form loss.** A jump to the new epoch's first
   offset is real loss. This design does not count it in `skipped_before`,
   so a consumer reads it as a drop. Confirm that is the wanted signal.
3. **The batch cap.** 32 MiB per append, or lower to bound seek cost and
   head-of-line delay.
4. **#3365.** Whether to reserve a stream id in the frame header now, before
   the freeze, so several streams can share a group later without another
   format change.
