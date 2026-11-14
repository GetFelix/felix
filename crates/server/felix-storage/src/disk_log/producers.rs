//! Each idempotent producer's place in a shard's log, derived from the log.
//!
//! A producer's batches are stored with marks (`RecordMark`): the first record
//! names the producer, the sequence and the batch length, and the rest say
//! they continue it. Replaying the marks gives, per producer, the newest
//! sequence held and where its recent batches landed. Because the marks are
//! in the records, every replica that holds a batch knows whose it is, so a
//! promoted leader, a move's destination and a restarted broker all answer a
//! re-send the way the leader that took it would have.
//!
//! What is remembered is a function of the log alone: a producer is known
//! while one of its batches is still in the log, and forgotten once retention
//! has removed them all or it is the least recently written of more than
//! [`MAX_PRODUCERS`]. The snapshot written at each rollover only saves
//! rescanning sealed segments on open; losing it costs a longer open, never
//! an answer. See `docs/storage-format.md`, "Producer state".
//!
//! Each remembered batch also keeps a [`PayloadDigest`] of its payloads,
//! computed from the records as they are observed, so a leader can tell a
//! re-send from a different batch reusing the sequence.
//!
//! A batch whose writer stopped partway is remembered as cut, with how many
//! of its records the log holds. On a Raft log the later records are pieces
//! (`RecordMark::Piece`) that name the batch and their place in it, so the
//! missing ones finish the batch wherever they land, and a log that has
//! lost a batch's first record still knows the batch is there. See
//! `docs/storage-format.md`, "Producer marks".

use felix_common::random::HashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use super::segments::SegmentSet;
use crate::log::{BatchPiece, Offset, PayloadDigest, ProducerBatch, RecordMark};
use crate::metrics_names;
use crate::segment::{ReadBudget, TrimRecords};

/// Batches remembered per producer. A re-send follows its original closely, so
/// this covers a producer's in-flight pipeline rather than its history.
pub(crate) const WINDOW: usize = 64;

/// Producers remembered per shard. Past this the one whose newest batch is
/// oldest is forgotten, and its next batch is refused as unknown.
pub(crate) const MAX_PRODUCERS: usize = 4096;

/// Cut batches remembered whole per shard. Past this the oldest keeps only
/// its producer and sequence, enough to refuse its re-send.
pub(crate) const MAX_CUTS: usize = MAX_PRODUCERS;

/// Where a producer's batch stands in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerSequence {
    /// The log holds no batch from this producer.
    Unknown,
    /// The next batch this producer owes.
    Next,
    /// Already held, at these offsets (inclusive). `digest` is `None` for a
    /// batch known only from a snapshot written before digests were kept.
    Held {
        first: Offset,
        last: Offset,
        digest: Option<PayloadDigest>,
    },
    /// The batch at the tail, of which only the first `held` of `len` records
    /// arrived: a leader that stopped partway through writing or shipping it.
    /// `digest` covers the `held` records, and is `None` as for [`Self::Held`].
    Partial {
        first: Offset,
        held: u32,
        len: u32,
        digest: Option<PayloadDigest>,
    },
    /// Past the next expected: batches in between never arrived.
    Gap { expected: u64 },
    /// Older than the batches still remembered.
    Expired,
}

/// A batch the log holds some records of and not all: open at the tail, or
/// cut by a record that did not continue it.
///
/// A writer that appends the records from position `held` on, as pieces
/// ([`RecordMark::Piece`]), finishes it. The log then holds the batch once,
/// as [`ProducerSequence::Held`] from `first` to the last piece.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnfinishedBatch {
    /// The offset of the batch's first record. `None` when the batch cannot
    /// be finished: the log no longer holds it from its first record
    /// (retention, or a copy that starts inside it), or its pieces do not
    /// follow one another. Writing any of it again would repeat the records
    /// that are here, so its re-send has to be refused.
    pub first: Option<Offset>,
    /// The position the batch's next record has.
    pub held: u32,
    pub len: u32,
    /// Of the `held` records. `None` when `first` is, and for a batch known
    /// only from a snapshot written before digests were kept.
    pub digest: Option<PayloadDigest>,
}

/// Every producer a shard's log holds batches for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProducerState {
    producers: HashMap<u64, Producer>,
    /// The newest batch, while records of it are still to come.
    open: Option<OpenBatch>,
    /// Per producer, its newest batch if the log holds only part of it and
    /// another record has followed. It stays until the producer opens a
    /// batch again or the last record held of it leaves the log.
    cuts: HashMap<u64, OpenBatch>,
    /// Per producer, the sequence of a cut batch the table no longer holds:
    /// a purge passed its last record, or it went past [`MAX_CUTS`]. Some of
    /// it was written, so its re-send, or anything older, is refused until
    /// a purge passes a later batch of the producer.
    forgotten: HashMap<u64, u64>,
    /// Whether cut batches are kept: on a Raft log, where pieces can finish
    /// one. Anywhere else a cut batch is abandoned and written again whole.
    /// Not part of the snapshot.
    keeps_cuts: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Producer {
    /// Sequence of the newest batch held.
    last_sequence: u64,
    /// The newest batches, newest last; the last entry is `last_sequence`,
    /// and the ones before it count down by one.
    recent: VecDeque<HeldBatch>,
}

impl Producer {
    fn last_offset(&self) -> Offset {
        self.recent.back().map(|batch| batch.last).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeldBatch {
    first: Offset,
    /// Not `first` plus the length for a batch finished after a cut: other
    /// records lie between its pieces.
    last: Offset,
    /// The batch's length when its later records are pieces, zero
    /// otherwise. A log that has lost the first record of such a batch
    /// still sees the rest of it.
    pieces: u32,
    digest: Option<PayloadDigest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenBatch {
    batch: ProducerBatch,
    /// `None` when the batch cannot be finished; see
    /// [`UnfinishedBatch::first`].
    first: Option<Offset>,
    /// The position its next record has.
    held: u32,
    /// One past the last record held of it.
    next: Offset,
    /// Whether a piece has continued it.
    pieces: bool,
    /// Of the `held` records so far.
    digest: Option<PayloadDigest>,
}

impl OpenBatch {
    fn is(&self, producer_id: u64, sequence: u64) -> bool {
        self.batch.producer_id == producer_id && self.batch.sequence == sequence
    }

    fn unfinished(&self) -> UnfinishedBatch {
        UnfinishedBatch {
            first: self.first,
            held: self.held,
            len: self.batch.len,
            digest: self.digest,
        }
    }

    /// What is left of the batch in a log that starts at `base`. Without
    /// pieces only its first record says whose it is, so it goes with that
    /// record. With them it stays, unfinishable, while one is still held.
    fn pruned(self, base: Offset) -> Option<Self> {
        match self.first {
            Some(first) if first >= base => Some(self),
            _ if self.pieces && self.next > base => Some(Self {
                first: None,
                digest: None,
                ..self
            }),
            _ => None,
        }
    }
}

impl ProducerState {
    #[cfg(test)]
    pub(crate) fn keeping_cuts() -> Self {
        Self {
            keeps_cuts: true,
            ..Self::default()
        }
    }

    /// Where `sequence` stands for `producer_id`.
    pub(crate) fn classify(&self, producer_id: u64, sequence: u64) -> ProducerSequence {
        if self.is_forgotten(producer_id, sequence) {
            return ProducerSequence::Expired;
        }
        if let Some(open) = &self.open
            && open.is(producer_id, sequence)
        {
            return match open.first {
                Some(first) => ProducerSequence::Partial {
                    first,
                    held: open.held,
                    len: open.batch.len,
                    digest: open.digest,
                },
                None => ProducerSequence::Expired,
            };
        }
        let Some(producer) = self.producers.get(&producer_id) else {
            return ProducerSequence::Unknown;
        };
        let expected = producer.last_sequence + 1;
        if sequence == expected {
            return ProducerSequence::Next;
        }
        if sequence > expected {
            return ProducerSequence::Gap { expected };
        }
        let back = (producer.last_sequence - sequence) as usize;
        match producer.recent.len().checked_sub(back + 1) {
            Some(index) => {
                let batch = producer.recent[index];
                ProducerSequence::Held {
                    first: batch.first,
                    last: batch.last,
                    digest: batch.digest,
                }
            }
            None => ProducerSequence::Expired,
        }
    }

    /// The batch `sequence` of `producer_id` if the log holds part of it and
    /// not all. [`Self::classify`] does not count a cut batch, so a writer
    /// that can finish one asks this first.
    pub(crate) fn unfinished(&self, producer_id: u64, sequence: u64) -> Option<UnfinishedBatch> {
        self.open
            .iter()
            .chain(self.cuts.get(&producer_id))
            .find(|open| open.is(producer_id, sequence))
            .map(OpenBatch::unfinished)
            .or_else(|| {
                (self.forgotten.get(&producer_id) == Some(&sequence)).then_some(UnfinishedBatch {
                    first: None,
                    held: 0,
                    len: 0,
                    digest: None,
                })
            })
    }

    fn is_forgotten(&self, producer_id: u64, sequence: u64) -> bool {
        self.forgotten
            .get(&producer_id)
            .is_some_and(|cut| sequence <= *cut)
    }

    /// The sequence `producer_id` owes next, or `None` for a producer the log
    /// holds no batch from.
    pub(crate) fn next_sequence(&self, producer_id: u64) -> Option<u64> {
        self.producers
            .get(&producer_id)
            .map(|producer| producer.last_sequence + 1)
    }

    /// Whether a batch is waiting for more of its records. While one is, an
    /// unmarked append has to be observed too, since it ends the batch.
    pub(crate) fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// Account for the record at `offset`, whose payload's
    /// [`marked_digest`] is `record`. Records are observed in offset order; an
    /// unmarked one only matters while a batch is open, and may be skipped
    /// otherwise.
    pub(crate) fn observe(&mut self, offset: Offset, mark: RecordMark, record: u64) {
        match mark {
            RecordMark::Continues => {
                if let Some(open) = &mut self.open
                    && open.next == offset
                {
                    open.held += 1;
                    open.next = offset + 1;
                    open.digest = open.digest.map(|digest| digest.then(record));
                    if open.held >= open.batch.len {
                        let open = self.open.take().expect("open batch");
                        self.complete(open, offset);
                    }
                } else {
                    // Its opening record is not here (trimmed, or before the
                    // base a replica was rebuilt at), so the batch is unusable.
                    self.cut_open();
                }
            }
            RecordMark::Opens(batch) => {
                // Whatever wrote a batch still open stopped, and the records
                // after it belong to someone else.
                self.cut_open();
                // The producer has started over: nothing finishes its cut
                // batch now.
                self.cuts.remove(&batch.producer_id);
                let open = OpenBatch {
                    batch,
                    first: Some(offset),
                    held: 1,
                    next: offset + 1,
                    pieces: false,
                    digest: Some(PayloadDigest::EMPTY.then(record)),
                };
                if batch.len <= 1 {
                    self.complete(open, offset);
                } else {
                    self.open = Some(open);
                }
            }
            RecordMark::Piece(piece) => self.observe_piece(offset, piece, record),
            RecordMark::None
            | RecordMark::GenerationStart
            | RecordMark::Commit
            | RecordMark::Trim { .. }
            | RecordMark::Membership
            | RecordMark::CounterDelta
            | RecordMark::CounterCheckpoint => self.cut_open(),
        }
    }

    fn observe_piece(&mut self, offset: Offset, piece: BatchPiece, record: u64) {
        let known = match self
            .open
            .take_if(|open| open.batch.producer_id == piece.producer_id)
        {
            Some(open) => Some(open),
            None => {
                self.cut_open();
                self.cuts.remove(&piece.producer_id)
            }
        };
        let open = match known {
            Some(known)
                if known.batch.sequence == piece.sequence
                    && known.batch.len == piece.len
                    && known.held == piece.position =>
            {
                OpenBatch {
                    held: known.held.saturating_add(1),
                    next: offset + 1,
                    pieces: true,
                    digest: known.digest.map(|digest| digest.then(record)),
                    ..known
                }
            }
            // The records before this one are not all here, or not in
            // order. What is here must never be written again, so the
            // batch is kept as one that cannot be finished.
            _ => OpenBatch {
                batch: ProducerBatch {
                    producer_id: piece.producer_id,
                    sequence: piece.sequence,
                    len: piece.len,
                },
                first: None,
                held: piece.position.saturating_add(1),
                next: offset + 1,
                pieces: true,
                digest: None,
            },
        };
        if open.held < open.batch.len {
            self.open = Some(open);
        } else if open.first.is_some() {
            self.complete(open, offset);
        } else {
            self.cut(open);
        }
    }

    /// The open batch, if any, will not be continued where it stands.
    fn cut_open(&mut self) {
        if let Some(open) = self.open.take() {
            self.cut(open);
        }
    }

    fn cut(&mut self, open: OpenBatch) {
        if !self.keeps_cuts {
            return;
        }
        self.cuts.insert(open.batch.producer_id, open);
        if self.cuts.len() > MAX_CUTS
            && let Some(oldest) = self
                .cuts
                .iter()
                .min_by_key(|(_, cut)| cut.next)
                .map(|(id, _)| *id)
        {
            if let Some(cut) = self.cuts.remove(&oldest) {
                self.forget(cut);
            }
            self.producers.remove(&oldest);
        }
    }

    /// Close an open batch the log has moved past without finishing: the next
    /// record is at `tail` and did not continue it.
    pub(crate) fn settle(&mut self, tail: Offset) {
        if self.open.as_ref().is_some_and(|open| open.next < tail) {
            self.cut_open();
        }
    }

    /// Forget batches retention removed, and producers left with none.
    pub(crate) fn prune(&mut self, base: Offset) {
        if let Some(open) = self.open.take() {
            match open.pruned(base) {
                Some(open) => self.open = Some(open),
                None if self.keeps_cuts => self.forget(open),
                None => {}
            }
        }
        for (_, cut) in std::mem::take(&mut self.cuts) {
            match cut.pruned(base) {
                Some(cut) => {
                    self.cuts.insert(cut.batch.producer_id, cut);
                }
                None => self.forget(cut),
            }
        }
        let mut headless = Vec::new();
        let forgotten = &mut self.forgotten;
        self.producers.retain(|id, producer| {
            let mut sequence = producer
                .last_sequence
                .wrapping_sub(producer.recent.len() as u64)
                .wrapping_add(1);
            while let Some(batch) = producer.recent.front().copied()
                && batch.first < base
            {
                producer.recent.pop_front();
                // A purged record is committed, so no truncation brings the
                // producer back below this batch. An open alone could.
                if forgotten.get(id).is_some_and(|cut| *cut <= sequence) {
                    forgotten.remove(id);
                }
                // The producer's newest batch, written as pieces, with
                // some still here: a log read from `base` sees them.
                if producer.recent.is_empty() && batch.pieces != 0 && batch.last >= base {
                    headless.push(OpenBatch {
                        batch: ProducerBatch {
                            producer_id: *id,
                            sequence,
                            len: batch.pieces,
                        },
                        first: None,
                        held: batch.pieces,
                        next: batch.last + 1,
                        pieces: true,
                        digest: None,
                    });
                }
                sequence = sequence.wrapping_add(1);
            }
            !producer.recent.is_empty()
        });
        let open = self.open.map(|open| open.batch.producer_id);
        for batch in headless {
            // A batch the producer opened later has replaced it.
            if open != Some(batch.batch.producer_id)
                && !self.cuts.contains_key(&batch.batch.producer_id)
            {
                self.cut(batch);
            }
        }
    }

    /// Forget every unfinished batch with a record below `base`, ahead of a
    /// reset that drops the log and starts it again at `base`.
    pub(crate) fn forget_before(&mut self, base: Offset) {
        if !self.keeps_cuts {
            return;
        }
        let unfinished: Vec<_> = self
            .open
            .take()
            .into_iter()
            .chain(std::mem::take(&mut self.cuts).into_values())
            .collect();
        for batch in unfinished {
            if batch.first.is_none_or(|first| first < base) {
                self.forget(batch);
            }
        }
    }

    /// The cut batches the table no longer holds, by producer.
    pub(crate) fn forgotten(&self) -> &HashMap<u64, u64> {
        &self.forgotten
    }

    /// Take in forgotten batches saved earlier. The higher sequence wins.
    fn remember(&mut self, saved: HashMap<u64, u64>) {
        for (id, sequence) in saved {
            let kept = self.forgotten.entry(id).or_insert(sequence);
            *kept = (*kept).max(sequence);
        }
    }

    /// Keep only enough of a cut batch to refuse its re-send. Written again
    /// it would repeat the records the log held of it.
    fn forget(&mut self, cut: OpenBatch) {
        let kept = self
            .forgotten
            .entry(cut.batch.producer_id)
            .or_insert(cut.batch.sequence);
        *kept = (*kept).max(cut.batch.sequence);
    }

    /// `last` is the offset of the batch's last record.
    fn complete(&mut self, open: OpenBatch, last: Offset) {
        let OpenBatch {
            batch,
            first,
            pieces,
            digest,
            ..
        } = open;
        // A batch without its first record is cut, never completed.
        debug_assert!(
            first.is_some(),
            "completed a batch whose first record is gone"
        );
        let Some(first) = first else { return };
        let producer = self
            .producers
            .entry(batch.producer_id)
            .or_insert_with(|| Producer {
                last_sequence: batch.sequence,
                recent: VecDeque::new(),
            });
        // The window has to be consecutive sequences. Anything else -- the
        // first batch seen, or one after a history this log never held --
        // starts it again.
        if producer.recent.is_empty() || batch.sequence != producer.last_sequence + 1 {
            producer.recent.clear();
        }
        producer.last_sequence = batch.sequence;
        if producer.recent.len() == WINDOW {
            producer.recent.pop_front();
        }
        producer.recent.push_back(HeldBatch {
            first,
            last,
            pieces: if pieces { batch.len } else { 0 },
            digest,
        });
        if self.producers.len() > MAX_PRODUCERS
            && let Some(coldest) = self
                .producers
                .iter()
                .min_by_key(|(_, producer)| producer.last_offset())
                .map(|(id, _)| *id)
        {
            self.producers.remove(&coldest);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.producers.len()
    }
}

// The snapshot file.
//
// magic "FLPS" u32, version u16, reserved u16, as_of u64, producers u32,
// crc32 u32 over everything after the header, then:
//   open batch: present u8, and when 1 a batch in progress
//   cut batches: count u32, then per batch a batch in progress
//   forgotten cuts: count u32, then per cut producer_id u64, sequence u64
//   per producer: id u64, last_sequence u64, batches u16,
//                 then per batch: first u64, last u64, pieces u32, digest
// A batch in progress is producer_id u64, sequence u64, len u32,
// has_first u8, first u64 (zero when absent), held u32, next u64, pieces u8,
// digest. A digest is present u8 then value u64 (zero when absent).
//
// Version 1 has no digest fields. Its batches read as having none, and a
// re-send of one is answered the way it was before digests were kept.
// Versions 1 to 3 have no forgotten cuts. Versions 1 and 2 have no cut batches. Their open batch is producer_id,
// sequence, len, first u64, held u32, digest, and their held batches store
// a length (u32) in place of `last` and `pieces`: every batch they describe
// is one run with no pieces.

const SNAPSHOT_MAGIC: u32 = 0x464C_5053;
const SNAPSHOT_VERSION: u16 = 4;
pub(super) const SNAPSHOT_HEADER_LEN: usize = 24;

pub(super) fn snapshot_file_name() -> &'static str {
    "producers"
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(snapshot_file_name())
}

pub(super) fn encode(state: &ProducerState, as_of: Offset) -> Vec<u8> {
    let mut body = Vec::new();
    match &state.open {
        None => body.push(0),
        Some(open) => {
            body.push(1);
            encode_open(&mut body, open);
        }
    }
    body.extend_from_slice(&(state.cuts.len() as u32).to_be_bytes());
    for cut in state.cuts.values() {
        encode_open(&mut body, cut);
    }
    body.extend_from_slice(&(state.forgotten.len() as u32).to_be_bytes());
    for (id, sequence) in &state.forgotten {
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(&sequence.to_be_bytes());
    }
    for (id, producer) in &state.producers {
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(&producer.last_sequence.to_be_bytes());
        body.extend_from_slice(&(producer.recent.len() as u16).to_be_bytes());
        for batch in &producer.recent {
            body.extend_from_slice(&batch.first.to_be_bytes());
            body.extend_from_slice(&batch.last.to_be_bytes());
            body.extend_from_slice(&batch.pieces.to_be_bytes());
            encode_digest(&mut body, batch.digest);
        }
    }
    let mut out = Vec::with_capacity(SNAPSHOT_HEADER_LEN + body.len());
    out.extend_from_slice(&SNAPSHOT_MAGIC.to_be_bytes());
    out.extend_from_slice(&SNAPSHOT_VERSION.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&as_of.to_be_bytes());
    out.extend_from_slice(&(state.producers.len() as u32).to_be_bytes());
    out.extend_from_slice(&crate::segment::format::crc32(&[&body]).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn encode_open(body: &mut Vec<u8>, open: &OpenBatch) {
    body.extend_from_slice(&open.batch.producer_id.to_be_bytes());
    body.extend_from_slice(&open.batch.sequence.to_be_bytes());
    body.extend_from_slice(&open.batch.len.to_be_bytes());
    body.push(u8::from(open.first.is_some()));
    body.extend_from_slice(&open.first.unwrap_or(0).to_be_bytes());
    body.extend_from_slice(&open.held.to_be_bytes());
    body.extend_from_slice(&open.next.to_be_bytes());
    body.push(u8::from(open.pieces));
    encode_digest(body, open.digest);
}

fn encode_digest(body: &mut Vec<u8>, digest: Option<PayloadDigest>) {
    body.push(u8::from(digest.is_some()));
    body.extend_from_slice(&digest.map_or(0, PayloadDigest::to_bits).to_be_bytes());
}

pub(super) fn decode(bytes: &[u8]) -> Option<(ProducerState, Offset)> {
    let mut reader = Reader(bytes);
    if reader.u32()? != SNAPSHOT_MAGIC {
        return None;
    }
    let version = reader.u16()?;
    if !(1..=SNAPSHOT_VERSION).contains(&version) {
        return None;
    }
    let with_digests = version >= 2;
    let with_cuts = version >= 3;
    let with_forgotten = version >= 4;
    reader.u16()?;
    let as_of = reader.u64()?;
    let count = reader.u32()? as usize;
    let crc = reader.u32()?;
    if crate::segment::format::crc32(&[reader.0]) != crc {
        return None;
    }
    let open = match reader.u8()? {
        0 => None,
        1 => Some(reader.open(with_digests, with_cuts)?),
        _ => return None,
    };
    let mut cuts = HashMap::default();
    if with_cuts {
        for _ in 0..reader.u32()? {
            let cut = reader.open(with_digests, with_cuts)?;
            cuts.insert(cut.batch.producer_id, cut);
        }
    }
    let mut forgotten = HashMap::default();
    if with_forgotten {
        for _ in 0..reader.u32()? {
            forgotten.insert(reader.u64()?, reader.u64()?);
        }
    }
    let mut producers =
        HashMap::with_capacity_and_hasher(count.min(MAX_PRODUCERS), Default::default());
    for _ in 0..count {
        let id = reader.u64()?;
        let last_sequence = reader.u64()?;
        let batches = reader.u16()? as usize;
        let mut recent = VecDeque::with_capacity(batches.min(WINDOW));
        for _ in 0..batches {
            let first = reader.u64()?;
            let (last, pieces) = if with_cuts {
                (reader.u64()?, reader.u32()?)
            } else {
                let len = u64::from(reader.u32()?);
                (first.checked_add(len)?.checked_sub(1)?, 0)
            };
            recent.push_back(HeldBatch {
                first,
                last,
                pieces,
                digest: reader.digest(with_digests)?,
            });
        }
        producers.insert(
            id,
            Producer {
                last_sequence,
                recent,
            },
        );
    }
    reader.0.is_empty().then_some((
        ProducerState {
            producers,
            open,
            cuts,
            forgotten,
            keeps_cuts: false,
        },
        as_of,
    ))
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.take().map(u16::from_be_bytes)
    }
    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_be_bytes)
    }
    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_be_bytes)
    }
    fn flag(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
    /// A batch in progress, in the layout of a snapshot with cut batches or
    /// of one before them.
    fn open(&mut self, with_digests: bool, with_cuts: bool) -> Option<OpenBatch> {
        let batch = ProducerBatch {
            producer_id: self.u64()?,
            sequence: self.u64()?,
            len: self.u32()?,
        };
        if !with_cuts {
            let first = self.u64()?;
            let held = self.u32()?;
            return Some(OpenBatch {
                batch,
                first: Some(first),
                held,
                next: first.checked_add(u64::from(held))?,
                pieces: false,
                digest: self.digest(with_digests)?,
            });
        }
        let has_first = self.flag()?;
        let first = self.u64()?;
        Some(OpenBatch {
            batch,
            first: has_first.then_some(first),
            held: self.u32()?,
            next: self.u64()?,
            pieces: self.flag()?,
            digest: self.digest(with_digests)?,
        })
    }

    /// A batch's digest, `Some(None)` when the snapshot has none for it.
    fn digest(&mut self, with_digests: bool) -> Option<Option<PayloadDigest>> {
        if !with_digests {
            return Some(None);
        }
        let present = self.u8()?;
        let bits = self.u64()?;
        match present {
            0 => Some(None),
            1 => Some(Some(PayloadDigest::from_bits(bits))),
            _ => None,
        }
    }
}

/// Rebuild producer state for the log `segments` holds.
///
/// Starts from the snapshot when it describes a prefix of this log, and from
/// the oldest record otherwise, then replays the marks up to the tail. On open
/// the active segment's marks come from recovery's scan (`active_marks`), so
/// with a current snapshot nothing is read that recovery did not already read.
/// `keeps_cuts` is whether this is a Raft log.
pub(super) fn rebuild(
    dir: &Path,
    segments: &SegmentSet,
    active_marks: Option<&[(Offset, RecordMark, u64)]>,
    keeps_cuts: bool,
) -> crate::Result<ProducerState> {
    let base = segments.base_offset();
    let tail = segments.tail_offset();
    let (mut state, from) = match load(dir) {
        Some((state, as_of)) if base <= as_of && as_of <= tail => (state, as_of),
        // Nothing before the first v3 segment can carry a mark, so a log
        // written before marks existed costs nothing to open.
        _ => (
            ProducerState::default(),
            segments.first_markable_offset().max(base),
        ),
    };
    state.keeps_cuts = keeps_cuts;
    if keeps_cuts {
        // Not in the records and maybe not in the snapshot: see
        // `store_forgotten`.
        state.remember(load_forgotten(dir)?);
    } else {
        state.cuts.clear();
        state.forgotten.clear();
    }
    state.prune(base);

    let active_base = segments.active().base_offset();
    let read_to = match active_marks {
        Some(_) => active_base,
        None => tail,
    };
    let mut next = from;
    while next < read_to {
        let records = segments.read(
            next,
            // A trim ends a batch, so the rebuild sees it.
            ReadBudget::new(REBUILD_READ_BYTES, usize::MAX).with_trims(TrimRecords::Include),
        )?;
        let Some(last) = records.last() else { break };
        let after = last.offset + 1;
        for record in records
            .into_iter()
            .take_while(|record| record.offset < read_to)
        {
            let digest = marked_digest(record.mark, &record.payload);
            state.observe(record.offset, record.mark, digest);
        }
        next = after;
    }
    if let Some(marks) = active_marks {
        for (offset, mark, digest) in marks.iter().filter(|(offset, ..)| *offset >= from) {
            state.observe(*offset, *mark, *digest);
        }
    }
    state.settle(tail);
    if from < active_base {
        metrics::counter!(metrics_names::PRODUCER_STATE_REBUILT_TOTAL).increment(1);
    }
    Ok(state)
}

/// A record's contribution to its batch's [`PayloadDigest`]: the payload's
/// digest for a marked record, and zero for an unmarked one, which belongs to
/// no batch.
pub(crate) fn marked_digest(mark: RecordMark, payload: &[u8]) -> u64 {
    match mark {
        RecordMark::None
        | RecordMark::GenerationStart
        | RecordMark::Commit
        | RecordMark::Trim { .. }
        | RecordMark::Membership
        | RecordMark::CounterDelta
        | RecordMark::CounterCheckpoint => 0,
        RecordMark::Opens(_) | RecordMark::Continues | RecordMark::Piece(_) => {
            crate::log::record_digest(payload)
        }
    }
}

/// How much one read of a rebuild may hold in memory.
const REBUILD_READ_BYTES: usize = 1024 * 1024;

/// The saved state and the offset it is as of, or `None` when there is no
/// usable snapshot. Unreadable is the same as absent: the log has the answer.
pub(super) fn load(dir: &Path) -> Option<(ProducerState, Offset)> {
    let bytes = crate::fs::read(path_in(dir)).ok()?;
    let decoded = decode(&bytes);
    if decoded.is_none() {
        tracing::warn!(
            dir = %dir.display(),
            "the producer snapshot is unreadable; rebuilding producer state from the log",
        );
    }
    decoded
}

/// Save `state` as of `as_of`, through a temporary and a rename so a reader
/// never sees half a file.
///
/// Not flushed. A snapshot lost to a crash is rebuilt from the log on the
/// next open, the same bargain the sparse index makes.
pub(super) fn store(dir: &Path, state: &ProducerState, as_of: Offset) -> std::io::Result<()> {
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    crate::fs::write(&temporary, encode(state, as_of))?;
    crate::fs::rename(&temporary, &path)
}

/// Remove the snapshot, when the records it describes are no longer the log's.
pub(super) fn discard(dir: &Path) -> std::io::Result<()> {
    match crate::fs::remove_file(path_in(dir)) {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

const FORGOTTEN_MAGIC: u32 = 0x464C_5046;

pub(super) fn forgotten_file_name() -> &'static str {
    "producers.forgotten"
}

pub(super) fn forgotten_temporary_name() -> &'static str {
    "producers.forgotten.tmp"
}

// Layout: magic u32, crc32 of the rest u32, count u32, then per batch
// producer_id u64, sequence u64.
fn encode_forgotten(forgotten: &HashMap<u64, u64>) -> Vec<u8> {
    let mut body = Vec::with_capacity(4 + forgotten.len() * 16);
    body.extend_from_slice(&(forgotten.len() as u32).to_be_bytes());
    for (id, sequence) in forgotten {
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(&sequence.to_be_bytes());
    }
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&FORGOTTEN_MAGIC.to_be_bytes());
    out.extend_from_slice(&crate::segment::format::crc32(&[&body]).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn decode_forgotten(bytes: &[u8]) -> Option<HashMap<u64, u64>> {
    let mut reader = Reader(bytes);
    if reader.u32()? != FORGOTTEN_MAGIC {
        return None;
    }
    let crc = reader.u32()?;
    if crate::segment::format::crc32(&[reader.0]) != crc {
        return None;
    }
    let mut forgotten = HashMap::default();
    for _ in 0..reader.u32()? {
        forgotten.insert(reader.u64()?, reader.u64()?);
    }
    Some(forgotten)
}

/// The forgotten batches saved by [`store_forgotten`]; none when the file is
/// absent. Unreadable is an error: the log does not have the answer, and
/// opening without it could take a re-send as new.
pub(super) fn load_forgotten(dir: &Path) -> crate::Result<HashMap<u64, u64>> {
    let bytes = match crate::fs::read(dir.join(forgotten_file_name())) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::default()),
        Err(err) => return Err(crate::StorageError::Io(err)),
    };
    decode_forgotten(&bytes).ok_or_else(|| {
        crate::StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} is unreadable",
                dir.join(forgotten_file_name()).display()
            ),
        ))
    })
}

/// Save the forgotten batches, flushed. A batch forgotten because its
/// records were purged cannot be read back from the log, and the snapshot is
/// not flushed and is discarded by a truncation, so this file is written
/// before the records go.
pub(super) fn store_forgotten(dir: &Path, forgotten: &HashMap<u64, u64>) -> crate::Result<()> {
    use std::io::Write;

    let path = dir.join(forgotten_file_name());
    // Not `with_extension`, which would name the snapshot's temporary.
    let temporary = dir.join(forgotten_temporary_name());
    {
        let mut file = crate::fs::File::create(&temporary).map_err(crate::StorageError::Io)?;
        file.write_all(&encode_forgotten(forgotten))
            .map_err(crate::StorageError::Io)?;
        crate::io::sync_all(&file)?;
    }
    crate::fs::rename(&temporary, &path).map_err(crate::StorageError::Io)?;
    crate::io::sync_dir(dir)?;
    Ok(())
}

#[cfg(test)]
mod tests;
