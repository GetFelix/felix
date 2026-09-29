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

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use super::segments::SegmentSet;
use crate::log::{Offset, PayloadDigest, ProducerBatch, RecordMark};
use crate::metrics_names;
use crate::segment::ReadBudget;

/// Batches remembered per producer. A re-send follows its original closely, so
/// this covers a producer's in-flight pipeline rather than its history.
pub(crate) const WINDOW: usize = 64;

/// Producers remembered per shard. Past this the one whose newest batch is
/// oldest is forgotten, and its next batch is refused as unknown.
pub(crate) const MAX_PRODUCERS: usize = 4096;

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

/// Every producer a shard's log holds batches for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProducerState {
    producers: HashMap<u64, Producer>,
    /// The newest batch, while records of it are still to come.
    open: Option<OpenBatch>,
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
        self.recent
            .back()
            .map(|batch| batch.first + u64::from(batch.len) - 1)
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeldBatch {
    first: Offset,
    len: u32,
    digest: Option<PayloadDigest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenBatch {
    batch: ProducerBatch,
    first: Offset,
    held: u32,
    /// Of the `held` records so far.
    digest: Option<PayloadDigest>,
}

impl ProducerState {
    /// Where `sequence` stands for `producer_id`.
    pub(crate) fn classify(&self, producer_id: u64, sequence: u64) -> ProducerSequence {
        if let Some(open) = &self.open
            && open.batch.producer_id == producer_id
            && open.batch.sequence == sequence
        {
            return ProducerSequence::Partial {
                first: open.first,
                held: open.held,
                len: open.batch.len,
                digest: open.digest,
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
                    last: batch.first + u64::from(batch.len) - 1,
                    digest: batch.digest,
                }
            }
            None => ProducerSequence::Expired,
        }
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
                    && open.first + u64::from(open.held) == offset
                {
                    open.held += 1;
                    open.digest = open.digest.map(|digest| digest.then(record));
                    if open.held >= open.batch.len {
                        let open = self.open.take().expect("open batch");
                        self.complete(open);
                    }
                } else {
                    // Its opening record is not here (trimmed, or before the
                    // base a replica was rebuilt at), so the batch is unusable.
                    self.open = None;
                }
            }
            RecordMark::Opens(batch) => {
                // A batch still open is abandoned: whatever wrote it stopped,
                // and the records after it belong to someone else.
                self.open = None;
                let open = OpenBatch {
                    batch,
                    first: offset,
                    held: 1,
                    digest: Some(PayloadDigest::EMPTY.then(record)),
                };
                if batch.len <= 1 {
                    self.complete(open);
                } else {
                    self.open = Some(open);
                }
            }
            RecordMark::None | RecordMark::GenerationStart | RecordMark::Commit => self.open = None,
        }
    }

    /// Close an open batch the log has moved past without finishing: the next
    /// record is at `tail` and did not continue it.
    pub(crate) fn settle(&mut self, tail: Offset) {
        if let Some(open) = &self.open
            && open.first + u64::from(open.held) < tail
        {
            self.open = None;
        }
    }

    /// Forget batches retention removed, and producers left with none.
    pub(crate) fn prune(&mut self, base: Offset) {
        if self.open.as_ref().is_some_and(|open| open.first < base) {
            self.open = None;
        }
        self.producers.retain(|_, producer| {
            while producer
                .recent
                .front()
                .is_some_and(|batch| batch.first < base)
            {
                producer.recent.pop_front();
            }
            !producer.recent.is_empty()
        });
    }

    fn complete(&mut self, open: OpenBatch) {
        let OpenBatch {
            batch,
            first,
            digest,
            ..
        } = open;
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
            len: batch.len,
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
//   open batch: present u8, and when 1: producer_id u64, sequence u64, len u32,
//               first u64, held u32, digest
//   per producer: id u64, last_sequence u64, batches u16,
//                 then per batch: first u64, len u32, digest
// where digest is present u8 then value u64 (zero when absent).
//
// Version 1 has no digest fields. Its batches read as having none, and a
// re-send of one is answered the way it was before digests were kept.

const SNAPSHOT_MAGIC: u32 = 0x464C_5053;
const SNAPSHOT_VERSION: u16 = 2;
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
            body.extend_from_slice(&open.batch.producer_id.to_be_bytes());
            body.extend_from_slice(&open.batch.sequence.to_be_bytes());
            body.extend_from_slice(&open.batch.len.to_be_bytes());
            body.extend_from_slice(&open.first.to_be_bytes());
            body.extend_from_slice(&open.held.to_be_bytes());
            encode_digest(&mut body, open.digest);
        }
    }
    for (id, producer) in &state.producers {
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(&producer.last_sequence.to_be_bytes());
        body.extend_from_slice(&(producer.recent.len() as u16).to_be_bytes());
        for batch in &producer.recent {
            body.extend_from_slice(&batch.first.to_be_bytes());
            body.extend_from_slice(&batch.len.to_be_bytes());
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

fn encode_digest(body: &mut Vec<u8>, digest: Option<PayloadDigest>) {
    body.push(u8::from(digest.is_some()));
    body.extend_from_slice(&digest.map_or(0, PayloadDigest::to_bits).to_be_bytes());
}

pub(super) fn decode(bytes: &[u8]) -> Option<(ProducerState, Offset)> {
    let mut reader = Reader(bytes);
    if reader.u32()? != SNAPSHOT_MAGIC {
        return None;
    }
    let with_digests = match reader.u16()? {
        1 => false,
        SNAPSHOT_VERSION => true,
        _ => return None,
    };
    reader.u16()?;
    let as_of = reader.u64()?;
    let count = reader.u32()? as usize;
    let crc = reader.u32()?;
    if crate::segment::format::crc32(&[reader.0]) != crc {
        return None;
    }
    let open = match reader.u8()? {
        0 => None,
        1 => Some(OpenBatch {
            batch: ProducerBatch {
                producer_id: reader.u64()?,
                sequence: reader.u64()?,
                len: reader.u32()?,
            },
            first: reader.u64()?,
            held: reader.u32()?,
            digest: reader.digest(with_digests)?,
        }),
        _ => return None,
    };
    let mut producers = HashMap::with_capacity(count.min(MAX_PRODUCERS));
    for _ in 0..count {
        let id = reader.u64()?;
        let last_sequence = reader.u64()?;
        let batches = reader.u16()? as usize;
        let mut recent = VecDeque::with_capacity(batches.min(WINDOW));
        for _ in 0..batches {
            recent.push_back(HeldBatch {
                first: reader.u64()?,
                len: reader.u32()?,
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
    reader
        .0
        .is_empty()
        .then_some((ProducerState { producers, open }, as_of))
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
pub(super) fn rebuild(
    dir: &Path,
    segments: &SegmentSet,
    active_marks: Option<&[(Offset, RecordMark, u64)]>,
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
    state.prune(base);

    let active_base = segments.active().base_offset();
    let read_to = match active_marks {
        Some(_) => active_base,
        None => tail,
    };
    let mut next = from;
    while next < read_to {
        let records = segments.read(next, ReadBudget::new(REBUILD_READ_BYTES, usize::MAX))?;
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
        RecordMark::None | RecordMark::GenerationStart | RecordMark::Commit => 0,
        RecordMark::Opens(_) | RecordMark::Continues => crate::log::record_digest(payload),
    }
}

/// How much one read of a rebuild may hold in memory.
const REBUILD_READ_BYTES: usize = 1024 * 1024;

/// The saved state and the offset it is as of, or `None` when there is no
/// usable snapshot. Unreadable is the same as absent: the log has the answer.
pub(super) fn load(dir: &Path) -> Option<(ProducerState, Offset)> {
    let bytes = std::fs::read(path_in(dir)).ok()?;
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
    std::fs::write(&temporary, encode(state, as_of))?;
    std::fs::rename(&temporary, &path)
}

/// Remove the snapshot, when the records it describes are no longer the log's.
pub(super) fn discard(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path_in(dir)) {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;
