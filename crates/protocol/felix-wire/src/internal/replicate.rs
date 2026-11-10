//! Leader-to-follower replication: records, bootstrap, rebuild, and the
//! follower's answers.

use bytes::Bytes;

use super::{ErrorCode, ShardRef};
use crate::error::{Error, Result};

/// Records the leader has committed, shipped to a follower.
///
/// Append-only, and identified by the offsets the *leader* assigned: a follower
/// stores a record at the leader's offset or not at all. That is what makes the
/// two logs comparable by offset, which every other part of replication relies
/// on.
///
/// `shard.generation` is the leader's epoch. A follower that knows of a newer
/// one refuses, because a leader at an older epoch may already have been
/// replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateRecords {
    pub correlation_id: u64,
    pub shard: ShardRef,
    /// Offset of `payloads[0]`. Payload `i` belongs at `first_offset + i`.
    pub first_offset: u64,
    /// Over the payload bytes and the marks, in order. Checked before anything
    /// is written, so a batch corrupted in transit is refused rather than
    /// stored. See [`batch_checksum`].
    pub checksum: u64,
    pub payloads: Vec<Bytes>,
    /// One per payload, or empty when no record in the batch is marked. Only a
    /// stream shard's records carry marks, and a batch with any travels as
    /// `ReplicateMarkedRecords`.
    pub marks: Vec<ProducerMark>,
    /// The principal that published each record, or empty when no record in
    /// the batch carries one. They travel in the marks section, so a batch
    /// with any goes as a kind that has one. Sent only once the fleet
    /// finalized `publisher_principal`: an older peer refuses the batch.
    pub publishers: Vec<Option<Bytes>>,
    /// One past the last record the leader knows is committed, when it knows.
    ///
    /// A follower holding the leader's records below it must never discard
    /// them: they were acknowledged on a majority. `None` is the behaviour
    /// from before the field existed, and the batch travels as its log's own
    /// kind, byte for byte what an older follower reads. `Some` travels as
    /// `ReplicateCommittedRecords`, which an older follower refuses.
    pub commit_offset: Option<u64>,
    /// Where each generation that wrote a record in this batch began in the
    /// sender's log, oldest first: the one the first record belongs to, and
    /// every later one starting at or before the batch's end, which includes
    /// the sender's own when nothing of it follows yet.
    ///
    /// `None` is a sender that predates labels: the batch travels as any
    /// other kind, and the follower labels what it appends with the sender's
    /// generation. `Some` travels as `ReplicateLabelledRecords`, sent only to
    /// a peer that advertised `GENERATION_LABELS`.
    pub generations: Option<Vec<GenerationStart>>,
    /// Each record's append time on the leader, in microseconds since the
    /// Unix epoch, one per payload. The follower stores them as they are, so
    /// a replica promoted later reports the times the leader's readers saw.
    ///
    /// `None` is a sender that predates them, and the follower stamps the
    /// records with its own clock. `Some` travels only on a labelled batch,
    /// as `ReplicateTimedRecords`, sent only to a peer that advertised
    /// `RECORD_TIMES`; on a batch without generations it is not sent.
    /// Not covered by the checksum, so an unlabelled batch checksums as it
    /// always has.
    pub times: Option<Vec<u64>>,
    /// Each record's key-and-headers block, one per payload, when any record
    /// has one; storage keeps the bytes as given. `None` is a batch no record
    /// of which has a block, byte for byte what a peer that predates them
    /// reads. `Some` travels only on a timed batch, as
    /// `ReplicateHeaderedRecords`, sent only to a peer that advertised
    /// `RECORD_HEADERS`. Covered by the checksum. `Some` holding no block is
    /// the same message as `None`: it is sent as `None` and decodes as `None`.
    pub headers: Option<Vec<Option<Bytes>>>,
    /// The generation that wrote each record, as runs: entry `i` covers from
    /// its `start_offset` to the next entry's, the last to the batch end. The
    /// first starts at `first_offset`; offsets strictly increase within the
    /// batch and generations strictly increase. `None` is byte for byte the
    /// frame without it; `Some` travels as `ReplicateStampedRecords`, sent
    /// only to a peer that advertised `WRITER_GENERATIONS`. Not covered by the
    /// checksum, like times; the decoder checks its shape.
    pub writers: Option<Vec<GenerationStart>>,
    /// The generation of the replica set the sender counts. `None` is a
    /// sender that does not check sets, and the batch travels as it always
    /// did; `Some` wraps it in a `SetStamped` frame, sent only to a peer that
    /// advertised [`PeerCapabilities::ELECTION`](super::PeerCapabilities::ELECTION).
    /// Not covered by the checksum.
    pub set_generation: Option<u64>,
}

impl ReplicateRecords {
    /// Whether any record in the batch is a head trim.
    pub fn holds_trim(&self) -> bool {
        self.marks
            .iter()
            .any(|mark| matches!(mark, ProducerMark::Trim { .. }))
    }
}

/// Whether `writers` is a well-formed set of runs for a batch of `len`
/// records starting at `first_offset`.
pub(super) fn writers_well_formed(
    first_offset: u64,
    len: usize,
    writers: &[GenerationStart],
) -> bool {
    if len == 0 {
        return writers.is_empty();
    }
    let end = first_offset.saturating_add(len as u64);
    writers
        .first()
        .is_some_and(|w| w.start_offset == first_offset)
        && writers.iter().all(|w| w.start_offset < end)
        && writers.windows(2).all(|pair| {
            pair[0].start_offset < pair[1].start_offset && pair[0].generation < pair[1].generation
        })
}

/// One leadership generation, and the offset of its first record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationStart {
    pub generation: u64,
    pub start_offset: u64,
}

/// Which idempotent producer's batch a replicated record belongs to.
///
/// Shipped with the record so a follower stores it exactly as the leader did:
/// the marks are what let a promoted replica answer a producer's re-send.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProducerMark {
    #[default]
    None,
    /// The first record of a producer's batch of `len` records.
    Opens {
        producer_id: u64,
        sequence: u64,
        len: u32,
    },
    /// A later record of the batch the record before it belongs to.
    Continues,
    /// A leader's generation-start record. Belongs to no producer; it rides
    /// the marks because the marks are how a record's kind travels. Written
    /// only once the fleet finalized `generation_start`, so every peer reads
    /// it; an older one would refuse the batch with
    /// `UnknownInternalProducerMark`.
    GenerationStart,
    /// An atomic commit record. Written only once the fleet finalized
    /// `atomic_commit`, for the same reason as `GenerationStart`.
    Commit,
    /// A head trim record asking that reads below `floor` be refused. Its
    /// payload is the trim's headers. Written only once the fleet finalized
    /// `head_trim`, and sent only to a peer that advertised
    /// [`PeerCapabilities::HEAD_TRIM`](super::PeerCapabilities::HEAD_TRIM).
    Trim { floor: u64 },
}

/// The follower stored the batch.
///
/// `durable_offset` is one past the last record the follower has on disk, so it
/// is both an acknowledgement and the offset the leader should send next. It
/// reports **durable** data, never buffered: a follower that acknowledged
/// before its own fsync would let the leader believe a record survived a
/// failure it would not have survived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateOk {
    pub correlation_id: u64,
    pub durable_offset: u64,
    /// The follower's copy is a learner: re-admitted after a wipe and still
    /// catching up, so it may have lost a promise it made before the wipe.
    /// The leader leaves it out of every mark until an answer without this
    /// arrives. `false` travels as the original kind, byte for byte; `true`
    /// as `ReplicateLearnerOk`, sent only to a caller that offered
    /// [`PeerCapabilities::LEARNER_ACKS`](super::PeerCapabilities::LEARNER_ACKS).
    pub learner: bool,
}

/// The follower refused, and where it stands.
///
/// `expected_offset` is what the follower wants next. For `LogGap` it is how
/// the leader repairs without a separate negotiation; for the rest it is
/// diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateError {
    pub correlation_id: u64,
    pub code: ErrorCode,
    pub expected_offset: u64,
    pub detail: String,
    /// What `detail` says, as fields a leader reads instead of the text.
    /// Sent only to a caller that offered
    /// [`PeerCapabilities::REFUSAL_REASONS`](super::PeerCapabilities::REFUSAL_REASONS);
    /// `None` encodes the original body, byte for byte, and a leader then
    /// reads `detail` as it always has.
    pub reason: Option<RefusalReason>,
}

/// Why a replica refused a sender at generation `sender`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefusalReason {
    /// Its ballot holds `generation`, the sender's, for `leader`.
    PromisedTo { generation: u64, leader: String },
    /// Its ballot holds the newer `accepted`.
    Accepted { accepted: u64, sender: u64 },
    /// Its routing view is at `have`.
    View { have: u64, sender: u64 },
    /// The control plane's confirmed assignment is at `assigned`.
    Assigned { assigned: u64, sender: u64 },
    /// Its route does not name it a replica of the shard.
    NotAReplica,
    /// It answers a fence but does not vote: a learner, or a copy replaced
    /// until a set change re-admits it. Its position counts for nothing.
    NotVoter,
}

/// The leader has nothing older than `base_offset` left.
///
/// Sent when a follower's position is below everything the leader still holds,
/// so shipping cannot reach it: the records in between are gone from the leader
/// too. It says "the surviving log starts here" — which is the one fact a
/// follower cannot work out for itself, and the fact it needs before it may
/// place a log that begins anywhere other than zero.
///
/// A follower with records of its own refuses. Discarding them is an operator's
/// decision, not a leader's.
///
/// A separate kind rather than a field on [`ReplicateRecords`]: this protocol
/// freezes existing body layouts, and an older peer already rejects an unknown
/// kind rather than misreading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateBootstrap {
    pub correlation_id: u64,
    pub shard: ShardRef,
    /// Offset of the oldest record the leader still holds.
    pub base_offset: u64,
}

/// Discard a follower's copy of one log and start again at `base_offset`.
///
/// Sent by the leader to a follower whose replication has halted -- a
/// diverged copy, or one that refused a bootstrap -- and only under the
/// leader's rebuild policy. The follower checks it is a replica of the shard
/// at this generation, discards every record it holds for that log, places
/// an empty log at `base_offset`, and answers `ReplicateOk` with that offset;
/// the leader then ships from there as it would to any follower that far
/// behind. A peer that predates this kind refuses it, and the halt stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateRebuild {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub log: ReplicaLog,
    /// Offset of the oldest record the leader still holds, where the new copy
    /// begins.
    pub base_offset: u64,
}

/// Which of a shard's logs a rebuild is about.
///
/// A field rather than five kinds, unlike the records and bootstrap messages:
/// those share a body with something else and the kind is what tells them
/// apart, whereas nothing shares this body. The values are this protocol's,
/// not the broker's own enum, so a renumbering there cannot change the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplicaLog {
    Stream = 1,
    Cache = 2,
    GroupCursors = 3,
    GroupDeadLetters = 4,
    Counters = 5,
}

impl ReplicaLog {
    /// Parse the wire value; an unknown one is an error.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(ReplicaLog::Stream),
            2 => Ok(ReplicaLog::Cache),
            3 => Ok(ReplicaLog::GroupCursors),
            4 => Ok(ReplicaLog::GroupDeadLetters),
            5 => Ok(ReplicaLog::Counters),
            other => Err(Error::UnknownInternalReplicaLog(other)),
        }
    }
}

/// The checksum a [`ReplicateRecords`] batch carries.
///
/// Defined once, here, so the leader and the follower cannot compute it
/// differently — a checksum the two sides disagree about reports corruption on
/// every healthy batch, which is worse than not having one.
///
/// It covers each payload's length and its bytes, in order. Including the
/// length is what stops `["ab", "c"]` and `["a", "bc"]` hashing alike: they are
/// different records, and a follower storing one where the leader has the other
/// is exactly the divergence this is here to catch.
///
/// Marks are covered after the payloads, and only when present, so an
/// unmarked batch checksums exactly as it always has. Publishers are covered
/// with the marks, as they travel. Header blocks are covered last, as they
/// travel, and only when some record has one; `headers` is one per payload,
/// or empty.
pub fn batch_checksum(
    payloads: &[Bytes],
    marks: &[ProducerMark],
    publishers: &[Option<Bytes>],
    headers: &[Option<Bytes>],
) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    for payload in payloads {
        hasher.update(&(payload.len() as u32).to_be_bytes());
        hasher.update(payload);
    }
    if !marks.is_empty() || !publishers.is_empty() {
        let mut marks = marks.to_vec();
        marks.resize(marks.len().max(publishers.len()), ProducerMark::None);
        let mut encoded = bytes::BytesMut::new();
        put_marks(&mut encoded, &marks, publishers);
        hasher.update(&encoded);
    }
    if any_block(headers) {
        // Tagged, so a section of blocks cannot stand in for marks.
        hasher.update(b"H");
        let mut encoded = bytes::BytesMut::new();
        put_blocks(&mut encoded, headers);
        hasher.update(&encoded);
    }
    u64::from(hasher.finalize())
}

/// Whether any record in `blocks` has a key-and-headers block.
pub(super) fn any_block(blocks: &[Option<Bytes>]) -> bool {
    blocks.iter().any(Option::is_some)
}

/// Key-and-headers blocks as they travel: a `u32` count, then per record a
/// presence byte and, when set, a `u32` length and the block.
pub(super) fn put_blocks(out: &mut bytes::BytesMut, blocks: &[Option<Bytes>]) {
    use bytes::BufMut;
    out.put_u32(blocks.len() as u32);
    for block in blocks {
        match block {
            None => out.put_u8(0),
            Some(block) => {
                out.put_u8(1);
                out.put_u32(block.len() as u32);
                out.extend_from_slice(block);
            }
        }
    }
}

/// Read blocks written by [`put_blocks`] for `count` records. A count that
/// differs, or a section with no block in it, is refused: the encoder writes
/// neither, so accepting them would give one message two encodings.
pub(super) fn take_blocks(
    body: &mut Bytes,
    count: usize,
    require_block: bool,
) -> Result<Vec<Option<Bytes>>> {
    use bytes::Buf;
    let declared = take_u32(body)? as usize;
    if declared != count {
        return Err(Error::InvalidRecordHeaders(
            "headers count differs from payloads",
        ));
    }
    // Each entry takes at least its presence byte.
    if declared > body.remaining() {
        return Err(Error::Incomplete);
    }
    let mut blocks = Vec::with_capacity(declared);
    for _ in 0..declared {
        match take_u8(body)? {
            0 => blocks.push(None),
            1 => {
                let len = take_u32(body)? as usize;
                if len > body.remaining() {
                    return Err(Error::Incomplete);
                }
                blocks.push(Some(body.split_to(len)));
            }
            _ => return Err(Error::Incomplete),
        }
    }
    if require_block && !any_block(&blocks) {
        return Err(Error::InvalidRecordHeaders(
            "headers section holds no block",
        ));
    }
    Ok(blocks)
}

fn take_u32(body: &mut Bytes) -> Result<u32> {
    use bytes::Buf;
    if body.remaining() < 4 {
        return Err(Error::Incomplete);
    }
    Ok(body.get_u32())
}

fn take_u8(body: &mut Bytes) -> Result<u8> {
    use bytes::Buf;
    if !body.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(body.get_u8())
}

/// Set on a mark byte when the record's publisher follows the mark: a `u8`
/// length, then the principal.
const MARK_PUBLISHER: u8 = 0x80;

/// Marks as they travel: one byte per record (0 none, 1 opens, 2 continues,
/// 3 generation start, 4 commit, 5 trim), an opening record's byte followed by
/// its producer id, sequence and length, a trim's by its floor. A record with a publisher has
/// [`MARK_PUBLISHER`] set and its publisher after the rest of the mark.
/// `publishers` is one per mark, or empty.
pub(super) fn put_marks(
    out: &mut bytes::BytesMut,
    marks: &[ProducerMark],
    publishers: &[Option<Bytes>],
) {
    use bytes::BufMut;
    for (index, mark) in marks.iter().enumerate() {
        let publisher = publishers.get(index).and_then(Option::as_ref);
        let flag = if publisher.is_some() {
            MARK_PUBLISHER
        } else {
            0
        };
        match mark {
            ProducerMark::None => out.put_u8(flag),
            ProducerMark::Opens {
                producer_id,
                sequence,
                len,
            } => {
                out.put_u8(1 | flag);
                out.put_u64(*producer_id);
                out.put_u64(*sequence);
                out.put_u32(*len);
            }
            ProducerMark::Continues => out.put_u8(2 | flag),
            ProducerMark::GenerationStart => out.put_u8(3 | flag),
            ProducerMark::Commit => out.put_u8(4 | flag),
            ProducerMark::Trim { floor } => {
                out.put_u8(5 | flag);
                out.put_u64(*floor);
            }
        }
        if let Some(publisher) = publisher {
            // A storage limit too, so a longer one never reaches here.
            let len = publisher.len().min(usize::from(u8::MAX));
            out.put_u8(len as u8);
            out.extend_from_slice(&publisher[..len]);
        }
    }
}

/// Read `count` marks written by [`put_marks`], and the publishers, empty
/// when no record had one.
pub(super) fn take_marks(
    body: &mut Bytes,
    count: usize,
) -> Result<(Vec<ProducerMark>, Vec<Option<Bytes>>)> {
    use bytes::Buf;
    let mut marks = Vec::with_capacity(count.min(body.remaining()));
    let mut publishers = Vec::new();
    for index in 0..count {
        if !body.has_remaining() {
            return Err(Error::Incomplete);
        }
        let byte = body.get_u8();
        let mark = match byte & !MARK_PUBLISHER {
            0 => ProducerMark::None,
            1 => {
                if body.remaining() < 20 {
                    return Err(Error::Incomplete);
                }
                ProducerMark::Opens {
                    producer_id: body.get_u64(),
                    sequence: body.get_u64(),
                    len: body.get_u32(),
                }
            }
            2 => ProducerMark::Continues,
            3 => ProducerMark::GenerationStart,
            4 => ProducerMark::Commit,
            5 => {
                if body.remaining() < 8 {
                    return Err(Error::Incomplete);
                }
                ProducerMark::Trim {
                    floor: body.get_u64(),
                }
            }
            _ => return Err(Error::UnknownInternalProducerMark(byte)),
        };
        marks.push(mark);
        if byte & MARK_PUBLISHER != 0 {
            if !body.has_remaining() {
                return Err(Error::Incomplete);
            }
            let len = usize::from(body.get_u8());
            if body.remaining() < len {
                return Err(Error::Incomplete);
            }
            publishers.resize(index, None);
            publishers.push(Some(body.split_to(len)));
        }
    }
    if !publishers.is_empty() {
        publishers.resize(count, None);
    }
    Ok((marks, publishers))
}
