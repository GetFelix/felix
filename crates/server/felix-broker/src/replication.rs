//! The follower's half of replication: storing what a leader shipped.
//!
//! # The rule
//!
//! A follower stores a record at the **leader's** offset or not at all. That is
//! what makes the two logs comparable by offset, and every other part of
//! replication rests on it — the acknowledged mark, the catch-up range, the
//! caught-up test that gates promotion.
//!
//! The log underneath appends at its own tail and cannot be told where to put a
//! record. So the follower does not ask it to: it checks that the batch begins
//! exactly at its tail, and refuses otherwise. Position is verified rather than
//! commanded, which is stricter and needs nothing from the storage layer.
//!
//! # Why each refusal is its own answer
//!
//! | The batch | Meaning | What the leader does |
//! | --- | --- | --- |
//! | starts past the tail | records are missing in between | resume from `expected_offset` |
//! | is entirely below the tail | a retry of something already stored | nothing; already acknowledged |
//! | straddles the tail | a retry that overlaps | the new suffix is stored |
//! | disagrees on stored bytes | the logs have diverged | stop |
//!
//! The middle two are why a retry is safe. Replication has to be able to resend
//! a batch whose acknowledgement was lost, and resending must not duplicate a
//! record: an overlap is resolved by position, and the bytes are checked rather
//! than assumed.
//!
//! # Durable, not buffered
//!
//! [`apply`] returns only after the batch satisfies the log's fsync policy. A
//! follower that acknowledged sooner would let the leader believe a record had
//! survived a failure it would not have survived — and under
//! `ConsistencyLevel::Quorum` that belief is the guarantee.
use bytes::Bytes;
use felix_storage::log::{ProducerBatch, RecordMark};
use felix_wire::internal::ProducerMark;

use crate::durable::StreamLog;
use crate::error::{BrokerError, Result};

/// What applying a replication batch did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// One past the last record now durably stored. Both the acknowledgement
    /// and the offset the leader should send next.
    pub durable_offset: u64,
    /// Records actually written. Zero for a batch that was entirely a retry.
    pub appended: usize,
}

/// Why a batch was not applied.
///
/// Separate from [`BrokerError`] because these are answers about *position*,
/// which the leader acts on, rather than failures of this broker.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Divergence {
    /// The batch starts past the follower's tail. Applying it would leave a
    /// hole, and a log with a hole cannot be read back.
    #[error("batch starts at {first_offset}, expected {expected}")]
    Gap { expected: u64, first_offset: u64 },
    /// The batch disagrees with bytes already stored. Records are never
    /// rewritten, so there is no repair: progress stops here.
    #[error("batch disagrees with the stored record at offset {offset}")]
    Conflict { offset: u64, expected: u64 },
    /// The batch did not survive the trip.
    #[error("batch checksum mismatch (leader {leader:#x}, computed {computed:#x})")]
    Corrupt { leader: u64, computed: u64 },
}

impl Divergence {
    /// The offset the follower wants next, for the leader to resume from.
    pub fn expected_offset(&self) -> u64 {
        match self {
            Self::Gap { expected, .. } => *expected,
            Self::Conflict { expected, .. } => *expected,
            Self::Corrupt { .. } => 0,
        }
    }
}

/// Store a batch the leader shipped, at the leader's offsets.
///
/// `first_offset` is where `payloads[0]` belongs, and `marks` are the
/// records' producer marks (empty when none is marked), stored with them so
/// this follower knows each idempotent producer's place as the leader does.
/// `publishers` are the records' publishers, empty when none has one.
/// `times` are the records' append times on the leader, stored as they are;
/// empty stamps them with this broker's clock. Times are not compared on a
/// retry: a copy stored from a leader that did not send them holds this
/// broker's own.
/// `headers` are the records' key-and-headers blocks, stored with them and
/// covered by the checksum, or `None` when no record has one.
/// Returns once the batch is durable.
#[allow(clippy::too_many_arguments)]
pub async fn apply(
    log: &StreamLog,
    first_offset: u64,
    checksum: u64,
    payloads: &[Bytes],
    marks: &[ProducerMark],
    publishers: &[Option<Bytes>],
    times: &[u64],
    headers: Option<&[Option<Bytes>]>,
) -> Result<std::result::Result<Applied, Divergence>> {
    // Everything below is decided from one reading of the tail, and the write
    // is made only if the tail is still there. A resend on a second lane can
    // land in between; without the check this batch would then be appended
    // after it, at offsets the leader never gave these records. A lost race
    // is decided again from the new tail, where the other copy is an overlap
    // to verify.
    for _ in 0..MAX_APPLY_RACES {
        if let Some(applied) = apply_at_tail(
            log,
            first_offset,
            checksum,
            payloads,
            marks,
            publishers,
            times,
            headers,
        )
        .await?
        {
            return Ok(applied);
        }
    }
    Err(BrokerError::Storage(format!(
        "the follower log kept moving under a replication batch at {first_offset}"
    )))
}

/// How often [`apply`] re-reads the tail after losing a race before giving up.
/// Each loss means another batch for this log was stored, and a leader only
/// has so many in flight.
const MAX_APPLY_RACES: usize = 16;

/// One attempt at [`apply`]. `None` when another append moved the tail after
/// it was read.
#[allow(clippy::too_many_arguments)]
async fn apply_at_tail(
    log: &StreamLog,
    first_offset: u64,
    checksum: u64,
    payloads: &[Bytes],
    marks: &[ProducerMark],
    publishers: &[Option<Bytes>],
    times: &[u64],
    headers: Option<&[Option<Bytes>]>,
) -> Result<Option<std::result::Result<Applied, Divergence>>> {
    let tail = log.tail_offset().await?;

    // Checked before the tail is consulted for anything else: a batch that did
    // not survive the trip says nothing reliable about position either.
    let computed = felix_wire::internal::batch_checksum(
        payloads,
        marks,
        publishers,
        headers.unwrap_or_default(),
    );
    if computed != checksum {
        return Ok(Some(Err(Divergence::Corrupt {
            leader: checksum,
            computed,
        })));
    }

    // One block per payload, or none; anything else would pair a block with
    // the wrong record.
    if headers.is_some_and(|headers| headers.len() != payloads.len()) {
        return Err(BrokerError::Storage(
            "a replication batch's headers differ in count from its payloads".to_string(),
        ));
    }

    if payloads.is_empty() {
        // Nothing to store, and nothing wrong: an empty batch is a position
        // probe, and the tail is the answer.
        return Ok(Some(Ok(Applied {
            durable_offset: tail,
            appended: 0,
        })));
    }

    if first_offset > tail {
        return Ok(Some(Err(Divergence::Gap {
            expected: tail,
            first_offset,
        })));
    }

    // The batch reaches back into what is already stored. That is a retry, so
    // the overlap is verified rather than trusted, and only the suffix past the
    // tail is new.
    let marks: Vec<RecordMark> = (0..payloads.len())
        .map(|index| mark_from_wire(marks.get(index).copied().unwrap_or_default()))
        .collect();
    let publishers: Vec<Option<Bytes>> = if publishers.is_empty() {
        Vec::new()
    } else {
        (0..payloads.len())
            .map(|index| publishers.get(index).cloned().flatten())
            .collect()
    };
    let overlap = (tail - first_offset).min(payloads.len() as u64) as usize;
    if overlap > 0
        && let Some(divergence) = conflict_in(
            log,
            first_offset,
            &payloads[..overlap],
            &marks[..overlap],
            publishers.get(..overlap).unwrap_or_default(),
            headers.map(|headers| &headers[..overlap]),
            tail,
        )
        .await?
    {
        return Ok(Some(Err(divergence)));
    }

    let fresh = &payloads[overlap..];
    if fresh.is_empty() {
        // Wholly a retry of records already held. Answer with the end of the
        // batch, not the tail: the leader resumes from this, and a tail past
        // the batch would skip records nothing has compared. A follower that
        // kept an uncommitted record from a dead leader has exactly that shape,
        // and the skip is what lets the new leader reuse the offset without
        // ever noticing they disagree.
        return Ok(Some(Ok(Applied {
            durable_offset: first_offset + payloads.len() as u64,
            appended: 0,
        })));
    }

    let Some(pending) = log
        .begin_append_marked_at(
            tail,
            fresh,
            &marks[overlap..],
            publishers.get(overlap..).unwrap_or_default(),
            times.get(overlap..).unwrap_or_default(),
            headers.map(|headers| &headers[overlap..]),
        )
        .await?
    else {
        return Ok(None);
    };
    log.commit(&pending).await?;

    Ok(Some(Ok(Applied {
        durable_offset: tail + fresh.len() as u64,
        appended: fresh.len(),
    })))
}

/// Compare a batch's overlapping prefix against what is already stored.
///
/// Reads only the overlap, which is bounded by the batch, so a retry costs a
/// read proportional to the resend rather than to the log. Every overlapping
/// offset is compared, or the batch is refused with an error the leader
/// retries: accepting an overlap that was only partly compared would append
/// after a stored record nothing checked.
async fn conflict_in(
    log: &StreamLog,
    first_offset: u64,
    overlapping: &[Bytes],
    marks: &[RecordMark],
    publishers: &[Option<Bytes>],
    headers: Option<&[Option<Bytes>]>,
    tail: u64,
) -> Result<Option<Divergence>> {
    let wanted: usize = overlapping
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            let block = headers
                .and_then(|headers| headers[index].as_ref())
                .map_or(0, Bytes::len);
            payload.len() + block + 32
        })
        .sum();
    let end = first_offset + overlapping.len() as u64;
    let mut next = first_offset;
    // The budget is sized from the leader's records, and a stored record at
    // the same offset can be larger (a dead leader's write the new leader
    // filled with something smaller), so one read may stop short.
    while next < end {
        let stored = match log.read_replicated_from(next, wanted.max(1)).await {
            Ok(stored) => stored,
            // Retention discarded the records from here to `oldest`. There is
            // nothing left to compare them against, and refusing on that basis
            // would stall a follower for a reason that is not divergence. What
            // survives past them is still compared.
            Err(BrokerError::CursorTooOld { oldest, .. }) if oldest > next => {
                next = oldest;
                continue;
            }
            Err(err) => return Err(err),
        };
        let read_from = next;
        for record in stored.iter().take_while(|record| record.offset < end) {
            if record.offset != next {
                break;
            }
            let index = (next - first_offset) as usize;
            // A mark that differs is a different record even with the same
            // bytes: it would leave this replica and the leader disagreeing
            // about where a producer stands. So is a different publisher, or
            // a different key-and-headers block.
            let publisher = publishers.get(index).cloned().flatten();
            let block = headers.and_then(|headers| headers[index].as_ref());
            if record.payload != overlapping[index]
                || record.mark != marks[index]
                || record.publisher != publisher
                || record.headers.as_deref() != block
            {
                return Ok(Some(Divergence::Conflict {
                    offset: next,
                    expected: tail,
                }));
            }
            next += 1;
        }
        if next == read_from {
            // Below the tail every offset holds a record, so a read that does
            // not return this one is a hole or a failing read. Neither says
            // the logs agree.
            return Err(BrokerError::Storage(format!(
                "could not read the stored record at {next} to compare a \
                 replication batch from {first_offset} against it"
            )));
        }
    }
    Ok(None)
}

/// The records' publishers as they travel between brokers: one per record,
/// or empty when none has one, so such a batch is unchanged on the wire.
pub fn publishers_to_wire(records: &[felix_storage::log::LogRecord]) -> Vec<Option<Bytes>> {
    if records.iter().all(|record| record.publisher.is_none()) {
        return Vec::new();
    }
    records
        .iter()
        .map(|record| record.publisher.clone())
        .collect()
}

/// The records' key-and-headers blocks as they travel between brokers: one
/// per record, or `None` when no record has one, so such a batch is unchanged
/// on the wire.
pub fn headers_to_wire(records: &[felix_storage::log::LogRecord]) -> Option<Vec<Option<Bytes>>> {
    if records.iter().all(|record| record.headers.is_none()) {
        return None;
    }
    Some(
        records
            .iter()
            .map(|record| record.headers.as_deref().cloned())
            .collect(),
    )
}

/// A record's mark as it travels between brokers. `None` for the v9
/// membership and counter kinds, which the replication wire does not carry
/// yet: a leader must hold a follower before such a record, never send it
/// with another mark.
pub fn mark_to_wire(mark: RecordMark) -> Option<ProducerMark> {
    Some(match mark {
        RecordMark::None => ProducerMark::None,
        RecordMark::Opens(batch) => ProducerMark::Opens {
            producer_id: batch.producer_id,
            sequence: batch.sequence,
            len: batch.len,
        },
        RecordMark::Continues => ProducerMark::Continues,
        RecordMark::GenerationStart => ProducerMark::GenerationStart,
        RecordMark::Commit => ProducerMark::Commit,
        RecordMark::Trim { floor } => ProducerMark::Trim { floor },
        RecordMark::Membership | RecordMark::CounterDelta | RecordMark::CounterCheckpoint => {
            return None;
        }
    })
}

/// A shipped mark as the log stores it.
pub fn mark_from_wire(mark: ProducerMark) -> RecordMark {
    match mark {
        ProducerMark::None => RecordMark::None,
        ProducerMark::Opens {
            producer_id,
            sequence,
            len,
        } => RecordMark::Opens(ProducerBatch {
            producer_id,
            sequence,
            len,
        }),
        ProducerMark::Continues => RecordMark::Continues,
        ProducerMark::GenerationStart => RecordMark::GenerationStart,
        ProducerMark::Commit => RecordMark::Commit,
        ProducerMark::Trim { floor } => RecordMark::Trim { floor },
    }
}

#[cfg(test)]
mod tests;
