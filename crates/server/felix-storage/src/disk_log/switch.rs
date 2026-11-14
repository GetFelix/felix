//! Starting a Raft member on a shard directory the hybrid wrote: the voter
//! side of the switch from the hybrid to Raft.
//!
//! The seal leaves every voter holding the same records from the seal's
//! start `S` up to the base `N`, where the hybrid's generation-start record
//! of generation `G` sits. A voter keeps those records as they are and
//! starts its member with the log purged through `N`, so nothing at or
//! below `N` is ever read as an entry, generation 0 records included.
//! Records are never rewritten. The design is in `docs/per-shard-raft.md`,
//! "Moving a shard from the hybrid to Raft", and the rules are the ones
//! `docs/formal/FelixRaftSwitch.tla` checks.
//!
//! [`start_voter`] writes the member's state in a fixed order and the
//! engine marker last, so a marked directory has all of it on disk and an
//! unmarked one is started again from the beginning.

#[cfg(test)]
mod tests;

use std::io;
use std::path::Path;

use sha2::{Digest, Sha256};

use super::ballot::{self, Ballot};
use super::engine_marker;
use super::replica_state::{self, ReplicaState};
use super::set;
use crate::log::{AppendOnlyLog, LogConfig, LogRecord, Offset, ReadRange, RecordMark};
use crate::raft_log::{
    SnapshotId, SnapshotStore, StoredSnapshot, store_purged, term_of_generation,
};
use crate::{DiskLog, Result, StorageError};

/// How many payload bytes one read of a digest takes.
const DIGEST_PAGE: usize = 4 << 20;

/// A digest of a range of records: SHA-256 over each record's offset,
/// timestamp, mark, payload, publisher and headers. The generation in the
/// header is left out, because one copy may hold generation 0 where
/// another holds the real one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RangeDigest(pub [u8; 32]);

/// Where a shard's Raft log starts, as the control plane records it at the
/// switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwitchBase {
    /// The seal's generation: the generation of the record at `offset`.
    /// Never 0.
    pub generation: u64,
    /// The offset of the seal's generation-start record. The first Raft
    /// entry is the next one.
    pub offset: Offset,
    /// The seal's start: the highest log start among the copies that
    /// answered it. The digest covers `start..offset`.
    pub start: Offset,
    pub digest: RangeDigest,
}

impl SwitchBase {
    /// The Raft term of the base's last entry. Every entry after the switch
    /// has a higher one.
    pub fn term(&self) -> Option<u64> {
        term_of_generation(self.generation)
    }
}

/// Why a directory cannot start as a voter of a switch's base. Each one
/// leaves the directory unchanged; the copy starts as a learner instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotAVoter {
    /// The log starts above the seal's start.
    StartsAbove { first: Offset, start: Offset },
    /// The log ends before the base.
    EndsBelow { tail: Offset, offset: Offset },
    /// The record at the base is not the seal's generation-start record.
    NotTheSeal { offset: Offset, generation: u64 },
    /// The records from the seal's start to the base differ from the ones
    /// the seal recorded.
    DigestDiffers { start: Offset, offset: Offset },
    /// The directory's member already started, from another base (`None`:
    /// as an empty Raft log).
    AnotherBase {
        found: Option<(u64, Offset)>,
        base: (u64, Offset),
    },
}

impl std::fmt::Display for NotAVoter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartsAbove { first, start } => {
                write!(
                    f,
                    "the log starts at {first}, above the seal's start {start}"
                )
            }
            Self::EndsBelow { tail, offset } => {
                write!(
                    f,
                    "the log ends at {tail}, so it does not hold the base at {offset}"
                )
            }
            Self::NotTheSeal { offset, generation } => write!(
                f,
                "the record at {offset} is not the seal's record of generation {generation}"
            ),
            Self::DigestDiffers { start, offset } => write!(
                f,
                "the records from {start} to {offset} differ from the ones the seal recorded"
            ),
            Self::AnotherBase { found, base } => write!(
                f,
                "the directory's member started from base {found:?}, not {base:?}"
            ),
        }
    }
}

/// The digest of the records `start..end` of `log`. Fails if the log does
/// not hold every offset of the range.
pub async fn digest(log: &DiskLog, start: Offset, end: Offset) -> Result<RangeDigest> {
    let mut hash = Sha256::new();
    let mut next = start;
    while next < end {
        let records = log
            .read_range_with_trims(ReadRange {
                start: next,
                max_bytes: DIGEST_PAGE,
            })
            .await?;
        if records.is_empty() {
            return Err(missing(log, next));
        }
        for record in records.iter().take_while(|record| record.offset < end) {
            if record.offset != next {
                return Err(missing(log, next));
            }
            hash_record(&mut hash, record);
            next += 1;
        }
    }
    Ok(RangeDigest(hash.finalize().into()))
}

fn missing(log: &DiskLog, offset: Offset) -> StorageError {
    StorageError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: no record at {offset} to digest", log.label()),
    ))
}

fn hash_record(hash: &mut Sha256, record: &LogRecord) {
    hash.update(record.offset.to_be_bytes());
    hash.update(record.timestamp_micros.to_be_bytes());
    hash_mark(hash, record.mark);
    hash_bytes(hash, Some(&record.payload));
    hash_bytes(hash, record.publisher.as_deref());
    hash_bytes(hash, record.headers.as_deref().map(|headers| &headers[..]));
}

fn hash_mark(hash: &mut Sha256, mark: RecordMark) {
    let (kind, a, b, c) = match mark {
        RecordMark::None => (0u8, 0, 0, 0),
        RecordMark::Opens(batch) => (1, batch.producer_id, batch.sequence, u64::from(batch.len)),
        RecordMark::Continues => (2, 0, 0, 0),
        RecordMark::GenerationStart => (3, 0, 0, 0),
        RecordMark::Commit => (4, 0, 0, 0),
        RecordMark::Trim { floor } => (5, floor, 0, 0),
        RecordMark::Membership => (6, 0, 0, 0),
        RecordMark::CounterDelta => (7, 0, 0, 0),
        RecordMark::CounterCheckpoint => (8, 0, 0, 0),
    };
    hash.update([kind]);
    for word in [a, b, c] {
        hash.update(word.to_be_bytes());
    }
}

/// Length-prefixed, with absent and empty told apart.
fn hash_bytes(hash: &mut Sha256, bytes: Option<&[u8]>) {
    match bytes {
        None => hash.update([0]),
        Some(bytes) => {
            hash.update([1]);
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
    }
}

/// Start `dir` as a voter of the switch's `base`.
///
/// `hybrid` is the configuration the hybrid opened the directory with.
/// `vote_for` is this member's node id as the Raft log stores it, and
/// `snapshot` the base snapshot the state machine reads, at the base's
/// entry. When this returns `Ok`, the directory opens as a Raft log
/// (`LogConfig::raft_log`) whose first entry is `base.offset + 1`, with a
/// vote at the base's term, so the member's first election is at a term
/// above every generation the hybrid used.
///
/// In this order, each step on disk before the next:
///
/// 1. Check that the log holds `base.start..=base.offset`, that the record
///    at `base.offset` is the seal's, and that the digest matches.
/// 2. Cut every record above `base.offset`. None was acknowledged.
/// 3. Write the snapshot, then the purged id at the base.
/// 4. Write the vote at the base's term for this member, and the commit at
///    the base.
/// 5. Remove the hybrid's replica-set file.
/// 6. Write the engine marker with the base.
///
/// A crash anywhere before 6 leaves an unmarked directory, and calling
/// this again redoes every step with the same result. A directory already
/// marked with this base is left as it is. One marked with another base,
/// or as an empty Raft log, is refused.
pub async fn start_voter(
    dir: &Path,
    label: &str,
    hybrid: LogConfig,
    base: &SwitchBase,
    vote_for: &str,
    snapshot: StoredSnapshot,
) -> Result<std::result::Result<(), NotAVoter>> {
    let Some(term) = base.term() else {
        return Err(invalid(format!("{label}: a switch base at generation 0")));
    };
    if snapshot.last
        != Some(SnapshotId {
            term,
            index: base.offset,
        })
    {
        return Err(invalid(format!(
            "{label}: the base snapshot covers {:?}, not the base at {}",
            snapshot.last, base.offset
        )));
    }
    match engine_marker::read_marker(dir, label)? {
        None => {}
        Some(Some(found)) if found == (base.generation, base.offset) => return Ok(Ok(())),
        Some(found) => {
            return Ok(Err(NotAVoter::AnotherBase {
                found,
                base: (base.generation, base.offset),
            }));
        }
    }
    if let Err(not) = check_and_cut(dir, label, hybrid, base).await? {
        return Ok(Err(not));
    }
    crash_point(1)?;
    write_member_state(dir, label, base, term, vote_for, snapshot).await?;
    engine_marker::mark_switched(dir, base.generation, base.offset)?;
    Ok(Ok(()))
}

/// Steps 1 and 2.
async fn check_and_cut(
    dir: &Path,
    label: &str,
    hybrid: LogConfig,
    base: &SwitchBase,
) -> Result<std::result::Result<(), NotAVoter>> {
    let hybrid = LogConfig {
        raft_log: false,
        ..hybrid
    };
    let log = DiskLog::open(dir, label, hybrid)?;
    let checked = check(&log, base).await;
    let cut = match checked {
        Ok(Ok(())) => log.truncate(base.offset + 1).await,
        _ => Ok(()),
    };
    let shut = log.shutdown().await;
    let checked = checked?;
    cut?;
    shut?;
    Ok(checked)
}

async fn check(log: &DiskLog, base: &SwitchBase) -> Result<std::result::Result<(), NotAVoter>> {
    let first = log.base_offset();
    if first > base.start {
        return Ok(Err(NotAVoter::StartsAbove {
            first,
            start: base.start,
        }));
    }
    let tail = log.tail_offset().await?;
    if tail <= base.offset {
        return Ok(Err(NotAVoter::EndsBelow {
            tail,
            offset: base.offset,
        }));
    }
    let at_base = log
        .read_range_with_trims(ReadRange {
            start: base.offset,
            max_bytes: 1,
        })
        .await?;
    let is_seal = at_base.first().is_some_and(|record| {
        record.offset == base.offset
            && record.mark == RecordMark::GenerationStart
            && record.generation == Some(base.generation)
    });
    if !is_seal {
        return Ok(Err(NotAVoter::NotTheSeal {
            offset: base.offset,
            generation: base.generation,
        }));
    }
    if digest(log, base.start, base.offset).await? != base.digest {
        return Ok(Err(NotAVoter::DigestDiffers {
            start: base.start,
            offset: base.offset,
        }));
    }
    Ok(Ok(()))
}

/// Steps 3 to 5, on a directory no log has open.
async fn write_member_state(
    dir: &Path,
    label: &str,
    base: &SwitchBase,
    term: u64,
    vote_for: &str,
    snapshot: StoredSnapshot,
) -> Result<()> {
    let snapshots = SnapshotStore::open(dir).map_err(StorageError::Io)?;
    let installed = snapshots.install(snapshot).await;
    let closed = snapshots.close().await;
    if !installed.map_err(StorageError::Io)? {
        return Err(invalid(format!(
            "{label}: holds a snapshot past the switch's base at {}",
            base.offset
        )));
    }
    closed.map_err(StorageError::Io)?;
    crash_point(2)?;
    // After the snapshot: an open refuses a purged id its snapshot does not
    // cover.
    store_purged(dir, base.offset, base.generation).map_err(StorageError::Io)?;
    crash_point(3)?;
    // The vote is the floor: without it the member would campaign at term
    // 1, below the base, and its entries would compare older than the
    // base's. A vote for itself at the base's term grants nobody else
    // anything, since no candidate stands at that term.
    let generation = crate::raft_log::generation_of(term).map_err(StorageError::Io)?;
    ballot::store(
        dir,
        &Ballot {
            generation,
            leader: vote_for.to_string(),
            committed: false,
        },
    )?;
    crash_point(4)?;
    replica_state::store(
        dir,
        &ReplicaState {
            accepted_generation: generation,
            commit_offset: base.offset + 1,
            hold_at_commit: false,
            committed_generation: generation,
        },
    )?;
    crash_point(5)?;
    set::remove(dir)?;
    crash_point(6)?;
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// The step after which [`start_voter`] stops as a crash would.
    static CRASH_AFTER: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn crash_point(step: u8) -> Result<()> {
    if CRASH_AFTER.with(|at| at.get()) == step {
        return Err(StorageError::Io(io::Error::other(format!(
            "crashed after step {step}"
        ))));
    }
    Ok(())
}

#[cfg(not(test))]
fn crash_point(_: u8) -> Result<()> {
    Ok(())
}

fn invalid(why: String) -> StorageError {
    StorageError::Io(io::Error::new(io::ErrorKind::InvalidInput, why))
}
