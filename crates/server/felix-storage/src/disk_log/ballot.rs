//! Which leader this replica accepted its generation from.
//!
//! The control plane names one leader per generation, so the generation
//! alone used to be enough. A replica that can be asked by two nodes at the
//! same generation has to remember whom it answered, or both can gather a
//! majority. See `docs/replication-design.md`, "Ballots".
//!
//! A file of its own beside `replica` rather than a field in it: a build that
//! predates ballots ignores it, so rolling back still opens the shard. It is
//! written before `replica` when a generation is raised, so after a crash
//! between the two it may name a generation `replica` does not have yet, and
//! the open takes the higher. One older than `replica` names no leader for the
//! accepted generation.
//!
//! Like `replica` it cannot be rebuilt, so a file that does not decode fails
//! the open.
//!
//! Format v2 adds the vote's committed flag (openraft's `Vote::committed`).
//! Only a committed vote is written as v2, and only through the test hook
//! until the fleet gate for segment format v9 wires a real caller, so every
//! ballot a broker writes today is one the previous release reads. That
//! release refuses a v2 file at open: its decoder takes only v1. That comes
//! after the open's torn-tail repair and mark write, which both builds read.
//!
//! `DiskLog::accept_generation` and the open's reading of the two files live
//! here, where `scripts/check_spec_pairing.py` pairs them with the formal model.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::crash_point;
use super::replica_state::{self, ReplicaState};
use super::{DiskLog, GenerationCheck, ReplicaSet};
use crate::io::sync_dir;
use crate::{Result, StorageError};

/// `"FLBL"`.
const MAGIC: u32 = 0x464C_424C;
/// The newest format this build reads.
const VERSION: u16 = 2;
/// The format of a ballot whose vote is not committed.
const UNCOMMITTED_VERSION: u16 = 1;
/// magic(4) + version(2) + leader length(2) + generation(8), then the
/// leader's node id and a crc(4) over everything before it.
pub(super) const HEADER_LEN: usize = 16;
/// v2: a flags byte after the generation, then the leader and the crc.
const COMMITTED_HEADER_LEN: usize = HEADER_LEN + 1;
const FLAG_COMMITTED: u8 = 1;
/// Node ids are short; this only bounds what a damaged length can claim.
pub(super) const MAX_LEADER_LEN: usize = 1024;
const FILE_NAME: &str = "ballot";

/// A generation, the leader it was accepted from, and whether that vote is
/// committed: a majority granted it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Ballot {
    pub(super) generation: u64,
    pub(super) leader: String,
    pub(super) committed: bool,
}

/// Why a ballot file does not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Undecodable {
    Damaged,
    /// A format newer than this build reads.
    Newer(u16),
}

/// The accepted generation as the log holds it: the generation, the leader
/// named at it, and whether that vote is committed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Accepted {
    pub(super) generation: u64,
    pub(super) leader: Option<Arc<str>>,
    /// Only ever set with a leader, and cleared by any raise.
    pub(super) committed: bool,
}

impl Ballot {
    /// v1 unless the vote is committed, so a ballot without the flag stays
    /// one the previous release reads.
    pub(super) fn encode(&self) -> Vec<u8> {
        let leader = self.leader.as_bytes();
        let mut out = Vec::with_capacity(COMMITTED_HEADER_LEN + leader.len() + 4);
        out.extend_from_slice(&MAGIC.to_be_bytes());
        let version = if self.committed {
            VERSION
        } else {
            UNCOMMITTED_VERSION
        };
        out.extend_from_slice(&version.to_be_bytes());
        out.extend_from_slice(&(leader.len() as u16).to_be_bytes());
        out.extend_from_slice(&self.generation.to_be_bytes());
        if self.committed {
            out.push(FLAG_COMMITTED);
        }
        out.extend_from_slice(leader);
        let crc = crate::segment::format::crc32(&[&out]);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    pub(super) fn decode(bytes: &[u8]) -> std::result::Result<Self, Undecodable> {
        decode_up_to(bytes, VERSION)
    }
}

/// Decode a ballot as a build that reads formats up to `newest` would.
///
/// The crc closes the file in every format, so it is checked before the
/// version: a damaged version field is damage, never a newer format.
fn decode_up_to(bytes: &[u8], newest: u16) -> std::result::Result<Ballot, Undecodable> {
    use Undecodable::Damaged;

    if bytes.len() < HEADER_LEN + 4 {
        return Err(Damaged);
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    let crc = u32::from_be_bytes(crc.try_into().map_err(|_| Damaged)?);
    if crate::segment::format::crc32(&[body]) != crc {
        return Err(Damaged);
    }
    let magic = u32::from_be_bytes(body[0..4].try_into().map_err(|_| Damaged)?);
    let version = u16::from_be_bytes(body[4..6].try_into().map_err(|_| Damaged)?);
    let len = u16::from_be_bytes(body[6..8].try_into().map_err(|_| Damaged)?) as usize;
    if magic != MAGIC || version == 0 {
        return Err(Damaged);
    }
    if version > newest {
        return Err(Undecodable::Newer(version));
    }
    let (header_len, committed) = if version == UNCOMMITTED_VERSION {
        (HEADER_LEN, false)
    } else {
        match body.get(HEADER_LEN) {
            Some(&FLAG_COMMITTED) => (COMMITTED_HEADER_LEN, true),
            Some(0) => (COMMITTED_HEADER_LEN, false),
            // An unknown flag could change what the vote means.
            _ => return Err(Damaged),
        }
    };
    if len > MAX_LEADER_LEN || body.len() != header_len + len {
        return Err(Damaged);
    }
    let leader = String::from_utf8(body[header_len..].to_vec()).map_err(|_| Damaged)?;
    // A given-up generation names no leader, so no vote of it is committed.
    if committed && leader.is_empty() {
        return Err(Damaged);
    }
    Ok(Ballot {
        generation: u64::from_be_bytes(body[8..16].try_into().map_err(|_| Damaged)?),
        leader,
        committed,
    })
}

impl DiskLog {
    /// The highest leadership generation a leader of this shard was accepted
    /// at here, as a follower or as the leader itself. Zero if none.
    pub fn accepted_generation(&self) -> u64 {
        self.inner.accepted_generation.load(Ordering::Acquire)
    }

    /// The leader this log accepted its generation from, if it was named.
    pub fn accepted_leader(&self) -> Option<Arc<str>> {
        self.inner.ballot.read().leader.clone()
    }

    /// Whether the vote for [`Self::accepted_leader`] at the accepted
    /// generation is committed: a majority granted it (openraft's
    /// `Vote::committed`). Never true without a leader, and false again once
    /// a newer generation is accepted.
    pub fn accepted_vote_committed(&self) -> bool {
        self.inner.ballot.read().committed
    }

    /// Accept `leader` at `generation`, persisting it first if it is new.
    ///
    /// Returns once a raised generation is on disk, so a caller that
    /// acknowledges afterwards has made a promise that survives a restart: a
    /// leader older than this one is refused from here on, whatever the
    /// routing view says after the restart.
    ///
    /// With a leader named, the promise is a ballot: at the generation already
    /// accepted only that leader is [`GenerationCheck::Current`], and any
    /// other is [`GenerationCheck::Promised`]. A generation accepted with no
    /// leader named takes the first one that asks, persisted before this
    /// returns. `None` checks the generation alone.
    pub async fn accept_generation(
        &self,
        generation: u64,
        leader: Option<&str>,
    ) -> Result<GenerationCheck> {
        let raised = self.raise_ballot(generation, leader, false, None).await?;
        Ok(raised.expect("no set to check"))
    }

    /// [`Self::accept_generation`] for a candidate that read `kept` before
    /// standing: the ballot is raised only while the kept set is still
    /// `kept`, checked under the writer's lock that every set raise takes,
    /// so no set lands between the check and the ballot. `None`, with
    /// nothing written, when the set moved. `Elect`'s guard on the kept set
    /// in `docs/formal/FelixShard.tla` is atomic with its promise.
    pub async fn accept_generation_on_set(
        &self,
        generation: u64,
        leader: &str,
        kept: ReplicaSet,
    ) -> Result<Option<GenerationCheck>> {
        self.raise_ballot(generation, Some(leader), false, Some(kept))
            .await
    }

    /// `committed` marks the vote committed; it needs a leader, or `store`
    /// refuses it before writing. A raise
    /// without it, or to another generation, clears the mark.
    async fn raise_ballot(
        &self,
        generation: u64,
        leader: Option<&str>,
        committed: bool,
        kept: Option<ReplicaSet>,
    ) -> Result<Option<GenerationCheck>> {
        if kept.is_none()
            && let Some(found) = check(&self.inner.ballot.read(), generation, leader, committed)
        {
            return Ok(Some(found));
        }
        let inner = Arc::clone(&self.inner);
        let leader: Option<Arc<str>> = leader.map(Arc::from);
        felix_common::blocking::spawn(move || {
            let mut persisted = inner.replica_persisted.lock();
            if let Some(kept) = &kept
                && inner.replica_set.read().as_ref() != Some(kept)
            {
                return Ok(None);
            }
            // Re-checked under the writer's lock: a concurrent request may
            // have raised it, or named its leader, while this waited.
            if let Some(found) = check(
                &inner.ballot.read(),
                generation,
                leader.as_deref(),
                committed,
            ) {
                return Ok(Some(found));
            }
            // Before `replica`, so a crash between the two leaves a ballot
            // the open takes the generation from, never a raised generation
            // with no leader. The committed mark is in the same file, so it
            // is on disk exactly when its vote is.
            if let Some(leader) = &leader {
                store(
                    &inner.dir,
                    &Ballot {
                        generation,
                        leader: leader.to_string(),
                        committed,
                    },
                )?;
            }
            let raised = generation > inner.accepted_generation.load(Ordering::Acquire);
            if raised {
                let state = replica_state::ReplicaState {
                    accepted_generation: generation,
                    commit_offset: inner.commit_offset.load(Ordering::Acquire),
                    hold_at_commit: inner.hold_at_commit.load(Ordering::Acquire),
                };
                replica_state::store(&inner.dir, &state)?;
                inner
                    .commit_persisted
                    .store(state.commit_offset, Ordering::Release);
                *persisted = (state, Some(tokio::time::Instant::now()));
            }
            *inner.ballot.write() = Accepted {
                generation,
                committed: committed && leader.is_some(),
                leader,
            };
            inner
                .accepted_generation
                .store(generation, Ordering::Release);
            Ok(Some(if raised {
                GenerationCheck::Raised
            } else {
                GenerationCheck::Current
            }))
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Give up the generation this log accepted: no leader is accepted at it
    /// from here on, this broker included. A newer generation still is, and
    /// its ballot replaces this one. On disk before this returns.
    ///
    /// For a broker that led that generation and restarted after a crash that
    /// may have cost it records it acknowledged (#1295). Kept as a ballot
    /// naming no leader, so it survives restarts and an older build refuses
    /// to lead the generation too.
    pub async fn give_up_generation(&self) -> Result<u64> {
        let inner = Arc::clone(&self.inner);
        felix_common::blocking::spawn(move || {
            // The writer's lock, so a raise cannot land between the read and
            // the write and be given up unseen.
            let _persisted = inner.replica_persisted.lock();
            let generation = inner.accepted_generation.load(Ordering::Acquire);
            store(
                &inner.dir,
                &Ballot {
                    generation,
                    leader: String::new(),
                    committed: false,
                },
            )?;
            *inner.ballot.write() = Accepted {
                generation,
                leader: Some(Arc::from("")),
                committed: false,
            };
            Ok(generation)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Whether this log gave up its accepted generation and has accepted no
    /// newer one since. See [`Self::give_up_generation`].
    pub fn gave_up(&self) -> bool {
        self.inner.ballot.read().leader.as_deref() == Some("")
    }
}

#[cfg(any(debug_assertions, test, feature = "fault-injection"))]
impl DiskLog {
    /// Mark the vote for `leader` at `generation` committed, accepting it
    /// first if it is new, the way a build past the v9 fleet gate will. The
    /// ballot is then written in format v2, which the previous release
    /// refuses. Not an API: tests use it until the gated caller exists.
    #[doc(hidden)]
    pub async fn commit_vote_for_tests(
        &self,
        generation: u64,
        leader: &str,
    ) -> Result<GenerationCheck> {
        let raised = self
            .raise_ballot(generation, Some(leader), true, None)
            .await?;
        Ok(raised.expect("no set to check"))
    }
}

/// What the accepted ballot says of `leader` at `generation`, or `None` when
/// the answer needs a write: a raise, the first leader named at the
/// accepted generation, or `committed` marking that leader's vote.
pub(super) fn check(
    accepted: &Accepted,
    generation: u64,
    leader: Option<&str>,
    committed: bool,
) -> Option<GenerationCheck> {
    if generation < accepted.generation {
        return Some(GenerationCheck::Superseded {
            accepted: accepted.generation,
        });
    }
    if generation > accepted.generation {
        return None;
    }
    match (leader, &accepted.leader) {
        (None, _) => Some(GenerationCheck::Current),
        (Some(leader), Some(promised)) if **promised == *leader => {
            (!committed || accepted.committed).then_some(GenerationCheck::Current)
        }
        (Some(_), Some(promised)) => Some(GenerationCheck::Promised {
            leader: promised.to_string(),
        }),
        (Some(_), None) => None,
    }
}

/// The generation, leader and committed mark a shard opens with, from its
/// `replica` state and its ballot.
pub(super) fn reconcile(dir: &Path, replica: &ReplicaState) -> Result<Accepted> {
    // A ballot ahead of `replica` is a raise that crashed between the two
    // writes; it was never answered, but taking it only refuses more. One
    // behind it belongs to an older generation, and so does its mark.
    Ok(match load(dir)? {
        Some(ballot) if ballot.generation >= replica.accepted_generation => Accepted {
            generation: ballot.generation,
            leader: Some(Arc::<str>::from(ballot.leader)),
            committed: ballot.committed,
        },
        _ => Accepted {
            generation: replica.accepted_generation,
            leader: None,
            committed: false,
        },
    })
}

/// Read a shard's ballot, or `None` if it has never kept one.
pub(super) fn load(dir: &Path) -> Result<Option<Ballot>> {
    let path = path_in(dir);
    match crate::fs::read(&path) {
        Ok(bytes) => Ballot::decode(&bytes).map(Some).map_err(|err| {
            let why = match err {
                Undecodable::Damaged => "does not decode".to_string(),
                Undecodable::Newer(version) => format!(
                    "is ballot format v{version}, written by a newer build; this build \
                     reads up to v{VERSION}"
                ),
            };
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} {why}; it names the leader this replica accepted \
                     its generation from and cannot be rebuilt from the log",
                    path.display()
                ),
            ))
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Write a shard's ballot durably, as `replica_state::store` writes its state:
/// a crash leaves the old ballot or the new one.
pub(super) fn store(dir: &Path, ballot: &Ballot) -> Result<()> {
    use std::io::Write;

    if ballot.leader.len() > MAX_LEADER_LEN {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("a leader id of {} bytes is too long", ballot.leader.len()),
        )));
    }
    // The decoder refuses this, so writing it would fail the next open.
    if ballot.committed && ballot.leader.is_empty() {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a committed vote needs a leader",
        )));
    }
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    let step = || crash_point::reached(dir).map_err(StorageError::Io);
    step()?;
    {
        let mut file = crate::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&ballot.encode()).map_err(StorageError::Io)?;
        step()?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    step()?;
    crate::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    step()?;
    sync_dir(dir).map_err(StorageError::Io)?;
    Ok(())
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
