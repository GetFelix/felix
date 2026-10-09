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
//! `DiskLog::accept_generation` and the open's reading of the two files live
//! here, where `scripts/check_spec_pairing.py` pairs them with the formal model.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::replica_state::{self, ReplicaState};
use super::{DiskLog, GenerationCheck};
use crate::io::sync_dir;
use crate::{Result, StorageError};

/// `"FLBL"`.
const MAGIC: u32 = 0x464C_424C;
const VERSION: u16 = 1;
/// magic(4) + version(2) + leader length(2) + generation(8), then the
/// leader's node id and a crc(4) over everything before it.
pub(super) const HEADER_LEN: usize = 16;
/// Node ids are short; this only bounds what a damaged length can claim.
pub(super) const MAX_LEADER_LEN: usize = 1024;
const FILE_NAME: &str = "ballot";

/// A generation and the leader it was accepted from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Ballot {
    pub(super) generation: u64,
    pub(super) leader: String,
}

impl Ballot {
    pub(super) fn encode(&self) -> Vec<u8> {
        let leader = self.leader.as_bytes();
        let mut out = Vec::with_capacity(HEADER_LEN + leader.len() + 4);
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&(leader.len() as u16).to_be_bytes());
        out.extend_from_slice(&self.generation.to_be_bytes());
        out.extend_from_slice(leader);
        let crc = crate::segment::format::crc32(&[&out]);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN + 4 {
            return None;
        }
        let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
        let len = u16::from_be_bytes(bytes[6..8].try_into().ok()?) as usize;
        if magic != MAGIC || version != VERSION || len > MAX_LEADER_LEN {
            return None;
        }
        if bytes.len() != HEADER_LEN + len + 4 {
            return None;
        }
        let body = &bytes[..HEADER_LEN + len];
        let crc = u32::from_be_bytes(bytes[HEADER_LEN + len..].try_into().ok()?);
        if crate::segment::format::crc32(&[body]) != crc {
            return None;
        }
        Some(Self {
            generation: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            leader: String::from_utf8(body[HEADER_LEN..].to_vec()).ok()?,
        })
    }
}

impl DiskLog {
    /// The highest leadership generation a leader of this shard was accepted
    /// at here, as a follower or as the leader itself. Zero if none.
    pub fn accepted_generation(&self) -> u64 {
        self.inner.accepted_generation.load(Ordering::Acquire)
    }

    /// The leader this log accepted its generation from, if it was named.
    pub fn accepted_leader(&self) -> Option<Arc<str>> {
        self.inner.ballot.read().1.clone()
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
        if let Some(found) = check(&self.inner.ballot.read(), generation, leader) {
            return Ok(found);
        }
        let inner = Arc::clone(&self.inner);
        let leader: Option<Arc<str>> = leader.map(Arc::from);
        tokio::task::spawn_blocking(move || {
            let mut persisted = inner.replica_persisted.lock();
            // Re-checked under the writer's lock: a concurrent request may
            // have raised it, or named its leader, while this waited.
            if let Some(found) = check(&inner.ballot.read(), generation, leader.as_deref()) {
                return Ok(found);
            }
            // Before `replica`, so a crash between the two leaves a ballot
            // the open takes the generation from, never a raised generation
            // with no leader.
            if let Some(leader) = &leader {
                store(
                    &inner.dir,
                    &Ballot {
                        generation,
                        leader: leader.to_string(),
                    },
                )?;
            }
            let raised = generation > inner.accepted_generation.load(Ordering::Acquire);
            if raised {
                let state = replica_state::ReplicaState {
                    accepted_generation: generation,
                    commit_offset: inner.commit_offset.load(Ordering::Acquire),
                };
                replica_state::store(&inner.dir, &state)?;
                *persisted = (state, Some(std::time::Instant::now()));
            }
            *inner.ballot.write() = (generation, leader);
            inner
                .accepted_generation
                .store(generation, Ordering::Release);
            Ok(if raised {
                GenerationCheck::Raised
            } else {
                GenerationCheck::Current
            })
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }
}

/// What the accepted ballot says of `leader` at `generation`, or `None` when
/// the answer needs a write: a raise, or the first leader named at the
/// accepted generation.
pub(super) fn check(
    (accepted, promised): &(u64, Option<Arc<str>>),
    generation: u64,
    leader: Option<&str>,
) -> Option<GenerationCheck> {
    if generation < *accepted {
        return Some(GenerationCheck::Superseded {
            accepted: *accepted,
        });
    }
    if generation > *accepted {
        return None;
    }
    match (leader, promised) {
        (None, _) => Some(GenerationCheck::Current),
        (Some(leader), Some(promised)) if **promised == *leader => Some(GenerationCheck::Current),
        (Some(_), Some(promised)) => Some(GenerationCheck::Promised {
            leader: promised.to_string(),
        }),
        (Some(_), None) => None,
    }
}

/// The generation and leader a shard opens with, from its `replica` state
/// and its ballot.
pub(super) fn reconcile(dir: &Path, replica: &ReplicaState) -> Result<(u64, Option<Arc<str>>)> {
    // A ballot ahead of `replica` is a raise that crashed between the two
    // writes; it was never answered, but taking it only refuses more.
    Ok(match load(dir)? {
        Some(ballot) if ballot.generation >= replica.accepted_generation => {
            (ballot.generation, Some(Arc::<str>::from(ballot.leader)))
        }
        _ => (replica.accepted_generation, None),
    })
}

/// Read a shard's ballot, or `None` if it has never kept one.
pub(super) fn load(dir: &Path) -> Result<Option<Ballot>> {
    let path = path_in(dir);
    match std::fs::read(&path) {
        Ok(bytes) => Ballot::decode(&bytes).map(Some).ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} does not decode; it names the leader this replica accepted \
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
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&ballot.encode()).map_err(StorageError::Io)?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    sync_dir(dir).map_err(StorageError::Io)?;
    Ok(())
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
