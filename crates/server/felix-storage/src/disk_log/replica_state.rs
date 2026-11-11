//! What this broker has promised about a shard as a replica: the highest
//! leadership generation it has accepted, and how much of the log it knows is
//! committed.
//!
//! Both have to survive a restart. The routing view a follower checks a sender
//! against is rebuilt from the control plane after one, and may lag; the
//! accepted generation is what still refuses a leader this broker has already
//! seen superseded. The commit offset is what keeps a truncation or a rebuild
//! from discarding records a majority acknowledged. See
//! `docs/replication-design.md`, "Divergence and truncation".
//!
//! It also records whether retention is held at the commit offset, so a log
//! under `Quorum` replication is held from the moment it opens rather than
//! from its first replication pass or batch.
//!
//! Unlike the generation history this is not derived and cannot be rebuilt, so
//! a file that is present but does not decode fails the open instead of
//! reading as empty.

use std::path::{Path, PathBuf};

use super::crash_point;
use crate::io::sync_dir;
use crate::log::Offset;
use crate::{Result, StorageError};

/// `"FLRS"`.
const MAGIC: u32 = 0x464C_5253;
const VERSION: u16 = 1;
/// A file that also holds the committed generation. Only a Raft log writes
/// one, so every other log's file stays readable by older builds.
const VERSION_COMMITTED: u16 = 2;
/// magic(4) + version(2) + flags(2) + generation(8) + commit(8) + crc(4)
const ENCODED_LEN: usize = 28;
/// [`ENCODED_LEN`] with the committed generation(8) before the crc.
const ENCODED_LEN_COMMITTED: usize = 36;
const FILE_NAME: &str = "replica";
/// Retention is held at the commit offset. The flags were a reserved zero
/// that no build checked, so an older build reads this file unchanged.
const FLAG_HOLD_AT_COMMIT: u16 = 1;

/// The persisted state. The generation and the commit offset only ever rise,
/// except that an offline restore to a backup point lowers the commit offset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ReplicaState {
    pub(super) accepted_generation: u64,
    /// One past the last record known committed. Records below it that this
    /// log holds must not be cut.
    pub(super) commit_offset: Offset,
    /// See `DiskLog::hold_retention_at_commit`.
    pub(super) hold_at_commit: bool,
    /// The generation of the record just below `commit_offset` when a Raft
    /// log saved it, zero when not known. An index alone does not name an
    /// entry: a log can hold another term's entry there.
    pub(super) committed_generation: u64,
}

impl ReplicaState {
    pub(super) fn encode(&self) -> Vec<u8> {
        let (version, len) = if self.committed_generation == 0 {
            (VERSION, ENCODED_LEN)
        } else {
            (VERSION_COMMITTED, ENCODED_LEN_COMMITTED)
        };
        let mut out = vec![0u8; len];
        out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        out[4..6].copy_from_slice(&version.to_be_bytes());
        let flags = if self.hold_at_commit {
            FLAG_HOLD_AT_COMMIT
        } else {
            0
        };
        out[6..8].copy_from_slice(&flags.to_be_bytes());
        out[8..16].copy_from_slice(&self.accepted_generation.to_be_bytes());
        out[16..24].copy_from_slice(&self.commit_offset.to_be_bytes());
        if version == VERSION_COMMITTED {
            out[24..32].copy_from_slice(&self.committed_generation.to_be_bytes());
        }
        let body = len - 4;
        let crc = crate::segment::format::crc32(&[&out[..body]]);
        out[body..].copy_from_slice(&crc.to_be_bytes());
        out
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < ENCODED_LEN {
            return None;
        }
        let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
        let flags = u16::from_be_bytes(bytes[6..8].try_into().ok()?);
        let len = match version {
            VERSION => ENCODED_LEN,
            VERSION_COMMITTED => ENCODED_LEN_COMMITTED,
            _ => return None,
        };
        if magic != MAGIC || bytes.len() != len {
            return None;
        }
        let body = len - 4;
        let crc = u32::from_be_bytes(bytes[body..].try_into().ok()?);
        if crate::segment::format::crc32(&[&bytes[..body]]) != crc {
            return None;
        }
        let committed_generation = if version == VERSION_COMMITTED {
            u64::from_be_bytes(bytes[24..32].try_into().ok()?)
        } else {
            0
        };
        // Version 2 with no generation is not something `encode` writes.
        if version == VERSION_COMMITTED && committed_generation == 0 {
            return None;
        }
        Some(Self {
            accepted_generation: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            commit_offset: u64::from_be_bytes(bytes[16..24].try_into().ok()?),
            hold_at_commit: flags & FLAG_HOLD_AT_COMMIT != 0,
            committed_generation,
        })
    }
}

/// Read a shard's replica state. Absent reads as zero: a shard that has never
/// been replicated to, or one written before this file existed.
pub(super) fn load(dir: &Path) -> Result<ReplicaState> {
    let path = path_in(dir);
    match crate::fs::read(&path) {
        Ok(bytes) => ReplicaState::decode(&bytes).ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} does not decode; it holds the highest generation this \
                     replica accepted and cannot be rebuilt from the log",
                    path.display()
                ),
            ))
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(ReplicaState::default()),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Write a shard's replica state durably: a temporary, fsynced, renamed over
/// the old file, and the directory synced, so a crash leaves the old state or
/// the new one and never neither.
pub(super) fn store(dir: &Path, state: &ReplicaState) -> Result<()> {
    use std::io::Write;

    let started = std::time::Instant::now();
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    let step = || crash_point::reached(dir).map_err(StorageError::Io);
    step()?;
    {
        let mut file = crate::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&state.encode()).map_err(StorageError::Io)?;
        step()?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    step()?;
    crate::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    step()?;
    sync_dir(dir).map_err(StorageError::Io)?;
    metrics::counter!(crate::metrics_names::REPLICA_STATE_WRITES_TOTAL).increment(1);
    metrics::histogram!(crate::metrics_names::REPLICA_STATE_WRITE_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    Ok(())
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
