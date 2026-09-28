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
//! Unlike the generation history this is not derived and cannot be rebuilt, so
//! a file that is present but does not decode fails the open instead of
//! reading as empty.

use std::path::{Path, PathBuf};

use crate::io::sync_dir;
use crate::log::Offset;
use crate::{Result, StorageError};

/// `"FLRS"`.
const MAGIC: u32 = 0x464C_5253;
const VERSION: u16 = 1;
/// magic(4) + version(2) + reserved(2) + generation(8) + commit(8) + crc(4)
pub(super) const ENCODED_LEN: usize = 28;
const FILE_NAME: &str = "replica";

/// The persisted state. Both fields only ever rise, except that an offline
/// restore to a backup point lowers the commit offset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ReplicaState {
    pub(super) accepted_generation: u64,
    /// One past the last record known committed. Records below it that this
    /// log holds must not be cut.
    pub(super) commit_offset: Offset,
}

impl ReplicaState {
    pub(super) fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut out = [0u8; ENCODED_LEN];
        out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        out[4..6].copy_from_slice(&VERSION.to_be_bytes());
        out[8..16].copy_from_slice(&self.accepted_generation.to_be_bytes());
        out[16..24].copy_from_slice(&self.commit_offset.to_be_bytes());
        let crc = crate::segment::format::crc32(&[&out[0..24]]);
        out[24..28].copy_from_slice(&crc.to_be_bytes());
        out
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ENCODED_LEN {
            return None;
        }
        let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
        let crc = u32::from_be_bytes(bytes[24..28].try_into().ok()?);
        if magic != MAGIC || version != VERSION {
            return None;
        }
        if crate::segment::format::crc32(&[&bytes[0..24]]) != crc {
            return None;
        }
        Some(Self {
            accepted_generation: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            commit_offset: u64::from_be_bytes(bytes[16..24].try_into().ok()?),
        })
    }
}

/// Read a shard's replica state. Absent reads as zero: a shard that has never
/// been replicated to, or one written before this file existed.
pub(super) fn load(dir: &Path) -> Result<ReplicaState> {
    let path = path_in(dir);
    match std::fs::read(&path) {
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

    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&state.encode()).map_err(StorageError::Io)?;
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
