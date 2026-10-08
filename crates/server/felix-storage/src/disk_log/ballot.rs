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

use std::path::{Path, PathBuf};

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
