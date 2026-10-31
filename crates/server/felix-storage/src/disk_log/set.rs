//! Which replicas this one counts as the shard's set.
//!
//! A replica answers a fence or a batch at the set of an assignment, and a
//! restart must not let it forget which set that was. This file keeps the
//! assignment's generation and members, and while a change is pending the set
//! it is leaving, so both majorities stay checkable. See
//! `docs/replication-design.md`, "Electing without the control plane".
//!
//! A file of its own beside `ballot`: a build that predates it ignores it, so
//! rolling back still opens the shard. A missing file means no set was ever
//! recorded, which is every shard on an older build.
//!
//! Like `ballot` it cannot be rebuilt, so a file that does not decode fails
//! the open instead of reading as no set: a replica that forgot its set would
//! count the wrong majority.
//!
//! Nothing reads or writes it yet; the replica's set checks do.

use std::path::{Path, PathBuf};

#[allow(unused_imports)]
use crate::io::sync_dir;
use crate::{Result, StorageError, StorageId};

/// `"FLRS"`.
const MAGIC: u32 = 0x464C_5253;
const VERSION: u16 = 1;
/// magic(4) + version(2) + flags(2), then the set and, when the flag is
/// set, the set it is leaving, then a crc(4) over everything before it.
pub(super) const HEADER_LEN: usize = 8;
/// The flags bit for a set being left.
const FLAG_LEAVING: u16 = 1;
/// Only bounds what a damaged count can claim.
pub(super) const MAX_MEMBERS: usize = 64;
/// Node ids are short; this only bounds what a damaged length can claim.
pub(super) const MAX_NODE_LEN: usize = 1024;
const FILE_NAME: &str = "replica-set";

/// A replica in a set: its node id and the storage identity the fence counts
/// it under, when placement named one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Member {
    pub(super) node: String,
    pub(super) storage: Option<StorageId>,
}

/// One assignment's replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Membership {
    pub(super) generation: u64,
    pub(super) members: Vec<Member>,
}

/// The set a replica answers at, and the one it is leaving while a change is
/// pending. Cleared by writing a set without it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReplicaSet {
    pub(super) current: Membership,
    pub(super) leaving: Option<Membership>,
}

impl ReplicaSet {
    pub(super) fn encode(&self) -> Vec<u8> {
        let flags = if self.leaving.is_some() {
            FLAG_LEAVING
        } else {
            0
        };
        let mut out = Vec::with_capacity(HEADER_LEN + 64);
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&flags.to_be_bytes());
        self.current.encode_into(&mut out);
        if let Some(leaving) = &self.leaving {
            leaving.encode_into(&mut out);
        }
        let crc = crate::segment::format::crc32(&[&out]);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        let body_len = bytes.len().checked_sub(4)?;
        let (body, crc) = bytes.split_at(body_len);
        if body.len() < HEADER_LEN
            || u32::from_be_bytes(body[0..4].try_into().ok()?) != MAGIC
            || u16::from_be_bytes(body[4..6].try_into().ok()?) != VERSION
            || crate::segment::format::crc32(&[body]) != u32::from_be_bytes(crc.try_into().ok()?)
        {
            return None;
        }
        let flags = u16::from_be_bytes(body[6..8].try_into().ok()?);
        if flags & !FLAG_LEAVING != 0 {
            return None;
        }
        let mut rest = &body[HEADER_LEN..];
        let current = Membership::decode_from(&mut rest)?;
        let leaving = if flags & FLAG_LEAVING != 0 {
            Some(Membership::decode_from(&mut rest)?)
        } else {
            None
        };
        // Slack would let two byte strings mean one set.
        rest.is_empty().then_some(Self { current, leaving })
    }
}

impl Membership {
    /// generation(8) + count(2), then per member: node length(2), the node id,
    /// has_storage(1) and, when set, the storage id(16).
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.generation.to_be_bytes());
        out.extend_from_slice(&(self.members.len() as u16).to_be_bytes());
        for member in &self.members {
            out.extend_from_slice(&(member.node.len() as u16).to_be_bytes());
            out.extend_from_slice(member.node.as_bytes());
            match &member.storage {
                Some(id) => {
                    out.push(1);
                    out.extend_from_slice(&id.to_bytes());
                }
                None => out.push(0),
            }
        }
    }

    fn decode_from(rest: &mut &[u8]) -> Option<Self> {
        let generation = u64::from_be_bytes(take(rest, 8)?.try_into().ok()?);
        let count = u16::from_be_bytes(take(rest, 2)?.try_into().ok()?) as usize;
        if count > MAX_MEMBERS {
            return None;
        }
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            let len = u16::from_be_bytes(take(rest, 2)?.try_into().ok()?) as usize;
            if len > MAX_NODE_LEN {
                return None;
            }
            let node = String::from_utf8(take(rest, len)?.to_vec()).ok()?;
            let storage = match take(rest, 1)?[0] {
                0 => None,
                1 => Some(StorageId::from_bytes(take(rest, 16)?.try_into().ok()?)),
                _ => return None,
            };
            members.push(Member { node, storage });
        }
        Some(Self {
            generation,
            members,
        })
    }

    fn check(&self) -> Result<()> {
        let too_long = self.members.iter().any(|m| m.node.len() > MAX_NODE_LEN);
        if self.members.len() > MAX_MEMBERS || too_long {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a replica set past the size the file allows",
            )));
        }
        Ok(())
    }
}

fn take<'a>(rest: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (head, tail) = rest.split_at_checked(n)?;
    *rest = tail;
    Some(head)
}

/// Read a shard's replica set, or `None` if it has never kept one.
pub(super) fn load(dir: &Path) -> Result<Option<ReplicaSet>> {
    let path = path_in(dir);
    match std::fs::read(&path) {
        Ok(bytes) => ReplicaSet::decode(&bytes).map(Some).ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} does not decode; it names the replicas this one counts \
                     and cannot be rebuilt from the log",
                    path.display()
                ),
            ))
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Write a shard's replica set durably, as `ballot::store` writes a ballot: a
/// crash leaves the old set or the new one. A caller that answers at the set
/// afterwards has made a promise that survives a restart.
pub(super) fn store(dir: &Path, set: &ReplicaSet) -> Result<()> {
    use std::io::Write;

    set.current.check()?;
    if let Some(leaving) = &set.leaving {
        leaving.check()?;
    }
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&set.encode()).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    Ok(())
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
