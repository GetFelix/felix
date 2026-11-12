//! The id of the last purged entry, kept in `raft-purged`.
//!
//! Segments are deleted whole, so the log's files cannot say where a purge
//! stopped, and a purge past the log's end leaves no entry to carry the id.
//! The file is written before anything is deleted: the log starts after the
//! id it names whatever bytes remain below.
//!
//! Nothing can rebuild it, so a file that does not decode fails the open.

use std::io;
use std::path::Path;

use super::{invalid, state_file};
use crate::log::Offset;

/// `"FLPG"`.
const MAGIC: u32 = 0x464C_5047;
const VERSION: u16 = 1;
/// magic(4) + version(2) + reserved(2) + index(8) + generation(8) + crc(4)
const ENCODED_LEN: usize = 28;
const FILE_NAME: &str = "raft-purged";

/// The last purged entry: its index and the generation that stands for its
/// term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Purged {
    pub(super) index: Offset,
    pub(super) generation: u64,
}

impl Purged {
    pub(super) fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut out = [0u8; ENCODED_LEN];
        out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        out[4..6].copy_from_slice(&VERSION.to_be_bytes());
        out[8..16].copy_from_slice(&self.index.to_be_bytes());
        out[16..24].copy_from_slice(&self.generation.to_be_bytes());
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
        let generation = u64::from_be_bytes(bytes[16..24].try_into().ok()?);
        // Generation 0 is no term, so it names no entry.
        (generation != 0).then_some(Self {
            index: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            generation,
        })
    }
}

pub(super) fn load(dir: &Path) -> io::Result<Option<Purged>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    Purged::decode(&bytes).map(Some).ok_or_else(|| {
        invalid(format!(
            "{} does not decode; it holds the id of the last purged entry and cannot be rebuilt from the log",
            dir.join(FILE_NAME).display()
        ))
    })
}

pub(super) fn store(dir: &Path, purged: &Purged) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[&purged.encode()])
}
