//! The copy's reservation, kept in `raft-reserved`: the highest Raft index
//! the copy may report as flushed before the file is raised again.
//!
//! On a relaxed broker a copy can count an entry toward a commit before
//! the entry is on disk, so a power cut can take a committed entry from
//! every copy that held it. A re-form after that must not give the lost
//! indexes out again, and the reservation is how it knows how far to pad:
//! every count is at or below a reservation that was synced first. A
//! rotation carries the reservation to the new copy, so it outlives the
//! copy that took it. See `docs/storage-format.md`, "`raft-reserved`".
//!
//! Nothing can rebuild it, so a file that does not decode is an error.

use std::io;
use std::path::Path;

use super::{invalid, state_file};

/// `"FLRS"`.
const MAGIC: u32 = 0x464C_5253;
const VERSION: u16 = 1;
/// magic(4) + version(2) + reserved(2) + index(8) + crc(4)
const ENCODED_LEN: usize = 20;
pub(crate) const FILE_NAME: &str = "raft-reserved";

fn encode(index: u64) -> [u8; ENCODED_LEN] {
    let mut out = [0u8; ENCODED_LEN];
    out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&VERSION.to_be_bytes());
    out[8..16].copy_from_slice(&index.to_be_bytes());
    let crc = crate::segment::format::crc32(&[&out[0..16]]);
    out[16..20].copy_from_slice(&crc.to_be_bytes());
    out
}

fn decode(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != ENCODED_LEN {
        return None;
    }
    let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
    let crc = u32::from_be_bytes(bytes[16..20].try_into().ok()?);
    if magic != MAGIC || version != VERSION {
        return None;
    }
    if crate::segment::format::crc32(&[&bytes[0..16]]) != crc {
        return None;
    }
    Some(u64::from_be_bytes(bytes[8..16].try_into().ok()?))
}

/// The reservation in `dir`, or `None` when the copy never took one.
pub(crate) fn load(dir: &Path) -> io::Result<Option<u64>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    decode(&bytes).map(Some).ok_or_else(|| {
        invalid(format!(
            "{} does not decode; it holds the copy's reserved Raft index and cannot be rebuilt",
            dir.join(FILE_NAME).display()
        ))
    })
}

/// Replace the reservation in `dir` with `index`, synced.
pub(crate) fn store(dir: &Path, index: u64) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[&encode(index)])
}

/// Whether `path` is the reservation file in `dir` or the temporary its
/// write leaves.
pub(crate) fn is_reservation(dir: &Path, path: &Path) -> bool {
    path == dir.join(FILE_NAME) || path == state_file::temporary(dir, FILE_NAME)
}
