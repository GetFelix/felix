//! The carried floor of a Raft stream log, kept in `raft-floor`.
//!
//! A head floor is normally read back from the trim record that set it. A
//! member that installs a stream snapshot skips the log up to the snapshot,
//! and with it the trim records below that point, so the floor it already
//! had and the one the snapshot carries are both kept here instead, with the
//! trim's headers. The file only ever rises ([`raise`]): an install never
//! lowers a member's durable floor, which a later re-form reads to choose
//! the offsets it gives out.
//!
//! Nothing can rebuild it, so a file that does not decode fails the open.

use std::io;
use std::path::Path;

use bytes::Bytes;

use crate::log::Offset;
use crate::state_file;

/// `"FLFR"`.
const MAGIC: u32 = 0x464C_4652;
const VERSION: u16 = 1;
/// magic(4) + version(2) + reserved(2) + offset(8) + floor(8) + headers length(4)
const HEADER_LEN: usize = 28;
const CRC_LEN: usize = 4;
pub(crate) const FILE_NAME: &str = "raft-floor";

/// A floor this log keeps without the trim record that set it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarriedFloor {
    /// Where the trim record that set the floor sits in the stream.
    pub offset: Offset,
    /// Reads below it are refused.
    pub floor: Offset,
    /// The trim's headers, returned with a refused read.
    pub headers: Bytes,
}

impl CarriedFloor {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.headers.len() + CRC_LEN);
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&self.offset.to_be_bytes());
        out.extend_from_slice(&self.floor.to_be_bytes());
        out.extend_from_slice(&(self.headers.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.headers);
        let crc = crate::segment::format::crc32(&[&out]);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN + CRC_LEN {
            return None;
        }
        let (body, crc) = bytes.split_at(bytes.len() - CRC_LEN);
        if crate::segment::format::crc32(&[body]) != u32::from_be_bytes(crc.try_into().ok()?) {
            return None;
        }
        let magic = u32::from_be_bytes(body[0..4].try_into().ok()?);
        let version = u16::from_be_bytes(body[4..6].try_into().ok()?);
        if magic != MAGIC || version != VERSION {
            return None;
        }
        let len = u32::from_be_bytes(body[24..28].try_into().ok()?) as usize;
        (body.len() == HEADER_LEN + len).then(|| Self {
            offset: u64::from_be_bytes(body[8..16].try_into().expect("8 bytes")),
            floor: u64::from_be_bytes(body[16..24].try_into().expect("8 bytes")),
            headers: Bytes::copy_from_slice(&body[HEADER_LEN..]),
        })
    }
}

pub(crate) fn load(dir: &Path) -> io::Result<Option<CarriedFloor>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    CarriedFloor::decode(&bytes).map(Some).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} does not decode; it holds a floor this log's records no longer show",
                dir.join(FILE_NAME).display()
            ),
        )
    })
}

/// Store `floor` if it is above the one stored, and answer the floor the
/// file holds afterwards. Synced before it returns.
pub(crate) fn raise(dir: &Path, floor: CarriedFloor) -> io::Result<CarriedFloor> {
    if let Some(kept) = load(dir)?
        && kept.floor >= floor.floor
    {
        return Ok(kept);
    }
    state_file::store(dir, FILE_NAME, &[&floor.encode()])?;
    Ok(floor)
}

#[cfg(test)]
mod tests;
