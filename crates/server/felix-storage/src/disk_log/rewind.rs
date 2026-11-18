//! A reset or truncation in progress, kept in `rewind-pending`.
//!
//! A rewind drops records and then rewrites the state that described them,
//! the generation history among it, in several durable steps. An open that
//! found the records gone and the history not would trust both, and one that
//! found the history gone and the records not would lose the history the
//! divergence check reads. So the marker is stored before the first step and
//! removed after the last, and an open that finds it does the rewind again
//! before anything reads the log: every crash point reopens as before the
//! rewind or as after it.
//!
//! Nothing can rebuild it, so a file that does not decode fails the open.

use std::io;
use std::path::Path;

use crate::disk_log::crash_point;
use crate::log::Offset;
use crate::state_file;

/// `"FLRW"`.
const MAGIC: u32 = 0x464C_5257;
const VERSION: u16 = 1;
/// magic(4) + version(2) + kind(1) + reserved(1) + offset(8) + crc(4)
const LEN: usize = 20;
const KIND_RESET: u8 = 1;
const KIND_CUT: u8 = 2;
pub(crate) const FILE_NAME: &str = "rewind-pending";

/// The rewind an open must finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rewind {
    /// Drop every record and start again at this base.
    Reset(Offset),
    /// Drop the records from this offset on.
    Cut(Offset),
}

impl Rewind {
    fn encode(self) -> [u8; LEN] {
        let (kind, offset) = match self {
            Self::Reset(base) => (KIND_RESET, base),
            Self::Cut(offset) => (KIND_CUT, offset),
        };
        let mut out = [0u8; LEN];
        out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        out[4..6].copy_from_slice(&VERSION.to_be_bytes());
        out[6] = kind;
        out[8..16].copy_from_slice(&offset.to_be_bytes());
        let crc = crate::segment::format::crc32(&[&out[..16]]);
        out[16..20].copy_from_slice(&crc.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != LEN {
            return None;
        }
        let crc = u32::from_be_bytes(bytes[16..20].try_into().ok()?);
        if crate::segment::format::crc32(&[&bytes[..16]]) != crc {
            return None;
        }
        let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
        if magic != MAGIC || version != VERSION {
            return None;
        }
        let offset = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
        match bytes[6] {
            KIND_RESET => Some(Self::Reset(offset)),
            KIND_CUT => Some(Self::Cut(offset)),
            _ => None,
        }
    }
}

/// The rewind a crash interrupted, if any.
pub(crate) fn load(dir: &Path) -> io::Result<Option<Rewind>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    Rewind::decode(&bytes).map(Some).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} does not decode; a reset or truncation was cut short and \
                 this log cannot tell which",
                dir.join(FILE_NAME).display()
            ),
        )
    })
}

/// Store `rewind` durably, before its first step.
pub(crate) fn begin(dir: &Path, rewind: Rewind) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[&rewind.encode()])
}

/// Remove the marker durably, after the rewind's last step. Before anything
/// is appended: a marker left over new records would drop them at the next
/// open.
pub(crate) fn finish(dir: &Path) -> io::Result<()> {
    crash_point::reached(dir)?;
    state_file::remove(dir, FILE_NAME)
}

#[cfg(test)]
mod tests;
