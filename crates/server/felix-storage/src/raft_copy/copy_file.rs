//! The copy file in a shard directory: the copy id and the storage id it was
//! minted under, written together by one rename.

use std::io;
use std::path::Path;

use crate::StorageId;
use crate::raft_log::state_file;

/// The file's name in the shard directory.
pub(crate) const FILE_NAME: &str = crate::disk_log::RAFT_COPY_FILE;
/// `"FLCP"`.
const MAGIC: u32 = 0x464C_4350;
const VERSION: u16 = 1;
/// magic(4) + version(2) + copy(8) + storage id(16) + crc(4) over the rest.
const ENCODED_LEN: usize = 34;

/// Which copy a shard directory holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyFile {
    /// Never 0.
    pub copy: u64,
    /// The root's storage id when the copy id was minted.
    pub minted: StorageId,
}

impl CopyFile {
    fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut out = [0u8; ENCODED_LEN];
        out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        out[4..6].copy_from_slice(&VERSION.to_be_bytes());
        out[6..14].copy_from_slice(&self.copy.to_be_bytes());
        out[14..30].copy_from_slice(&self.minted.to_bytes());
        let crc = crate::segment::format::crc32(&[&out[..30]]);
        out[30..].copy_from_slice(&crc.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ENCODED_LEN {
            return None;
        }
        let crc = u32::from_be_bytes(bytes[30..].try_into().ok()?);
        if crate::segment::format::crc32(&[&bytes[..30]]) != crc
            || u32::from_be_bytes(bytes[0..4].try_into().ok()?) != MAGIC
            || u16::from_be_bytes(bytes[4..6].try_into().ok()?) != VERSION
        {
            return None;
        }
        let copy = u64::from_be_bytes(bytes[6..14].try_into().ok()?);
        (copy != 0).then_some(Self {
            copy,
            minted: StorageId::from_bytes(bytes[14..30].try_into().ok()?),
        })
    }
}

/// The copy file in `dir`, or `None` when there is none. One that does not
/// decode is an error: it was written by one rename, so damage is not a
/// crash, and guessing could keep an id the directory should not have.
pub(crate) fn load(dir: &Path) -> io::Result<Option<CopyFile>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    CopyFile::decode(&bytes).map(Some).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{}: the copy file does not decode",
                dir.join(FILE_NAME).display()
            ),
        )
    })
}

/// Write `copy` to `dir`, synced with the directory.
pub(crate) fn store(dir: &Path, copy: &CopyFile) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[&copy.encode()])
}
