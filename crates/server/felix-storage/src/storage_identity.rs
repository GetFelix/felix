//! The identity of a broker's data directory.
//!
//! A broker keeps one random id in `<durable root>/storage-id`, written the
//! first time it opens the directory. A restart keeps it; a wiped or replaced
//! directory comes back with a new one, so a peer can tell "the same disk,
//! restarted" from "a disk that has lost what it acknowledged". See
//! `docs/durable-storage.md`, "Storage identity".
//!
//! Unlike a shard's `replica` or `ballot`, a file that does not decode does
//! not fail the open. Replacing it with a new id only ever claims less: peers
//! treat the directory as one that may have lost data.

use std::path::{Path, PathBuf};

use crate::io::sync_dir;
use crate::{Result, StorageError};

/// `"FLID"`. Not `FLSI`, which a segment index already starts with.
const MAGIC: u32 = 0x464C_4944;
const VERSION: u16 = 1;
/// magic(4) + version(2) + id(16) + crc(4) over everything before it.
pub(crate) const ENCODED_LEN: usize = 26;
/// The file's name under the durable root. Not a directory, so shard
/// discovery passes over it.
pub(crate) const FILE_NAME: &str = "storage-id";

/// A data directory's identity: 16 random bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageId([u8; 16]);

impl StorageId {
    /// The id these 16 bytes are.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// 32 lowercase hex digits.
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        self.0.iter().fold(String::with_capacity(32), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
    }

    pub(crate) fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut out = [0u8; ENCODED_LEN];
        out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        out[4..6].copy_from_slice(&VERSION.to_be_bytes());
        out[6..22].copy_from_slice(&self.0);
        let crc = crate::segment::format::crc32(&[&out[..22]]);
        out[22..].copy_from_slice(&crc.to_be_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ENCODED_LEN {
            return None;
        }
        let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
        let crc = u32::from_be_bytes(bytes[22..].try_into().ok()?);
        if magic != MAGIC
            || version != VERSION
            || crate::segment::format::crc32(&[&bytes[..22]]) != crc
        {
            return None;
        }
        Some(Self(bytes[6..22].try_into().ok()?))
    }
}

impl std::fmt::Display for StorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Where the id [`load_or_create`] returned came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Read from the file.
    Loaded,
    /// There was no file: a new directory, a wiped one, or one an older
    /// build wrote.
    Created,
    /// The file did not decode and was replaced.
    ReplacedCorrupt,
}

/// The id kept under `root`, or a new one from `fresh` written there when
/// there is none or it does not decode. `root` must exist.
pub fn load_or_create(
    root: &Path,
    fresh: impl FnOnce() -> [u8; 16],
) -> Result<(StorageId, Origin)> {
    let path = path_in(root);
    let origin = match std::fs::read(&path) {
        Ok(bytes) => match StorageId::decode(&bytes) {
            Some(id) => return Ok((id, Origin::Loaded)),
            None => Origin::ReplacedCorrupt,
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Origin::Created,
        Err(err) => return Err(StorageError::Io(err)),
    };
    let id = StorageId(fresh());
    store(root, &id)?;
    match origin {
        Origin::ReplacedCorrupt => tracing::warn!(
            path = %path.display(),
            storage_id = %id,
            "the storage id did not decode; replaced it with a new one, so peers treat this \
             directory as one that may have lost data"
        ),
        _ => tracing::info!(
            path = %path.display(),
            storage_id = %id,
            "no storage id in the data directory; created one"
        ),
    }
    Ok((id, origin))
}

/// Write `fresh` as the id under `root`, whatever was there. For a directory
/// whose contents are now an older copy, such as after a restore.
pub fn replace(root: &Path, fresh: [u8; 16]) -> Result<StorageId> {
    let id = StorageId(fresh);
    store(root, &id)?;
    Ok(id)
}

/// A crash leaves the old file or the new one, never a mix.
fn store(root: &Path, id: &StorageId) -> Result<()> {
    use std::io::Write;

    let path = path_in(root);
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&id.encode()).map_err(StorageError::Io)?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    sync_dir(root).map_err(StorageError::Io)?;
    Ok(())
}

fn path_in(root: &Path) -> PathBuf {
    root.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
