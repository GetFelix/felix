//! The two files store liveness keeps in a data root: the store epoch and
//! the disk probe. See "Store liveness" in `docs/per-shard-raft.md`.
//!
//! Both go through the crate's I/O seam, so a fault injected there (a slow or
//! failing flush) reaches the probe exactly as it reaches a log's flush.

use std::path::Path;

use crate::{Result, StorageError, fs, io};

/// Not directories, so shard discovery passes over them.
const EPOCH_FILE: &str = "store-epoch";
const PROBE_FILE: &str = "store-liveness.probe";

/// Raise the store epoch kept in `root` and return the new value.
///
/// Called once per start. The new value is synced before it is returned, so
/// an epoch a peer has seen is never handed out again. A file that does not
/// decode fails the start: guessing could repeat an epoch.
pub fn next_epoch(root: &Path) -> Result<u64> {
    let path = root.join(EPOCH_FILE);
    let last = match fs::read(&path) {
        Ok(bytes) => {
            let bytes: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{} is not a store epoch", path.display()),
                ))
            })?;
            u64::from_le_bytes(bytes)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => return Err(StorageError::Io(err)),
    };
    let next = last.checked_add(1).ok_or_else(|| {
        StorageError::Io(std::io::Error::other("the store epoch cannot rise further"))
    })?;
    crate::run_state::write_atomically(root, EPOCH_FILE, &next.to_le_bytes(), true)?;
    Ok(next)
}

/// A small file in a data root, rewritten and synced to show that the volume
/// still takes synced writes.
#[derive(Debug)]
pub struct Probe {
    file: fs::File,
    beats: u64,
}

impl Probe {
    /// Open or create the probe file in `root`.
    pub fn open(root: &Path) -> std::io::Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join(PROBE_FILE))?;
        Ok(Self { file, beats: 0 })
    }

    /// Write eight bytes in place and sync them. Blocks for as long as the
    /// device takes.
    pub fn beat(&mut self) -> std::io::Result<()> {
        self.beats = self.beats.wrapping_add(1);
        io::write_at(&self.file, &self.beats.to_le_bytes(), 0)?;
        io::sync_data(&self.file)
    }
}

#[cfg(test)]
mod tests;
