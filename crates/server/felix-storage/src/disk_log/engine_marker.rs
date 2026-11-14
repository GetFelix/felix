//! Which engine writes a shard directory.
//!
//! A Raft member and the hybrid engine lay the same segments out
//! differently: a Raft log holds entries no majority agreed on above its
//! commit index, and the hybrid reads everything flushed as its own. So a
//! directory one engine wrote must never be opened by the other. Nothing in
//! the segments says which wrote them, hence this file.
//!
//! Only Raft writes one. A directory with segments and no marker is the
//! hybrid's, which is every directory from before the marker existed.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};

use super::recovery::discover_segment_ids;

const FILE_NAME: &str = "engine";
const RAFT: &[u8] = b"raft\n";

/// Mark `dir` as a Raft member's log, creating it if it is not there.
///
/// Call before the log is opened for the first time. The marker is durable
/// when this returns, so no entry is ever written to an unmarked directory.
/// Refused when the directory already holds segments without a marker: the
/// hybrid engine wrote those.
pub fn claim_for_raft(dir: &Path, label: &str) -> Result<()> {
    crate::fs::create_dir_all(dir).map_err(StorageError::Io)?;
    if marked_raft(dir, label)? {
        return Ok(());
    }
    if !discover_segment_ids(dir)?.is_empty() {
        return Err(refused(format!(
            "{label}: {} holds a log the hybrid engine wrote, and this broker runs the shard on \
             Raft; one shard's brokers must all run the same engine",
            dir.display()
        )));
    }
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let mut file = crate::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(RAFT).map_err(StorageError::Io)?;
        crate::io::sync_all(&file)?;
    }
    crate::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    crate::io::sync_dir(dir).map_err(StorageError::Io)?;
    Ok(())
}

/// Whether [`claim_for_raft`] has marked `dir`.
pub(super) fn claimed_for_raft(dir: &Path, label: &str) -> Result<bool> {
    marked_raft(dir, label)
}

/// Refuse to open `dir` as a hybrid log when a Raft member wrote it.
pub(super) fn refuse_raft_directory(dir: &Path, label: &str) -> Result<()> {
    if marked_raft(dir, label)? {
        return Err(refused(format!(
            "{label}: {} holds a Raft member's log, and this broker runs the shard on the \
             hybrid engine; one shard's brokers must all run the same engine",
            dir.display()
        )));
    }
    Ok(())
}

fn marked_raft(dir: &Path, label: &str) -> Result<bool> {
    match crate::fs::read(path_in(dir)) {
        Ok(bytes) if bytes == RAFT => Ok(true),
        // Written by a build that knows an engine this one does not.
        Ok(bytes) => Err(refused(format!(
            "{label}: {} names an engine this build does not know: {:?}",
            path_in(dir).display(),
            String::from_utf8_lossy(&bytes)
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(StorageError::Io(err)),
    }
}

fn refused(why: String) -> StorageError {
    StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, why))
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
