//! Which engine writes a shard directory.
//!
//! A Raft member and the hybrid engine lay the same segments out
//! differently: a Raft log holds entries no majority agreed on above its
//! commit index, and the hybrid reads everything flushed as its own. So a
//! directory one engine wrote must never be opened by the other. Nothing in
//! the segments says which wrote them, hence this file.
//!
//! Only Raft writes one. A directory with segments and no marker is the
//! hybrid's, which is every directory from before the marker existed. A
//! directory the hybrid wrote becomes a Raft log only through the switch,
//! whose marker also names the base the member started from.

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
    write(dir, RAFT)
}

/// Mark `dir`, which the hybrid wrote, as a Raft member's log that starts
/// after the switch's base: generation `generation` at offset `offset`.
///
/// The caller has already written everything the member opens on (the
/// purged id, the snapshot, the vote floor), so a marked directory is a
/// started member and an unmarked one is redone from the start. The base
/// is kept so a second start from another base is refused.
#[cfg(feature = "raft")]
pub(super) fn mark_switched(dir: &Path, generation: u64, offset: u64) -> Result<()> {
    let mut bytes = RAFT.to_vec();
    bytes.extend_from_slice(format!("base {generation} {offset}\n").as_bytes());
    write(dir, &bytes)
}

/// The base a switched directory's member started from, as
/// [`mark_switched`] wrote it: `None` for a directory not marked, or marked
/// by [`claim_for_raft`].
pub fn switched_base(dir: &Path, label: &str) -> Result<Option<(u64, u64)>> {
    Ok(read_marker(dir, label)?.flatten())
}

fn write(dir: &Path, bytes: &[u8]) -> Result<()> {
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let mut file = crate::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(bytes).map_err(StorageError::Io)?;
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
    Ok(read_marker(dir, label)?.is_some())
}

/// `None` with no marker, `Some(None)` for a Raft log claimed empty, and
/// `Some(Some(base))` for one switched from the hybrid at `base`.
pub(super) fn read_marker(dir: &Path, label: &str) -> Result<Option<Option<(u64, u64)>>> {
    let bytes = match crate::fs::read(path_in(dir)) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(StorageError::Io(err)),
    };
    if bytes == RAFT {
        return Ok(Some(None));
    }
    if let Some(base) = bytes.strip_prefix(RAFT).and_then(parse_base) {
        return Ok(Some(Some(base)));
    }
    // Written by a build that knows an engine this one does not.
    Err(refused(format!(
        "{label}: {} names an engine this build does not know: {:?}",
        path_in(dir).display(),
        String::from_utf8_lossy(&bytes)
    )))
}

fn parse_base(line: &[u8]) -> Option<(u64, u64)> {
    let line = std::str::from_utf8(line).ok()?.strip_suffix('\n')?;
    let mut words = line.strip_prefix("base ")?.split(' ');
    let generation = words.next()?.parse().ok()?;
    let offset = words.next()?.parse().ok()?;
    (words.next().is_none() && generation != 0).then_some((generation, offset))
}

fn refused(why: String) -> StorageError {
    StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, why))
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
