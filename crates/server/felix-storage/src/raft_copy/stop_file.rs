//! The stop marker a re-form's `apply` leaves in a shard directory,
//! `raft-stopped`: the group epoch whose member it stopped, and its last
//! entry.
//!
//! A copy stopped in its current epoch never votes, acknowledges or appends
//! there again. It comes back only by rotating to a new copy id, once a
//! leader is heard committing past its last entry (see
//! `docs/per-shard-raft.md`, "Re-forming a group"). A marker for an older epoch is spent: a re-form started the
//! copy in a later one. See `docs/storage-format.md`, "`raft-stopped`".

use std::io;
use std::path::Path;

use crate::disk_log::dir_lock::DirLock;
use crate::raft_log::state_file;

pub(crate) const FILE_NAME: &str = "raft-stopped";

/// A directory whose member a re-form stopped in the epoch it is still at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoppedCopy {
    pub epoch: u64,
    /// The copy id it was stopped under, `None` for a directory without one.
    pub copy: Option<u64>,
    /// The index of its last entry when it stopped. A leader that commits
    /// past it committed without this copy.
    pub last: Option<u64>,
}

impl std::fmt::Display for StoppedCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the copy was stopped by a re-form in group epoch {} and rejoins only under a new copy id",
            self.epoch
        )
    }
}

impl std::error::Error for StoppedCopy {}

impl StoppedCopy {
    /// The marker an [`io::Error`] carries, if it is one.
    pub fn of(err: &io::Error) -> Option<Self> {
        err.get_ref()?.downcast_ref::<Self>().copied()
    }
}

/// Stop the copy in `dir` in its current group epoch, synced before this
/// returns. `last` is the index of its last entry, from the log its member
/// closed with. Its member must already be closed, and a re-form reads the
/// copy's report only after this. Refused with
/// [`io::ErrorKind::ResourceBusy`] while any log has `dir` open.
pub fn stop(dir: &Path, last: Option<u64>) -> io::Result<()> {
    stop_holding(dir, last, &DirLock::take(dir)?)
}

/// [`stop`], for a caller that holds `dir`'s lock.
pub(super) fn stop_holding(dir: &Path, last: Option<u64>, _held: &DirLock) -> io::Result<()> {
    let epoch = stop_epoch(dir)?;
    let last = last.map_or_else(|| "-".to_owned(), |index| index.to_string());
    state_file::store(
        dir,
        FILE_NAME,
        &[format!("stopped {epoch} {last}\n").as_bytes()],
    )
}

/// The epoch `dir`'s copy is stopped in, if it is stopped in the epoch it
/// is at. A marker that does not decode is an error: it may stop the copy.
pub fn stopped(dir: &Path) -> io::Result<Option<StoppedCopy>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    let (epoch, last) = decode(&bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} does not decode", dir.join(FILE_NAME).display()),
        )
    })?;
    if epoch != stop_epoch(dir)? {
        return Ok(None);
    }
    let copy = super::copy_file::load(dir)?.map(|found| found.copy);
    Ok(Some(StoppedCopy { epoch, copy, last }))
}

/// The epoch a marker is keyed by. A copy with no `raft-epoch` reads as 0
/// on both sides, so its marker holds until a start writes one, and only a
/// start of a later epoch writes `raft-epoch` into a directory that has
/// been served.
fn stop_epoch(dir: &Path) -> io::Result<u64> {
    Ok(super::group_epoch(dir)?.unwrap_or(0))
}

fn decode(bytes: &[u8]) -> Option<(u64, Option<u64>)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (epoch, last) = text
        .strip_prefix("stopped ")?
        .strip_suffix('\n')?
        .split_once(' ')?;
    let last = match last {
        "-" => None,
        index => Some(index.parse().ok()?),
    };
    Some((epoch.parse().ok()?, last))
}
