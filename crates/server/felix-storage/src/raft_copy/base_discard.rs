//! Dropping a re-formed stream base a learner adopted but can never
//! install the gap snapshot over, so it can pull the base again. Bounded
//! per epoch and backed off, so a base that keeps failing to match is
//! dropped a few times and then left, never pulled in a loop.

use std::io;
use std::path::Path;
use std::time::Duration;

/// Present while a discard waits for the copy's next open.
pub(crate) const DISCARD: &str = "base-discard";
/// The epoch's first index and how many discards it has had.
const DISCARDS: &str = "base-discards";
/// Discards one epoch's base gets before the copy keeps what it holds.
pub const MAX_BASE_DISCARDS: u32 = 4;
const FIRST_WAIT: Duration = Duration::from_secs(2);
const MAX_WAIT: Duration = Duration::from_secs(60);

/// What [`request_discard`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseDiscard {
    /// The next open drops the base. This is the epoch's nth discard.
    Requested(u32),
    /// The base has not been refused long enough for the next discard.
    Wait,
    /// The epoch's discards are spent; the copy keeps its base.
    Exhausted,
}

/// How long a base must be refused before its discard after `done` others.
pub fn discard_wait(done: u32) -> Duration {
    FIRST_WAIT
        .checked_mul(1 << done.min(16))
        .map_or(MAX_WAIT, |wait| wait.min(MAX_WAIT))
}

/// Ask the next open of `dir` to drop the base of the epoch at
/// `first_index`, refused for `refused_for` so far. The count is kept on
/// disk, so a reopen does not reset it.
pub fn request_discard(
    dir: &Path,
    first_index: u64,
    refused_for: Duration,
) -> io::Result<BaseDiscard> {
    let done = discards(dir, first_index)?;
    if done >= MAX_BASE_DISCARDS {
        return Ok(BaseDiscard::Exhausted);
    }
    if refused_for < discard_wait(done) {
        return Ok(BaseDiscard::Wait);
    }
    let tmp = dir.join(format!("{DISCARDS}.tmp"));
    crate::fs::write(&tmp, format!("{first_index} {}", done + 1))?;
    crate::io::sync_all(&crate::fs::File::open(&tmp)?)?;
    crate::fs::rename(&tmp, dir.join(DISCARDS))?;
    let marker = crate::fs::File::create(dir.join(DISCARD))?;
    crate::io::sync_all(&marker)?;
    crate::io::sync_dir(dir)?;
    Ok(BaseDiscard::Requested(done + 1))
}

/// Discards the epoch at `first_index` has had. A count for another epoch
/// is zero for this one.
fn discards(dir: &Path, first_index: u64) -> io::Result<u32> {
    let text = match crate::fs::read_to_string(dir.join(DISCARDS)) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    let mut fields = text.split_whitespace().map(str::parse::<u64>);
    match (fields.next(), fields.next()) {
        (Some(Ok(epoch)), Some(Ok(count))) if epoch == first_index => {
            Ok(u32::try_from(count).unwrap_or(u32::MAX))
        }
        _ => Ok(0),
    }
}

/// Run before `dir`'s log opens: drop the base a discard asked for, unless
/// the copy took a snapshot or a purged id since, which means it caught up
/// and may hold acknowledged records. True when the log was emptied.
pub(crate) fn discard_pending(dir: &Path) -> io::Result<bool> {
    let marker = dir.join(DISCARD);
    if !crate::fs::try_exists(&marker)? {
        return Ok(false);
    }
    let caught_up = crate::fs::read_dir(dir)?.any(|entry| {
        entry.is_ok_and(|entry| {
            entry.file_name().to_str().is_some_and(|name| {
                name.starts_with(crate::raft_log::purged::FILE_NAME)
                    || name.starts_with(crate::raft_log::snapshot::FILE_NAME)
            })
        })
    });
    if !caught_up {
        crate::disk_log::store_saved_commit(dir, 0, 0).map_err(io::Error::other)?;
        super::start::empty_log(dir)?;
    }
    super::staging::discard(dir)?;
    crate::fs::remove_file(&marker)?;
    crate::io::sync_dir(dir)?;
    Ok(!caught_up)
}

#[cfg(test)]
mod tests;
