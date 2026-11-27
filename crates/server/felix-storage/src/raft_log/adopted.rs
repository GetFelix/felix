//! The mark of a re-form base a learner pulled and adopted, kept as
//! `raft-base-adopted` until the gap snapshot installs over it.
//!
//! While it is there the log shows openraft no entry, no last id and no
//! purged id, as a blank learner's does. The base is of the old epoch and
//! below every voter's purged id, so openraft has no use for it; shown, a
//! base that starts above index 0 with nothing purged is a log openraft
//! cannot open. The stream and the base reads see it all along. The
//! install's purge through the gap's end puts the purged id on disk, and
//! from then on the log looks as a re-formed voter's does: the base below
//! the purged id, the epoch's entries above it.

use std::io;
use std::path::Path;

use super::state_file;

pub(crate) const FILE_NAME: &str = "raft-base-adopted";

/// Mark `dir`'s log as an adopted base, synced.
pub(crate) fn store(dir: &Path) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[&[]])
}

/// Whether `dir`'s log is marked as an adopted base.
pub(crate) fn held(dir: &Path) -> io::Result<bool> {
    Ok(state_file::load(dir, FILE_NAME)?.is_some())
}

pub(crate) fn remove(dir: &Path) -> io::Result<()> {
    state_file::remove(dir, FILE_NAME)
}
