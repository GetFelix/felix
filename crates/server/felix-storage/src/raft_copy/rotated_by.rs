//! Why a kept copy was rotated, in a one-word `rotated-by` file inside the
//! kept directory.
//!
//! A re-form trusts a kept copy's vote only after a power cut: the open that
//! rotated it checked the vote mark first, so the copy still holds the
//! newest vote it took. A restored copy can hold an older vote than one it
//! gave. See `docs/storage-format.md`, "`rotated-by`".

use std::io;
use std::path::Path;

use super::judge::RotateReason;
use crate::raft_log::state_file;

pub(crate) const FILE_NAME: &str = "rotated-by";

/// How a kept copy came to be, as a re-form reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotatedBy {
    /// The log was below its mark, so it lost an unsynced tail and kept its
    /// newest vote.
    PowerCut,
    /// Anything else: a restore, a lost disk, a poisoned log. Its vote can
    /// be older than one it gave.
    Restore,
}

impl RotatedBy {
    /// The word for each reason. Only a log below its mark is a power cut:
    /// a poisoned log can hold less than it reported, and every other
    /// reason can bring back an older disk or vote.
    pub fn of(reason: RotateReason) -> Self {
        match reason {
            RotateReason::LogBelowMark => Self::PowerCut,
            RotateReason::Empty
            | RotateReason::NoCopyFile
            | RotateReason::StorageId
            | RotateReason::NoMarks
            | RotateReason::OtherCopy
            | RotateReason::VoteBelowMark
            | RotateReason::Poisoned => Self::Restore,
        }
    }

    fn word(self) -> &'static str {
        match self {
            Self::PowerCut => "power-cut",
            Self::Restore => "restore",
        }
    }
}

/// The reason kept in `dir`. A kept directory without the file is from
/// before it existed or from a crash before it was written, and reads as a
/// restore. A file that does not decode is an error.
pub fn load(dir: &Path) -> io::Result<RotatedBy> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(RotatedBy::Restore);
    };
    match bytes.as_slice() {
        b"power-cut\n" => Ok(RotatedBy::PowerCut),
        b"restore\n" => Ok(RotatedBy::Restore),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} does not decode", dir.join(FILE_NAME).display()),
        )),
    }
}

/// Write `by` into the kept directory `dir`, synced.
pub(crate) fn store(dir: &Path, by: RotatedBy) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[by.word().as_bytes(), b"\n"])
}
