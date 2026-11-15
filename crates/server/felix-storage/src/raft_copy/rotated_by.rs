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
pub(crate) enum RotatedBy {
    /// The log was below its mark, so it lost an unsynced tail and kept its
    /// newest vote. `to` is the copy id the rotation minted: the copy it
    /// speaks for, and no later one.
    PowerCut { to: u64 },
    /// Anything else: a restore, a lost disk, a poisoned log. Its vote can
    /// be older than one it gave.
    Restore,
}

impl RotatedBy {
    /// The word for each reason. Only a log below its mark is a power cut:
    /// a poisoned log can hold less than it reported, and every other
    /// reason can bring back an older disk or vote.
    pub(crate) fn of(reason: RotateReason, to: u64) -> Self {
        match reason {
            RotateReason::LogBelowMark => Self::PowerCut { to },
            RotateReason::Empty
            | RotateReason::NoCopyFile
            | RotateReason::StorageId
            | RotateReason::NoMarks
            | RotateReason::OtherCopy
            | RotateReason::VoteBelowMark
            | RotateReason::Poisoned => Self::Restore,
        }
    }

    fn encode(self) -> String {
        match self {
            Self::PowerCut { to } => format!("power-cut {to:016x}\n"),
            Self::Restore => "restore\n".to_owned(),
        }
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes == b"restore\n" {
            return Some(Self::Restore);
        }
        let hex =
            std::str::from_utf8(bytes.strip_prefix(b"power-cut ")?.strip_suffix(b"\n")?).ok()?;
        if hex.len() != 16 {
            return None;
        }
        let to = u64::from_str_radix(hex, 16).ok().filter(|to| *to != 0)?;
        Some(Self::PowerCut { to })
    }
}

/// The reason kept in `dir`. A kept directory without the file is from
/// before it existed or from a crash before it was written, and reads as a
/// restore. A file that does not decode is an error.
pub(crate) fn load(dir: &Path) -> io::Result<RotatedBy> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(RotatedBy::Restore);
    };
    RotatedBy::decode(&bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} does not decode", dir.join(FILE_NAME).display()),
        )
    })
}

/// Write `by` into the kept directory `dir`, synced.
pub(crate) fn store(dir: &Path, by: RotatedBy) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[by.encode().as_bytes()])
}
