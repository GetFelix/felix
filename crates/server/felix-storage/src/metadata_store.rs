//! The files of the control plane's metadata store, beside its Raft log, and
//! the rule that decides whether a member may start on a data directory.
//!
//! A member's data directory holds `store/`: the Raft log in `store/log/`,
//! the snapshot in `store/raft-snapshot` ([`SnapshotStore`], shared with
//! every other Raft group) and the member file in `store/member`
//! ([`MemberFile`]). A directory an older release wrote holds `raft.redb`
//! instead. The design is in `docs/metadata-raft-design.md`, "Moving to the
//! segment store"; the byte formats are in `docs/storage-format.md`.
//!
//! [`decide_start`] reads names and the member file only. It never opens
//! `raft.redb` and never writes, so a refusal leaves the directory as it
//! was found.

mod member;
#[cfg(test)]
mod tests;

pub use member::{Member, MemberFile, Started};

use std::io;
use std::path::Path;

#[cfg(doc)]
use crate::raft_log::SnapshotStore;

/// The store's directory, inside a member's data directory.
pub const STORE_DIR: &str = "store";
/// The Raft log's directory, inside [`STORE_DIR`].
pub const LOG_DIR: &str = "log";
/// The store an older release kept in the data directory, or the stub the
/// conversion leaves under the same name.
pub const REDB_FILE: &str = "raft.redb";

/// What a member starts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// Neither a store nor a redb file: a new member.
    New,
    /// A store, with no redb file or with the stub of a converted one.
    Existing,
}

/// Whether a member may start on `data_dir`, from the names in it and the
/// store's member file.
///
/// A `raft.redb` is refused unless the store beside it carries the
/// converted mark: the mark is written before the conversion replaces the
/// file with its stub, so with the mark the file is the stub, and without
/// it the file may hold a group's state that this build cannot read. A
/// name that cannot be examined and a member file that does not decode are
/// refused too. Nothing is written.
pub fn decide_start(data_dir: &Path) -> io::Result<Start> {
    let redb = data_dir.join(REDB_FILE);
    let store = data_dir.join(STORE_DIR);
    let has_store = present(&store)?;
    if !present(&redb)? {
        return Ok(if has_store {
            Start::Existing
        } else {
            Start::New
        });
    }
    let converted = has_store && member::read(&store)?.is_some_and(|member| member.converted);
    if converted {
        return Ok(Start::Existing);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{} holds a redb store that was not converted. Stop the member and convert its \
             data directory with the conversion tool (docs/metadata-raft-design.md, \
             \"Upgrading an existing cluster\")",
            redb.display()
        ),
    ))
}

/// Whether `path` exists. An error other than "not found" is an error: a
/// file that cannot be examined must not read as absent.
fn present(path: &Path) -> io::Result<bool> {
    match crate::fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}
