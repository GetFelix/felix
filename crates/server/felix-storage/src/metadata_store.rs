//! The files of the control plane's metadata store, beside its Raft log, and
//! the rule that decides whether a member may start on a data directory.
//!
//! A member's data directory holds `store/`: the Raft log in `store/log/`,
//! the snapshot in `store/raft-snapshot` ([`SnapshotStore`], shared with
//! every other Raft group), the member file in `store/member`
//! ([`MemberFile`]), the formation file in `store/formation`
//! ([`Formation`]) and the lock file `store/lock`. [`MetadataStore::open`]
//! is the one way to open them together. A directory an older release wrote
//! holds `raft.redb` instead. The design is in `docs/metadata-raft-design.md`, "Moving to the
//! segment store"; the byte formats are in `docs/storage-format.md`.
//!
//! [`decide_start`] reads names and the member file only. It never opens
//! `raft.redb` and never writes, so a refusal leaves the directory as it
//! was found.

mod formation;
mod lock;
mod member;
mod store;
#[cfg(test)]
mod tests;

pub use formation::Formation;
pub use member::{Member, MemberFile, Started};
pub use store::MetadataStore;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Start {
    /// No store, and no redb file: nothing here says this member ever ran.
    /// A member that was in the cluster and lost its whole data directory
    /// looks the same, and may have voted in a term it no longer knows. A
    /// member that joins a cluster already running must start with its
    /// vote withheld.
    New,
    /// A whole store: its member file, which decoded, and its log
    /// directory, where the vote lives. A store whose log directory is
    /// gone is refused, never reported here.
    ///
    /// With [`Member::started`] set the member may have voted. This does
    /// not say its log still holds that vote: only the directory's name is
    /// checked. A crash right after the mark, or a log directory that was
    /// emptied, leaves a started member with a blank log.
    /// [`MetadataStore::open`] finds that and withholds the vote or reports
    /// it.
    Existing(Member),
}

/// Whether a member may start on `data_dir`, from the names in it and the
/// store's member file.
///
/// A store must hold a member file that decodes and a log directory, or
/// the start is refused: a store is created with both, so one without
/// either has lost state it cannot rebuild, possibly the member's vote. A
/// `raft.redb` is refused unless the store beside it carries the converted
/// mark: the mark is written before the conversion replaces the file with
/// its stub, so with the mark the file is the stub, and without it the
/// file may hold a group's state that this build cannot read. A name that
/// cannot be examined is refused too. Nothing is written.
pub fn decide_start(data_dir: &Path) -> io::Result<Start> {
    let redb = data_dir.join(REDB_FILE);
    let store = data_dir.join(STORE_DIR);
    let member = if present(&store)? {
        Some(member::existing(&store)?)
    } else {
        None
    };
    let converted = member.as_ref().is_some_and(|member| member.converted);
    if !present(&redb)? || converted {
        return Ok(member.map_or(Start::New, Start::Existing));
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{} holds a control-plane store from Felix 0.7 or earlier, which this build \
             cannot read. Nothing was changed. Keep the cluster on 0.7, or start every \
             control-plane member and every broker on an empty data directory, which drops \
             the group's metadata (docs-site deployment/upgrades, \"Control plane metadata \
             in 0.8\")",
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
