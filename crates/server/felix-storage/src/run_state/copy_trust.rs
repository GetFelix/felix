//! What the copies on a data directory hold of what their seat acknowledged,
//! kept across restarts and apart from any copy's ballot.
//!
//! A start that rotated the storage id may have lost acknowledged records on
//! any copy, and a later clean restart cannot tell. So [`super::begin`]
//! records in `<root>/copy-trust`, before it rotates, that no copy is
//! trusted. From then on a copy is trusted only once [`CopyTrust::trust`]
//! names it, which the broker calls when a leader ships the copy level
//! through what that leader acknowledged. The next rotation forgets every
//! name again. A directory without the file never rotated its id.
//!
//! The file also names the copies this broker led before such a crash
//! ([`CopyTrust::doubt`]) until a newer leader ships to them. The copy's
//! ballot says so only until it takes a newer leader's, and a clean restart
//! has nothing else. And it names the copies that took a ballot while their
//! promises counted for nothing ([`CopyTrust::unkeep`]) until they are
//! readmitted, since that ballot looks like any other after a restart. See `docs/storage-format.md`, "Copy trust".

use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::{Result, StorageError};
use felix_common::random::HashSet;

/// The file's name under the durable root. Not a directory, so shard
/// discovery passes over it.
pub const FILE_NAME: &str = "copy-trust";

const HEADER: &str = "copy_trust\n";
const DISTRUSTED: &str = "distrusted";
const TRUSTED: &str = "trusted ";
const DOUBTED: &str = "doubted ";
const UNKEPT: &str = "unkept ";

/// What the file records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct State {
    /// The directory rotated its id: only `trusted` copies are.
    distrusted: bool,
    trusted: HashSet<Vec<u8>>,
    doubted: HashSet<Vec<u8>>,
    unkept: HashSet<Vec<u8>>,
}

/// Record, durably, that every copy on `root` may have lost acknowledged
/// records: no copy is trusted until [`CopyTrust::trust`] names it again.
/// Doubted and unkept copies stay so.
pub(crate) fn distrust_all(root: &Path) -> Result<()> {
    let mut state = read(root)?;
    state.distrusted = true;
    state.trusted.clear();
    super::write_atomically(root, FILE_NAME, &encode(&state), true)
}

fn read(root: &Path) -> Result<State> {
    match crate::fs::read(root.join(FILE_NAME)) {
        Ok(bytes) => Ok(decode(&bytes).unwrap_or_else(|| {
            tracing::warn!(
                root = %root.display(),
                "the copy-trust record does not decode; trusting no copy until a leader \
                 vouches for it"
            );
            State {
                distrusted: true,
                ..State::default()
            }
        })),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// The copies on one data directory, as [`FILE_NAME`] records them. A copy
/// is named by bytes the caller chooses.
#[derive(Debug)]
pub struct CopyTrust {
    root: PathBuf,
    state: Mutex<State>,
}

impl CopyTrust {
    /// Read the record under `root`. One that does not decode trusts no copy
    /// and doubts none.
    pub fn open(root: &Path) -> Result<Self> {
        Ok(Self {
            root: root.to_path_buf(),
            state: Mutex::new(read(root)?),
        })
    }

    /// Whether the copy named `key` holds everything its seat acknowledged:
    /// the directory never rotated its id, or [`Self::trust`] named the copy
    /// since it last did.
    pub fn trusted(&self, key: &[u8]) -> bool {
        let state = self.state.lock();
        !state.distrusted || state.trusted.contains(key)
    }

    /// Record, durably, that the copy named `key` holds everything its seat
    /// acknowledged. [`Self::trusted`] says so only once the record is
    /// synced. Blocks on the sync.
    pub fn trust(&self, key: &[u8]) -> Result<()> {
        self.update(|state| state.distrusted && state.trusted.insert(key.to_vec()))
    }

    /// Whether [`Self::doubt`] named `key` and [`Self::clear_doubt`] has not
    /// since.
    pub fn doubted(&self, key: &[u8]) -> bool {
        self.state.lock().doubted.contains(key)
    }

    /// Record, durably, that the copy named `key` is one this broker led
    /// before a crash that may have cost it acknowledged records. Call before
    /// the copy takes a newer leader's ballot. Blocks on the sync.
    pub fn doubt(&self, key: &[u8]) -> Result<()> {
        self.update(|state| state.doubted.insert(key.to_vec()))
    }

    /// Record, durably, that a newer leader has shipped to the copy named
    /// `key` since the crash. Blocks on the sync.
    pub fn clear_doubt(&self, key: &[u8]) -> Result<()> {
        self.update(|state| state.doubted.remove(key))
    }

    /// Whether [`Self::unkeep`] named `key` and [`Self::clear_unkept`] has
    /// not since.
    pub fn unkept(&self, key: &[u8]) -> bool {
        self.state.lock().unkept.contains(key)
    }

    /// Record, durably, that the copy named `key` is about to take a ballot
    /// while its promises count for nothing, as a learner does. That ballot
    /// is no kept promise, even after a crash that keeps it. Call before the
    /// ballot is written. Blocks on the sync.
    pub fn unkeep(&self, key: &[u8]) -> Result<()> {
        self.update(|state| state.unkept.insert(key.to_vec()))
    }

    /// Record, durably, that the copy named `key` was readmitted: its
    /// promises count again. Blocks on the sync.
    pub fn clear_unkept(&self, key: &[u8]) -> Result<()> {
        self.update(|state| state.unkept.remove(key))
    }

    /// Apply `change` and write the result, if it changed anything, before
    /// any reader sees it. Held under the lock so writes do not cross.
    fn update(&self, change: impl FnOnce(&mut State) -> bool) -> Result<()> {
        let mut state = self.state.lock();
        let mut next = state.clone();
        if !change(&mut next) {
            return Ok(());
        }
        super::write_atomically(&self.root, FILE_NAME, &encode(&next), true)?;
        *state = next;
        Ok(())
    }
}

/// The header, `distrusted` after a rotation, then one line per named copy,
/// its key in lowercase hex, sorted.
fn encode(state: &State) -> Vec<u8> {
    use std::fmt::Write;

    let mut out = String::from(HEADER);
    if state.distrusted {
        out.push_str(DISTRUSTED);
        out.push('\n');
    }
    for (prefix, keys) in [
        (TRUSTED, &state.trusted),
        (DOUBTED, &state.doubted),
        (UNKEPT, &state.unkept),
    ] {
        let mut keys: Vec<&Vec<u8>> = keys.iter().collect();
        keys.sort_unstable();
        for key in keys {
            out.push_str(prefix);
            for byte in key {
                let _ = write!(out, "{byte:02x}");
            }
            out.push('\n');
        }
    }
    out.into_bytes()
}

fn decode(bytes: &[u8]) -> Option<State> {
    let body = std::str::from_utf8(bytes).ok()?.strip_prefix(HEADER)?;
    let mut state = State::default();
    for line in body.lines() {
        if line == DISTRUSTED {
            state.distrusted = true;
        } else if let Some(hex) = line.strip_prefix(TRUSTED) {
            state.trusted.insert(unhex(hex)?);
        } else if let Some(hex) = line.strip_prefix(DOUBTED) {
            state.doubted.insert(unhex(hex)?);
        } else {
            state.unkept.insert(unhex(line.strip_prefix(UNKEPT)?)?);
        }
    }
    Some(state)
}

fn unhex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests;
