//! The snapshot a Raft group keeps: one file, `raft-snapshot`, holding the
//! id of the last entry the snapshot covers, the caller's metadata and the
//! data.
//!
//! The three are one file replaced by one rename, so a crash leaves the old
//! snapshot or the new one and never one's data under the other's id. Two
//! files could not promise that. See `docs/replication-engine.md`,
//! "Snapshots".

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use super::{generation_of, invalid, state_file, term_of_generation};
use crate::log::Offset;

/// `"FLSS"`.
const MAGIC: u32 = 0x464C_5353;
const VERSION: u16 = 1;
/// magic(4) + version(2) + reserved(2) + index(8) + generation(8) +
/// meta length(4) + data length(8) + crc(4)
const HEADER_LEN: usize = 40;
pub(crate) const FILE_NAME: &str = "raft-snapshot";

/// The last entry a snapshot covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotId {
    pub term: u64,
    pub index: Offset,
}

/// A snapshot as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    /// `None` for a snapshot of a state machine that has applied nothing.
    pub last: Option<SnapshotId>,
    /// Whatever else the caller keeps with the id, such as the membership.
    pub meta: Bytes,
    pub data: Bytes,
}

/// The one snapshot of a Raft group, in a directory. Cheap to clone; every
/// clone sees the same snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    dir: PathBuf,
    /// Whether a snapshot is stored, and the last entry it covers. Held
    /// across a write, so two installs do not interleave.
    stored: parking_lot::Mutex<Option<Option<SnapshotId>>>,
    /// The last entry the stored snapshot covers, behind a lock that is never
    /// held across I/O. A purge reads it on a runtime worker.
    covered: parking_lot::Mutex<Option<SnapshotId>>,
}

impl SnapshotStore {
    /// Open the snapshot kept in `dir`, which must exist. A file that does
    /// not decode fails the open: the log below it may be purged, so
    /// reading it as "no snapshot" would lose the state it holds. A
    /// temporary a crash left is deleted.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        let stored = read(&dir)?.map(|snapshot| snapshot.last);
        state_file::remove_temporary(&dir, FILE_NAME)?;
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                covered: parking_lot::Mutex::new(stored.flatten()),
                stored: parking_lot::Mutex::new(stored),
            }),
        })
    }

    /// The last entry the stored snapshot covers, or `None` when there is
    /// no snapshot or it covers nothing.
    pub fn covers(&self) -> Option<SnapshotId> {
        *self.shared.covered.lock()
    }

    /// Take the lock an install or a read holds across its file I/O.
    #[cfg(test)]
    pub(crate) fn hold_io(&self) -> parking_lot::MutexGuard<'_, Option<Option<SnapshotId>>> {
        self.shared.stored.lock()
    }

    /// The stored snapshot, read from disk and checked.
    pub async fn current(&self) -> io::Result<Option<StoredSnapshot>> {
        let shared = Arc::clone(&self.shared);
        felix_common::blocking::spawn(move || {
            let _stored = shared.stored.lock();
            read(&shared.dir)
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Replace the stored snapshot, on disk before this returns. Returns
    /// false, with nothing written, when the stored one covers a later
    /// entry: a snapshot never goes back.
    pub async fn install(&self, snapshot: StoredSnapshot) -> io::Result<bool> {
        let shared = Arc::clone(&self.shared);
        felix_common::blocking::spawn(move || {
            let mut stored = shared.stored.lock();
            let index = |last: Option<SnapshotId>| last.map(|last| last.index);
            if stored.is_some_and(|stored| index(stored) > index(snapshot.last)) {
                return Ok(false);
            }
            let header = encode_header(&snapshot)?;
            state_file::store(
                &shared.dir,
                FILE_NAME,
                &[&header, &snapshot.meta, &snapshot.data],
            )?;
            *stored = Some(snapshot.last);
            *shared.covered.lock() = snapshot.last;
            Ok(true)
        })
        .await
        .map_err(io::Error::other)?
    }
}

fn encode_header(snapshot: &StoredSnapshot) -> io::Result<[u8; HEADER_LEN]> {
    let (index, generation) = match snapshot.last {
        None => (0, 0),
        Some(last) => (last.index, generation_of(last.term)?),
    };
    let meta_len = u32::try_from(snapshot.meta.len())
        .map_err(|_| invalid("snapshot metadata is over 4 GiB".to_string()))?;
    let mut out = [0u8; HEADER_LEN];
    out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&VERSION.to_be_bytes());
    out[8..16].copy_from_slice(&index.to_be_bytes());
    out[16..24].copy_from_slice(&generation.to_be_bytes());
    out[24..28].copy_from_slice(&meta_len.to_be_bytes());
    out[28..36].copy_from_slice(&(snapshot.data.len() as u64).to_be_bytes());
    let crc = crate::segment::format::crc32(&[&out[0..36], &snapshot.meta, &snapshot.data]);
    out[36..40].copy_from_slice(&crc.to_be_bytes());
    Ok(out)
}

pub(super) fn decode(bytes: &[u8]) -> Option<StoredSnapshot> {
    let header = bytes.get(..HEADER_LEN)?;
    let magic = u32::from_be_bytes(header[0..4].try_into().ok()?);
    let version = u16::from_be_bytes(header[4..6].try_into().ok()?);
    if magic != MAGIC || version != VERSION {
        return None;
    }
    // A later version may give these bytes a meaning this build would ignore.
    if header[6..8] != [0, 0] {
        return None;
    }
    let index = u64::from_be_bytes(header[8..16].try_into().ok()?);
    let generation = u64::from_be_bytes(header[16..24].try_into().ok()?);
    let meta_len = u32::from_be_bytes(header[24..28].try_into().ok()?) as usize;
    let data_len = usize::try_from(u64::from_be_bytes(header[28..36].try_into().ok()?)).ok()?;
    let crc = u32::from_be_bytes(header[36..40].try_into().ok()?);
    let body = &bytes[HEADER_LEN..];
    if meta_len.checked_add(data_len)? != body.len() {
        return None;
    }
    let (meta, data) = body.split_at(meta_len);
    if crate::segment::format::crc32(&[&header[0..36], meta, data]) != crc {
        return None;
    }
    let last = match term_of_generation(generation) {
        Some(term) => Some(SnapshotId { term, index }),
        None if index == 0 => None,
        None => return None,
    };
    Some(StoredSnapshot {
        last,
        meta: Bytes::copy_from_slice(meta),
        data: Bytes::copy_from_slice(data),
    })
}

fn read(dir: &Path) -> io::Result<Option<StoredSnapshot>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    decode(&bytes).map(Some).ok_or_else(|| {
        invalid(format!(
            "{} does not decode; the log below it may be purged, so it cannot be read as no snapshot",
            dir.join(FILE_NAME).display()
        ))
    })
}
