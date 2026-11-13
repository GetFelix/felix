//! The snapshot a Raft group keeps: one file, `raft-snapshot`, holding the
//! id of the last entry the snapshot covers, the caller's metadata and the
//! data.
//!
//! The three are one file replaced by one rename, so a crash leaves the old
//! snapshot or the new one and never one's data under the other's id. Two
//! files could not promise that. See `docs/replication-engine.md`,
//! "Snapshots".
//!
//! A snapshot sent by a peer is written to `raft-snapshot.recv` as it
//! arrives ([`StagedSnapshot`]) and renamed over the stored one once it is
//! whole and checked. A sender streams the stored file through
//! [`StoredReader`]. The framing both sides share is in
//! [`super::transfer`].

mod staged;

pub use staged::{StagedSnapshot, StoredReader};

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
/// Where a snapshot a peer sends is written until it is whole. Apart from
/// `raft-snapshot.tmp`, because a local build can finish while a receive
/// is in flight.
pub(crate) const RECEIVE_NAME: &str = "raft-snapshot.recv";
/// How long a close waits for a receive in flight to let go of its file.
const RECEIVE_DRAIN: std::time::Duration = std::time::Duration::from_secs(10);

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

/// A snapshot's data as a Raft group on this store hands it around: what
/// openraft calls `SnapshotData`. Only a just-built snapshot is held in
/// memory; one being sent is read from its file, and one being received is
/// written to its own.
#[derive(Debug)]
pub enum SnapshotData {
    /// Data built from the state machine, not yet sent anywhere.
    Memory(Bytes),
    /// The stored snapshot's file, for a sender.
    Stored(StoredReader),
    /// A snapshot received from a peer, whole and durable but not yet the
    /// stored one.
    Staged(StagedSnapshot),
}

impl SnapshotData {
    /// The data's length.
    pub fn data_len(&self) -> u64 {
        match self {
            Self::Memory(data) => data.len() as u64,
            Self::Stored(reader) => reader.data_len(),
            Self::Staged(staged) => staged.data_len(),
        }
    }

    /// The next piece of the data to send, at most `max` bytes, or `None`
    /// at its end. A staged snapshot is not sent anywhere.
    pub async fn next_chunk(&mut self, max: usize) -> io::Result<Option<Bytes>> {
        match self {
            Self::Memory(data) if data.is_empty() => Ok(None),
            Self::Memory(data) => Ok(Some(data.split_to(max.max(1).min(data.len())))),
            Self::Stored(reader) => reader.next_chunk(max).await,
            Self::Staged(_) => Err(io::Error::other("a received snapshot is not sent on")),
        }
    }
}

/// The one snapshot of a Raft group, in a directory. Cheap to clone; every
/// clone sees the same snapshot, and a [`Self::close`] through one closes
/// them all.
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
    /// Set under `stored`, so an install either finishes before the close
    /// or is refused.
    closed: std::sync::atomic::AtomicBool,
    /// Held while a clone can still install. openraft builds a snapshot on
    /// a task it does not wait for, so a clone can outlive the store.
    keep_alive: parking_lot::Mutex<Option<Arc<dyn std::any::Any + Send + Sync>>>,
    /// One receive at a time: there is one receive file.
    receiving: Arc<tokio::sync::Semaphore>,
    /// Set by a close, so a receive waiting on its sender gives up.
    closing: tokio::sync::watch::Sender<bool>,
    /// Makes the next close fail, for tests.
    #[cfg(test)]
    fail_close: std::sync::atomic::AtomicBool,
}

impl SnapshotStore {
    /// Open the snapshot kept in `dir`, which must exist. A file that does
    /// not decode fails the open: the log below it may be purged, so
    /// reading it as "no snapshot" would lose the state it holds. A
    /// temporary or a partly received snapshot a crash left is deleted;
    /// neither is ever read.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        let stored = read(&dir)?.map(|snapshot| snapshot.last);
        state_file::remove_temporary(&dir, FILE_NAME)?;
        staged::remove_leftover(&dir)?;
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                covered: parking_lot::Mutex::new(stored.flatten()),
                stored: parking_lot::Mutex::new(stored),
                closed: std::sync::atomic::AtomicBool::new(false),
                keep_alive: parking_lot::Mutex::new(None),
                receiving: Arc::new(tokio::sync::Semaphore::new(1)),
                closing: tokio::sync::watch::Sender::new(false),
                #[cfg(test)]
                fail_close: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }

    /// Hold `keep_alive` until [`Self::close`] or the last clone is dropped.
    /// The metadata store passes its lock.
    pub(crate) fn keep_alive(&self, keep_alive: Arc<dyn std::any::Any + Send + Sync>) {
        *self.shared.keep_alive.lock() = Some(keep_alive);
    }

    /// Refuse every later install, through this store or any clone, once an
    /// install in flight has finished, then let go of the keep-alive value.
    /// A receive in flight is told to stop, and the close waits for it to
    /// drop its file. If it does not within [`RECEIVE_DRAIN`], the close
    /// fails and keeps the keep-alive value.
    pub(crate) async fn close(&self) -> io::Result<()> {
        #[cfg(test)]
        if self
            .shared
            .fail_close
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(io::Error::other("injected: the snapshot close fails"));
        }
        let shared = Arc::clone(&self.shared);
        felix_common::blocking::spawn(move || {
            let _stored = shared.stored.lock();
            shared
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
        })
        .await
        .map_err(io::Error::other)?;
        self.shared.closing.send_replace(true);
        let drained = tokio::time::timeout(RECEIVE_DRAIN, self.shared.receiving.acquire()).await;
        if !matches!(drained, Ok(Ok(_))) {
            return Err(io::Error::other(format!(
                "a snapshot receive in {} did not stop",
                self.shared.dir.display()
            )));
        }
        self.shared.keep_alive.lock().take();
        Ok(())
    }

    /// Start receiving a snapshot that covers up to `last`, with `meta`
    /// and `data_len` bytes of data still to come. Fails at once when
    /// another receive holds the file or the store is closed. A close
    /// waits for the [`StagedSnapshot`] to be dropped before it lets go of
    /// the keep-alive value, so the store's lock is held under it.
    pub async fn begin_receive(
        &self,
        last: Option<SnapshotId>,
        meta: Bytes,
        data_len: u64,
    ) -> io::Result<StagedSnapshot> {
        StagedSnapshot::begin(Arc::clone(&self.shared), last, meta, data_len).await
    }

    /// A reader over the stored snapshot's file, or `None` when there is
    /// none. It reads the file it opened, whatever later replaces it.
    pub async fn open_current(&self) -> io::Result<Option<StoredReader>> {
        StoredReader::open(Arc::clone(&self.shared)).await
    }

    /// Make the next [`Self::close`] fail without closing anything.
    #[cfg(test)]
    pub(crate) fn fail_next_close(&self) {
        self.shared
            .fail_close
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
    /// entry: a snapshot never goes back. Fails once the store is closed.
    pub async fn install(&self, snapshot: StoredSnapshot) -> io::Result<bool> {
        let shared = Arc::clone(&self.shared);
        felix_common::blocking::spawn(move || {
            let mut stored = shared.stored.lock();
            if shared.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(io::Error::other(crate::StorageError::Closed(
                    shared.dir.display().to_string(),
                )));
            }
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
    let mut out = header_without_crc(
        snapshot.last,
        snapshot.meta.len(),
        snapshot.data.len() as u64,
    )?;
    let crc = crate::segment::format::crc32(&[&out[0..36], &snapshot.meta, &snapshot.data]);
    out[36..40].copy_from_slice(&crc.to_be_bytes());
    Ok(out)
}

/// The header with its crc left zero.
fn header_without_crc(
    last: Option<SnapshotId>,
    meta_len: usize,
    data_len: u64,
) -> io::Result<[u8; HEADER_LEN]> {
    let (index, generation) = match last {
        None => (0, 0),
        Some(last) => (last.index, generation_of(last.term)?),
    };
    let meta_len = u32::try_from(meta_len)
        .map_err(|_| invalid("snapshot metadata is over 4 GiB".to_string()))?;
    let mut out = [0u8; HEADER_LEN];
    out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&VERSION.to_be_bytes());
    out[8..16].copy_from_slice(&index.to_be_bytes());
    out[16..24].copy_from_slice(&generation.to_be_bytes());
    out[24..28].copy_from_slice(&meta_len.to_be_bytes());
    out[28..36].copy_from_slice(&data_len.to_be_bytes());
    Ok(out)
}

/// What a header says, before the body is read.
struct Header {
    last: Option<SnapshotId>,
    meta_len: usize,
    data_len: u64,
    crc: u32,
}

fn decode_header(header: &[u8]) -> Option<Header> {
    let header = header.get(..HEADER_LEN)?;
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
    let last = match term_of_generation(generation) {
        Some(term) => Some(SnapshotId { term, index }),
        None if index == 0 => None,
        None => return None,
    };
    Some(Header {
        last,
        meta_len: u32::from_be_bytes(header[24..28].try_into().ok()?) as usize,
        data_len: u64::from_be_bytes(header[28..36].try_into().ok()?),
        crc: u32::from_be_bytes(header[36..40].try_into().ok()?),
    })
}

#[cfg(test)]
pub(super) fn decode(bytes: &[u8]) -> Option<StoredSnapshot> {
    decode_owned(Bytes::copy_from_slice(bytes))
}

/// [`decode`] without copying the body out of `bytes`.
fn decode_owned(bytes: Bytes) -> Option<StoredSnapshot> {
    let header = decode_header(&bytes)?;
    let data_len = usize::try_from(header.data_len).ok()?;
    let body = bytes.slice(HEADER_LEN..);
    if header.meta_len.checked_add(data_len)? != body.len() {
        return None;
    }
    let (meta, data) = (body.slice(..header.meta_len), body.slice(header.meta_len..));
    if crate::segment::format::crc32(&[&bytes[0..36], &meta, &data]) != header.crc {
        return None;
    }
    Some(StoredSnapshot {
        last: header.last,
        meta,
        data,
    })
}

fn read(dir: &Path) -> io::Result<Option<StoredSnapshot>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    decode_owned(Bytes::from(bytes)).map(Some).ok_or_else(|| {
        invalid(format!(
            "{} does not decode; the log below it may be purged, so it cannot be read as no snapshot",
            dir.join(FILE_NAME).display()
        ))
    })
}
