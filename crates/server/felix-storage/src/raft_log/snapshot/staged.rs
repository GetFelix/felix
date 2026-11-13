//! A snapshot on its way in from a peer, and the stored one on its way out.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::{
    FILE_NAME, HEADER_LEN, RECEIVE_NAME, Shared, SnapshotId, StoredSnapshot, decode_header,
    decode_owned, header_without_crc,
};
use crate::disk_log::crash_point;
use crate::raft_log::invalid;

/// A snapshot being received into `raft-snapshot.recv`.
///
/// Written in the stored file's format, so committing it is a rename. The
/// file is deleted when this is dropped without [`Self::commit`], and the
/// receive slot is let go of only after that.
pub struct StagedSnapshot {
    shared: Arc<Shared>,
    file: Option<crate::fs::File>,
    last: Option<SnapshotId>,
    header: [u8; HEADER_LEN],
    data_len: u64,
    written: u64,
    crc: crc32fast::Hasher,
    finished: bool,
    committed: bool,
    closing: tokio::sync::watch::Receiver<bool>,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

impl std::fmt::Debug for StagedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedSnapshot")
            .field("last", &self.last)
            .field("data_len", &self.data_len)
            .field("written", &self.written)
            .finish()
    }
}

impl StagedSnapshot {
    pub(super) async fn begin(
        shared: Arc<Shared>,
        last: Option<SnapshotId>,
        meta: Bytes,
        data_len: u64,
    ) -> io::Result<Self> {
        let slot = Arc::clone(&shared.receiving)
            .try_acquire_owned()
            .map_err(|_| busy(&shared.dir))?;
        if shared.closed.load(Ordering::Acquire) {
            return Err(closed(&shared.dir));
        }
        let header = header_without_crc(last, meta.len(), data_len)?;
        let mut crc = crc32fast::Hasher::new();
        crc.update(&header[0..36]);
        crc.update(&meta);
        let dir = shared.dir.clone();
        let file = felix_common::blocking::spawn(move || {
            crash_point::reached(&dir)?;
            // Read too: the install reads the snapshot back through it.
            let mut file = crate::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(dir.join(RECEIVE_NAME))?;
            file.write_all(&header)?;
            file.write_all(&meta)?;
            Ok::<_, io::Error>(file)
        })
        .await
        .map_err(io::Error::other)?;
        let closing = shared.closing.subscribe();
        let mut staged = Self {
            shared,
            file: None,
            last,
            header,
            data_len,
            written: 0,
            crc,
            finished: false,
            committed: false,
            closing,
            _slot: slot,
        };
        // Set after `staged` exists, so a failed create is still unlinked.
        staged.file = Some(file?);
        Ok(staged)
    }

    /// The last entry the snapshot covers.
    pub fn last(&self) -> Option<SnapshotId> {
        self.last
    }

    /// The data length the sender declared.
    pub fn data_len(&self) -> u64 {
        self.data_len
    }

    /// The data bytes written so far.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Resolves once the store is closing: a receive waiting on its sender
    /// stops and drops this. Owns what it waits on, so it can be awaited
    /// while this is being written.
    pub fn closing(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut closing = self.closing.clone();
        async move {
            let _ = closing.wait_for(|closing| *closing).await;
        }
    }

    /// Append `chunk` to the data. More than the declared length is an
    /// error.
    pub async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        if self.finished {
            return Err(invalid("snapshot data after its end".to_string()));
        }
        if self.written.saturating_add(chunk.len() as u64) > self.data_len {
            return Err(invalid(format!(
                "snapshot data is longer than the {} bytes declared",
                self.data_len
            )));
        }
        if *self.closing.borrow() {
            return Err(closed(&self.shared.dir));
        }
        self.crc.update(&chunk);
        self.written += chunk.len() as u64;
        let mut file = self.file.take().ok_or_else(broken)?;
        let (file, wrote) = felix_common::blocking::spawn(move || {
            let wrote = file.write_all(&chunk);
            (file, wrote)
        })
        .await
        .map_err(io::Error::other)?;
        self.file = Some(file);
        wrote
    }

    /// The data is all here: write the header's crc and make the file
    /// durable. Fails when less than the declared length arrived.
    pub async fn finish(&mut self) -> io::Result<()> {
        if self.written != self.data_len {
            return Err(invalid(format!(
                "snapshot data ended at {} of the {} bytes declared",
                self.written, self.data_len
            )));
        }
        let crc = self.crc.clone().finalize();
        self.header[36..40].copy_from_slice(&crc.to_be_bytes());
        let mut file = self.file.take().ok_or_else(broken)?;
        let dir = self.shared.dir.clone();
        let (file, done) = felix_common::blocking::spawn(move || {
            let done = (|| {
                crash_point::reached(&dir)?;
                file.seek(SeekFrom::Start(36))?;
                file.write_all(&crc.to_be_bytes())?;
                crate::io::sync_all(&file)
            })();
            (file, done)
        })
        .await
        .map_err(io::Error::other)?;
        self.file = Some(file);
        done?;
        self.finished = true;
        Ok(())
    }

    /// The whole snapshot, read back from the file and checked against its
    /// crc. The one full copy a receive makes.
    pub async fn read(&mut self) -> io::Result<StoredSnapshot> {
        if !self.finished {
            return Err(invalid("snapshot read before it was finished".to_string()));
        }
        let mut file = self.file.take().ok_or_else(broken)?;
        let (file, bytes) = felix_common::blocking::spawn(move || {
            let read = (|| {
                file.seek(SeekFrom::Start(0))?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                Ok::<_, io::Error>(bytes)
            })();
            (file, read)
        })
        .await
        .map_err(io::Error::other)?;
        self.file = Some(file);
        decode_owned(Bytes::from(bytes?))
            .ok_or_else(|| invalid("a received snapshot does not decode".to_string()))
    }

    /// Make this the stored snapshot. Returns false, and deletes the file,
    /// when the stored one covers a later entry: a snapshot never goes
    /// back. Fails once the store is closed.
    pub async fn commit(mut self) -> io::Result<bool> {
        if !self.finished {
            return Err(invalid(
                "snapshot committed before it was finished".to_string(),
            ));
        }
        // Closed first, so the rename never replaces a file under an open
        // descriptor on Windows.
        drop(self.file.take());
        let shared = Arc::clone(&self.shared);
        let last = self.last;
        let renamed = felix_common::blocking::spawn(move || {
            let mut stored = shared.stored.lock();
            if shared.closed.load(Ordering::Acquire) {
                return Err(closed(&shared.dir));
            }
            let index = |last: Option<SnapshotId>| last.map(|last| last.index);
            if stored.is_some_and(|stored| index(stored) > index(last)) {
                return Ok(false);
            }
            let step = || crash_point::reached(&shared.dir);
            step()?;
            crate::fs::rename(shared.dir.join(RECEIVE_NAME), shared.dir.join(FILE_NAME))?;
            step()?;
            crate::io::sync_dir(&shared.dir)?;
            *stored = Some(last);
            *shared.covered.lock() = last;
            Ok(true)
        })
        .await
        .map_err(io::Error::other)?;
        if let Ok(true) = renamed {
            self.committed = true;
        }
        renamed
    }
}

impl Drop for StagedSnapshot {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        drop(self.file.take());
        // Best effort: a file left here is deleted by the next open.
        let _ = crate::fs::remove_file(self.shared.dir.join(RECEIVE_NAME));
    }
}

/// The stored snapshot's file, read in pieces for a sender. Holds the
/// descriptor it opened, so a later install does not change what it reads.
pub struct StoredReader {
    file: Option<crate::fs::File>,
    last: Option<SnapshotId>,
    meta: Bytes,
    data_len: u64,
    remaining: u64,
    crc: crc32fast::Hasher,
    expected: u32,
}

impl std::fmt::Debug for StoredReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredReader")
            .field("last", &self.last)
            .field("data_len", &self.data_len)
            .field("remaining", &self.remaining)
            .finish()
    }
}

impl StoredReader {
    pub(super) async fn open(shared: Arc<Shared>) -> io::Result<Option<Self>> {
        felix_common::blocking::spawn(move || {
            let _stored = shared.stored.lock();
            let path = shared.dir.join(FILE_NAME);
            let mut file = match crate::fs::File::open(&path) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(err) => return Err(err),
            };
            let damaged = || invalid(format!("{} does not decode", path.display()));
            let mut header = [0u8; HEADER_LEN];
            file.read_exact(&mut header).map_err(|_| damaged())?;
            let decoded = decode_header(&header).ok_or_else(damaged)?;
            let body = decoded.meta_len as u64 + decoded.data_len;
            if file.metadata()?.len() != HEADER_LEN as u64 + body {
                return Err(damaged());
            }
            let mut meta = vec![0u8; decoded.meta_len];
            file.read_exact(&mut meta)?;
            let mut crc = crc32fast::Hasher::new();
            crc.update(&header[0..36]);
            crc.update(&meta);
            Ok(Some(Self {
                file: Some(file),
                last: decoded.last,
                meta: Bytes::from(meta),
                data_len: decoded.data_len,
                remaining: decoded.data_len,
                crc,
                expected: decoded.crc,
            }))
        })
        .await
        .map_err(io::Error::other)?
    }

    /// The last entry the snapshot covers.
    pub fn last(&self) -> Option<SnapshotId> {
        self.last
    }

    /// The caller's metadata kept with the snapshot.
    pub fn meta(&self) -> &Bytes {
        &self.meta
    }

    /// The data's length.
    pub fn data_len(&self) -> u64 {
        self.data_len
    }

    /// The next piece of the data, at most `max` bytes, or `None` at its
    /// end. The end is reported only once the file's crc matched, so a
    /// damaged file fails the send instead of reaching a peer.
    pub async fn next_chunk(&mut self, max: usize) -> io::Result<Option<Bytes>> {
        if self.remaining == 0 {
            if self.crc.clone().finalize() != self.expected {
                return Err(invalid(
                    "the stored snapshot's crc does not match".to_string(),
                ));
            }
            return Ok(None);
        }
        let len = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(max.max(1));
        let mut file = self.file.take().ok_or_else(broken)?;
        let (file, read) = felix_common::blocking::spawn(move || {
            let mut chunk = vec![0u8; len];
            let read = file.read_exact(&mut chunk).map(|()| chunk);
            (file, read)
        })
        .await
        .map_err(io::Error::other)?;
        self.file = Some(file);
        let chunk = read?;
        self.crc.update(&chunk);
        self.remaining -= chunk.len() as u64;
        Ok(Some(Bytes::from(chunk)))
    }
}

/// Delete the receive file a crash left. It is never read.
pub(super) fn remove_leftover(dir: &Path) -> io::Result<()> {
    match crate::fs::remove_file(dir.join(RECEIVE_NAME)) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

fn busy(dir: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("a snapshot is already being received in {}", dir.display()),
    )
}

fn closed(dir: &Path) -> io::Error {
    io::Error::other(crate::StorageError::Closed(dir.display().to_string()))
}

/// A blocking call that panicked took the file with it.
fn broken() -> io::Error {
    io::Error::other("the snapshot file was lost to a failed write")
}
