//! Copying sealed segments to an object store.
//!
//! A pass runs on the retention timer, before retention. For each sealed
//! segment without a recorded copy, oldest first, it:
//!
//! 1. uploads the segment's valid bytes, computing their CRC-32 as it goes;
//! 2. makes the object durable and reads it back, checking size and CRC;
//! 3. records it in the shard's manifest, fsynced.
//!
//! Retention then deletes only segments the manifest records. That order is
//! the whole safety argument: a crash at any point leaves the local segment,
//! or a recorded and verified copy, and usually both. The object is written
//! under a key no recorded copy uses, so a recorded copy is never rewritten.
//!
//! Reads do not use the copies yet; below the local head they still report
//! `Trimmed`.

pub(crate) mod manifest;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, WriteMultipart};

use self::manifest::ManifestEntry;
use super::LogInner;
use super::segments::SealedLocator;
use crate::log::{OffloadTarget, SegmentDescriptor};
use crate::{Result, StorageError, metrics_names};

pub(crate) use self::manifest::Manifest;

/// Bytes read from the segment, and from the copy, per step. Bounds a pass's
/// memory to a few of these whatever the segment size.
const CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Parts in flight at once during an upload.
const UPLOAD_CONCURRENCY: usize = 2;

/// Where one log's segments are copied.
pub(super) struct Offloader {
    store: LocalFileSystem,
    root: PathBuf,
    /// The shard's directory name, which is unique under a storage root and
    /// safe in a path. Every key starts with it.
    prefix: String,
    /// Set when the log stops, so a pass ends between segments instead of
    /// holding shutdown up for the rest of its uploads.
    halted: AtomicBool,
    #[cfg(test)]
    pub(super) faults: test_hooks::Faults,
}

impl std::fmt::Debug for Offloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Offloader")
            .field("root", &self.root)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// What one offload pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct OffloadOutcome {
    pub(super) segments: usize,
    pub(super) bytes: u64,
}

impl Offloader {
    pub(super) fn open(target: &OffloadTarget, shard_dir: &Path) -> Result<Self> {
        let OffloadTarget::LocalDir(root) = target;
        crate::io::create_dir_all_durable(root)?;
        let store = LocalFileSystem::new_with_prefix(root).map_err(object_error)?;
        let prefix = shard_dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .ok_or(StorageError::InvalidConfig(
                "an offloaded log's directory must have a name",
            ))?;
        Ok(Self {
            store,
            root: root.clone(),
            prefix,
            halted: AtomicBool::new(false),
            #[cfg(test)]
            faults: test_hooks::Faults::default(),
        })
    }

    pub(super) fn halt(&self) {
        self.halted.store(true, Ordering::Release);
    }

    fn key_for(&self, descriptor: &SegmentDescriptor) -> String {
        format!(
            "{}/{:020}-{:020}.segment",
            self.prefix, descriptor.base_offset, descriptor.id
        )
    }

    /// Copy one sealed segment and verify the copy. Returns the entry to
    /// record; recording it is the caller's job.
    async fn copy(&self, locator: &SealedLocator, label: &str) -> Result<ManifestEntry> {
        let descriptor = locator.descriptor().clone();
        let key = self.key_for(&descriptor);
        let location = ObjectPath::from(key.as_str());

        let (oldest, newest) = {
            let locator = locator.clone();
            let label = label.to_string();
            blocking(move || {
                let files = super::sealed::SealedFiles::new(1);
                Ok((
                    locator.oldest_timestamp(&label)?,
                    locator.newest_timestamp(&files, &label)?,
                ))
            })
            .await?
        };

        // Any object already at this key is not recorded (only unrecorded
        // segments are copied), so replacing it loses nothing. It is a copy
        // a crash interrupted, or one of a segment a truncation since cut.
        let file = Arc::new(std::fs::File::open(locator.path())?);
        let upload = self
            .store
            .put_multipart(&location)
            .await
            .map_err(object_error)?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, CHUNK_BYTES);
        let mut hasher = crc32fast::Hasher::new();
        let mut position = 0u64;
        while position < descriptor.size_bytes {
            let want = (descriptor.size_bytes - position).min(CHUNK_BYTES as u64) as usize;
            let chunk = {
                let file = Arc::clone(&file);
                blocking(move || {
                    let mut buf = vec![0u8; want];
                    let mut filled = 0;
                    while filled < want {
                        let read = crate::io::read_at(
                            &file,
                            &mut buf[filled..],
                            position + filled as u64,
                        )?;
                        if read == 0 {
                            return Err(StorageError::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "segment is shorter than its descriptor",
                            )));
                        }
                        filled += read;
                    }
                    Ok(bytes::Bytes::from(buf))
                })
                .await
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    let _ = writer.abort().await;
                    return Err(err);
                }
            };
            hasher.update(&chunk);
            position += chunk.len() as u64;
            writer
                .wait_for_capacity(UPLOAD_CONCURRENCY)
                .await
                .map_err(object_error)?;
            writer.put(chunk);
        }
        writer.finish().await.map_err(object_error)?;
        let checksum = hasher.finalize();

        self.make_durable(&location).await?;
        #[cfg(test)]
        self.faults.after_upload(&self.object_file(&location)?)?;
        self.verify(&location, descriptor.size_bytes, checksum)
            .await?;

        Ok(ManifestEntry {
            segment_id: descriptor.id,
            base_offset: descriptor.base_offset,
            last_offset: descriptor.last_offset,
            size_bytes: descriptor.size_bytes,
            checksum,
            oldest_timestamp_micros: oldest.unwrap_or(0),
            newest_timestamp_micros: newest.unwrap_or(0),
            key,
        })
    }

    /// Flush the object and every directory between it and the root.
    ///
    /// The local backend's own fsync option would do the same, but these go
    /// through `crate::io`, which is what the power-loss tests observe.
    async fn make_durable(&self, location: &ObjectPath) -> Result<()> {
        let path = self.object_file(location)?;
        let root = self.root.clone();
        blocking(move || {
            crate::io::sync_all(&std::fs::File::open(&path)?)?;
            let mut dir = path.parent();
            while let Some(current) = dir {
                crate::io::sync_dir(current)?;
                if current == root {
                    break;
                }
                dir = current.parent();
            }
            Ok(())
        })
        .await
    }

    /// Read the copy back and check it is the bytes that were sent.
    async fn verify(&self, location: &ObjectPath, size_bytes: u64, checksum: u32) -> Result<()> {
        let meta = self.store.head(location).await.map_err(object_error)?;
        if meta.size != size_bytes {
            return Err(mismatch(location, "size", size_bytes, meta.size));
        }
        let mut hasher = crc32fast::Hasher::new();
        let mut position = 0u64;
        while position < size_bytes {
            let end = (position + CHUNK_BYTES as u64).min(size_bytes);
            let chunk = self
                .store
                .get_range(location, position..end)
                .await
                .map_err(object_error)?;
            if chunk.is_empty() {
                return Err(mismatch(location, "size", size_bytes, position));
            }
            hasher.update(&chunk);
            position += chunk.len() as u64;
        }
        let found = hasher.finalize();
        if found != checksum {
            return Err(mismatch(
                location,
                "checksum",
                u64::from(checksum),
                u64::from(found),
            ));
        }
        Ok(())
    }

    fn object_file(&self, location: &ObjectPath) -> Result<PathBuf> {
        self.store
            .path_to_filesystem(location)
            .map_err(object_error)
    }
}

impl LogInner {
    /// Copy every sealed segment without a recorded copy, oldest first.
    ///
    /// Stops at the first failure: the segments after it stay local, and
    /// retention deletes oldest first, so nothing behind a failed copy is at
    /// risk. The next pass starts again from there.
    pub(super) async fn offload_pass(self: &Arc<Self>) -> Result<OffloadOutcome> {
        let mut outcome = OffloadOutcome::default();
        let Some(offloader) = self.offloader.as_ref() else {
            return Ok(outcome);
        };
        let (generation, sealed) = self.segments.read().sealed_locators();
        let pending: Vec<SealedLocator> = {
            let manifest = self.manifest.lock();
            sealed
                .into_iter()
                .filter(|locator| {
                    let descriptor = locator.descriptor();
                    if manifest.records(descriptor) {
                        return false;
                    }
                    if manifest.overlaps(descriptor.base_offset, descriptor.last_offset) {
                        // A recorded copy of other records at these offsets.
                        // Recording this one too would leave two answers for
                        // one offset, so it stays local until someone looks.
                        tracing::warn!(
                            shard = %self.label,
                            segment = descriptor.id,
                            "offload manifest disagrees with a local segment; not copying it",
                        );
                        return false;
                    }
                    true
                })
                .collect()
        };
        for locator in pending {
            if offloader.halted.load(Ordering::Acquire) {
                break;
            }
            let entry = match offloader.copy(&locator, &self.label).await {
                Ok(entry) => entry,
                Err(err) => {
                    metrics::counter!(metrics_names::OFFLOAD_FAILURES_TOTAL).increment(1);
                    return Err(err);
                }
            };
            #[cfg(test)]
            offloader.faults.stop(test_hooks::Stop::Uploaded)?;
            let recorded = {
                let inner = Arc::clone(self);
                let entry = entry.clone();
                let descriptor = locator.descriptor().clone();
                blocking(move || inner.record_offload(generation, &descriptor, entry)).await?
            };
            if !recorded {
                // The log changed under the copy. The next pass looks again.
                break;
            }
            #[cfg(test)]
            offloader.faults.stop(test_hooks::Stop::Recorded)?;
            outcome.segments += 1;
            outcome.bytes += entry.size_bytes;
            metrics::counter!(metrics_names::OFFLOAD_SEGMENTS_TOTAL).increment(1);
            metrics::counter!(metrics_names::OFFLOAD_BYTES_TOTAL).increment(entry.size_bytes);
        }
        Ok(outcome)
    }

    /// Record a verified copy, unless the segment it was taken from changed.
    ///
    /// Under the manifest lock, which truncation takes before it cuts: a
    /// segment still here at the generation it was copied at has the bytes
    /// that were copied.
    fn record_offload(
        &self,
        generation: u64,
        descriptor: &SegmentDescriptor,
        entry: ManifestEntry,
    ) -> Result<bool> {
        let mut manifest = self.manifest.lock();
        {
            let segments = self.segments.read();
            segments.check_open()?;
            if segments.generation() != generation || !segments.holds_sealed(descriptor) {
                return Ok(false);
            }
        }
        let mut next = manifest.clone();
        next.insert(entry)?;
        manifest::store(&self.dir, &next)?;
        *manifest = next;
        Ok(true)
    }

    /// Drop the manifest's entries at or after `offset`, durably, before a
    /// truncation cuts there. Called holding the manifest lock.
    pub(super) fn forget_offloaded_from(&self, manifest: &mut Manifest, offset: u64) -> Result<()> {
        let mut next = manifest.clone();
        if next.forget_from(offset) {
            manifest::store(&self.dir, &next)?;
            *manifest = next;
        }
        Ok(())
    }

    /// Drop every manifest entry, durably, before a reset discards the log.
    /// Called holding the manifest lock.
    pub(super) fn forget_all_offloaded(&self, manifest: &mut Manifest) -> Result<()> {
        let mut next = manifest.clone();
        if next.clear() {
            manifest::store(&self.dir, &next)?;
            *manifest = next;
        }
        Ok(())
    }
}

async fn blocking<T, F>(work: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
}

fn object_error(err: object_store::Error) -> StorageError {
    StorageError::Io(std::io::Error::other(err))
}

fn mismatch(location: &ObjectPath, what: &str, expected: u64, found: u64) -> StorageError {
    StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("offloaded copy {location} has {what} {found}, expected {expected}"),
    ))
}

#[cfg(test)]
pub(super) mod test_hooks {
    //! Faults for the offload tests: stop a pass at a step, as a crash
    //! would, or damage a copy before it is verified.

    use std::path::Path;

    use parking_lot::Mutex;

    use crate::{Result, StorageError};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Stop {
        /// The copy is uploaded and verified; the manifest is not written.
        Uploaded,
        /// The manifest records the copy; the local segment is still there.
        Recorded,
        /// Retention unlinked the first recorded segment.
        Unlinked,
    }

    #[derive(Debug, Default)]
    pub(crate) struct Faults {
        stop: Mutex<Option<Stop>>,
        stopped: Mutex<bool>,
        corrupt_next: Mutex<bool>,
    }

    impl Faults {
        pub(crate) fn stop_at(&self, stop: Stop) {
            *self.stop.lock() = Some(stop);
        }

        /// Whether an armed stop has fired.
        pub(crate) fn stopped(&self) -> bool {
            *self.stopped.lock()
        }

        pub(crate) fn corrupt_next_upload(&self) {
            *self.corrupt_next.lock() = true;
        }

        /// Fails, ending the sweep, when the armed stop is `at`.
        pub(crate) fn stop(&self, at: Stop) -> Result<()> {
            let mut armed = self.stop.lock();
            if *armed == Some(at) {
                *armed = None;
                *self.stopped.lock() = true;
                return Err(StorageError::Io(std::io::Error::other(format!(
                    "stopped for a test at {at:?}"
                ))));
            }
            Ok(())
        }

        pub(crate) fn after_upload(&self, object: &Path) -> Result<()> {
            if std::mem::take(&mut *self.corrupt_next.lock()) {
                let mut bytes = std::fs::read(object)?;
                let last = bytes.len() - 1;
                bytes[last] ^= 0xff;
                std::fs::write(object, bytes)?;
            }
            Ok(())
        }
    }
}
