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
//! The store is opened by the first pass, not by `DiskLog::open`, so a
//! missing or read-only archive never stops a log from serving. While passes
//! keep failing, nothing un-copied is deleted and the local disk grows; the
//! failures and the bytes retention is holding are reported (see `Health`).
//!
//! Reads do not use the copies yet; below the local head they still report
//! `Trimmed`.

pub(crate) mod manifest;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, WriteMultipart};
use parking_lot::Mutex;

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
/// How often a lasting failure is logged: first at once, then backing off
/// to this cap, so an outage stays visible without flooding the log.
const FIRST_REPORT_AFTER: Duration = Duration::from_secs(30);
const MAX_REPORT_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Where one log's segments are copied.
pub(super) struct Offloader {
    /// Opened by a pass, and dropped after a failed one so the next pass
    /// creates the directory again if the mount came back empty.
    store: Mutex<Option<Arc<LocalFileSystem>>>,
    root: PathBuf,
    /// The shard's directory name, which is unique under a storage root and
    /// safe in a path. Every key starts with it.
    prefix: String,
    /// Set when the log stops, so a pass ends between segments instead of
    /// holding shutdown up for the rest of its uploads.
    halted: AtomicBool,
    pub(super) health: Mutex<Health>,
    #[cfg(test)]
    pub(super) faults: test_hooks::Faults,
}

/// Whether passes are failing, and what that is costing on local disk.
#[derive(Debug, Default)]
pub(super) struct Health {
    /// Failed passes since the last one that succeeded.
    pub(super) consecutive_failures: u64,
    /// Bytes retention would delete but keeps because they have no recorded
    /// copy. This log's share of `OFFLOAD_HELD_BYTES`.
    pub(super) held_bytes: u64,
    next_report: Option<Instant>,
    report_interval: Duration,
    /// The last pass's failure was logged, so the retention after it
    /// reports what is being held too.
    reported: bool,
}

impl Health {
    /// Whether a lasting problem is due another log line, backing off from
    /// `FIRST_REPORT_AFTER` to `MAX_REPORT_INTERVAL`.
    fn report_due(&mut self, now: Instant) -> bool {
        if self.next_report.is_some_and(|at| now < at) {
            return false;
        }
        self.report_interval = if self.next_report.is_none() {
            FIRST_REPORT_AFTER
        } else {
            (self.report_interval * 2).min(MAX_REPORT_INTERVAL)
        };
        self.next_report = Some(now + self.report_interval);
        true
    }
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
        let prefix = shard_dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .ok_or(StorageError::InvalidConfig(
                "an offloaded log's directory must have a name",
            ))?;
        Ok(Self {
            store: Mutex::new(None),
            root: root.clone(),
            prefix,
            halted: AtomicBool::new(false),
            health: Mutex::new(Health::default()),
            #[cfg(test)]
            faults: test_hooks::Faults::default(),
        })
    }

    pub(super) fn halt(&self) {
        self.halted.store(true, Ordering::Release);
    }

    /// The store, opening it (and creating its directory) if no pass has
    /// yet, or the last one failed.
    async fn store(&self) -> Result<Arc<LocalFileSystem>> {
        if let Some(store) = self.store.lock().as_ref() {
            return Ok(Arc::clone(store));
        }
        let root = self.root.clone();
        let store = blocking(move || {
            crate::io::create_dir_all_durable(&root)?;
            LocalFileSystem::new_with_prefix(&root).map_err(object_error)
        })
        .await?;
        let store = Arc::new(store);
        *self.store.lock() = Some(Arc::clone(&store));
        Ok(store)
    }

    /// Count a pass's result and log a change, or a lasting failure on a
    /// backoff.
    fn note_pass(&self, label: &str, result: &Result<OffloadOutcome>) {
        let mut health = self.health.lock();
        match result {
            Ok(_) => {
                if health.consecutive_failures > 0 {
                    metrics::gauge!(metrics_names::OFFLOAD_FAILING_LOGS).decrement(1.0);
                    tracing::info!(
                        shard = %label,
                        failed_passes = health.consecutive_failures,
                        archive = %self.root.display(),
                        "offload is copying again"
                    );
                }
                health.consecutive_failures = 0;
                health.next_report = None;
                health.reported = false;
            }
            Err(err) => {
                *self.store.lock() = None;
                metrics::counter!(metrics_names::OFFLOAD_FAILURES_TOTAL).increment(1);
                if health.consecutive_failures == 0 {
                    metrics::gauge!(metrics_names::OFFLOAD_FAILING_LOGS).increment(1.0);
                }
                health.consecutive_failures += 1;
                health.reported = health.report_due(Instant::now());
                if health.reported {
                    tracing::warn!(
                        shard = %label,
                        error = %err,
                        failed_passes = health.consecutive_failures,
                        archive = %self.root.display(),
                        "offload pass failed; un-copied segments stay on local disk until it succeeds"
                    );
                }
            }
        }
    }

    /// Record how many bytes retention kept for want of a copy, and say so
    /// loudly while it is keeping any.
    pub(super) fn note_held(&self, label: &str, segments: usize, bytes: u64) {
        let mut health = self.health.lock();
        let delta = bytes as f64 - health.held_bytes as f64;
        if delta != 0.0 {
            metrics::gauge!(metrics_names::OFFLOAD_HELD_BYTES).increment(delta);
        }
        health.held_bytes = bytes;
        // A failing pass logs its own error on the same backoff; this line
        // adds what the outage is costing.
        if bytes > 0 && std::mem::take(&mut health.reported) {
            tracing::error!(
                shard = %label,
                held_segments = segments,
                held_bytes = bytes,
                failed_passes = health.consecutive_failures,
                archive = %self.root.display(),
                "retention is keeping segments past their bound until offload can copy them; local disk will keep growing"
            );
        }
    }

    fn key_for(&self, descriptor: &SegmentDescriptor) -> String {
        format!(
            "{}/{:020}-{:020}.segment",
            self.prefix, descriptor.base_offset, descriptor.id
        )
    }

    /// Copy one sealed segment and verify the copy. Returns the entry to
    /// record; recording it is the caller's job.
    async fn copy(
        &self,
        store: &LocalFileSystem,
        locator: &SealedLocator,
        label: &str,
    ) -> Result<ManifestEntry> {
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
        let upload = store.put_multipart(&location).await.map_err(object_error)?;
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

        self.make_durable(store, &location).await?;
        #[cfg(test)]
        self.faults
            .after_upload(&store.path_to_filesystem(&location).map_err(object_error)?)?;
        self.verify(store, &location, descriptor.size_bytes, checksum)
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
    async fn make_durable(&self, store: &LocalFileSystem, location: &ObjectPath) -> Result<()> {
        let path = store.path_to_filesystem(location).map_err(object_error)?;
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
    async fn verify(
        &self,
        store: &LocalFileSystem,
        location: &ObjectPath,
        size_bytes: u64,
        checksum: u32,
    ) -> Result<()> {
        let meta = store.head(location).await.map_err(object_error)?;
        if meta.size != size_bytes {
            return Err(mismatch(location, "size", size_bytes, meta.size));
        }
        let mut hasher = crc32fast::Hasher::new();
        let mut position = 0u64;
        while position < size_bytes {
            let end = (position + CHUNK_BYTES as u64).min(size_bytes);
            let chunk = store
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
}

impl Drop for Offloader {
    /// Take this log's share out of the process-wide gauges.
    fn drop(&mut self) {
        let health = self.health.get_mut();
        if health.consecutive_failures > 0 {
            metrics::gauge!(metrics_names::OFFLOAD_FAILING_LOGS).decrement(1.0);
        }
        if health.held_bytes > 0 {
            metrics::gauge!(metrics_names::OFFLOAD_HELD_BYTES).decrement(health.held_bytes as f64);
        }
    }
}

impl LogInner {
    /// Copy every sealed segment without a recorded copy, oldest first.
    ///
    /// Stops at the first failure: the segments after it stay local, and
    /// retention deletes oldest first, so nothing behind a failed copy is at
    /// risk. The next pass starts again from there.
    ///
    /// A failure, opening the store included, is counted and logged here and
    /// returned; the next pass retries.
    pub(super) async fn offload_pass(self: &Arc<Self>) -> Result<OffloadOutcome> {
        let Some(offloader) = self.offloader.as_ref() else {
            return Ok(OffloadOutcome::default());
        };
        let result = self.copy_pending(offloader).await;
        offloader.note_pass(&self.label, &result);
        result
    }

    async fn copy_pending(self: &Arc<Self>, offloader: &Offloader) -> Result<OffloadOutcome> {
        let mut outcome = OffloadOutcome::default();
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
        if pending.is_empty() {
            return Ok(outcome);
        }
        let store = offloader.store().await?;
        for locator in pending {
            if offloader.halted.load(Ordering::Acquire) {
                break;
            }
            let entry = offloader.copy(&store, &locator, &self.label).await?;
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
