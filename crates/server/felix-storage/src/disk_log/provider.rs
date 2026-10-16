//! One [`DiskLog`] per shard under a common root directory.

use std::path::{Path, PathBuf};

use std::collections::HashMap;

use super::{DiskLog, layout};
use crate::incarnation::{self, Incarnations};
use crate::log::{BoxFuture, LogConfig, LogProvider, Offset, Retention, RetentionHold, ShardKey};
use crate::shard_slots::ShardSlots;
use crate::{Result, StorageError};

/// Opens one [`DiskLog`] per shard under a common root directory.
///
/// Repeated opens of the same shard return the same log. Two independent
/// writers over one directory would interleave offsets and corrupt the segment,
/// so the cache is a correctness requirement, not an optimisation. Opens of
/// different shards run in parallel; see `crate::shard_slots`.
///
/// Each stream opens at the incarnation set with
/// [`Self::set_stream_incarnation`], 0 when none was set. Logs are cached per
/// incarnation, so a handle to a deleted stream's log is never handed to the
/// stream created after it.
#[derive(Debug)]
pub struct DiskLogProvider {
    root: PathBuf,
    config: LogConfig,
    open_logs: ShardSlots<(ShardKey, u64), DiskLog>,
    incarnations: Incarnations,
    dirs: std::sync::Arc<incarnation::DirIndex>,
    /// Retention set for one stream, keyed by tenant, namespace and stream.
    /// A bound a stream leaves unset comes from `config`.
    stream_retention: parking_lot::RwLock<HashMap<(String, String, String), Retention>>,
    /// Whether each stream's logs are held at the commit offset, as set by
    /// [`Self::set_stream_retention_hold`].
    stream_hold: parking_lot::RwLock<HashMap<(String, String, String), bool>>,
    /// The floors file under `.streams/`, read on first use.
    floors: parking_lot::Mutex<Option<incarnation::marker::Floors>>,
    /// Runs inside every open, under the shard's lock, so tests can make an
    /// open slow and watch what waits on it.
    #[cfg(test)]
    open_hook: parking_lot::Mutex<Option<OpenHook>>,
}

#[cfg(test)]
#[derive(Clone)]
struct OpenHook(std::sync::Arc<dyn Fn(&ShardKey) + Send + Sync>);

#[cfg(test)]
impl std::fmt::Debug for OpenHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OpenHook")
    }
}

impl DiskLogProvider {
    pub fn new(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        config.validate()?;
        let root = root.into();
        crate::io::create_dir_all_durable(&root)?;
        // Whatever a delete left before a crash or restart.
        incarnation::purge_in_background(root.clone());
        Ok(Self {
            root,
            config,
            open_logs: ShardSlots::new(),
            incarnations: Incarnations::default(),
            dirs: Default::default(),
            stream_retention: parking_lot::RwLock::new(HashMap::new()),
            stream_hold: parking_lot::RwLock::new(HashMap::new()),
            floors: parking_lot::Mutex::new(None),
            #[cfg(test)]
            open_hook: parking_lot::Mutex::new(None),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &LogConfig {
        &self.config
    }

    /// Open or return the cached log for `shard`.
    ///
    /// Fails with [`StorageError::Closed`] while [`Self::close_shard`] is
    /// closing it.
    pub fn open_shard(&self, shard: &ShardKey) -> Result<DiskLog> {
        self.open_with(shard, None, None)
    }

    /// The log for `shard` if it is open already. Unlike [`Self::open_shard`]
    /// this never creates or recovers anything, so a read-only caller can
    /// look without leaving a directory behind.
    pub fn opened_shard(&self, shard: &ShardKey) -> Option<DiskLog> {
        let incarnation = self.incarnation_of(shard)?;
        self.open_logs.get_open(&(shard.clone(), incarnation))
    }

    /// Open or return the cached log for `shard`, creating it to begin at
    /// `base_offset` if it does not exist yet.
    ///
    /// For a replica being given a shard whose early history is already gone.
    /// An existing shard keeps its own base, so this is safe to call on every
    /// contact rather than only the first.
    pub fn open_shard_at(&self, shard: &ShardKey, base_offset: Offset) -> Result<DiskLog> {
        self.open_with(shard, Some(base_offset), None)
    }

    /// [`Self::open_shard`], or [`Self::open_shard_at`] with a base, only when
    /// `incarnation` is the one set for the stream: refused with
    /// [`StorageError::Closed`] otherwise, before anything is created.
    ///
    /// For a caller acting for a named incarnation (a replication request, an
    /// assignment), so a delete and create landing after its own check cannot
    /// hand it the new stream's log.
    pub fn open_incarnation(
        &self,
        shard: &ShardKey,
        base_offset: Option<Offset>,
        incarnation: u64,
    ) -> Result<DiskLog> {
        self.open_with(shard, base_offset, Some(incarnation))
    }

    /// Close `shard`'s log and forget it, for a shard this broker no longer
    /// holds. Everything accepted is flushed first.
    ///
    /// Handles already given out fail with [`StorageError::Closed`] from here
    /// on. The next open recovers the shard afresh from disk. A no-op for a
    /// shard that is not open.
    pub async fn close_shard(&self, shard: &ShardKey) -> Result<()> {
        for ((key, incarnation), _) in self.open_logs.open_entries() {
            if &key == shard {
                self.open_logs
                    .close(&(key, incarnation), |log: DiskLog| async move {
                        log.close().await
                    })
                    .await?;
            }
        }
        Ok(())
    }

    /// Open `tenant/namespace/stream` at `incarnation` from here on, or open
    /// nothing for it with `None`, for a stream being deleted.
    ///
    /// Logs already open at another incarnation stay open until
    /// [`Self::retire_stream`] closes them.
    pub fn set_stream_incarnation(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        incarnation: Option<u64>,
    ) {
        match incarnation {
            Some(incarnation) => self
                .incarnations
                .set(tenant, namespace, stream, incarnation),
            None => self.incarnations.retire(tenant, namespace, stream),
        }
    }

    /// The incarnation set for a stream with [`Self::set_stream_incarnation`],
    /// or `None` when none is set or it was retired.
    pub fn stream_incarnation(&self, tenant: &str, namespace: &str, stream: &str) -> Option<u64> {
        self.incarnations.registered(tenant, namespace, stream)
    }

    /// Open no stream's logs until [`Self::set_stream_incarnation`] names its
    /// incarnation: for a broker that learns its streams from the control
    /// plane, where a log opened at 0 before that would be the wrong one.
    pub fn require_registration(&self) {
        self.incarnations.require_registration();
    }

    /// Close every log of one incarnation of a stream and move all its shard
    /// directories out of service. Their records are gone once this returns:
    /// nothing opens them again, and they are deleted in the background. Safe
    /// to repeat.
    pub async fn retire_stream(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        incarnation: u64,
    ) -> Result<()> {
        for ((key, held), _) in self.open_logs.open_entries() {
            if held != incarnation
                || key.tenant != tenant
                || key.namespace != namespace
                || key.stream != stream
            {
                continue;
            }
            self.open_logs
                .close(
                    &(key, held),
                    |log: DiskLog| async move { log.close().await },
                )
                .await?;
        }
        self.dirs
            .retire_stream_off_runtime(&self.root, tenant, namespace, stream, incarnation)
            .await
    }

    /// This broker's marker for a stream, if it has one. See
    /// [`crate::StreamMarker`].
    pub fn stream_marker(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<crate::MarkerRead> {
        incarnation::marker::read(&self.root, tenant, namespace, stream)
    }

    /// The incarnations of a stream that have a shard directory here.
    pub fn stream_incarnations_on_disk(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<std::collections::BTreeSet<u64>> {
        Ok(self
            .dirs
            .stream_dirs(&self.root, tenant, namespace, stream)?
            .into_iter()
            .map(|(_, incarnation, _)| incarnation)
            .collect())
    }

    /// Replace a stream's marker, durably.
    pub fn write_stream_marker(&self, marker: &crate::StreamMarker) -> Result<()> {
        incarnation::marker::write(&self.root, marker)
    }

    /// The deleted floor recorded for a stream with
    /// [`Self::record_stream_floors`]: every incarnation up to it is deleted.
    /// Kept apart from the stream's marker, which may hold its own.
    pub fn stream_floor(&self, tenant: &str, namespace: &str, stream: &str) -> Result<Option<u64>> {
        let key = (
            tenant.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        Ok(self.floors()?.get(&key).copied())
    }

    /// Record deleted floors for many streams with one flush, for catalog
    /// tombstones of streams with nothing here to delete. A floor never goes
    /// down.
    pub fn record_stream_floors(&self, floors: &[(&str, &str, &str, u64)]) -> Result<()> {
        let mut held = self.floors.lock();
        let mut next = match held.as_ref() {
            Some(floors) => floors.clone(),
            None => incarnation::marker::read_floors(&self.root)?,
        };
        for &(tenant, namespace, stream, deleted) in floors {
            let floor = next
                .entry((
                    tenant.to_string(),
                    namespace.to_string(),
                    stream.to_string(),
                ))
                .or_insert(deleted);
            *floor = (*floor).max(deleted);
        }
        incarnation::marker::write_floors(&self.root, &next)?;
        *held = Some(next);
        Ok(())
    }

    /// Forget a stream's recorded floor, as a restore to a backup point does.
    pub fn clear_stream_floor(&self, tenant: &str, namespace: &str, stream: &str) -> Result<()> {
        let mut held = self.floors.lock();
        let mut next = match held.as_ref() {
            Some(floors) => floors.clone(),
            None => incarnation::marker::read_floors(&self.root)?,
        };
        let key = (
            tenant.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        if next.remove(&key).is_some() {
            incarnation::marker::write_floors(&self.root, &next)?;
        }
        *held = Some(next);
        Ok(())
    }

    fn floors(&self) -> Result<parking_lot::MappedMutexGuard<'_, incarnation::marker::Floors>> {
        let mut held = self.floors.lock();
        if held.is_none() {
            *held = Some(incarnation::marker::read_floors(&self.root)?);
        }
        Ok(parking_lot::MutexGuard::map(held, |floors| {
            floors.get_or_insert_with(Default::default)
        }))
    }

    /// Remove a stream's marker, durably.
    pub fn remove_stream_marker(&self, tenant: &str, namespace: &str, stream: &str) -> Result<()> {
        incarnation::marker::remove(&self.root, tenant, namespace, stream)
    }

    /// Whether this broker has seen a stream with an incarnation: recorded by
    /// [`Self::record_incarnations_in_play`], or shown by a marker naming one.
    pub fn incarnations_in_play(&self) -> Result<bool> {
        incarnation::marker::in_play(&self.root)
    }

    /// Record, durably, that streams here carry incarnations, so a restart
    /// knows before the catalog says so.
    pub fn record_incarnations_in_play(&self) -> Result<()> {
        incarnation::marker::record_in_play(&self.root)
    }

    /// Every stream whose delete started and did not finish.
    pub fn retiring_streams(&self) -> Result<Vec<crate::StreamMarker>> {
        incarnation::marker::retiring(&self.root)
    }

    /// Every stream marker that reads back.
    pub fn stream_markers(&self) -> Result<Vec<crate::StreamMarker>> {
        incarnation::marker::all(&self.root)
    }

    /// Close every open log of one incarnation of a stream, keeping its
    /// directories: for an incarnation this broker stops serving without
    /// evidence that it was deleted.
    pub async fn close_incarnation(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        incarnation: u64,
    ) -> Result<()> {
        for ((key, held), _) in self.open_logs.open_entries() {
            if held != incarnation
                || key.tenant != tenant
                || key.namespace != namespace
                || key.stream != stream
            {
                continue;
            }
            self.open_logs
                .close(
                    &(key, held),
                    |log: DiskLog| async move { log.close().await },
                )
                .await?;
        }
        Ok(())
    }

    /// Bound one stream's logs by `retention`, falling back to this
    /// provider's configuration for a bound it leaves unset. Applies to the
    /// stream's shards already open and to every one opened later.
    pub fn set_stream_retention(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        retention: Retention,
    ) -> Result<()> {
        let effective = retention.or(self.config.retention());
        self.config.check_retention(effective)?;
        let scope = (
            tenant.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        self.stream_retention.write().insert(scope, retention);
        for ((key, _), log) in self.open_logs.open_entries() {
            if key.tenant != tenant || key.namespace != namespace || key.stream != stream {
                continue;
            }
            match log.set_retention(effective) {
                // Closing because the shard moved away; the next open reads
                // the map.
                Err(StorageError::Closed(_)) => {}
                other => other?,
            }
        }
        Ok(())
    }

    /// Hold one stream's logs at the commit offset, or lift the hold: for a
    /// stream whose consistency became `Quorum`, or stopped being it. Applies
    /// to the stream's shards already open, written through before this
    /// returns, and to every one opened later, before its retention first
    /// runs. Ignored when this provider's configuration lifts every hold.
    pub async fn set_stream_retention_hold(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        hold: bool,
    ) -> Result<()> {
        if self.config.retention_hold == RetentionHold::Lifted {
            return Ok(());
        }
        let scope = (
            tenant.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        self.stream_hold.write().insert(scope, hold);
        for ((key, _), log) in self.open_logs.open_entries() {
            if key.tenant != tenant || key.namespace != namespace || key.stream != stream {
                continue;
            }
            match log.hold_retention_at_commit(hold).await {
                Err(StorageError::Closed(_)) => {}
                other => other?,
            }
        }
        Ok(())
    }

    /// Shard keys this provider currently has open.
    pub fn open_shards(&self) -> Vec<ShardKey> {
        self.open_logs
            .open_entries()
            .into_iter()
            .map(|((key, _), _)| key)
            .collect()
    }

    /// Flush and stop every open log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        let logs = self.open_logs.open_values();
        let mut first_error = None;
        for log in logs {
            if let Err(err) = log.shutdown().await {
                tracing::error!(shard = %log.label(), error = %err, "failed to flush log on shutdown");
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_open_hook(&self, hook: impl Fn(&ShardKey) + Send + Sync + 'static) {
        *self.open_hook.lock() = Some(OpenHook(std::sync::Arc::new(hook)));
    }

    fn incarnation_of(&self, shard: &ShardKey) -> Option<u64> {
        self.incarnations
            .resolve(&shard.tenant, &shard.namespace, &shard.stream)
    }

    fn open_with(
        &self,
        shard: &ShardKey,
        base_offset: Option<Offset>,
        expected: Option<u64>,
    ) -> Result<DiskLog> {
        let incarnation = self
            .incarnation_of(shard)
            .filter(|incarnation| expected.is_none_or(|expected| expected == *incarnation))
            .ok_or_else(|| StorageError::Closed(layout::shard_label(shard)))?;
        self.open_logs.get_or_open(
            &(shard.clone(), incarnation),
            || {
                #[cfg(test)]
                {
                    let hook = self.open_hook.lock().clone();
                    if let Some(hook) = hook {
                        (hook.0)(shard);
                    }
                }
                // Again under the slot's lock: a delete that retired the stream
                // after the lookup above closes only the slots it can see, and
                // this one must not open behind it.
                if self.incarnation_of(shard) != Some(incarnation) {
                    return Err(StorageError::Closed(layout::shard_label(shard)));
                }
                let dir = layout::incarnation_dir(&self.root, shard, incarnation);
                let label = layout::shard_label(shard);
                let config = self.config_for(shard);
                self.dirs
                    .created(&layout::incarnation_dir_name(shard, incarnation));
                match base_offset {
                    Some(base) => DiskLog::open_at(dir, label, config, base),
                    None => DiskLog::open(dir, label, config),
                }
            },
            || StorageError::Closed(layout::shard_label(shard)),
        )
    }
}

impl DiskLogProvider {
    /// The configuration `shard` opens with: this provider's, with its
    /// stream's retention and retention hold.
    fn config_for(&self, shard: &ShardKey) -> LogConfig {
        let scope = (
            shard.tenant.clone(),
            shard.namespace.clone(),
            shard.stream.clone(),
        );
        let mut config = self.config.clone();
        if let Some(retention) = self.stream_retention.read().get(&scope) {
            let effective = retention.or(self.config.retention());
            config.retention_bytes = effective.bytes;
            config.retention_age = effective.age;
        }
        if self.config.retention_hold != RetentionHold::Lifted
            && let Some(hold) = self.stream_hold.read().get(&scope)
        {
            config.retention_hold = if *hold {
                RetentionHold::Held
            } else {
                RetentionHold::Lifted
            };
        }
        config
    }
}

impl LogProvider for DiskLogProvider {
    type Log = DiskLog;

    fn open(&self, shard: &ShardKey) -> BoxFuture<'_, Result<Self::Log>> {
        let shard = shard.clone();
        Box::pin(async move { self.open_shard(&shard) })
    }
}

#[cfg(test)]
mod tests;
