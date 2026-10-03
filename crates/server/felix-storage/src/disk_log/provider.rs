//! One [`DiskLog`] per shard under a common root directory.

use std::path::{Path, PathBuf};

use std::collections::HashMap;

use super::{DiskLog, layout};
use crate::log::{BoxFuture, LogConfig, LogProvider, Offset, Retention, ShardKey};
use crate::shard_slots::ShardSlots;
use crate::{Result, StorageError};

/// Opens one [`DiskLog`] per shard under a common root directory.
///
/// Repeated opens of the same shard return the same log. Two independent
/// writers over one directory would interleave offsets and corrupt the segment,
/// so the cache is a correctness requirement, not an optimisation. Opens of
/// different shards run in parallel; see `crate::shard_slots`.
#[derive(Debug)]
pub struct DiskLogProvider {
    root: PathBuf,
    config: LogConfig,
    open_logs: ShardSlots<ShardKey, DiskLog>,
    /// Retention set for one stream, keyed by tenant, namespace and stream.
    /// A bound a stream leaves unset comes from `config`.
    stream_retention: parking_lot::RwLock<HashMap<(String, String, String), Retention>>,
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
        Ok(Self {
            root,
            config,
            open_logs: ShardSlots::new(),
            stream_retention: parking_lot::RwLock::new(HashMap::new()),
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
        self.open_with(shard, None)
    }

    /// Open or return the cached log for `shard`, creating it to begin at
    /// `base_offset` if it does not exist yet.
    ///
    /// For a replica being given a shard whose early history is already gone.
    /// An existing shard keeps its own base, so this is safe to call on every
    /// contact rather than only the first.
    pub fn open_shard_at(&self, shard: &ShardKey, base_offset: Offset) -> Result<DiskLog> {
        self.open_with(shard, Some(base_offset))
    }

    /// Close `shard`'s log and forget it, for a shard this broker no longer
    /// holds. Everything accepted is flushed first.
    ///
    /// Handles already given out fail with [`StorageError::Closed`] from here
    /// on. The next open recovers the shard afresh from disk. A no-op for a
    /// shard that is not open.
    pub async fn close_shard(&self, shard: &ShardKey) -> Result<()> {
        self.open_logs
            .close(shard, |log: DiskLog| async move { log.close().await })
            .await
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
        for (key, log) in self.open_logs.open_entries() {
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

    /// Shard keys this provider currently has open.
    pub fn open_shards(&self) -> Vec<ShardKey> {
        self.open_logs
            .open_entries()
            .into_iter()
            .map(|(key, _)| key)
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

    fn open_with(&self, shard: &ShardKey, base_offset: Option<Offset>) -> Result<DiskLog> {
        self.open_logs.get_or_open(
            shard,
            || {
                #[cfg(test)]
                {
                    let hook = self.open_hook.lock().clone();
                    if let Some(hook) = hook {
                        (hook.0)(shard);
                    }
                }
                let dir = layout::shard_dir(&self.root, shard);
                let label = layout::shard_label(shard);
                let config = self.config_for(shard);
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
    /// stream's retention.
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
