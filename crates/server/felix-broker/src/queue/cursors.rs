//! Durable consumer-group cursors, as a projection over the log.
//!
//! A group's position is a key → latest-value mapping: the group's name to the
//! offset it will resume from. That is precisely the projection the cache
//! already is, so this is a [`LogCache`] on its own root rather than a second
//! implementation of durability, recovery, and compaction — which is what "one
//! core log, many semantics" is supposed to mean in practice.
//!
//! Its own root because a group's cursors are not user data: a cache named
//! `orders` and the group state for a stream named `orders` must not share a
//! directory. See `docs/cache-on-log.md` for the same argument about caches and
//! streams.
//!
//! A commit records the dead-letter log it needs beneath it ([`record`]). A
//! cursor whose dead letters are gone from this broker's log, after a crash
//! that kept one file's tail and not the other's or a replica's log taken
//! over by another's, no longer stands: [`ConsumerGroups::normalize`] puts the
//! group back where something still does before the shard is served.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::LogCache;
use felix_storage::log::LogConfig;
use parking_lot::{Mutex as SyncMutex, RwLock};

pub(crate) use self::record::DeadLetterMark;
use self::record::Stored;
use crate::durable::StreamLog;
use crate::error::{BrokerError, Result};

mod record;

/// Durable positions for every consumer group on every shard this broker leads.
#[derive(Debug)]
pub struct ConsumerGroups {
    cursors: LogCache,
    /// One lock per shard, held across the read-then-write of a commit.
    ///
    /// A commit has to be atomic to stay monotonic: two acknowledgements racing
    /// could both read the old position, and the one that lands second would
    /// move the group *backwards* — redelivering everything between. Per shard
    /// rather than global because a shard is already the unit one broker owns,
    /// so this adds no contention that ownership did not.
    locks: SyncMutex<HashMap<ShardKey, Arc<tokio::sync::Mutex<()>>>>,
    /// Whether to write each commit's dependency, which a build before it
    /// cannot read. On unless the fleet says otherwise.
    dependencies: Dependencies,
}

/// When cursor values carry their dependency.
struct Dependencies(RwLock<Arc<dyn Fn() -> bool + Send + Sync>>);

impl std::fmt::Debug for Dependencies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Dependencies")
    }
}

/// The dead-letter log a shard's cursors are checked against.
#[derive(Debug, Clone)]
pub(crate) struct DeadLetterLog {
    pub(crate) end: u64,
    pub(crate) epochs: Vec<felix_storage::log::Epoch>,
}

/// Where one group stands on one shard.
type ShardKey = (String, String, String, u32);

impl ConsumerGroups {
    /// Open group state rooted at `root`, recovering whatever is on disk.
    pub fn open(root: impl Into<std::path::PathBuf>, config: LogConfig) -> Result<Self> {
        Ok(Self {
            cursors: LogCache::open(root, config).map_err(BrokerError::from)?,
            locks: SyncMutex::new(HashMap::new()),
            dependencies: Dependencies(RwLock::new(Arc::new(|| true))),
        })
    }

    /// Write each commit's dependency on the dead-letter log only while
    /// `enabled` says so. A build that predates it refuses such a value, so a
    /// cluster waits for its fleet; until then a cursor carries only its
    /// offset and is taken to need nothing.
    pub fn record_dependencies_when(&self, enabled: impl Fn() -> bool + Send + Sync + 'static) {
        *self.dependencies.0.write() = Arc::new(enabled);
    }

    fn dependencies(&self) -> bool {
        (self.dependencies.0.read())()
    }

    /// Where `group` will resume on this shard.
    ///
    /// `None` means the group has never committed, and the caller decides where
    /// a new group starts — this does not invent a position for it.
    pub async fn committed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<Option<u64>> {
        // Settled, so a commit cancelled while its write was in flight is seen
        // here rather than overwritten by the next one.
        Ok(self
            .stored(tenant_id, namespace, stream, shard, group)
            .await?
            .map(|stored| stored.offset))
    }

    pub(crate) async fn stored(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<Option<Stored>> {
        let stored = self
            .cursors
            .get_settled_checked(tenant_id, namespace, stream, shard, group)
            .await
            .map_err(BrokerError::from)?;
        stored.as_deref().map(Stored::decode).transpose()
    }

    /// Record that `group` has processed everything below `offset`.
    ///
    /// Returns the position the group now holds, which is not always `offset`:
    /// a commit that would move it backwards is ignored and the current one
    /// returned. A late acknowledgement from a consumer that has already been
    /// superseded must not rewind the group and redeliver what the rest of it
    /// has finished.
    pub async fn commit(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<u64> {
        let live = AtomicBool::new(false);
        self.commit_unless(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            &live,
            DeadLetterMark::default(),
        )
        .await
    }

    /// [`Self::commit`], skipped when `retired` is set by the time the shard's
    /// lock is held. A seek sets it under the same lock, so a commit from the
    /// tracker it replaced cannot land after it and move the new position.
    ///
    /// `requires` is the dead-letter log the commit passes records on, synced
    /// by the caller first.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn commit_unless(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
        retired: &AtomicBool,
        requires: DeadLetterMark,
    ) -> Result<u64> {
        let lock = self.lock_for(tenant_id, namespace, stream, shard);
        let _guard = lock.lock().await;

        let current = self
            .stored(tenant_id, namespace, stream, shard, group)
            .await?;
        if retired.load(Ordering::Acquire) {
            return Ok(current.map_or(0, |current| current.offset));
        }
        if let Some(current) = current
            && current.offset >= offset
        {
            return Ok(current.offset);
        }
        let origin = match current {
            _ if requires.end == 0 => offset,
            Some(current) => current.origin,
            None => 0,
        };
        let stored = Stored {
            offset,
            requires,
            origin,
        };
        self.write(tenant_id, namespace, stream, shard, group, stored)
            .await?;
        Ok(offset)
    }

    /// Hold the shard's commit lock. While it is held no commit lands, so a
    /// caller can read, move or forget a position without one slipping in.
    pub(crate) async fn lock_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.lock_for(tenant_id, namespace, stream, shard)
            .lock_owned()
            .await
    }

    /// Put `group` at `offset`, backwards or forwards, or forget it with
    /// `None`. The caller holds [`Self::lock_shard`].
    pub(crate) async fn set_locked(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: Option<u64>,
    ) -> Result<()> {
        match offset {
            Some(offset) => {
                self.write(
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    Stored::placed(offset),
                )
                .await
            }
            None => self
                .cursors
                .delete_checked(tenant_id, namespace, stream, shard, group)
                .await
                .map(drop)
                .map_err(BrokerError::from),
        }
    }

    /// Put each group on this shard whose latest record no longer stands on
    /// `dead_letters` back where one does: its latest record that still
    /// stands, or else its origin. Returns how many moved.
    ///
    /// A group is never left without a position. One with none is created
    /// afresh by a seek `if_new`, at the tail if that is what it asks, past
    /// records it never finished. The moves are synced before this returns,
    /// so a crash cannot bring back a record they replaced.
    pub(crate) async fn normalize(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        dead_letters: &DeadLetterLog,
    ) -> Result<usize> {
        let _guard = self.lock_shard(tenant_id, namespace, stream, shard).await;
        let entries = self
            .cursors
            .live_entries_checked(tenant_id, namespace, stream, shard)
            .await
            .map_err(BrokerError::from)?;
        let stands = |stored: &Stored| {
            stored
                .requires
                .held_by(dead_letters.end, &dead_letters.epochs)
        };
        let mut history = None;
        let mut moved = 0;
        for entry in entries {
            let latest = Stored::decode(&entry.value)?;
            if stands(&latest) {
                continue;
            }
            if history.is_none() {
                history = Some(self.history(tenant_id, namespace, stream, shard).await?);
            }
            let standing = history
                .as_ref()
                .and_then(|history| history.get(&entry.key))
                .and_then(|records| records.iter().rev().find(|stored| stands(stored)).copied())
                .unwrap_or(Stored::placed(latest.origin));
            self.write(tenant_id, namespace, stream, shard, &entry.key, standing)
                .await?;
            moved += 1;
        }
        if moved > 0 {
            self.sync(tenant_id, namespace, stream, shard).await?;
        }
        Ok(moved)
    }

    /// Take another replica's cursor records, `records` being its cursor log
    /// in order: each group goes to the further of its position here and its
    /// latest there. Appended, never cut, so no group this broker had is
    /// erased and a crash part way leaves each group at one or the other. A
    /// group deleted there keeps its position here. Returns how many moved.
    ///
    /// For a promoted leader taking a replica's group state. What it takes
    /// is checked against the dead-letter log like any cursor before the
    /// shard is served.
    pub async fn adopt(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        records: &[bytes::Bytes],
    ) -> Result<usize> {
        let mut latest: HashMap<String, Option<Stored>> = HashMap::new();
        for record in records {
            match felix_storage::cache::CacheOp::decode(record) {
                Ok(felix_storage::cache::CacheOp::Put { key, value, .. }) => {
                    if let Ok(stored) = Stored::decode(&value) {
                        latest.insert(key, Some(stored));
                    }
                }
                Ok(felix_storage::cache::CacheOp::Delete { key }) => {
                    latest.insert(key, None);
                }
                Err(_) => {}
            }
        }
        let _guard = self.lock_shard(tenant_id, namespace, stream, shard).await;
        let mut moved = 0;
        for (group, theirs) in latest {
            let Some(theirs) = theirs else {
                continue;
            };
            let ours = self
                .stored(tenant_id, namespace, stream, shard, &group)
                .await?;
            if ours.is_some_and(|ours| ours.offset >= theirs.offset) {
                continue;
            }
            self.write(tenant_id, namespace, stream, shard, &group, theirs)
                .await?;
            moved += 1;
        }
        if moved > 0 {
            self.sync(tenant_id, namespace, stream, shard).await?;
        }
        Ok(moved)
    }

    /// Each group's records in this shard's log, oldest first, from its last
    /// delete on.
    async fn history(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<HashMap<String, Vec<Stored>>> {
        let log = StreamLog::from_log(self.shard_log(tenant_id, namespace, stream, shard).await?);
        let tail = log.tail_offset().await?;
        let mut next = log.base_offset();
        let mut history: HashMap<String, Vec<Stored>> = HashMap::new();
        while next < tail {
            let records = log.read_log_from(next, 1 << 20).await?;
            let Some(last) = records.last() else {
                break;
            };
            next = last.offset + 1;
            for record in records
                .iter()
                .filter(|record| record.mark == felix_storage::log::RecordMark::None)
            {
                match felix_storage::cache::CacheOp::decode(&record.payload) {
                    Ok(felix_storage::cache::CacheOp::Put { key, value, .. }) => {
                        if let Ok(stored) = Stored::decode(&value) {
                            history.entry(key).or_default().push(stored);
                        }
                    }
                    Ok(felix_storage::cache::CacheOp::Delete { key }) => {
                        history.insert(key, Vec::new());
                    }
                    Err(_) => {}
                }
            }
        }
        Ok(history)
    }

    async fn sync(&self, tenant_id: &str, namespace: &str, stream: &str, shard: u32) -> Result<()> {
        StreamLog::from_log(self.shard_log(tenant_id, namespace, stream, shard).await?)
            .sync()
            .await
    }

    async fn write(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        stored: Stored,
    ) -> Result<()> {
        self.cursors
            .put_checked(
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                stored.encode(self.dependencies()),
                // No expiry. A group's position is not something that should
                // quietly disappear because nobody consumed for a while --
                // forgetting it silently restarts the group from wherever the
                // caller's default is, which is either a replay or a data loss.
                None,
            )
            .await
            .map_err(BrokerError::from)
    }

    /// Forget a group's position on this shard.
    ///
    /// Returns whether it had one. For a group being torn down; a group that is
    /// merely idle keeps its position.
    pub async fn forget(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<bool> {
        let lock = self.lock_for(tenant_id, namespace, stream, shard);
        let _guard = lock.lock().await;
        let removed = self
            .cursors
            .delete_checked(tenant_id, namespace, stream, shard, group)
            .await
            .map_err(BrokerError::from)?;
        Ok(removed.is_some())
    }

    /// The log a shard's cursors are written to.
    ///
    /// For replication: the cursors have to reach a replica alongside the
    /// records they describe.
    pub async fn shard_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<felix_storage::disk_log::DiskLog> {
        self.cursors
            .shard_log(tenant_id, namespace, stream, shard)
            .await
            .map_err(BrokerError::from)
    }

    /// [`ConsumerGroups::shard_log`], created at `base_offset` if absent.
    pub async fn shard_log_at(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<felix_storage::disk_log::DiskLog> {
        self.cursors
            .shard_log_at(tenant_id, namespace, stream, shard, base_offset)
            .await
            .map_err(BrokerError::from)
    }

    /// Forget what was read from a shard's log, for replication after it cut
    /// records from it. The next read replays the log.
    pub(crate) async fn forget_index(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<()> {
        self.cursors
            .forget_index(tenant_id, namespace, stream, shard)
            .await
            .map_err(BrokerError::from)
    }

    /// Close a shard's cursor log, for a shard this broker no longer holds.
    /// The next read or commit opens it afresh.
    pub async fn close_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<()> {
        self.cursors
            .close_shard(tenant_id, namespace, stream, shard)
            .await
            .map_err(BrokerError::from)
    }

    /// Flush every open cursor log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.cursors.shutdown().await.map_err(BrokerError::from)
    }

    fn lock_for(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            shard,
        );
        Arc::clone(self.locks.lock().entry(key).or_default())
    }
}

#[cfg(test)]
mod tests;
