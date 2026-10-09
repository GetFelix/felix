//! A cache that is a log.
//!
//! `put` and `delete` append a record; an in-memory index maps each key to the
//! offset of the record that currently defines it; `get` reads the log at that
//! offset. See `docs/cache-on-log.md` for the design and the reasoning behind
//! each part of it.
//!
//! This is what makes "one core log, many semantics" true of the cache rather
//! than aspirational: crash safety, group commit, the fsync policy, and
//! eventually replication are all inherited from the log rather than
//! reimplemented beside it.

mod compaction;
mod record;
mod shard;
mod write;

pub use record::CacheOp;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex;

use self::shard::{CacheShard, Index, ShardState, now_millis};
use self::write::{FinishOnDrop, Observer, StagedWrite};
use crate::cache::{
    CacheChange, CacheCondition, CacheObserver, CacheSnapshotEntry, ConditionalWrite, StorageApi,
    VersionedValue,
};
use crate::commit_order::CommitSequencer;
use crate::compaction::Compactor;
use crate::disk_log::{DiskLog, layout};
use crate::log::{AppendOnlyLog, AppendRecord, LogConfig, ShardKey};
use crate::shard_slots::ShardSlots;
use crate::{Result, StorageError};

/// A cache backed by the same log streams are.
#[derive(Debug)]
pub struct LogCache {
    root: PathBuf,
    config: LogConfig,
    shards: ShardSlots<CacheId, Arc<CacheShard>>,
    compactor: Arc<Compactor>,
    /// Told about every applied write, while the shard's write lock is held —
    /// which is what makes the order it sees the shard's order.
    observer: Observer,
}

impl LogCache {
    /// Open the cache store rooted at `root`.
    ///
    /// Callers pass the *cache* root, which is deliberately not the stream root:
    /// a shard directory is named from a hash of its tenant, namespace and
    /// stream, so a cache and a stream sharing a name would otherwise interleave
    /// their records in one directory.
    pub fn open(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        let root = root.into();
        crate::io::create_dir_all_durable(&root).map_err(StorageError::Io)?;
        Ok(Self {
            root,
            config,
            shards: ShardSlots::new(),
            compactor: Arc::new(Compactor::from_env()),
            observer: Arc::new(SyncMutex::new(None)),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The write behind [`StorageApi::put`].
    ///
    /// The write has two halves, and the state lock spans neither fsync nor
    /// wait. Stage under a short lock (claim an offset and a turn), commit
    /// outside it (the fsync, group-committed with every concurrent writer),
    /// then wait the turn out and apply under the lock again. Returning only
    /// after `commit` keeps the ack durability-gated; applying only after
    /// `wait` keeps index and watch order equal to disk order.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<()> {
        self.put_entry(tenant_id, namespace, cache, shard, key, value, ttl, None)
            .await
            .map(|_| ())
    }

    /// The write behind [`StorageApi::put_if`]: [`LogCache::put_checked`],
    /// with the condition checked where the offset is claimed.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_if_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<std::time::Duration>,
        condition: CacheCondition,
    ) -> Result<ConditionalWrite> {
        self.put_entry(
            tenant_id,
            namespace,
            cache,
            shard,
            key,
            value,
            ttl,
            Some(condition),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_entry(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<std::time::Duration>,
        condition: Option<CacheCondition>,
    ) -> Result<ConditionalWrite> {
        let shard_index = shard;
        let shard = self.shard(tenant_id, namespace, cache, shard_index)?;
        let expires_at_millis = ttl.map_or(0, |ttl| now_millis() + ttl.as_millis() as u64);
        let op = CacheOp::Put {
            key: key.to_string(),
            value: value.clone(),
            expires_at_millis,
            version: None,
        };

        let observer = Arc::clone(&self.observer);
        let change = CacheChange {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            shard: shard_index,
            key: key.to_string(),
            value: Some(value),
            offset: 0,
            expires_at_millis,
        };
        // Staged on a task of its own: once the append thread has the record
        // it is written, and the guard built from it is what applies it and
        // tells the watchers. A caller cancelled in between would leave the
        // put in the log and nowhere else.
        let staged = crate::task::run_to_end({
            let shard = Arc::clone(&shard);
            async move {
                let mut state = match condition {
                    Some(_) => shard.lock_settled(&change.key).await?,
                    None => {
                        let mut state = shard.state.lock().await;
                        shard.ensure_index(&mut state).await?;
                        state
                    }
                };
                if let Some(condition) = condition {
                    let current = live_version(&state, &change.key);
                    let holds = match condition {
                        CacheCondition::Absent => current.is_none(),
                        CacheCondition::Version(version) => current == Some(version),
                    };
                    if !holds {
                        return Ok(Err(current));
                    }
                }
                let payload = op.encode();
                let bytes = payload.len() as u64;
                // Claimed by the log the moment the offsets are consumed, so a
                // failed commit cannot strand the writers queued behind it.
                let (pending, turn) = state
                    .log
                    .append_claimed(
                        &[AppendRecord {
                            payload,
                            timestamp_micros: now_millis() * 1000,
                            mark: Default::default(),
                            publisher: None,
                        }],
                        &shard.sequencer,
                    )
                    .await?;
                state.sequenced_through = Some(pending.last_offset() + 1);
                let in_flight = shard.key_in_flight(&change.key);
                Ok(Ok(FinishOnDrop::new(StagedWrite {
                    shard: Arc::clone(&shard),
                    log: state.log.clone(),
                    change: CacheChange {
                        offset: pending.first_offset(),
                        ..change
                    },
                    pending,
                    turn,
                    op,
                    bytes,
                    observer,
                    _in_flight: in_flight,
                })))
            }
        })
        .await?;
        let mut staged = match staged {
            Ok(staged) => staged,
            Err(current) => {
                return Ok(ConditionalWrite {
                    applied: false,
                    version: current,
                });
            }
        };

        staged.commit().await?;
        let mut state = shard.state.lock().await;
        let version = staged.offset();
        staged.apply(&mut state);
        shard.maybe_compact(&state.index);
        Ok(ConditionalWrite {
            applied: true,
            version: Some(version),
        })
    }

    /// The read behind [`StorageApi::get`].
    pub async fn get_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let Some(entry) = state.index.entries.get(key).copied() else {
            return Ok(None);
        };
        if entry.is_expired(now_millis()) {
            // Lazy expiry, as the in-memory cache always did: reported absent
            // now, and the space reclaimed when the log is next compacted.
            return Ok(None);
        }
        shard.read_value(&state, entry).await
    }

    /// [`LogCache::get_checked`], once every write to `key` already staged
    /// has applied.
    ///
    /// For a read-modify-write under a caller's own lock. A put whose caller
    /// was cancelled still finishes on its own task, after that lock is gone,
    /// so a plain read can miss it and the write that follows lands on top.
    pub async fn get_settled_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let state = shard.lock_settled(key).await?;
        let Some(entry) = state.index.entries.get(key).copied() else {
            return Ok(None);
        };
        if entry.is_expired(now_millis()) {
            return Ok(None);
        }
        shard.read_value(&state, entry).await
    }

    /// The read behind [`StorageApi::get_versioned`].
    pub async fn get_versioned_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<VersionedValue>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let Some(entry) = state.index.entries.get(key).copied() else {
            return Ok(None);
        };
        if entry.is_expired(now_millis()) {
            return Ok(None);
        }
        Ok(shard
            .read_value(&state, entry)
            .await?
            .map(|value| VersionedValue {
                value,
                version: entry.version,
            }))
    }

    /// The delete behind [`StorageApi::delete_if`].
    pub async fn delete_if_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        version: u64,
    ) -> Result<ConditionalWrite> {
        let deleted = self
            .delete_entry(
                tenant_id,
                namespace,
                cache,
                shard,
                key,
                DeleteWhen::Version(version),
            )
            .await?;
        Ok(ConditionalWrite {
            applied: deleted.written,
            version: deleted.current,
        })
    }

    /// The delete behind [`StorageApi::delete`].
    ///
    /// Same two-half shape as [`LogCache::put_checked`]. The previous value is
    /// read at staging time; against concurrent writers to the same key, the
    /// delete's place in the shard's history is its disk offset.
    pub async fn delete_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.delete_entry(tenant_id, namespace, cache, shard, key, DeleteWhen::Always)
            .await
            .map(|deleted| deleted.previous)
    }

    /// Write a delete for each key in one shard whose TTL has passed, at most
    /// `limit` of them, so a watch hears that it is gone and compaction can
    /// drop it. Returns how many were written.
    ///
    /// For the shard's leader only, like any write. Each key is checked again
    /// when its delete is staged, under the lock a put stages under, so a
    /// put that refreshed it in between keeps its value. `keep_going` is asked
    /// before each delete, and the pass stops the first time it says no.
    pub async fn expire_due(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        limit: usize,
        keep_going: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<usize> {
        let now = now_millis();
        let due: Vec<String> = {
            let handle = self.shard(tenant_id, namespace, cache, shard)?;
            let mut state = handle.state.lock().await;
            handle.ensure_index(&mut state).await?;
            state
                .index
                .entries
                .iter()
                .filter(|(_, entry)| entry.is_expired(now))
                .map(|(key, _)| key.clone())
                .take(limit)
                .collect()
        };
        let mut written = 0;
        for key in due {
            if !keep_going() {
                break;
            }
            let deleted = self
                .delete_entry(
                    tenant_id,
                    namespace,
                    cache,
                    shard,
                    &key,
                    DeleteWhen::Expired(now),
                )
                .await?;
            written += usize::from(deleted.written);
        }
        Ok(written)
    }

    /// Every shard this store has open, as tenant, namespace, cache, shard.
    pub fn open_shards(&self) -> Vec<(String, String, String, u32)> {
        self.shards
            .open_entries()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    /// Stage and apply a delete of `key`, or nothing when the key is absent
    /// or `when` does not hold.
    async fn delete_entry(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        when: DeleteWhen,
    ) -> Result<Deleted> {
        let shard_index = shard;
        let shard = self.shard(tenant_id, namespace, cache, shard_index)?;
        let op = CacheOp::Delete {
            key: key.to_string(),
        };

        let observer = Arc::clone(&self.observer);
        let change = CacheChange {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            shard: shard_index,
            key: key.to_string(),
            value: None,
            offset: 0,
            expires_at_millis: 0,
        };
        // On a task of its own, for the reason `put_checked` gives.
        let staged = crate::task::run_to_end({
            let shard = Arc::clone(&shard);
            async move {
                // An expiry is conditional too: a put staged but not yet
                // applied may have refreshed the key, and a delete staged
                // behind it would erase that.
                let mut state = match when {
                    DeleteWhen::Always => {
                        let mut state = shard.state.lock().await;
                        shard.ensure_index(&mut state).await?;
                        state
                    }
                    DeleteWhen::Expired(_) | DeleteWhen::Version(_) => {
                        shard.lock_settled(&change.key).await?
                    }
                };
                let Some(entry) = state.index.entries.get(&change.key).copied() else {
                    return Ok(Err(None));
                };
                let live = !entry.is_expired(now_millis());
                let holds = match when {
                    DeleteWhen::Always => true,
                    DeleteWhen::Expired(now) => entry.is_expired(now),
                    DeleteWhen::Version(version) => live && entry.version == version,
                };
                if !holds {
                    return Ok(Err(live.then_some(entry.version)));
                }
                let previous = if live {
                    shard.read_value(&state, entry).await?
                } else {
                    None
                };
                let payload = op.encode();
                let bytes = payload.len() as u64;
                let (pending, turn) = state
                    .log
                    .append_claimed(
                        &[AppendRecord {
                            payload,
                            timestamp_micros: now_millis() * 1000,
                            mark: Default::default(),
                            publisher: None,
                        }],
                        &shard.sequencer,
                    )
                    .await?;
                state.sequenced_through = Some(pending.last_offset() + 1);
                let in_flight = shard.key_in_flight(&change.key);
                let staged = FinishOnDrop::new(StagedWrite {
                    shard: Arc::clone(&shard),
                    log: state.log.clone(),
                    change: CacheChange {
                        offset: pending.first_offset(),
                        ..change
                    },
                    pending,
                    turn,
                    op,
                    bytes,
                    observer,
                    _in_flight: in_flight,
                });
                Ok(Ok((previous, staged)))
            }
        })
        .await?;
        let (previous, mut staged) = match staged {
            Ok(staged) => staged,
            Err(current) => {
                return Ok(Deleted {
                    written: false,
                    previous: None,
                    current,
                });
            }
        };

        staged.commit().await?;
        let mut state = shard.state.lock().await;
        staged.apply(&mut state);
        Ok(Deleted {
            written: true,
            previous,
            current: None,
        })
    }

    /// Every live key in one shard of one cache.
    ///
    /// Expired entries are excluded, for the same reason `len` excludes them: a
    /// key nothing can read is not a key. Order is unspecified — the index is a
    /// hash map — so a caller that needs one sorts.
    pub async fn keys(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Vec<String>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let now = now_millis();
        Ok(state
            .index
            .entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired(now))
            .map(|(key, _)| key.clone())
            .collect())
    }

    /// Every live key in one shard with its current value and offset.
    ///
    /// Expired entries are excluded, as they are from every other read. The
    /// offsets are what let a watcher join this snapshot to live delivery
    /// without doubling: a change at or past the snapshot's tail arrives live,
    /// and one below it is already in here.
    pub async fn live_entries_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Vec<CacheSnapshotEntry>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let now = now_millis();
        let mut entries = Vec::new();
        for (key, entry) in &state.index.entries {
            if entry.is_expired(now) {
                continue;
            }
            if let Some(value) = shard.read_value(&state, *entry).await? {
                entries.push(CacheSnapshotEntry {
                    key: key.clone(),
                    value,
                    offset: entry.offset,
                    expires_at_millis: entry.expires_at_millis,
                });
            }
        }
        Ok(entries)
    }

    /// The log backing one cache shard.
    ///
    /// For replication, which ships a shard's records to followers and needs
    /// the same log the cache writes to — a second log over the same directory
    /// would interleave offsets and corrupt the segment.
    ///
    /// Fetch it per pass rather than holding it: closing the shard replaces
    /// it. Compaction trims its head, so a reader far enough behind can find
    /// its next offset gone; the live set was re-appended at the tail first,
    /// so starting again from the new base loses nothing live.
    pub async fn shard_log(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<DiskLog> {
        Ok(self
            .shard(tenant, namespace, cache, shard)?
            .current_log()
            .await)
    }

    /// Like [`LogCache::shard_log`], but creates the shard's log beginning at
    /// `base_offset` when it is not there yet.
    ///
    /// For a follower being given a cache whose early history the leader has
    /// already compacted away: its log starts where the surviving records do.
    /// An existing shard keeps the base recorded in its own first segment.
    pub async fn shard_log_at(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<DiskLog> {
        Ok(self
            .shard_with_base(tenant, namespace, cache, shard, Some(base_offset))?
            .current_log()
            .await)
    }

    /// Close one cache shard's log and forget it, for a shard this broker no
    /// longer holds. Everything accepted is flushed first.
    ///
    /// A write or read racing the close fails with [`StorageError::Closed`],
    /// as does one through a log handle given out earlier. The next call that
    /// touches the shard recovers it afresh from disk. A no-op for a shard
    /// that is not open.
    pub async fn close_shard(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<()> {
        let id = (
            tenant.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        self.shards
            .close(&id, |shard: Arc<CacheShard>| async move {
                // Marked under the state lock, so a compaction pass sees it at
                // its next step and stops, and nothing queued reopens the log.
                let mut state = shard.state.lock().await;
                state.closed = true;
                state.log.close().await
            })
            .await
    }

    /// Drop one shard's index, so the next touch replays its log.
    ///
    /// For replication after it cut records from the log: the index can
    /// point at offsets that are gone, or that now hold other records, and
    /// catching up from the tail never notices. Only while nothing writes to
    /// the shard, since the commit order restarts at the tail too.
    pub async fn forget_index(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<()> {
        let shard = self.shard(tenant, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        state.index = Index::default();
        state.sequenced_through = None;
        Ok(())
    }

    /// Flush every open cache. Call once during graceful shutdown.
    ///
    /// Compaction passes stop at their next step first; one cut short leaves
    /// only redundant copies, which the next pass after a restart reclaims.
    pub async fn shutdown(&self) -> Result<()> {
        self.compactor.shutdown().await;
        let shards = self.shards.open_values();
        for shard in shards {
            shard.state.lock().await.log.shutdown().await?;
        }
        Ok(())
    }

    fn shard(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Arc<CacheShard>> {
        self.shard_with_base(tenant, namespace, cache, shard, None)
    }

    fn shard_with_base(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        base_offset: Option<u64>,
    ) -> Result<Arc<CacheShard>> {
        let id = (
            tenant.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        let key = || ShardKey {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: cache.to_string(),
            shard,
        };
        // Two callers racing to open the same new cache must not both replay
        // the log and both create segment zero; the slot coalesces them.
        self.shards.get_or_open(
            &id,
            || {
                let key = key();
                let dir = layout::shard_dir(&self.root, &key);
                let label = layout::shard_label(&key);
                crate::legacy_swap::recover_legacy_swap(&dir)?;
                let log = match base_offset {
                    Some(base) => {
                        DiskLog::open_at(dir.clone(), label.clone(), self.config.clone(), base)?
                    }
                    None => DiskLog::open(dir.clone(), label.clone(), self.config.clone())?,
                };
                Ok(Arc::new(CacheShard {
                    label,
                    compactor: Arc::clone(&self.compactor),
                    compacting: Default::default(),
                    keys_in_flight: Default::default(),
                    key_settled: Default::default(),
                    state: Mutex::new(ShardState {
                        log,
                        index: Index::default(),
                        sequenced_through: None,
                        closed: false,
                    }),
                    // Aligned to the log's tail by the first `ensure_index`; until
                    // then nothing can reserve, because every writer passes through
                    // `ensure_index` first.
                    sequencer: Arc::new(CommitSequencer::new(0)),
                }))
            },
            || StorageError::Closed(layout::shard_label(&key())),
        )
    }
}

#[async_trait]
impl StorageApi for LogCache {
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<()> {
        self.put_checked(tenant_id, namespace, cache, shard, key, value, ttl)
            .await
    }

    async fn get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.get_checked(tenant_id, namespace, cache, shard, key)
            .await
    }

    async fn put_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<std::time::Duration>,
        condition: CacheCondition,
    ) -> Result<ConditionalWrite> {
        self.put_if_checked(
            tenant_id, namespace, cache, shard, key, value, ttl, condition,
        )
        .await
    }

    async fn delete_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        version: u64,
    ) -> Result<ConditionalWrite> {
        self.delete_if_checked(tenant_id, namespace, cache, shard, key, version)
            .await
    }

    async fn get_versioned(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<VersionedValue>> {
        self.get_versioned_checked(tenant_id, namespace, cache, shard, key)
            .await
    }

    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.delete_checked(tenant_id, namespace, cache, shard, key)
            .await
    }

    async fn shard_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Option<DiskLog> {
        match LogCache::shard_log(self, tenant_id, namespace, cache, shard).await {
            Ok(log) => Some(log),
            Err(err) => {
                tracing::error!(
                    tenant_id, namespace, cache, shard, error = %err,
                    "could not open a cache shard's log for replication",
                );
                None
            }
        }
    }

    async fn shard_log_at(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        base_offset: u64,
    ) -> Option<DiskLog> {
        match LogCache::shard_log_at(self, tenant_id, namespace, cache, shard, base_offset).await {
            Ok(log) => Some(log),
            Err(err) => {
                tracing::error!(
                    tenant_id, namespace, cache, shard, error = %err,
                    "could not open a cache shard's log to bootstrap it",
                );
                None
            }
        }
    }

    async fn close_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<()> {
        LogCache::close_shard(self, tenant_id, namespace, cache, shard).await
    }

    async fn forget_index(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<()> {
        LogCache::forget_index(self, tenant_id, namespace, cache, shard).await
    }

    fn open_shards(&self) -> Vec<(String, String, String, u32)> {
        LogCache::open_shards(self)
    }

    async fn expire_due(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        limit: usize,
        keep_going: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<usize> {
        LogCache::expire_due(self, tenant_id, namespace, cache, shard, limit, keep_going).await
    }

    fn set_change_observer(&self, observer: Arc<dyn CacheObserver>) -> bool {
        *self.observer.lock() = Some(observer);
        true
    }

    async fn live_entries(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Vec<CacheSnapshotEntry>> {
        self.live_entries_checked(tenant_id, namespace, cache, shard)
            .await
    }

    async fn applied_through(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Option<u64>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        // Applied writes release their turns under this lock, so with writers
        // in flight the sequencer is the frontier; idle, the tail is.
        let applied = shard.sequencer.next_offset();
        if state.sequenced_through == Some(applied) {
            return state.log.tail_offset().await.map(Some);
        }
        Ok(Some(applied))
    }

    async fn len(&self) -> usize {
        let shards = self.shards.open_values();
        let now = now_millis();
        let mut total = 0;
        for shard in shards {
            let mut state = shard.state.lock().await;
            if shard.ensure_index(&mut state).await.is_err() {
                continue;
            }
            // Expired entries are gone as far as any reader is concerned, so
            // counting them would report a size nothing can observe.
            total += state
                .index
                .entries
                .values()
                .filter(|entry| !entry.is_expired(now))
                .count();
        }
        total
    }

    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

/// Tenant, namespace, cache, and shard. The shard is part of the identity
/// because each one is a separate log in a separate directory.
type CacheId = (String, String, String, u32);

/// When [`LogCache::delete_entry`] writes its delete.
#[derive(Clone, Copy)]
enum DeleteWhen {
    Always,
    /// The entry had expired by this Unix millisecond.
    Expired(u64),
    /// The entry is live at this version.
    Version(u64),
}

/// What [`LogCache::delete_entry`] did.
struct Deleted {
    written: bool,
    /// The value the key held, if it was live.
    previous: Option<Bytes>,
    /// The live entry's version when nothing was written.
    current: Option<u64>,
}

/// The version of `key`'s live entry, if it has one.
fn live_version(state: &ShardState, key: &str) -> Option<u64> {
    state
        .index
        .entries
        .get(key)
        .filter(|entry| !entry.is_expired(now_millis()))
        .map(|entry| entry.version)
}

#[cfg(test)]
mod tests;
