//! Offsets a consumer group has given up on.
//!
//! Not a copy of the records. They are still in the stream's log, at the
//! offsets recorded here, readable by an ordinary replay — so dead-lettering
//! duplicates nothing and loses nothing. What it stores is the one fact the log
//! does not already hold: that this group tried this record too many times and
//! stopped.
//!
//! Durable, and written before the group's cursor moves past the record. The
//! other order would let a crash in between leave the cursor beyond a record
//! nothing says was ever attempted, which is a silent skip rather than a
//! dead letter.
//!
//! A key → latest-value projection again, like the cursors themselves. **One
//! log per stream shard**, exactly as the cursors are shaped, with the group
//! folded into the entry key rather than into the log's identity: replication
//! ships whole logs, and a log per `(stream, group)` is a set the shipping
//! driver cannot enumerate — groups appear whenever a consumer names one —
//! where a log per shard is exactly the unit the driver already walks. This is
//! what lets a dead-letter list reach a replica and survive its leader.
//!
//! An entry's value is its state. Empty means given up on; [`REDRIVEN`] means
//! an operator put it back and it has not been finished yet. Redriving is one
//! write that flips the value, so there is no moment where the record is in
//! neither state — a leader that dies mid-redrive leaves it either still dead
//! or owed, and the next leader reads which. The entry is deleted when the
//! redriven record is finally acknowledged.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use felix_storage::LogCache;
use felix_storage::log::LogConfig;
use parking_lot::Mutex as SyncMutex;

use super::reader::GroupKey;
use crate::error::{BrokerError, Result};

/// Every group's dead letters, on their own root.
#[derive(Debug)]
pub struct DeadLetters {
    entries: LogCache,
    /// Where an earlier layout kept one log per `(stream, group)`. Read-only:
    /// entries recorded before the per-shard layout are still listed and can
    /// still be discarded, and nothing is ever written there again. `None`
    /// once no legacy directory can exist.
    legacy_root: PathBuf,
    /// One lock per shard, held across each read-then-write of an entry's
    /// state. Without it a discard racing a redrive could delete the entry the
    /// redrive had just flipped, and the record would be owed nowhere.
    locks: SyncMutex<HashMap<ShardKey, Arc<tokio::sync::Mutex<()>>>>,
    /// Makes the next `record` fail, for tests of what a failed write leaves.
    #[cfg(test)]
    pub(crate) fail_next_record: std::sync::atomic::AtomicBool,
}

/// The value of an entry whose record was redriven and is not yet finished.
const REDRIVEN: &[u8] = b"redriven";

/// One stream shard: `(tenant, namespace, stream, shard)`.
type ShardKey = (String, String, String, u32);

impl DeadLetters {
    pub fn open(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        let root = root.into();
        Ok(Self {
            entries: LogCache::open(&root, config).map_err(BrokerError::from)?,
            legacy_root: root,
            locks: SyncMutex::new(HashMap::new()),
            #[cfg(test)]
            fail_next_record: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Record that `group` gave up on `offset`.
    ///
    /// Also how a redriven record that failed again goes back to being dead:
    /// the write replaces its [`REDRIVEN`] state.
    pub async fn record(&self, key: &GroupKey, offset: u64) -> Result<()> {
        #[cfg(test)]
        if self
            .fail_next_record
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(BrokerError::Storage("injected dead-letter failure".into()));
        }
        let lock = self.lock_for(key);
        let _guard = lock.lock().await;
        self.entries
            .put_checked(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                // Scoped by group inside the shard's log: two groups reading
                // one shard fail on different records, and merging their dead
                // letters would have each answering for the other's.
                &entry_key(&key.group, offset),
                bytes::Bytes::new(),
                None,
            )
            .await
            .map_err(BrokerError::from)
    }

    /// Offsets `group` has given up on, lowest first. Redriven ones are not
    /// listed: they are back in the queue.
    pub async fn list(&self, key: &GroupKey) -> Result<Vec<u64>> {
        let mut offsets = self.entries_in_state(key, false).await?;
        if let Some(legacy) = self.legacy(key) {
            offsets.extend(
                legacy
                    .keys(
                        &key.tenant_id,
                        &key.namespace,
                        &legacy_scope(key),
                        key.shard,
                    )
                    .await
                    .map_err(BrokerError::from)?
                    .into_iter()
                    .filter_map(|entry| entry.parse::<u64>().ok()),
            );
        }
        offsets.sort_unstable();
        offsets.dedup();
        Ok(offsets)
    }

    /// Offsets `group` had redriven and has not finished, lowest first.
    ///
    /// What a leader rebuilding the group reads to owe them again.
    pub async fn redriven(&self, key: &GroupKey) -> Result<Vec<u64>> {
        let mut offsets = self.entries_in_state(key, true).await?;
        offsets.sort_unstable();
        Ok(offsets)
    }

    /// Put a dead letter back in the queue: flip its entry to [`REDRIVEN`].
    ///
    /// Returns whether it was listed as dead. A legacy entry is moved into the
    /// per-shard log as redriven before it is deleted, so a crash between the
    /// two leaves it listed twice rather than nowhere.
    pub async fn redrive(&self, key: &GroupKey, offset: u64) -> Result<bool> {
        let lock = self.lock_for(key);
        let _guard = lock.lock().await;
        let entry = entry_key(&key.group, offset);
        match self.state(key, &entry).await? {
            Some(value) if value.is_empty() => {
                self.put(key, &entry, REDRIVEN).await?;
                return Ok(true);
            }
            Some(_) => return Ok(false),
            None => {}
        }
        let Some(legacy) = self.legacy(key) else {
            return Ok(false);
        };
        let listed = legacy
            .get_checked(
                &key.tenant_id,
                &key.namespace,
                &legacy_scope(key),
                key.shard,
                &offset.to_string(),
            )
            .await
            .map_err(BrokerError::from)?
            .is_some();
        if !listed {
            return Ok(false);
        }
        self.put(key, &entry, REDRIVEN).await?;
        legacy
            .delete_checked(
                &key.tenant_id,
                &key.namespace,
                &legacy_scope(key),
                key.shard,
                &offset.to_string(),
            )
            .await
            .map_err(BrokerError::from)?;
        Ok(true)
    }

    /// A redriven record was finished: drop its entry. Leaves an entry that
    /// has since gone back to dead alone.
    pub async fn finish_redrive(&self, key: &GroupKey, offset: u64) -> Result<()> {
        let lock = self.lock_for(key);
        let _guard = lock.lock().await;
        let entry = entry_key(&key.group, offset);
        if self.state(key, &entry).await?.as_deref() == Some(REDRIVEN) {
            self.entries
                .delete_checked(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &entry,
                )
                .await
                .map_err(BrokerError::from)?;
        }
        Ok(())
    }

    /// Drop one offset from the list.
    ///
    /// Returns whether it was there. Does not touch the record, which stays in
    /// the stream's log — this only says the group has stopped tracking it as
    /// an outstanding problem.
    ///
    /// A redriven entry is not a dead letter and is refused: dropping it would
    /// forget a record that is owed.
    pub async fn discard(&self, key: &GroupKey, offset: u64) -> Result<bool> {
        let lock = self.lock_for(key);
        let _guard = lock.lock().await;
        let entry = entry_key(&key.group, offset);
        match self.state(key, &entry).await? {
            Some(value) if value.is_empty() => {
                self.entries
                    .delete_checked(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        &entry,
                    )
                    .await
                    .map_err(BrokerError::from)?;
                return Ok(true);
            }
            Some(_) => return Ok(false),
            None => {}
        }
        // Recorded before the per-shard layout, perhaps. The legacy log is
        // opened only when its directory already exists, so a miss costs a
        // path check and never creates anything.
        let Some(legacy) = self.legacy(key) else {
            return Ok(false);
        };
        let removed = legacy
            .delete_checked(
                &key.tenant_id,
                &key.namespace,
                &legacy_scope(key),
                key.shard,
                &offset.to_string(),
            )
            .await
            .map_err(BrokerError::from)?;
        Ok(removed.is_some())
    }

    /// Drop every entry `group` has on this shard, dead or redriven, for a
    /// group being deleted. Returns how many went. The records stay in the
    /// stream's log.
    pub async fn forget_group(&self, key: &GroupKey) -> Result<usize> {
        let lock = self.lock_for(key);
        let _guard = lock.lock().await;
        let prefix = format!("{}\u{1f}", key.group);
        let entries: Vec<String> = self
            .entries
            .live_entries_checked(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
            .map_err(BrokerError::from)?
            .into_iter()
            .filter(|entry| entry.key.starts_with(&prefix))
            .map(|entry| entry.key)
            .collect();
        let mut removed = 0;
        for entry in &entries {
            self.entries
                .delete_checked(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    entry,
                )
                .await
                .map_err(BrokerError::from)?;
            removed += 1;
        }
        if let Some(legacy) = self.legacy(key) {
            let scope = legacy_scope(key);
            for entry in legacy
                .keys(&key.tenant_id, &key.namespace, &scope, key.shard)
                .await
                .map_err(BrokerError::from)?
            {
                let deleted = legacy
                    .delete_checked(&key.tenant_id, &key.namespace, &scope, key.shard, &entry)
                    .await
                    .map_err(BrokerError::from)?;
                removed += usize::from(deleted.is_some());
            }
        }
        Ok(removed)
    }

    /// The log a shard's dead letters are written to.
    ///
    /// For replication: the list of what a group gave up on has to reach a
    /// replica alongside the cursors that say what it finished.
    pub async fn shard_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<felix_storage::disk_log::DiskLog> {
        self.entries
            .shard_log(tenant_id, namespace, stream, shard)
            .await
            .map_err(BrokerError::from)
    }

    /// [`DeadLetters::shard_log`], created at `base_offset` if absent.
    pub async fn shard_log_at(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<felix_storage::disk_log::DiskLog> {
        self.entries
            .shard_log_at(tenant_id, namespace, stream, shard, base_offset)
            .await
            .map_err(BrokerError::from)
    }

    /// Close a shard's dead-letter log, for a shard this broker no longer
    /// holds. The next call that touches it opens it afresh.
    pub async fn close_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<()> {
        self.entries
            .close_shard(tenant_id, namespace, stream, shard)
            .await
            .map_err(BrokerError::from)
    }

    /// Flush every open dead-letter log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.entries.shutdown().await.map_err(BrokerError::from)
    }

    /// Settled, because callers write based on it under the shard lock, and a
    /// cancelled caller's write may still be landing.
    async fn state(&self, key: &GroupKey, entry: &str) -> Result<Option<bytes::Bytes>> {
        self.entries
            .get_settled_checked(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                entry,
            )
            .await
            .map_err(BrokerError::from)
    }

    async fn put(&self, key: &GroupKey, entry: &str, value: &'static [u8]) -> Result<()> {
        self.entries
            .put_checked(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                entry,
                bytes::Bytes::from_static(value),
                None,
            )
            .await
            .map_err(BrokerError::from)
    }

    /// This group's entries in the per-shard log that are redriven, or not.
    async fn entries_in_state(&self, key: &GroupKey, redriven: bool) -> Result<Vec<u64>> {
        let prefix = format!("{}\u{1f}", key.group);
        Ok(self
            .entries
            .live_entries_checked(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
            .map_err(BrokerError::from)?
            .into_iter()
            .filter(|entry| (entry.value.as_ref() == REDRIVEN) == redriven)
            .filter_map(|entry| entry.key.strip_prefix(&prefix)?.parse().ok())
            .collect())
    }

    fn lock_for(&self, key: &GroupKey) -> Arc<tokio::sync::Mutex<()>> {
        let shard = (
            key.tenant_id.clone(),
            key.namespace.clone(),
            key.stream.clone(),
            key.shard,
        );
        Arc::clone(self.locks.lock().entry(shard).or_default())
    }

    /// The legacy per-`(stream, group)` log, if one is on disk for this key.
    ///
    /// Gated on the directory existing: [`LogCache`] creates a shard directory
    /// on open, and probing for old data must not litter the root with empty
    /// logs for every group that never had any.
    fn legacy(&self, key: &GroupKey) -> Option<&LogCache> {
        let shard_key = felix_storage::log::ShardKey {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: legacy_scope(key),
            shard: key.shard,
        };
        felix_storage::disk_log::layout::shard_dir(&self.legacy_root, &shard_key)
            .exists()
            .then_some(&self.entries)
    }
}

/// One group's claim on one offset, inside the shard's log.
fn entry_key(group: &str, offset: u64) -> String {
    format!("{group}\u{1f}{offset}")
}

/// How the earlier layout named a `(stream, group)` log.
fn legacy_scope(key: &GroupKey) -> String {
    format!("{}\u{1f}{}", key.stream, key.group)
}

#[cfg(test)]
mod tests;
