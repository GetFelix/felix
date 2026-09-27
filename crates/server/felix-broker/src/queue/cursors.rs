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
use std::collections::HashMap;
use std::sync::Arc;

use felix_storage::LogCache;
use felix_storage::log::LogConfig;
use parking_lot::Mutex as SyncMutex;

use crate::error::{BrokerError, Result};

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
}

/// Where one group stands on one shard.
type ShardKey = (String, String, String, u32);

impl ConsumerGroups {
    /// Open group state rooted at `root`, recovering whatever is on disk.
    pub fn open(root: impl Into<std::path::PathBuf>, config: LogConfig) -> Result<Self> {
        Ok(Self {
            cursors: LogCache::open(root, config).map_err(storage_error)?,
            locks: SyncMutex::new(HashMap::new()),
        })
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
        let stored = self
            .cursors
            .get_checked(tenant_id, namespace, stream, shard, group)
            .await
            .map_err(storage_error)?;
        stored.as_deref().map(decode_offset).transpose()
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
        let lock = self.lock_for(tenant_id, namespace, stream, shard);
        let _guard = lock.lock().await;

        let current = self
            .committed(tenant_id, namespace, stream, shard, group)
            .await?;
        if let Some(current) = current
            && current >= offset
        {
            return Ok(current);
        }

        self.cursors
            .put_checked(
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                bytes::Bytes::copy_from_slice(&offset.to_be_bytes()),
                // No expiry. A group's position is not something that should
                // quietly disappear because nobody consumed for a while --
                // forgetting it silently restarts the group from wherever the
                // caller's default is, which is either a replay or a data loss.
                None,
            )
            .await
            .map_err(storage_error)?;
        Ok(offset)
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
            .map_err(storage_error)?;
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
            .map_err(storage_error)
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
            .map_err(storage_error)
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
            .map_err(storage_error)
    }

    /// Flush every open cursor log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.cursors.shutdown().await.map_err(storage_error)
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

/// A stored position, which is always the eight bytes `commit` wrote.
///
/// Anything else means the log holds something this code did not write, and
/// guessing at it would put a group at an offset nobody chose.
fn decode_offset(bytes: &[u8]) -> Result<u64> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| {
        BrokerError::Storage(format!(
            "a consumer group cursor was {} bytes, not 8",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(raw))
}

/// Storage failures reach callers as `BrokerError::Storage`, the same way the
/// durable stream path reports them.
fn storage_error(err: felix_storage::StorageError) -> BrokerError {
    BrokerError::Storage(err.to_string())
}

#[cfg(test)]
mod tests;
