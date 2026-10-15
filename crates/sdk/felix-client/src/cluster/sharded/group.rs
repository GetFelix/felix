//! One consumer group over every shard of a stream.
//!
//! A group is bound to one shard: its cursor and its claims live with that
//! shard's leader, which is the only broker that can serve it. Consuming a
//! whole multi-shard stream through a group therefore means one group per
//! shard, each served by whichever broker leads that shard. This does that,
//! following each shard's own `NotLeader`, and hands back records tagged with
//! the shard they came from so an acknowledgement goes to the same place.
//!
//! What it deliberately does not do is merge the shards into one order: see
//! [`ShardedGroup::poll`].

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::client::Client;
use crate::cluster::ClusterClient;

/// A consumer group read across every shard of a stream.
///
/// Built by [`ClusterClient::group_sharded`]. Each shard's group is served by
/// that shard's leader, found the way [`ClusterClient::group_poll`] finds it.
pub struct ShardedGroup {
    cluster: Arc<ClusterClient>,
    tenant_id: String,
    namespace: String,
    stream: String,
    group: String,
    shards: u32,
    /// Where the next poll starts, so no shard is favoured.
    next: AtomicU32,
}

impl ShardedGroup {
    pub(crate) fn new(
        cluster: Arc<ClusterClient>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
        shards: u32,
    ) -> Self {
        Self {
            cluster,
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            group: group.to_string(),
            shards,
            next: AtomicU32::new(0),
        }
    }

    /// How many shards this group spans.
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// Take up to `max_records` from one shard, visiting shards in turn.
    ///
    /// Each call starts at the shard after the one the last call started at and
    /// returns the first non-empty batch it finds, so every shard is visited
    /// and none can starve the others. An empty answer means no shard had
    /// anything.
    ///
    /// **Ordering is per shard.** Records from one shard arrive in offset order;
    /// records from different shards arrive in whatever order the polls find
    /// them, because there is no order between shards to preserve.
    ///
    /// Fails if a shard cannot be polled at all: skipping it would look exactly
    /// like a shard with nothing to deliver.
    pub async fn poll(&self, max_records: u32) -> Result<Vec<ShardedGroupRecord>> {
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.shards;
        for step in 0..self.shards {
            let shard = (start + step) % self.shards;
            let records = self
                .on_shard(shard, |client| async move {
                    client
                        .group_poll(
                            &self.tenant_id,
                            &self.namespace,
                            &self.stream,
                            shard,
                            &self.group,
                            max_records,
                        )
                        .await
                })
                .await
                .with_context(|| format!("poll shard {shard} of {}", self.stream))?;
            if !records.is_empty() {
                return Ok(records
                    .into_iter()
                    .map(|record| ShardedGroupRecord { shard, record })
                    .collect());
            }
        }
        Ok(Vec::new())
    }

    /// Finish a record, on the shard it was claimed from.
    pub async fn ack(&self, record: &ShardedGroupRecord) -> Result<()> {
        let shard = self.shard_of(record)?;
        let offset = record.record.offset;
        self.on_shard(shard, |client| async move {
            client
                .group_ack(
                    &self.tenant_id,
                    &self.namespace,
                    &self.stream,
                    shard,
                    &self.group,
                    offset,
                )
                .await
        })
        .await
    }

    /// Hand a record back to be redelivered at once, on the shard it was
    /// claimed from.
    pub async fn nack(&self, record: &ShardedGroupRecord) -> Result<()> {
        self.nack_after(record, Duration::ZERO).await
    }

    /// Hand a record back to be redelivered once `delay` has passed. See
    /// [`Client::group_nack_after`].
    pub async fn nack_after(&self, record: &ShardedGroupRecord, delay: Duration) -> Result<()> {
        let shard = self.shard_of(record)?;
        let record = &record.record;
        self.on_shard(shard, |client| async move {
            client
                .group_nack_after(
                    &self.tenant_id,
                    &self.namespace,
                    &self.stream,
                    shard,
                    &self.group,
                    record,
                    delay,
                )
                .await
        })
        .await
    }

    /// Keep the claim on a record standing for `extend` from now. See
    /// [`Client::group_extend`].
    pub async fn extend(&self, record: &ShardedGroupRecord, extend: Duration) -> Result<Duration> {
        let shard = self.shard_of(record)?;
        let record = &record.record;
        self.on_shard(shard, |client| async move {
            client
                .group_extend(
                    &self.tenant_id,
                    &self.namespace,
                    &self.stream,
                    shard,
                    &self.group,
                    record,
                    extend,
                )
                .await
        })
        .await
    }

    /// Give up on a record, listing it as a dead letter. See
    /// [`Client::group_dead_letter`].
    pub async fn dead_letter(&self, record: &ShardedGroupRecord) -> Result<()> {
        let shard = self.shard_of(record)?;
        let offset = record.record.offset;
        self.on_shard(shard, |client| async move {
            client
                .group_dead_letter(
                    &self.tenant_id,
                    &self.namespace,
                    &self.stream,
                    shard,
                    &self.group,
                    offset,
                )
                .await
        })
        .await
    }

    fn shard_of(&self, record: &ShardedGroupRecord) -> Result<u32> {
        let shard = record.shard;
        anyhow::ensure!(
            shard < self.shards,
            "shard {shard} is not one of this group's {} shards",
            self.shards
        );
        Ok(shard)
    }

    /// Run `op` against the broker that leads `shard`, following redirects.
    async fn on_shard<T, F, Fut>(&self, shard: u32, op: F) -> Result<T>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let key = (
            self.tenant_id.clone(),
            self.namespace.clone(),
            self.stream.clone(),
            shard,
        );
        self.cluster.on_group_shard(key, op).await
    }
}

impl std::fmt::Debug for ShardedGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedGroup")
            .field("stream", &self.stream)
            .field("group", &self.group)
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

/// A record claimed through a [`ShardedGroup`], and the shard it came from.
///
/// The shard is part of the record's identity: offsets are per shard, so
/// acknowledging offset 7 means nothing without saying which shard's 7.
#[derive(Debug, Clone)]
pub struct ShardedGroupRecord {
    /// The shard the record was claimed from.
    pub shard: u32,
    /// The record, with its offset in that shard.
    pub record: felix_wire::GroupRecord,
}
