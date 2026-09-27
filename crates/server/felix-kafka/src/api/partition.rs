//! Turning a topic and partition into a shard this broker may read or write.
//!
//! Every read and write answers the same questions in the same order, and the order
//! matters: the topic must parse, then the caller must be allowed to use it,
//! and only then is its existence looked up. Checking existence first would
//! tell an unauthorized caller which streams exist.

use std::sync::Arc;

use felix_broker::{StreamHandle, StreamLog};
use kafka_protocol::ResponseError;

use crate::cluster::{Placement, Principal, ShardRef};
use crate::service::Shared;
use crate::topic::{TopicStream, parse_topic};

/// A partition this broker leads and can read.
pub(super) struct Readable {
    pub(super) log: StreamLog,
    /// Woken when a publish to the shard commits.
    pub(super) appended: Arc<tokio::sync::Notify>,
    /// Asked on every read, because the mark moves while a fetch waits.
    committed: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
}

impl Readable {
    /// Where readers stop: `tail`, or the shard's commit point if that is
    /// lower. See [`crate::cluster::Cluster::committed_until`].
    pub(super) fn high_watermark(&self, tail: u64) -> u64 {
        // Never below the log start: a mark behind retention's trim says
        // nothing new is readable, not that trimmed offsets are.
        let floor = self.log.base_offset().min(tail);
        (self.committed)().map_or(tail, |committed| committed.clamp(floor, tail))
    }
}

pub(super) async fn resolve(
    shared: &Shared,
    principal: Option<&Principal>,
    topic: &str,
    partition: i32,
) -> Result<Readable, ResponseError> {
    let located = locate(shared, principal, topic, partition, Principal::may_read).await?;
    match shared.cluster.placement(&located.shard_ref()) {
        Placement::Local { .. } => {}
        // Both send the client back to Metadata, which names the leader or
        // says there is none yet.
        Placement::Remote { .. } | Placement::Unavailable => {
            return Err(ResponseError::NotLeaderOrFollower);
        }
    }
    let handle = located.handle(shared).await?;
    let log = handle
        .log()
        .cloned()
        .ok_or(ResponseError::UnknownTopicOrPartition)?;
    let appended = handle.appended();
    let cluster = Arc::clone(&shared.cluster);
    Ok(Readable {
        log,
        appended,
        committed: Arc::new(move || cluster.committed_until(&located.shard_ref(), &handle)),
    })
}

/// A partition the principal may write: the topic parses, `stream.publish`
/// allows it, and the stream is durable with that shard. Whether this broker
/// may write it now is the cluster's to say, at admission.
pub(super) async fn resolve_write(
    shared: &Shared,
    principal: Option<&Principal>,
    topic: &str,
    partition: i32,
) -> Result<Located, ResponseError> {
    locate(shared, principal, topic, partition, Principal::may_publish).await
}

/// A shard a topic and partition name, checked in the order the module doc
/// gives.
pub(super) struct Located {
    pub(super) tenant_id: String,
    pub(super) stream: TopicStream,
    pub(super) shard: u32,
}

impl Located {
    pub(super) fn shard_ref(&self) -> ShardRef<'_> {
        ShardRef {
            tenant_id: &self.tenant_id,
            namespace: &self.stream.namespace,
            stream: &self.stream.stream,
            shard: self.shard,
        }
    }

    /// The shard's handle on this broker.
    pub(super) async fn handle(&self, shared: &Shared) -> Result<StreamHandle, ResponseError> {
        shared
            .broker
            .resolve_stream_handle(
                &self.tenant_id,
                &self.stream.namespace,
                &self.stream.stream,
                self.shard,
            )
            .await
            .map_err(|err| crate::errors::from_broker(&err))
    }
}

async fn locate(
    shared: &Shared,
    principal: Option<&Principal>,
    topic: &str,
    partition: i32,
    allowed: fn(&Principal, &str, &str) -> bool,
) -> Result<Located, ResponseError> {
    let Some(principal) = principal else {
        return Err(ResponseError::TopicAuthorizationFailed);
    };
    let Some(stream) = parse_topic(topic, shared.settings.default_namespace.as_deref()) else {
        return Err(ResponseError::UnknownTopicOrPartition);
    };
    if !allowed(principal, &stream.namespace, &stream.stream) {
        return Err(ResponseError::TopicAuthorizationFailed);
    }
    let tenant_id = principal.tenant_id();
    let metadata = shared
        .broker
        .stream_metadata(tenant_id, &stream.namespace, &stream.stream)
        .await
        .filter(|metadata| metadata.durable)
        .ok_or(ResponseError::UnknownTopicOrPartition)?;
    let shard = u32::try_from(partition)
        .ok()
        .filter(|shard| *shard < metadata.shards)
        .ok_or(ResponseError::UnknownTopicOrPartition)?;
    Ok(Located {
        tenant_id: tenant_id.to_string(),
        stream,
        shard,
    })
}
